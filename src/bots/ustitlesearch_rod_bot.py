"""US Title Search Network ROD scraper — pulls real Deed of Trust
recordings for TN foreclosure leads.

We have a paid ustitlesearch.net account that gives:
  - UNLIMITED searches in Sumner + Rutherford counties
  - 15 single-day-search units across ~50 other TN counties
  - Returns: original principal, lender, date, instrument number,
    full instrument image (PDF/TIF) per recording

Per recording we extract:
  - Consideration Amount (= original principal of the DOT)
  - Lender (Reverse Parties on a DOT recording)
  - Document Date (origination date)
  - Book & Page (matches notice's instrument_no)

We then amortize from the document date at TN-avg fixed rate to today
to estimate current payoff. Confidence 0.85 (real recorded loan +
standard amortization; doesn't model HELOCs/refis but those would show
as separate Deed of Trust recordings in the same search).

Distress type: N/A (utility ROD scraper).
"""
from __future__ import annotations

import os
import re
import sys
import time
import traceback as tb
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._provenance import record_field

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    sync_playwright = None
    PWTimeout = Exception  # type: ignore[assignment, misc]


# Map TN county name → ustitlesearch SubscriptionId.
# Codes harvested from Subscription.asp on 2026-05-04.
SUBSCRIPTION_IDS = {
    'sumner':       54,    # AVAILABLE (unlimited)
    'rutherford':   58,    # AVAILABLE (unlimited)
    # Single-day-search counties (use 1 of 15 units each)
    'cheatham':     20,
    'robertson':    19,
    'dickson':      21,
    'putnam':       55,
    'sullivan':     53,
    'tipton':       16,
    'maury':        17,
    'wilson':        4,    # WILSON COUNTY (verify ID)
    # Many more available
}

# Counties Patrick has UNLIMITED access to (no unit consumption).
UNLIMITED_COUNTIES = {'sumner', 'rutherford'}

# TN-average 30Y fixed mortgage rates, by year. Used for amortizing
# original principal forward to today's estimated payoff.
TN_AVG_30Y_RATES = {
    2010: 4.69, 2011: 4.45, 2012: 3.66, 2013: 3.98, 2014: 4.17,
    2015: 3.85, 2016: 3.65, 2017: 3.99, 2018: 4.54, 2019: 3.94,
    2020: 3.11, 2021: 2.96, 2022: 5.34, 2023: 6.81, 2024: 6.74,
    2025: 6.85, 2026: 6.50,
}
DEFAULT_RATE = 6.50
TERM_YEARS = 30


def _amortize(original_principal: float, rate_pct: float, years_elapsed: float) -> float:
    """Standard 30Y amortization. Returns remaining balance after
    `years_elapsed` of payments at `rate_pct` annual."""
    if original_principal <= 0 or years_elapsed <= 0:
        return original_principal
    if years_elapsed >= TERM_YEARS:
        return 0.0
    r = (rate_pct / 100.0) / 12.0
    n = TERM_YEARS * 12
    paid = min(int(years_elapsed * 12), n)
    if r == 0:
        return original_principal * (1 - paid / n)
    factor = (1 + r) ** n
    monthly_payment = original_principal * r * factor / (factor - 1)
    factor_k = (1 + r) ** paid
    remaining = original_principal * factor_k - monthly_payment * (factor_k - 1) / r
    return max(0.0, remaining)


def _parse_money(s: str) -> Optional[float]:
    if not s:
        return None
    digits = re.sub(r'[^\d.]', '', str(s))
    try:
        return float(digits)
    except ValueError:
        return None


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip().split()[0]  # drop time
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


class UsTitleSearchRodBot(BotBase):
    name = 'ustitlesearch_rod'
    description = ('Pulls real Deed of Trust recordings from '
                    'ustitlesearch.net for TN foreclosure leads')
    throttle_seconds = 0.5
    expected_min_yield = 1
    max_leads_per_run = 30  # Conservative default; configurable

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status='running', started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        if sync_playwright is None:
            return self._fail(started, 'playwright not installed')

        username = os.environ.get('FALCO_USTITLESEARCH_USERNAME', '').strip()
        password = os.environ.get('FALCO_USTITLESEARCH_PASSWORD', '').strip()
        if not username or not password:
            return self._fail(started, 'FALCO_USTITLESEARCH_USERNAME/PASSWORD not set')

        client = _supabase()
        if client is None:
            return self._fail(started, 'no_supabase_client')

        max_leads = int(os.environ.get('FALCO_USTITLESEARCH_MAX_PER_RUN',
                                          self.max_leads_per_run))
        single_day_budget = int(os.environ.get('FALCO_USTITLESEARCH_SINGLE_DAY_BUDGET', '0'))
        sample_only = os.environ.get('FALCO_USTITLESEARCH_SAMPLE') == '1'

        candidates = self._fetch_candidates(client, max_leads)
        self.logger.info(f'{len(candidates)} candidate foreclosure leads to look up')

        attempted = 0
        matched = 0
        no_match = 0
        errors = 0
        single_day_used = 0

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(viewport={'width': 1280, 'height': 900})
                page = ctx.new_page()

                self._login(page, username, password)
                self.logger.info('logged in to ustitlesearch.net')

                # Group candidates by county to minimize county-select switches
                by_county: Dict[str, List[Dict[str, Any]]] = {}
                for c in candidates:
                    county = self._normalize_county(c.get('county'))
                    if not county or county not in SUBSCRIPTION_IDS:
                        continue
                    by_county.setdefault(county, []).append(c)

                for county, leads in by_county.items():
                    if county not in UNLIMITED_COUNTIES:
                        if single_day_used >= single_day_budget:
                            self.logger.info(
                                f'single-day budget exhausted ({single_day_budget}); '
                                f'skipping {len(leads)} leads in {county}'
                            )
                            continue
                    sub_id = SUBSCRIPTION_IDS[county]
                    self.logger.info(f'switching to county={county} (SubscriptionId={sub_id})')
                    page.goto(
                        f'http://www.ustitlesearch.net/changesubscription.asp?SubscriptionId={sub_id}',
                        timeout=20000,
                    )
                    time.sleep(1)
                    if county not in UNLIMITED_COUNTIES:
                        single_day_used += 1
                        self.logger.info(f'  consumed 1 single-day-search unit (total used: {single_day_used})')

                    for lead in leads:
                        if attempted >= max_leads:
                            break
                        attempted += 1
                        try:
                            result = self._lookup_lead(page, lead)
                        except Exception as e:
                            errors += 1
                            self.logger.warning(f'  lookup failed for {lead.get("id")}: {e}')
                            continue

                        if not result:
                            no_match += 1
                            continue
                        matched += 1

                        if sample_only:
                            self.logger.info(
                                f'  SAMPLE: {result["lender"]} | '
                                f'${result["original_principal"]:,.0f} on {result["document_date"]} | '
                                f'amortized payoff ~${result["estimated_payoff"]:,.0f}'
                            )
                            continue

                        self._write_signal(client, lead, result)

                browser.close()

        except Exception as e:
            error_message = f'{type(e).__name__}: {e}\n{tb.format_exc()}'
            self.logger.error(f'FAILED: {e}')
            return self._wrap(
                started, attempted, matched, no_match, errors,
                single_day_used, status='failed', error=error_message,
            )

        self.logger.info(
            f'attempted={attempted} matched={matched} no_match={no_match} '
            f'errors={errors} single_day_used={single_day_used}'
        )
        return self._wrap(
            started, attempted, matched, no_match, errors, single_day_used,
        )

    # ── Eligibility ────────────────────────────────────────────────────────
    def _fetch_candidates(self, client, max_leads: int) -> List[Dict[str, Any]]:
        """PROMOTE foreclosure leads we haven't already enriched, ordered
        by priority_score desc. Prefer Sumner/Rutherford first (free)."""
        out = []
        for table in ('homeowner_requests_staging', 'homeowner_requests'):
            page_idx = 0
            PAGE = 1000
            while True:
                try:
                    q = (
                        client.table(table)
                        .select('id, owner_name_records, full_name, '
                                'property_address, county, distress_type, '
                                'priority_score, raw_payload, phone_metadata')
                        .in_('distress_type', ['PRE_FORECLOSURE', 'TRUSTEE_NOTICE'])
                        .order('priority_score', desc=True)
                        .range(page_idx * PAGE, (page_idx + 1) * PAGE - 1)
                        .execute()
                    )
                    rows = getattr(q, 'data', None) or []
                    if not rows:
                        break
                    for r in rows:
                        # Skip if already enriched via this bot
                        pm = r.get('phone_metadata') or {}
                        if isinstance(pm, dict) and pm.get('rod_lookup'):
                            continue
                        # Need a borrower name to search
                        if not (r.get('owner_name_records') or r.get('full_name')):
                            continue
                        # Need a recognized county
                        county = self._normalize_county(r.get('county'))
                        if not county or county not in SUBSCRIPTION_IDS:
                            continue
                        r['__table__'] = table
                        out.append(r)
                    if len(rows) < PAGE:
                        break
                    page_idx += 1
                except Exception as e:
                    self.logger.warning(f'candidate fetch on {table} failed: {e}')
                    break
        # Sort: unlimited counties first, then by priority desc
        out.sort(key=lambda r: (
            0 if self._normalize_county(r.get('county')) in UNLIMITED_COUNTIES else 1,
            -(r.get('priority_score') or 0),
        ))
        return out[: max_leads * 3]  # over-pull; per-county loop applies max_leads

    @staticmethod
    def _normalize_county(c: Optional[str]) -> Optional[str]:
        if not c:
            return None
        s = c.lower().strip()
        s = s.replace(' county', '').strip()
        return s or None

    # ── Auth ───────────────────────────────────────────────────────────────
    def _login(self, page, username: str, password: str) -> None:
        page.goto('http://www.ustitlesearch.net/Logon.asp', timeout=20000)
        page.goto(
            f'http://www.ustitlesearch.net/logon.asp?AAABBBCCC=123&action=logon&'
            f'username={username}&password={password}&savepassword=true',
            timeout=20000,
        )
        body = page.content()
        if 'sessions permitted' in body.lower():
            for sid in re.findall(r'abandon\.asp\?sessionid=(\d+)', body):
                page.goto(f'http://www.ustitlesearch.net/abandon.asp?sessionid={sid}', timeout=15000)
            page.goto(
                f'http://www.ustitlesearch.net/logon.asp?AAABBBCCC=123&action=logon&'
                f'username={username}&password={password}&savepassword=true',
                timeout=20000,
            )
        page.goto('http://www.ustitlesearch.net/Subscription.asp', timeout=20000)

    # ── Per-lead lookup ────────────────────────────────────────────────────
    def _lookup_lead(self, page, lead: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        owner = lead.get('owner_name_records') or lead.get('full_name') or ''
        # Normalize owner — strip suffixes like "Jr"/"III"/"Etux"
        owner_clean = re.sub(r'\b(jr|sr|ii|iii|iv|etux|et\s+ux|etal|et\s+al)\.?\b',
                              '', owner, flags=re.IGNORECASE)
        owner_clean = re.sub(r'[,;]', ' ', owner_clean)
        owner_clean = re.sub(r'\s+', ' ', owner_clean).strip().upper()
        if not owner_clean:
            return None

        # Try last-name only for broad search
        last_name = owner_clean.split()[-1] if owner_clean else ''
        first_name = owner_clean.split()[0] if owner_clean else ''
        search_name = last_name if len(last_name) >= 3 else owner_clean

        # Date range: from notice's trust_date - 1 yr to today (covers refis)
        raw = lead.get('raw_payload') or {}
        trust_date_str = ''
        if isinstance(raw, dict):
            structured = raw.get('structured') or {}
            trust_date_str = structured.get('trust_date') or ''

        # Prefer book/page direct lookup if we have it
        book_page = None
        if isinstance(raw, dict):
            structured = raw.get('structured') or {}
            inst = (structured.get('instrument_no') or '').strip().rstrip(',;.')
            m = re.match(r'(\d+)[-/](\d+)', inst)
            if m:
                book_page = (m.group(1), m.group(2))

        # Run the party-name search (broader, more likely to hit)
        page.goto('http://www.ustitlesearch.net/searchbypartyname.asp', timeout=20000)
        page.fill('input[name="PartyName"]', search_name)
        page.fill('input[name="BeginningDate"]', '01/01/2010')
        page.fill('input[name="EndingDate"]', date.today().strftime('%m/%d/%Y'))
        page.select_option('select[name="InstrumentType"]', value='38')  # DT only
        # Default PageSize=25 paginates results — set 100 so all matches
        # for common surnames land on page 1 (we observed BROWNLOW = 29
        # records with default 25 hiding rows 26-29 incl. Drew Wilson).
        try:
            page.fill('input[name="PageSize"]', '100')
        except Exception:
            pass
        try:
            with page.expect_navigation(timeout=30000):
                page.evaluate('document.forms[0].submit()')
        except PWTimeout:
            return None
        time.sleep(1)

        results_html = page.content()
        # Find a row matching the borrower's first name + book/page
        matching_link = None
        if book_page:
            book, pg = book_page
            target_bp = f'{book}-{pg}'
            for tr_match in re.finditer(r'<tr[^>]*>(.*?)</tr>', results_html, re.DOTALL):
                tr_html = tr_match.group(1)
                if (target_bp in tr_html
                        and (first_name and first_name in tr_html.upper())):
                    href_match = re.search(
                        r'<a[^>]+href="(InstrumentDisplay\.asp[^"]+)"', tr_html
                    )
                    if href_match:
                        matching_link = href_match.group(1)
                        break

        if not matching_link:
            # Fallback: find first DT row matching first_name
            for tr_match in re.finditer(r'<tr[^>]*>(.*?)</tr>', results_html, re.DOTALL):
                tr_html = tr_match.group(1)
                if first_name and first_name in tr_html.upper():
                    href_match = re.search(
                        r'<a[^>]+href="(InstrumentDisplay\.asp[^"]+)"', tr_html
                    )
                    if href_match:
                        matching_link = href_match.group(1)
                        break

        if not matching_link:
            return None

        # HTML-entity-decode the URL before navigation. The matched link
        # comes from raw HTML where & is encoded as &amp;.
        import html as _html
        matching_link = _html.unescape(matching_link)

        page.goto(f'http://www.ustitlesearch.net/{matching_link}', timeout=20000)
        time.sleep(1)
        detail_text = page.inner_text('body')

        # Parse fields
        consideration_match = re.search(
            r'Consideration Amount[\s\S]*?\$([\d,]+(?:\.\d{2})?)',
            detail_text,
        )
        original_principal = _parse_money(consideration_match.group(1)) if consideration_match else None
        if not original_principal:
            return None

        doc_date_match = re.search(
            r'Document Date[\s\S]*?(\d{1,2}/\d{1,2}/\d{4})', detail_text
        )
        document_date = _parse_date(doc_date_match.group(1)) if doc_date_match else None

        # Lender = first Reverse Party
        lender = None
        rev_match = re.search(r'Reverse Parties[\s\S]*?Name[^\n]*\n([^\n]+)',
                                detail_text)
        if rev_match:
            lender = rev_match.group(1).strip()
            # remove trailing 'WHO' marker if present
            lender = re.sub(r'\s+WHO\s*$', '', lender).strip()

        # Amortize
        if document_date:
            yrs_elapsed = (date.today() - document_date).days / 365.25
            rate = TN_AVG_30Y_RATES.get(document_date.year, DEFAULT_RATE)
        else:
            yrs_elapsed = 5  # conservative default
            rate = DEFAULT_RATE
        estimated_payoff = _amortize(original_principal, rate, yrs_elapsed)

        return {
            'original_principal': original_principal,
            'lender': lender,
            'document_date': document_date.isoformat() if document_date else None,
            'rate_pct': rate,
            'years_elapsed': round(yrs_elapsed, 2),
            'estimated_payoff': round(estimated_payoff, 2),
            'detail_url': matching_link,
            'source': 'ustitlesearch_rod',
            'confidence': 0.85,
        }

    # ── Write ──────────────────────────────────────────────────────────────
    def _write_signal(self, client, lead: Dict[str, Any], result: Dict[str, Any]) -> None:
        table = lead['__table__']
        existing_meta = lead.get('phone_metadata') or {}
        if not isinstance(existing_meta, dict):
            existing_meta = {}
        existing_meta['rod_lookup'] = {
            'lender': result['lender'],
            'original_principal': result['original_principal'],
            'document_date': result['document_date'],
            'rate_pct': result['rate_pct'],
            'years_elapsed': result['years_elapsed'],
            'estimated_payoff': result['estimated_payoff'],
            'source': 'ustitlesearch_rod',
            'detail_url': result['detail_url'],
            'resolved_at': datetime.now(timezone.utc).isoformat(),
        }
        existing_meta['mortgage_signal'] = {
            'kind': 'rod_amortized',
            'source': 'ustitlesearch_rod',
            'amount': result['estimated_payoff'],
            'confidence': result['confidence'],
            'lender': result['lender'],
            'original_principal': result['original_principal'],
            'document_date': result['document_date'],
            'note': (
                f'Amortized from recorded DOT: ${result["original_principal"]:,.0f} '
                f'with {result["lender"]} on {result["document_date"]}, '
                f'TN-avg rate {result["rate_pct"]}%, '
                f'{result["years_elapsed"]} years elapsed.'
            ),
        }
        update = {
            'mortgage_balance': int(round(result['estimated_payoff'])),
            'phone_metadata': existing_meta,
        }
        try:
            client.table(table).update(update).eq('id', lead['id']).execute()
            if table == 'homeowner_requests':
                record_field(
                    client, lead['id'], 'mortgage_balance',
                    int(round(result['estimated_payoff'])),
                    'ustitlesearch_rod',
                    confidence=result['confidence'],
                    metadata={
                        'lender': result['lender'],
                        'original_principal': result['original_principal'],
                        'document_date': result['document_date'],
                    },
                )
        except Exception as e:
            self.logger.warning(f'  update failed id={lead["id"]}: {e}')

    # ── Helpers ────────────────────────────────────────────────────────────
    def _fail(self, started, msg: str) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status='failed', started_at=started, finished_at=finished,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
            error_message=msg,
        )
        return {'name': self.name, 'status': 'failed', 'error': msg,
                'matched': 0, 'staged': 0, 'duplicates': 0, 'fetched': 0}

    def _wrap(self, started, attempted, matched, no_match, errors,
                single_day_used, status: str = 'ok',
                error: Optional[str] = None) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=attempted, parsed_count=matched + no_match,
            staged_count=matched, duplicate_count=0,
            error_message=error,
        )
        return {
            'name': self.name, 'status': status,
            'attempted': attempted, 'matched': matched,
            'no_match': no_match, 'errors': errors,
            'single_day_used': single_day_used,
            'error': error,
            'staged': matched, 'duplicates': 0, 'fetched': attempted,
        }


def run() -> dict:
    bot = UsTitleSearchRodBot()
    return bot.run()


if __name__ == '__main__':
    print(run())
