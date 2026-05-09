"""
Middle-Tennessee Lis Pendens Register-of-Deeds scraper.

Pulls REAL recorded Lis Pendens (LP) filings from per-county ROD portals.
This is the EARLIEST public distress signal — typically 60-120 days
before any foreclosure sale notice. Distinct from our keyword-based
supplement (which uses PRE_FORECLOSURE); these recordings are tagged
distress_type="LIS_PENDENS".

Counties (priority order):
  1. Davidson    — davidsonportal.com (paid: $50/mo subscriber)
  2. Williamson  — proaccess.williamson-tn.org / titlesearcher.com (paid)
  3. Hamilton    — register.hamiltontn.gov OnlineRecordSearch (paid: $50/mo)

ACCESSIBILITY MATRIX (researched 2026-05-07):

  Davidson    PUBLIC URL: https://www.davidsonportal.com/
              ACCESSIBLE: NO (paid subscription, $50/mo)
              JS-HEAVY:   YES (ASP.NET portal, dynamic search forms)
              GATE:       FALCO_DAVIDSON_ROD_USER / _PASSWORD env vars
              NOTE:       No free public-web search. Lobby kiosk free at
                          222 3rd Ave N, Nashville — not scrapable.

  Williamson  PUBLIC URL: https://proaccess.williamson-tn.org/
              ACCESSIBLE: NO (paid subscription, $50-100/mo, 1-week trial)
              JS-HEAVY:   YES (Williamson "ProAccess" + titlesearcher)
              GATE:       FALCO_WILLIAMSON_ROD_USER / _PASSWORD env vars
              NOTE:       Williamson is also indexed on titlesearcher.com
                          (Tier 1: $50 primary). If using titlesearcher,
                          set FALCO_USTITLESEARCH_USERNAME/_PASSWORD —
                          then prefer that path (already proven in
                          ustitlesearch_rod_bot.py).

  Hamilton    PUBLIC URL: https://register.hamiltontn.gov/OnlineRecordSearch/
              ACCESSIBLE: NO (paid subscription, $50/mo, application gated)
              JS-HEAVY:   YES (ASP.NET WebForms; some flows use Silverlight)
              GATE:       FALCO_HAMILTON_ROD_USER / _PASSWORD env vars
              NOTE:       Lis Pendens doc-type code is "L06" per their
                          Document Requirement Guide.

Until credentials exist for a given county, that county's branch is
skipped at runtime (logged, no fake data). Wire credentials, the bot
auto-enables the county on next run.

Distress type: LIS_PENDENS (EARLY signal — pre-NOD).
"""

from __future__ import annotations

import os
import re
import time
import traceback as tb
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    sync_playwright = None
    PWTimeout = Exception  # type: ignore[assignment, misc]


# ── Per-county configuration ────────────────────────────────────────────────

LOOKBACK_DAYS = 30

# Doc-type tokens to search for. Different portals use different codes;
# keep both code + label and try whichever the portal accepts.
LP_DOC_TYPES = {
    "davidson":   {"label": "LIS PENDENS", "code": "LP"},
    "williamson": {"label": "LIS PENDENS", "code": "LP"},
    # Hamilton's Document Requirement Guide explicitly codes LP as "L06".
    "hamilton":   {"label": "LIEN LIS PENDENS", "code": "L06"},
}

COUNTY_DISPLAY = {
    "davidson":   "Davidson County",
    "williamson": "Williamson County",
    "hamilton":   "Hamilton County",
}

# Env-var prefixes per county for credentials.
COUNTY_ENV_PREFIX = {
    "davidson":   "FALCO_DAVIDSON_ROD",
    "williamson": "FALCO_WILLIAMSON_ROD",
    "hamilton":   "FALCO_HAMILTON_ROD",
}


# ── Field-extraction regexes (apply to whatever index text we get) ──────────

PARCEL_RE = re.compile(
    r"(?:MAP\s*&?\s*PARCEL|TAX\s*MAP|PARCEL)\s*(?:NO\.?|NUMBER|ID|#)?[:\s]+"
    r"([A-Z0-9][A-Z0-9\-\.]*\d[A-Z0-9\-\.]*)",
    re.IGNORECASE,
)
INSTR_NO_RE = re.compile(
    r"(?:Instrument|Inst\.?|Doc(?:ument)?)\s*(?:No\.?|Number|#)?[:\s]+"
    r"([0-9][0-9\-]*)",
    re.IGNORECASE,
)
BOOK_PAGE_RE = re.compile(
    r"Book\s+([0-9A-Z]+)[\s,]+Page\s+([0-9A-Z]+)",
    re.IGNORECASE,
)
ADDRESS_RE = re.compile(
    r"\b(\d{1,6}\s+[A-Z0-9][A-Z0-9 .,'\-]{4,60}"
    r"(?:STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|LANE|LN|COURT|CT|"
    r"BOULEVARD|BLVD|PLACE|PL|PIKE|HIGHWAY|HWY|WAY|TRAIL|TRL|CIRCLE|CIR))\b",
    re.IGNORECASE,
)


def _iso(d: date) -> str:
    return d.isoformat()


def _mdy(d: date) -> str:
    return f"{d.month}/{d.day}/{d.year}"


def _today() -> date:
    return date.today()


def _start_date(days: int = LOOKBACK_DAYS) -> date:
    return _today() - timedelta(days=days)


# ── Bot ─────────────────────────────────────────────────────────────────────


class MtnLisPendensRodBot(BotBase):
    name = "mtn_lis_pendens_rod"
    description = (
        "Per-county TN Register-of-Deeds Lis Pendens (LP) recordings — "
        "earliest public distress signal, pre-NOD"
    )
    throttle_seconds = 2.0  # courtesy delay between counties (per spec)
    expected_min_yield = 1

    # Subclass surface; left as no-op since run() is overridden for the
    # multi-county Playwright orchestration. scrape() is required by base.
    def scrape(self) -> List[LeadPayload]:
        return []

    # ── Top-level orchestrator ──────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        if sync_playwright is None:
            return self._finish(started, [], error="playwright not installed")

        all_leads: List[LeadPayload] = []
        per_county_counts: Dict[str, int] = {}
        skipped: Dict[str, str] = {}
        errors: List[str] = []

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(
                    viewport={"width": 1280, "height": 900},
                    user_agent=("FALCO-Lead-Research/1.0 "
                                "(+ops@falco.llc) Playwright"),
                )
                page = ctx.new_page()

                for county in ("davidson", "williamson", "hamilton"):
                    creds = self._load_creds(county)
                    if creds is None:
                        msg = (f"no credentials in env "
                               f"(set {COUNTY_ENV_PREFIX[county]}_USER / "
                               f"{COUNTY_ENV_PREFIX[county]}_PASSWORD)")
                        self.logger.info(f"  [{county}] SKIP — {msg}")
                        skipped[county] = msg
                        continue

                    self.logger.info(f"  [{county}] starting LP scrape")
                    try:
                        leads = self._scrape_county(page, county, creds)
                    except Exception as e:
                        err = f"{county}: {type(e).__name__}: {e}"
                        self.logger.warning(f"  [{county}] FAILED — {err}")
                        errors.append(err)
                        continue
                    per_county_counts[county] = len(leads)
                    all_leads.extend(leads)
                    self.logger.info(
                        f"  [{county}] {len(leads)} LP recordings"
                    )
                    # Throttle between counties (per spec).
                    time.sleep(self.throttle_seconds)

                browser.close()
        except Exception as e:
            err = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
            return self._finish(started, all_leads, error=err)

        self.logger.info(
            f"summary: counts={per_county_counts} "
            f"skipped={list(skipped.keys())} errors={len(errors)}"
        )

        return self._finish(
            started, all_leads,
            error="; ".join(errors) if errors else None,
            extra={"per_county": per_county_counts, "skipped": skipped},
        )

    # ── Per-county dispatchers ──────────────────────────────────────────────

    def _scrape_county(
        self, page, county: str, creds: Dict[str, str],
    ) -> List[LeadPayload]:
        if county == "davidson":
            return self._scrape_davidson(page, creds)
        if county == "williamson":
            return self._scrape_williamson(page, creds)
        if county == "hamilton":
            return self._scrape_hamilton(page, creds)
        return []

    # ── Davidson (davidsonportal.com) ───────────────────────────────────────
    #
    # Davidson's portal is an ASP.NET app that gates EVERYTHING behind a
    # subscriber login. Login form lives on the root; after auth, search
    # form lets you filter by document type + recording date range.
    # Result rows expose grantor/grantee, date, instrument number — full
    # text/address rarely in the index (we save what's there + image link).
    def _scrape_davidson(
        self, page, creds: Dict[str, str],
    ) -> List[LeadPayload]:
        login_url = "https://www.davidsonportal.com/"
        page.goto(login_url, timeout=30000)
        # Form field names harvested from the public login form. If they
        # change (ASP.NET sites do this), credentials env-vars stay the
        # same — only this block needs updating.
        try:
            page.fill('input[name="txtUsername"], input[name="UserName"]',
                      creds["user"])
            page.fill('input[name="txtPassword"], input[name="Password"]',
                      creds["password"])
            page.click('input[type="submit"], button[type="submit"]')
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            self.logger.warning("    davidson login timed out")
            return []

        if "logout" not in page.content().lower() and \
                "sign out" not in page.content().lower():
            self.logger.warning("    davidson login appears to have failed")
            return []

        # Navigate to document-type/date search. Path varies; portal exposes
        # a "Search" or "Document Search" link in its top nav.
        for label in ("Document Search", "Search Documents", "Search"):
            try:
                link = page.get_by_role("link", name=re.compile(label, re.I))
                if link.count() > 0:
                    link.first.click()
                    page.wait_for_load_state("networkidle", timeout=15000)
                    break
            except Exception:
                continue

        # Fill doc-type + date range. Field names will need verification on
        # the live portal (no public probe available without creds).
        start = _start_date()
        end = _today()
        try:
            self._maybe_select(page, "select[name*='DocType' i], "
                                     "select[name*='Document' i]",
                               LP_DOC_TYPES["davidson"]["label"])
            self._maybe_fill(page, "input[name*='Begin' i], "
                                   "input[name*='Start' i], "
                                   "input[name*='From' i]",
                             _mdy(start))
            self._maybe_fill(page, "input[name*='End' i], "
                                   "input[name*='Thru' i], "
                                   "input[name*='To' i]",
                             _mdy(end))
            page.click('input[type="submit"], button:has-text("Search")')
            page.wait_for_load_state("networkidle", timeout=20000)
        except PWTimeout:
            self.logger.warning("    davidson search submit timed out")
            return []

        return self._parse_index_table(
            page, county_key="davidson",
            source_url=page.url,
        )

    # ── Williamson (proaccess.williamson-tn.org / titlesearcher) ────────────
    #
    # Williamson's primary public index is "ProAccess" — same UI family as
    # Tyler/eFile portals: server-side search form with doc-type + date.
    # Williamson is ALSO indexed on titlesearcher.com (Tier-1), so if a
    # ustitlesearch session is available we could route through there
    # instead. For now this branch hits ProAccess directly.
    def _scrape_williamson(
        self, page, creds: Dict[str, str],
    ) -> List[LeadPayload]:
        login_url = "https://proaccess.williamson-tn.org/proaccess/login"
        page.goto(login_url, timeout=30000)
        try:
            page.fill('input[name*="user" i], input[type="text"]',
                      creds["user"])
            page.fill('input[type="password"]', creds["password"])
            page.click('button[type="submit"], input[type="submit"]')
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            self.logger.warning("    williamson login timed out")
            return []

        if "login" in page.url.lower():
            self.logger.warning("    williamson login appears to have failed")
            return []

        # ProAccess "Records Search" → Document Type = Lis Pendens, date range.
        try:
            for label in ("Records Search", "Document Search", "Search"):
                link = page.get_by_role("link", name=re.compile(label, re.I))
                if link.count() > 0:
                    link.first.click()
                    page.wait_for_load_state("networkidle", timeout=15000)
                    break
        except Exception:
            pass

        start = _start_date()
        end = _today()
        try:
            self._maybe_select(page, "select[name*='DocType' i], "
                                     "select[name*='Type' i]",
                               LP_DOC_TYPES["williamson"]["label"])
            self._maybe_fill(page, "input[name*='Begin' i], "
                                   "input[name*='Start' i], "
                                   "input[name*='From' i]",
                             _mdy(start))
            self._maybe_fill(page, "input[name*='End' i], "
                                   "input[name*='Thru' i], "
                                   "input[name*='To' i]",
                             _mdy(end))
            page.click('button:has-text("Search"), input[type="submit"]')
            page.wait_for_load_state("networkidle", timeout=20000)
        except PWTimeout:
            self.logger.warning("    williamson search submit timed out")
            return []

        return self._parse_index_table(
            page, county_key="williamson", source_url=page.url,
        )

    # ── Hamilton (register.hamiltontn.gov OnlineRecordSearch) ───────────────
    #
    # Hamilton's portal is ASP.NET WebForms with __VIEWSTATE / event-target
    # postbacks. Login → main search page → choose Document Type "L06"
    # (LIEN LIS PENDENS) → set date range → Search.
    def _scrape_hamilton(
        self, page, creds: Dict[str, str],
    ) -> List[LeadPayload]:
        base = "https://register.hamiltontn.gov/OnlineRecordSearch"
        page.goto(f"{base}/Home/Login.aspx", timeout=30000)
        try:
            page.fill('input[name*="UserName" i], input[id*="UserName" i]',
                      creds["user"])
            page.fill('input[name*="Password" i], input[id*="Password" i]',
                      creds["password"])
            page.click('input[type="submit"], button[type="submit"]')
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            self.logger.warning("    hamilton login timed out")
            return []

        if "login" in page.url.lower():
            self.logger.warning("    hamilton login appears to have failed")
            return []

        # Navigate to document search.
        try:
            page.goto(f"{base}/Search/Search.aspx", timeout=20000)
            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            pass

        start = _start_date()
        end = _today()
        try:
            # Hamilton's doc-type dropdown uses the "L06" code.
            self._maybe_select(page, "select[name*='DocType' i], "
                                     "select[name*='Type' i]",
                               LP_DOC_TYPES["hamilton"]["code"],
                               fallback_label=LP_DOC_TYPES["hamilton"]["label"])
            self._maybe_fill(page, "input[name*='Begin' i], "
                                   "input[name*='Start' i], "
                                   "input[name*='From' i]",
                             _mdy(start))
            self._maybe_fill(page, "input[name*='End' i], "
                                   "input[name*='Thru' i], "
                                   "input[name*='To' i]",
                             _mdy(end))
            page.click('input[type="submit"], button:has-text("Search")')
            page.wait_for_load_state("networkidle", timeout=20000)
        except PWTimeout:
            self.logger.warning("    hamilton search submit timed out")
            return []

        return self._parse_index_table(
            page, county_key="hamilton", source_url=page.url,
        )

    # ── Index-row parser (shared) ───────────────────────────────────────────
    #
    # Every TN ROD portal we hit emits the search results as an HTML <table>.
    # We harvest rows generically and let the per-county post-processor pick
    # out borrower / plaintiff / instrument / book-page from the cells.
    # If a portal returns a non-table layout, override per-county.
    def _parse_index_table(
        self, page, county_key: str, source_url: str,
    ) -> List[LeadPayload]:
        try:
            html = page.content()
        except Exception:
            return []

        # Use BeautifulSoup if available (already a dep via _base).
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            self.logger.warning("bs4 not available — cannot parse index")
            return []

        soup = BeautifulSoup(html, "html.parser")
        leads: List[LeadPayload] = []
        seen: set[str] = set()

        # Heuristic: find any table whose header row contains both a date-like
        # column and a doc-type or party column.
        for table in soup.find_all("table"):
            headers = [th.get_text(" ", strip=True).lower()
                        for th in table.find_all("th")]
            if not headers:
                # Some portals render headers in the first <tr><td>.
                first_tr = table.find("tr")
                if first_tr:
                    headers = [td.get_text(" ", strip=True).lower()
                                for td in first_tr.find_all(["td", "th"])]
            joined = " ".join(headers)
            if not any(k in joined for k in
                        ("date", "recorded", "filed")):
                continue
            if not any(k in joined for k in
                        ("grantor", "grantee", "party",
                         "instrument", "doc", "type", "name")):
                continue

            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue
                cells = [td.get_text(" ", strip=True) for td in tds]
                # Skip if no LP indicator anywhere on the row (defensive —
                # most portals already filter by our query, but if a portal
                # ignores the doc-type filter we'd pull noise otherwise).
                row_text = " ".join(cells).upper()
                if not any(tok in row_text for tok in
                            ("LIS PENDENS", "L06", " LP ", " LP,")):
                    # Allow rows where the doc-type column wasn't returned
                    # in text form — only filter when we have evidence the
                    # portal renders a doc-type column.
                    if "type" in joined or "doc" in joined:
                        continue

                lead = self._row_to_lead(
                    cells, headers, county_key, source_url, tr,
                )
                if lead is None:
                    continue
                if lead.pipeline_lead_key in seen:
                    continue
                seen.add(lead.pipeline_lead_key)
                leads.append(lead)

        return leads

    def _row_to_lead(
        self,
        cells: List[str],
        headers: List[str],
        county_key: str,
        source_url: str,
        tr,
    ) -> Optional[LeadPayload]:
        col: Dict[str, str] = {}
        for i, h in enumerate(headers):
            if i >= len(cells):
                break
            col[h] = cells[i]

        # Best-effort field extraction by header name.
        def pick(*keys: str) -> Optional[str]:
            for k in keys:
                for h, v in col.items():
                    if k in h and v:
                        return v
            return None

        instr = pick("instrument", "doc number", "document #",
                      "document number", "inst")
        book = pick("book")
        page_no = pick("page")
        date_str = pick("recorded", "filed", "date")
        # Defendant (the borrower being sued) is typically GRANTEE on a LP.
        # Plaintiff (lender / HOA / claimant) is typically GRANTOR.
        defendant = pick("grantee", "defendant", "to")
        plaintiff = pick("grantor", "plaintiff", "from")

        # Fallback: scan all cells for a pseudo-instrument number.
        if not instr:
            joined = " ".join(cells)
            m = INSTR_NO_RE.search(joined)
            if m:
                instr = m.group(1)
            else:
                m2 = BOOK_PAGE_RE.search(joined)
                if m2:
                    book, page_no = m2.group(1), m2.group(2)

        # Address rarely in the index — try to spot one anyway.
        address = None
        for c in cells:
            m = ADDRESS_RE.search(c)
            if m:
                address = m.group(0).strip()
                break

        # Parse parcel if present anywhere in the row.
        parcel = None
        joined_row = " ".join(cells)
        m_parcel = PARCEL_RE.search(joined_row)
        if m_parcel:
            parcel = m_parcel.group(1)

        # Need at least defendant + (instrument OR book/page) to form a key.
        identifier = instr or (f"{book}-{page_no}" if book and page_no else None)
        if not defendant or not identifier:
            return None

        # Filing date → ISO.
        filing_iso = self._parse_date_iso(date_str)

        admin_parts = [
            f"{COUNTY_DISPLAY[county_key]} ROD LP",
            f"inst={instr}" if instr else None,
            f"book={book} page={page_no}" if book and page_no else None,
            f"plaintiff={plaintiff}" if plaintiff else None,
            f"parcel={parcel}" if parcel else None,
            f"filed={filing_iso}" if filing_iso else None,
        ]
        admin_notes = " | ".join(p for p in admin_parts if p)

        # Stable lead key per (county, instrument).
        lead_key_seed = f"{county_key}|{identifier}|{defendant}"
        lead_key = self.make_lead_key(self.name, lead_key_seed)

        raw: Dict[str, Any] = {
            "county_key": county_key,
            "row_cells": cells,
            "row_headers": headers,
            "extracted": {
                "instrument_no": instr,
                "book": book,
                "page": page_no,
                "filing_date": filing_iso,
                "defendant": defendant,
                "plaintiff": plaintiff,
                "parcel": parcel,
                "address": address,
            },
        }

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=lead_key,
            property_address=address,
            county=COUNTY_DISPLAY[county_key],
            full_name=defendant,
            owner_name_records=defendant,
            distress_type="LIS_PENDENS",
            admin_notes=admin_notes,
            source_url=source_url,
            raw_payload=raw,
        )

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _load_creds(county: str) -> Optional[Dict[str, str]]:
        prefix = COUNTY_ENV_PREFIX[county]
        user = os.environ.get(f"{prefix}_USER", "").strip()
        pw = os.environ.get(f"{prefix}_PASSWORD", "").strip()
        if not user or not pw:
            return None
        return {"user": user, "password": pw}

    @staticmethod
    def _maybe_fill(page, selector: str, value: str) -> None:
        try:
            loc = page.locator(selector).first
            if loc.count() > 0:
                loc.fill(value)
        except Exception:
            pass

    @staticmethod
    def _maybe_select(
        page, selector: str, value: str,
        fallback_label: Optional[str] = None,
    ) -> None:
        try:
            loc = page.locator(selector).first
            if loc.count() == 0:
                return
            # Try value match, label match, then fallback label.
            for attempt in (value, value.upper(), value.lower(),
                             fallback_label):
                if not attempt:
                    continue
                try:
                    loc.select_option(value=attempt)
                    return
                except Exception:
                    pass
                try:
                    loc.select_option(label=attempt)
                    return
                except Exception:
                    pass
        except Exception:
            pass

    @staticmethod
    def _parse_date_iso(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        s = s.strip().split()[0]
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    # ── Run-summary bookkeeping ─────────────────────────────────────────────

    def _finish(
        self,
        started: datetime,
        leads: List[LeadPayload],
        error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Stage via base helper so dedup / health reporting stay consistent
        # with the rest of the pipeline.
        staged_count, duplicate_count = self._write_staging(leads)
        finished = datetime.now(timezone.utc)

        if error:
            status = "failed"
        elif not leads:
            # Differentiate "no creds set anywhere" (extra.skipped non-empty,
            # no leads, no error) from a true zero-yield run.
            if extra and extra.get("skipped") and \
                    len(extra["skipped"]) >= 3:
                status = "skipped_no_creds"
            else:
                status = "zero_yield"
        elif staged_count == 0 and duplicate_count > 0:
            status = "all_dupes"
        elif staged_count < self.expected_min_yield:
            status = "below_threshold"
        else:
            status = "ok"

        self._report_health(
            status=status,
            started_at=started,
            finished_at=finished,
            fetched_count=len(leads),
            parsed_count=len(leads),
            staged_count=staged_count,
            duplicate_count=duplicate_count,
            error_message=error,
            notes=extra,
        )

        result: Dict[str, Any] = {
            "name": self.name,
            "run_id": self.run_id,
            "status": status,
            "fetched": len(leads),
            "staged": staged_count,
            "duplicates": duplicate_count,
            "error": error,
        }
        if extra:
            result.update(extra)
        return result


def run() -> dict:
    bot = MtnLisPendensRodBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
