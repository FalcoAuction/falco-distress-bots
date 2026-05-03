"""
Tennessee Public Notice — statewide ASP.NET aggregator scraper.

tnpublicnotice.com is the Tennessee Press Association's master index
of legal notices published in EVERY participating TN newspaper. ONE
bot covers all 95 counties × four distress categories: Foreclosures,
Tax Sales, Delinquent Taxes, Probate Notices.

Hard limit: the per-notice DETAIL page is reCAPTCHA-walled. So we
work entirely off the search-result GridView, which exposes ENOUGH
without the gate:
  - Publication name (e.g. "Kingsport Times-News")
  - Date published
  - City
  - County
  - First ~250 chars of the notice body (truncated with "click 'view'
    to open the full text")
  - Notice ID (PK value, used for stable lead_key)

Property address + parcel often appear in the first 250 chars; the
existing notice-body regex patterns from nashville_ledger_bot can
extract them. When the excerpt is too short to find the address, the
lead is staged with publication + city + county metadata only — Chris
can still cross-reference via the deduplication layer with other bot
sources (Nashville Ledger, Memphis Daily News, Hamilton Herald).

Search is driven by Popular-Searches dropdown IDs:
  4  = Foreclosures
  22 = Delinquent Taxes
  23 = Probate Notices
  26 = Tax Sales

POST mechanics (took two passes to nail down):
  1. Initial GET captures __VIEWSTATE + cookieless session ID in URL
  2. POST __EVENTTARGET=...$btnGo1, ddlPopularSearches={ID}, dateRange=30
     fires the search; response includes the GridView populated with
     up to ddlPerPage rows (default 10).
  3. Result rows have onclick handlers with location.href='Details.aspx?
     SID=...&ID={NUMERIC}' — the SID is per-session, so we extract IDs
     and discard SID.

Distress types:
  PRE_FORECLOSURE (Foreclosures)
  TAX_LIEN (Tax Sales + Delinquent Taxes)
  PROBATE (Probate Notices)
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload


TNPN_BASE = "https://www.tnpublicnotice.com"
TNPN_SEARCH = TNPN_BASE + "/Search.aspx"

CATEGORY_TO_DISTRESS = {
    "4":  ("PRE_FORECLOSURE", "Foreclosures"),
    "22": ("TAX_LIEN", "Delinquent Taxes"),
    "23": ("PROBATE", "Probate Notices"),
    "26": ("TAX_LIEN", "Tax Sales"),
}

VIEWSTATE_RE = re.compile(r'name="__VIEWSTATE" id="__VIEWSTATE" value="([^"]*)"')
VIEWSTATEGENERATOR_RE = re.compile(r'name="__VIEWSTATEGENERATOR" id="__VIEWSTATEGENERATOR" value="([^"]*)"')
EVENTVALIDATION_RE = re.compile(r'name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="([^"]*)"')

DETAIL_LINK_RE = re.compile(r"Details\.aspx\?SID=[a-z0-9]+&amp;ID=(\d+)")
ROW_BLOCK_RE = re.compile(
    r"GridView1_ctl(\d{2})_btnView2.*?GridView1_ctl\d{2}_btnView2",
    re.DOTALL,
)

ADDRESS_HINT_RE = re.compile(
    r"(\d{1,5}\s+[A-Z][A-Z0-9 \.\-,'/]{3,80}?(?:STREET|ST\.?|ROAD|RD\.?|AVENUE|AVE\.?|"
    r"DRIVE|DR\.?|LANE|LN\.?|BLVD|BOULEVARD|COURT|CT\.?|CIRCLE|CIR\.?|PLACE|PL\.?|"
    r"WAY|HIGHWAY|HWY|PARKWAY|PKWY|TRAIL|TRL|TERRACE|PIKE)(?:\s+[A-Z]+)?)",
)
PARCEL_HINT_RE = re.compile(
    r"PARCEL[ ]*(?:NO\.?|NUMBER|ID)?[:\s]+([A-Z0-9][A-Z0-9\- \.,/]*\d[A-Z0-9\-\. ,/]*)",
    re.IGNORECASE,
)
DECEDENT_HINT_RE = re.compile(r"Estate of\s+([A-Z][A-Za-z\.\-' ]+?),\s*Deceased", re.IGNORECASE)
BORROWER_HINT_RE = re.compile(r"executed by\s+([A-Z][A-Za-z. ,'\-]+?),", re.IGNORECASE)


class TnPublicNoticeBot(BotBase):
    name = "tn_public_notice"
    description = "TN Public Notice statewide aggregator (4 categories, all 95 counties)"
    throttle_seconds = 1.5
    expected_min_yield = 10

    # date_range: search-form value, in days. 30 is the most useful for
    # current distress; 7 is too narrow, 60 is too noisy.
    date_range_days = 30
    # pagination: walk first N pages of results per category. Each page
    # holds 10 rows by default; tnpublicnotice supports up to 50/page so
    # we boost that and cap pages at a low number to keep run length
    # bounded.
    rows_per_page = 50
    max_pages_per_category = 4

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_ids: set[str] = set()

        for cat_id, (distress, category_label) in CATEGORY_TO_DISTRESS.items():
            self.logger.info(f"category {category_label} (id={cat_id})")
            ids_seen_this_cat = set()
            session_state = self._init_search_session()
            if session_state is None:
                self.logger.warning("  search session init failed")
                continue
            search_url, state, cookies = session_state

            page_state = self._submit_initial_search(search_url, state, cookies, cat_id)
            if page_state is None:
                continue

            for page_idx in range(self.max_pages_per_category):
                rows = self._extract_rows(page_state["html"])
                if not rows:
                    break
                for row in rows:
                    notice_id = row["notice_id"]
                    if notice_id in seen_ids or notice_id in ids_seen_this_cat:
                        continue
                    ids_seen_this_cat.add(notice_id)
                    seen_ids.add(notice_id)
                    lead = self._build_lead(row, distress, category_label)
                    if lead is not None:
                        leads.append(lead)
                # Walk to next page
                if page_idx + 1 >= self.max_pages_per_category:
                    break
                next_state = self._next_page(search_url, page_state, cookies)
                if next_state is None:
                    break
                page_state = next_state
        return leads

    # ── Session bootstrap ───────────────────────────────────────────────────

    def _init_search_session(self) -> Optional[Tuple[str, Dict[str, str], dict]]:
        res = self.fetch(TNPN_SEARCH)
        if res is None or res.status_code != 200:
            return None
        state = self._extract_form_state(res.text)
        # tnpublicnotice uses cookieless session — final URL embeds (S(...))
        return (res.url, state, dict(res.cookies))

    @staticmethod
    def _extract_form_state(html: str) -> Dict[str, str]:
        out = {}
        for name, pat in (
            ("__VIEWSTATE", VIEWSTATE_RE),
            ("__VIEWSTATEGENERATOR", VIEWSTATEGENERATOR_RE),
            ("__EVENTVALIDATION", EVENTVALIDATION_RE),
        ):
            m = pat.search(html)
            out[name] = m.group(1) if m else ""
        return out

    # ── Submit search ───────────────────────────────────────────────────────

    def _submit_initial_search(self, search_url: str, state: Dict[str, str],
                                 cookies: dict, category_id: str) -> Optional[Dict]:
        data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$as1$btnGo1",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": state.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": state.get("__EVENTVALIDATION", ""),
            "ctl00$ContentPlaceHolder1$as1$ddlPopularSearches": category_id,
            "ctl00$ContentPlaceHolder1$as1$txtSearch": "",
            "ctl00$ContentPlaceHolder1$as1$rdoType": "0",
            "ctl00$ContentPlaceHolder1$as1$txtExclude": "",
            "ctl00$ContentPlaceHolder1$as1$dateRange": str(self.date_range_days),
            "ctl00$ContentPlaceHolder1$as1$hdnLastScrollPos": "",
            "ctl00$ContentPlaceHolder1$as1$hdnCountyScrollPosition": "",
            "ctl00$ContentPlaceHolder1$as1$hdnCityScrollPosition": "",
            "ctl00$ContentPlaceHolder1$as1$hdnPubScrollPosition": "",
            "ctl00$ContentPlaceHolder1$as1$hdnField": "",
        }
        res = self.fetch(search_url, method="POST", data=data, cookies=cookies)
        if res is None or res.status_code != 200:
            return None
        # Skip per-page bump — testing showed it resets search context.
        # Default 10 rows/page is fine; we walk pagination via _next_page.
        return {"html": res.text, "state": self._extract_form_state(res.text)}

    def _set_per_page(self, search_url: str, state: Dict[str, str],
                       cookies: dict, html: str) -> Optional[Dict]:
        """Issue a postback against the GridView's ddlPerPage to bump rows
        per page. Idempotent if already at target value."""
        data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$WSExtendedGridNP1$GridView1$ctl01$ddlPerPage",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": state.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": state.get("__EVENTVALIDATION", ""),
            "ctl00$ContentPlaceHolder1$WSExtendedGridNP1$GridView1$ctl01$ddlPerPage": str(self.rows_per_page),
            "ctl00$ContentPlaceHolder1$WSExtendedGridNP1$GridView1$ctl01$ddlSortBy": "DatePublishedDate DESC",
        }
        res = self.fetch(search_url, method="POST", data=data, cookies=cookies)
        if res is None or res.status_code != 200:
            return {"html": html, "state": state}  # fall back to default page
        return {"html": res.text, "state": self._extract_form_state(res.text)}

    def _next_page(self, search_url: str, page_state: Dict, cookies: dict) -> Optional[Dict]:
        """Postback to walk to the next results page."""
        data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$WSExtendedGridNP1$GridView1$ctl01$btnNext",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": page_state["state"].get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": page_state["state"].get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": page_state["state"].get("__EVENTVALIDATION", ""),
            "ctl00$ContentPlaceHolder1$WSExtendedGridNP1$GridView1$ctl01$ddlPerPage": str(self.rows_per_page),
            "ctl00$ContentPlaceHolder1$WSExtendedGridNP1$GridView1$ctl01$ddlSortBy": "DatePublishedDate DESC",
        }
        res = self.fetch(search_url, method="POST", data=data, cookies=cookies)
        if res is None or res.status_code != 200:
            return None
        return {"html": res.text, "state": self._extract_form_state(res.text)}

    # ── Row extraction ──────────────────────────────────────────────────────

    @staticmethod
    def _extract_rows(html: str) -> List[Dict]:
        """Parse GridView rows out of the HTML."""
        rows: List[Dict] = []
        soup = BeautifulSoup(html, "html.parser")

        # Each result row is rendered in a nested table; the easiest way to
        # find them is via the hdnPKValue inputs (one per row).
        for hdn in soup.find_all("input", id=re.compile(r"GridView1_ctl\d+_hdnPKValue")):
            notice_id = hdn.get("value") or ""
            if not notice_id or not notice_id.isdigit():
                continue
            # The hdn input's parent <table class="nested"> holds the row's
            # info td. The EXCERPT lives in a sibling <tr> in the OUTER
            # table that wraps the nested table.
            inner_table = hdn.find_parent("table")
            if inner_table is None:
                continue
            # The "info" td has Paper/Date and City/County
            info_td = inner_table.find("td", class_=re.compile(r"\binfo\b"))
            paper = pub_date = city = county = ""
            if info_td:
                left = info_td.find("div", class_="left")
                right = info_td.find("div", class_="right")
                if left:
                    parts = [p.strip() for p in left.get_text("\n").split("\n") if p.strip()]
                    if parts:
                        paper = parts[0]
                    if len(parts) > 1:
                        pub_date = parts[1]
                if right:
                    txt = right.get_text(" ", strip=True)
                    cm = re.search(r"City:\s*([^|]+?)(?:\s+County:|$)", txt)
                    if cm:
                        city = cm.group(1).strip()
                    cm = re.search(r"County:\s*(.+)$", txt)
                    if cm:
                        county = cm.group(1).strip()
            # Excerpt: walk OUTER table's sibling rows. Inner table is
            # wrapped in a <td><tr>...inner_table...</tr></td>; the excerpt
            # row is the next <tr> at the same level.
            excerpt = ""
            outer_td = inner_table.find_parent("td")
            outer_tr = outer_td.find_parent("tr") if outer_td else None
            if outer_tr is not None:
                # Look in BOTH the nested table AND the next outer-tr for
                # a td colspan=3 (the excerpt cell)
                excerpt_td = inner_table.find("td", attrs={"colspan": "3"})
                if excerpt_td is None:
                    next_outer = outer_tr.find_next_sibling("tr")
                    if next_outer is not None:
                        excerpt_td = next_outer.find("td", attrs={"colspan": "3"})
                if excerpt_td is not None:
                    excerpt = excerpt_td.get_text(" ", strip=True)
                    excerpt = re.sub(
                        r"\s*click 'view' to open the full text\.?\s*$",
                        "",
                        excerpt,
                        flags=re.IGNORECASE,
                    )
            rows.append({
                "notice_id": notice_id,
                "paper": paper,
                "pub_date": pub_date,
                "city": city,
                "county": county,
                "excerpt": excerpt,
            })
        return rows

    # ── Lead construction ───────────────────────────────────────────────────

    def _build_lead(self, row: Dict, distress: str, category_label: str) -> Optional[LeadPayload]:
        excerpt = row.get("excerpt") or ""
        paper = row.get("paper") or ""
        pub_date = row.get("pub_date") or ""
        city = row.get("city") or ""
        county = (row.get("county") or "").strip().lower() or None

        property_address = self._extract_address(excerpt, city)
        parcel = self._extract_parcel(excerpt)
        full_name = self._extract_subject(excerpt, distress)

        admin_parts = [
            f"TN Public Notice {row['notice_id']}",
            f"category={category_label}",
            f"paper={paper}" if paper else "",
            f"pub={pub_date}" if pub_date else "",
        ]
        if parcel:
            admin_parts.append(f"parcel={parcel}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, row["notice_id"]),
            property_address=property_address,
            county=county,
            full_name=full_name,
            owner_name_records=full_name,
            distress_type=distress,
            admin_notes=" · ".join(p for p in admin_parts if p),
            source_url=f"{TNPN_BASE}/Details.aspx?ID={row['notice_id']}",
            raw_payload={
                "publication": "tn_public_notice",
                "category": category_label,
                "category_id": next(k for k, v in CATEGORY_TO_DISTRESS.items() if v[1] == category_label),
                "newspaper": paper,
                "publication_date": pub_date,
                "city": city,
                "county": county,
                "parcel": parcel,
                "subject": full_name,
                "excerpt": excerpt,
            },
        )

    # ── Excerpt parsing helpers ─────────────────────────────────────────────

    @staticmethod
    def _extract_address(excerpt: str, city: str) -> Optional[str]:
        m = ADDRESS_HINT_RE.search(excerpt)
        if not m:
            return None
        addr = m.group(1).strip().title()
        if city and city.lower() not in addr.lower():
            addr = f"{addr}, {city.title()}, TN"
        return addr

    @staticmethod
    def _extract_parcel(excerpt: str) -> Optional[str]:
        m = PARCEL_HINT_RE.search(excerpt)
        if not m:
            return None
        parcel = m.group(1).strip().rstrip(",.")
        return parcel or None

    @staticmethod
    def _extract_subject(excerpt: str, distress: str) -> Optional[str]:
        if distress == "PROBATE":
            m = DECEDENT_HINT_RE.search(excerpt)
            if m:
                return m.group(1).strip().title()
        if distress == "PRE_FORECLOSURE":
            m = BORROWER_HINT_RE.search(excerpt)
            if m:
                return m.group(1).strip().title()
        return None


def run() -> dict:
    bot = TnPublicNoticeBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
