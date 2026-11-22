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

from ._base import LeadPayload
from ._probate_staging import ProbateNoticeBot
from . import _trustee_notice as trustee_notice
from . import _court_order as court_order
from . import _court_notices as cn
from . import _roane_tax
from ._county_tax_notices import non_tax_reason, parcel_rows, fill_parcels
from ._county_tax_headings import newspaper_tax_heading
from . import _reviewed_tax_excerpts


TNPN_BASE = "https://www.tnpublicnotice.com"
TNPN_SEARCH = TNPN_BASE + "/Search.aspx"

CATEGORY_TO_DISTRESS = {
    "4":  ("PRE_FORECLOSURE", "Foreclosures"),
    "22": ("TAX_LIEN", "Delinquent Taxes"),
    "23": ("PROBATE", "Probate Notices"),
    "26": ("TAX_LIEN", "Tax Sales"),
    # Court orders and public sales: sorted by text in _build_lead
    # (_court_order.kind). Newspapers also file trustee's sales here.
    "28": ("COURT_ORDER", "Orders"),
    "27": ("COURT_ORDER", "Public sales"),
    "25": ("COURT_ORDER", "Forfeitures/Seizure"),
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


class TnPublicNoticeBot(ProbateNoticeBot):
    name = "tn_public_notice"
    description = "TN Public Notice statewide aggregator (foreclosure, tax, probate, court order and public sale categories)"
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
        roane_parcels = {}
        tax_tables = {}
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
                    if distress == "TAX_LIEN" and (row.get("county") or "").lower() in ("madison", "henry"):
                        parsed = parcel_rows(row["county"].lower(), row)
                        if parsed:
                            for lead in parsed:
                                tax_tables.setdefault(lead.county, {})[lead.pipeline_lead_key] = lead
                            continue
                    if (row.get("county") or "").strip().lower() == "roane" and distress == "TAX_LIEN":
                        parcels = _roane_tax.exhibit_leads(row)
                        if parcels is not None:
                            for parcel in parcels:
                                roane_parcels.setdefault(parcel.pipeline_lead_key, parcel)
                            continue
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
        _roane_tax.fill_roll(roane_parcels.values())
        for table in tax_tables.values():
            records = list(table.values())
            fill_parcels(records)
            leads.extend(records)
        return leads + list(roane_parcels.values())

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

        if distress == "PROBATE":
            lines = excerpt.splitlines() or [excerpt]
            fields = cn.extract_probate(lines)
            review = cn.probate_review(lines, fields)
            notice_id = row["notice_id"]
            return LeadPayload(
                bot_source=self.name, distress_type=distress, county=fields["county"] or county,
                pipeline_lead_key=self.make_lead_key(self.name, notice_id),
                full_name=fields["decedent"],
                source_url=f"{TNPN_BASE}/Details.aspx?ID={notice_id}",
                admin_notes=f"TN Public Notice {notice_id} · {review['stage']}",
                raw_payload={"publication": self.name, "notice_id": notice_id,
                             "category": category_label, "category_id": "23", "newspaper": paper,
                             "publication_date": pub_date, "city": city, "county": county,
                             "excerpt": excerpt, "subject": fields["decedent"], "parcel": None,
                             "court_notice": fields, "probate_review": review},
            )

        # The notice text decides the type, not the newspaper's category:
        # Sumner and Montgomery trustee sales arrive filed as "Delinquent
        # Taxes", and county-wide "taxes are due" notices name no property.
        kind = trustee_notice.classify(excerpt)
        if distress == "TAX_LIEN" and kind != "foreclosure":
            heading = newspaper_tax_heading(county, excerpt)
            if heading == "exclude":
                return None
            if heading == "foreclosure":
                kind = heading
        if distress == "TAX_LIEN":
            decision = _reviewed_tax_excerpts.disposition(county, excerpt)
            if decision == "Mortgage trustee sale":
                kind = "foreclosure"
            elif decision and kind != "foreclosure":
                return None
        tax_lead = None
        if distress == "TAX_LIEN" and kind != "foreclosure" and non_tax_reason(county, excerpt):
            return None
        if county == "lauderdale" and distress == "TAX_LIEN" and kind != "foreclosure":
            text = " ".join(excerpt.split())
            civic_notice = re.match(
                r"(?:REQUEST FOR BIDS FOR SURPLUS PROPERTY Sealed Bids are invited and will be received by the City of Ripley|"
                r"(?:PUBLIC NOTICE )?NOTICE OF INTENT TO DESTROY IMPOUNDED PROPERTY Lauderdale County Sheriff|"
                r"NOTICE Pursuant to Section [\d-]+, Tennessee Code Annotated, the property assessment records "
                r"of Lauderdale County will be available for public inspection|"
                r"NOTICE OF INTENT TO EXCEED CERTIFIED TAX RATE The City of Ripley)", text, re.I)
            if civic_notice and not re.search(r"\b(?:delinquent (?:real property |property )?tax|tax sale|back taxes)", text, re.I):
                return None
        if county == "carroll" and distress == "TAX_LIEN":
            text = " ".join(excerpt.split())
            tax_claim = re.search(r"\b(?:delinquent\s+(?:(?:real\s+)?property\s+|real estate\s+)?tax|"
                                  r"tax (?:lien|sale|suit)|back taxes|unpaid (?:property )?tax)", text, re.I)
            if not tax_claim and re.match(r"TS#:\s*[\w-]+\s+Whereas,", text, re.I) and re.search(
                    r"\bby Deed of\s*Trust\b", text, re.I):
                kind = "foreclosure"  # Newspaper OCR joins "ofTrust".
            elif not tax_claim and kind != "foreclosure" and re.search(
                    r"Trezevant Board of Mayor and Aldermen.*zoning map amendment|"
                    r"property assessment records of Carroll County will be available for public inspection|"
                    r"NOTICE TO CREDITORS.*PROBATE CASE NO|lease-to-own storage building has been left unattended|"
                    r"duty of the Grand Jurors to investigate|whereabouts of the father.*unknown|"
                    r"bid on the real property in this Estate has been raised|"
                    r"above styled Estate.*Probate Minute Book|CLERK AND MASTER AUCTION.*Attorney for Estate",
                    text, re.I):
                return None
        if county == "henderson" and distress == "TAX_LIEN":
            text = " ".join(excerpt.split())
            if re.match(r"NOTICE OF SUBSTITUTE TRUSTEE\?S SALE\b", text, re.I):
                kind = "foreclosure"
            elif kind != "foreclosure" and not re.search(
                r"\b(?:delinquent (?:real |property )*tax|tax lien|tax sale|back taxes)", text, re.I
            ):
                if re.search(
                    r"Henderson County is undergoing a Current Value Update|"
                    r"NOTICE OF INTENT TO EXCEED CERTIFIED TAX RATE The Town of Scotts Hill|"
                    r"City of Scotts Hill, Tennessee, hereby provides certain financial information for the Fiscal Year|"
                    r"TO: ANY UNKNOWN HEIRS OF MARY L\. HUNT,.*Gowan Road/Gurley Creek Road",
                    text, re.I,
                ):
                    return None
        if distress == "TAX_LIEN" and kind != "foreclosure" and county in {
                "hardin", "houston", "stewart", "trousdale"}:
            text = " ".join(excerpt.split())
            if county == "hardin" and re.match(
                    r"PUBLIC NOTICE [-–] LAND SALE FOR DELINQUENT TAXES SALE OF REAL PROPERTY "
                    r"BY THE CHANCERY COURT OF MONTGOMERY COUNTY\b", text, re.I):
                return None
            civic_or_private = {
                "hardin": r"(?:Page \d+[AB] |THURSDAY\b).{0,110}SENATE JOINT RESOLUTION NO\. 25 A RESOLUTION",
                "houston": (r"(?:OFFICIAL NOTICE OF ELECTION ON QUESTIONS SENATE JOINT RESOLUTION|"
                            r"Houston County Chancery Court .{0,140}NOTICE TO CREDITORS As Required by TCA|"
                            r"CHANCERY COURT AUCTION .{0,210}Jerry Wimberly, et al Vs Docket 2018-CV-687)"),
                "stewart": r"PUBLIC NOTICE Resolution 517-26 .{0,185}resolution to annex",
                "trousdale": (r"(?:Barbara Jeanene Hubbard CHANCERY COURT SALE .{0,140}THE ESTATE OF BARBARA JEANENE HUBBARD|"
                              r"DAVENPORT CHANCERY COURT SALE .{0,140}decree of the Probate Court|"
                              r"GENERAL TAX RATE [\d.]+ \$ Funds listed receive funding from tax levies|"
                              r"Notice of Public Hearings .{0,210}PUBLIC HEARING for REZONE)"),
            }
            if re.match(civic_or_private[county], text, re.I) and not re.search(
                    r"\b(?:delinquent (?:real property |property )?tax|tax sale|back taxes)", text, re.I):
                return None
        if county == "hawkins" and distress == "TAX_LIEN" and kind != "foreclosure":
            text = " ".join(excerpt.split())
            if not re.search(r"\b(?:delinquent (?:property |real property )?tax|tax sale|back taxes)", text, re.I):
                if re.match(r"(?:IN THE NOVEMBER 3, 2026 GENERAL ELECTION POLLS WILL OPEN|"
                            r"NOTICE OF SPECIAL CALLED MEETING AND PUBLIC HEARING The Board of Mayor and Aldermen "
                            r"of the Town of Rogersville|TOWN OF MOUNT CARMEL, TENNESSEE PUBLIC NOTICE "
                            r"ORDINANCE #26-549\b|Surgoinsville Utility District .+?Request for Bid on Surplus Property)", text, re.I):
                    return None
                if (re.match(r"(?:CHANCERY COURT SALE|IN THE CHANCERY COURT .+? LAND SALE NOTICE)", text, re.I)
                        and re.search(r"\bRE:\s*(?:ESTATE OF\b|Peggy Lane Castle\b.+?\bVS\.\s*Rebecca Kern\b)", text, re.I)):
                    return None  # Estate administrations and a private-party suit.
        if county == "roane" and distress == "TAX_LIEN" and kind != "foreclosure":
            if re.match(r"(?:Governor \(All Precincts\) Vote for|NOTICE of ELECTION Tennessee Constitutional Amendments|"
                        r"PUBLIC NOTICE NOTICE OF PROPERTY MAINTENANCE VIOLATION AND INTENT TO REMEDY|"
                        r"(?:NOTICE OF DELINQUENT TAX LAWSUIT )?IN THE CHANCERY COURT FOR CHEATHAM COUNTY)", excerpt, re.I):
                return None
            if (re.match(r"IN THE CHANCERY COURT FOR ROANE COUNTY", excerpt, re.I)
                    and re.search(r"CITY OF (?:KINGSTON|HARRIMAN|OLIVER SPRINGS|OAK RIDGE)", excerpt, re.I)
                    and "DELINQUENT TAXPAYERS" in excerpt.upper()
                    and not re.search(r"\b(?:parcel|property address)\b|\d+/\d+[A-Z]?/", excerpt, re.I)):
                return None  # A municipality and court caption alone do not identify a parcel.
        if county == "hardeman" and distress == "TAX_LIEN" and kind != "foreclosure":
            # One excerpt can be the lead item of an entire legal edition. Pull
            # tax exhibits from the full PDF rather than assign the meeting address.
            if not re.search(r"\b(?:delinquent (?:property )?tax|tax sale|back taxes)", excerpt, re.I):
                if re.search(r"Hardeman County (?:Beer Board will meet|Election Commission will meet|"
                             r"Regional Planning Commission will meet|Board of Zoning Appeals will meet)|"
                             r"INVITATION TO BID (?:HVAC ROOFTOP|Hardeman County Board of Education)|"
                             r"C D Farms in accordance with federal Guidelines|City of Bolivar Street Department is requesting bids|"
                             r"Town of Hickory Valley is currently accepting applications|"
                             r"HARDEMAN COUNTY ELECTION COMMISSION.{0,100}ELECTION PROCEDURES|"
                             r"LAND SALE IN THE CHANCERY COURT OF HARDEMAN COUNTY.{0,40}IN THE MATTER OF THE ESTATE OF",
                             excerpt, re.I | re.S):
                    return None
        if county == "rutherford" and distress == "TAX_LIEN" and kind != "foreclosure":
            text = " ".join(excerpt.split())
            # The paper's tax category also carries HOA sales and municipal notices.
            if re.match(r"(?:NOTICE OF HOMEOWNERS ASSOCIATION LIEN SALE|NOTICE of ELECTION Constitutional Amendments|"
                        r"(?:TN-\d+\s+)?NOTICE TO ALL PROPERTY OWNERS OF TAXABLE PROPERTY IN RUTHERFORD COUNTY|"
                        r"NOTICE OF PUBLIC HEARINGS - LEGAL NOTICE - (?:RESOLUTION |26-R-PSA)|"
                        r"Legal Notice of Public Hearing The Murfreesboro Planning Commission|"
                        r"The Town Council of the Town of Smyrna will hold a public hearing|"
                        r"LEGAL NOTICE Pursuant to the provisions of Article V, Section 5.05 of the Town Charter)", text, re.I
                        ) and not re.search(r"\b(?:delinquent (?:property )?tax|tax sale)", text, re.I):
                return None
            if re.search(r"MIDSOUTH CONSTRUCTION, L\.L\.C\., Case No\. : 24CV-406|"
                         r"IN RE: K\.W\.R\..*?JOHN DOE FATHERS", text, re.I):
                return None
            if re.fullmatch(r"IN THE CHANCERY COURT FOR RUTHERFORD COUNTY, TENNESSEE AT MURFREESBORO "
                            r"DELINQUENT PROPERTY TAX NOTICE AND ORDER FOR PUBLICATION CITY OF MURFREESBORO, "
                            r"A Municipal Corporation in Rutherford County, Tennessee, organized and existing "
                            r"under the laws of the State of[?….\s]*", text, re.I):
                return None  # Repeated caption; the full notice can identify the account.
        if county == "shelby" and distress == "TAX_LIEN" and kind != "foreclosure":
            # Category 22 includes environmental warrants, rate hearings and
            # case-wide announcements. The trustee extract identifies the parcels.
            if re.search(r"THIRTIETH JUDICIAL DISTRICT AT MEMPHIS Civil Warrant No\.|"
                         r"CH-26-0619 Part I TOWN OF COLLIERVILLE|"
                         r"NOTICE OF SUBSTITUE TRUSTEE.S SALE|"
                         r"NOTICE OF INTENT TO EXCEED CERTIFIED TAX RECAPTURE RATE|"
                         r"CITY OF MEMPHIS\s+\d{4} AD VALOREM PROPERTY TAXES|"
                         r"Ordinance No[.: ]+59(?:81|87)\b|"
                         r"Board of Mayor and Aldermen of the City of Germantown will hold public hearings|"
                         r"Town of Arlington.{0,140}Ordinance 2026-06", excerpt, re.I | re.S):
                return None
            if re.match(r"(?:NOTICE TO THE DELINQUENT|Size:\s*2 columns by 3 inches SHELBY COUNTY TAX SALE)",
                        excerpt, re.I) and not re.search(r"\b(?:parcel|property address)\b", excerpt, re.I):
                return None
            if ("in its own behalf" in excerpt.lower() and re.search(
                    r"MUNICIPALITIES OF [A-Z, ]+\s*\.{3}\s*$", excerpt, re.I)
                    and not re.search(r"\b(?:parcel|property address)\b", excerpt, re.I)):
                return None
        if county == "maury" and distress == "TAX_LIEN" and kind != "foreclosure":
            # The category contains county meeting announcements and citywide rate changes.
            municipal = re.match(
                r"\s*(?:LEGAL NOTICE OF ZONING MAP AMENDMENTS(?: AND TEMPORARY DATA CENTER MORATORIUM)? "
                r"MAURY COUNTY LEGISLATIVE BODY|LEGAL NOTICE OF PUBLIC HEARING MAURY COUNTY LEGISLATIVE BODY "
                r"Notice is hereby given that the Maury County Legislative Body will hold a Public Hearing|"
                r"City of Spring Hill Board of Mayor and Alderm[ae]n NOTICE OF INTENT TO EXCEED CERTIFIED TAX RATE|"
                r"NOTICE OF PUBLIC HEARING ON PROPERTY TAX LEVY ORDINANCE FOR THE CITY OF COLUMBIA)", excerpt, re.I)
            if municipal and not re.search(r"\b(?:delinquent (?:property )?tax(?:es)?|tax sales?|back taxes)\b", excerpt, re.I):
                return None
        if county == "robertson" and distress == "TAX_LIEN":
            if re.match(r"\s*(?:BOARD OF MAYOR AND ALDERMEN\b.{0,220}?City of White House|"
                        r"CITY OF WHITE HOUSE\s+Meeting of the Municipal Planning Commission|"
                        r"NOTICE OF PUBLIC HEARING CITY OF RIDGETOP\s+The Ridgetop Board of Zoning Appeals|"
                        r"ROBERTSON COUNTY, TENNESSEE NOTICE OF PUBLIC HEARING FOR THE BUDGET)", excerpt, re.I | re.S):
                return None
            # The truncated consolidated caption identifies tax years and cases only.
            if re.fullmatch(r"\s*IN THE CHANCERY COURT FOR ROBERTSON COUNTY, TENNESSEE, AT SPRINGFIELD\s+"
                            r"CONSOLIDATED CIVIL ACTION NUMBERS:\s*(?:CH\d+-CV-\d+\s*\(\d{4}\)[,;]?\s*)+"
                            r"ROBERTSON COUNTY, TENNESSEE vs\.\s+DE-?\s*LINQUENT TAXPAYERS FOR ROBERTSON COUNTY "
                            r"and CITY OF S(?:PRINGFIELD)?\s*(?:\.{3}|…)?\s*", excerpt, re.I):
                return None
        if county == "cheatham" and distress == "TAX_LIEN":
            if re.match(r"\s*(?:Governor \(All Precincts\) Vote for|Copyright Notice\s+Copyright Notice|"
                        r"(?:TN-\d+\s+)?Cheatham County Planning Commission\s+Meeting Agenda)", excerpt, re.I):
                return None
            if (re.search(r"Civil Action No\.?(?:\s+)19076\b", excerpt, re.I)
                    and not re.search(r"\b(?:parcel|property address|tract [A-Z]\b)", excerpt, re.I)):
                return None  # The full Ledger edition carries the individual tracts.
        if county == "sumner" and distress == "TAX_LIEN" and re.match(
            r"\s*(?:public\s+notice\s*[:\-]?\s*)?(?:"
            r"city of white house\s+meeting of the municipal planning commission\b|"
            r"distribution of excess proceeds from the \d{4} sumner county delinquent tax sale\b)",
            excerpt, re.I,
        ):
            return None
        if county == "wilson" and distress == "TAX_LIEN":
            # Category 22 includes zoning, towing and other counties' sales.
            tax_case = re.search(r"delinquent\s+(?:(?:real estate|property)\s+)?tax|back tax sale|tax sale", excerpt, re.I)
            if not tax_case:
                return None
            from .main_street_media_bot import LABELED_PARCEL_RE
            maps = LABELED_PARCEL_RE.findall(excerpt.upper())
            situs = re.search(r"\b(?:property address|street address)\s*:\s*(\d[^;\n]{3,90})", excerpt, re.I)
            address = self._extract_address(situs[1], city) if situs else None
            if len(maps) > 1 or not (maps or address):
                return None
            printed = "-".join(x.replace(" ", "") for x in maps[0] if x) if maps else None
            tax_lead = (printed, address)
            kind = None
        if county == "hamilton" and distress == "TAX_LIEN":
            # Newspaper search categories also return meetings, court calendars
            # and sales in other counties. A case number is not a street number.
            if re.search(r"\b(?:substitute\s+)?trustee['\u2019]?s?\s+sale\b", excerpt, re.I):
                kind = "foreclosure"
                venue = re.search(r"state of Tennessee,?\s+([a-z]+) county", excerpt, re.I)
                if venue and venue[1].lower() != county:
                    county, city = venue[1].lower(), ""
            else:
                tax_text = re.search(r"delinquent\s+(?:property\s+|real estate\s+)?tax|tax (?:sale|cause)", excerpt, re.I)
                parcel = re.search(r"\b(?:map(?:/group)?/parcel|parcel(?: number| no\.?| id)?)\s*[:#]\s*([0-9]{3}[A-Z]?[ -]+(?:[A-Z][ -]+)?[0-9]{3}(?:\.[0-9]+)?)\b", excerpt, re.I)
                situs = re.search(r"\b(?:property address|street address|located at)\s*[:#]?\s*(\d{1,6}\s+[^;\n]{3,90})", excerpt, re.I)
                if not tax_text or not (parcel or (situs and self._extract_address(situs[1], city))):
                    return None
                tax_lead = (parcel[1] if parcel else None, self._extract_address(situs[1], city) if situs else None)
                kind = None
        if kind == "general_tax":
            return None
        filed_as = None
        court_names: List[str] = []
        court_address: Optional[str] = None
        if distress in ("PRE_FORECLOSURE", "COURT_ORDER") and court_order.PARTITION_ACTION_RE.search(excerpt):
            distress = "PARTITION"
        elif distress == "COURT_ORDER":
            ck = court_order.kind(excerpt)
            if ck:
                distress = ck
                court_names = court_order.parties(excerpt)
                court_address = court_order.tenants_address(excerpt)
            elif kind == "foreclosure":
                distress = "PRE_FORECLOSURE"
            else:
                return None   # bids, storage and towing sales, other auctioneers' ads
        if kind == "foreclosure" and distress not in ("PROBATE", "PRE_FORECLOSURE", "PARTITION"):
            filed_as, distress = distress, "PRE_FORECLOSURE"
        address_text = trustee_notice.property_address_text(excerpt) if distress == "PRE_FORECLOSURE" else excerpt
        property_address = court_address or self._extract_address(address_text, city)
        parcel = self._extract_parcel(excerpt)
        if tax_lead is not None:
            parcel, property_address = tax_lead
        names = court_names or (trustee_notice.borrowers(excerpt) if distress == "PRE_FORECLOSURE" else [])
        full_name = names[0] if names else self._extract_subject(excerpt, distress)
        sale_date = trustee_notice.sale_date(excerpt) if distress == "PRE_FORECLOSURE" else None

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
            owner_name_records="; ".join(names) if names else full_name,
            distress_type=distress,
            trustee_sale_date=sale_date,
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
                **({"borrowers": names} if names else {}),
                **({"deed_of_trust": trustee_notice.deed_ref(excerpt)} if distress == "PRE_FORECLOSURE" else {}),
                **({"filed_as": filed_as} if filed_as else {}),
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
