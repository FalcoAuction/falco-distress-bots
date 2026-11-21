"""TN Foreclosure-LP — keyword-driven supplement to tn_public_notice.

What this captures (honest scope):
  Substitute trustee notices and foreclosure notices that reference
  "lis pendens" in their body text. tnpublicnotice.com indexes legal
  notices published in member newspapers — it does NOT carry standalone
  lis pendens recordings (those live at county Registers of Deeds,
  scraping which is a separate build).

  Retain supplemental publications with visible sale evidence and explicit
  partition actions. Compare exact publication URLs with the main feed.

What it does NOT capture:
  True 60-120-day-earlier lis pendens signal. That would require a
  per-county ROD scraper (Davidson via davidsonportal.com, Hamilton,
  Shelby, Williamson, etc.). Future build.

Distress type: PRE_FORECLOSURE — these are at the trustee-notice
stage, not lawsuit-stage. Tagging them anything else would be data
fraud against the dialer.

Run via:
  python -m src.bots.tn_lis_pendens_bot

Env knobs:
  FALCO_LP_DATE_RANGE_DAYS  (default 60)
  FALCO_LP_MAX_PAGES        (default 8)
"""
from __future__ import annotations

import os
import re
from typing import Dict, List, Optional

from ._base import LeadPayload
from ._court_order import PARTITION_ACTION_RE
from .tn_public_notice_bot import (
    TnPublicNoticeBot,

)


LP_KEYWORD = "lis pendens"

# Empty ddlPopularSearches doesn't trigger the search on the ASP.NET
# form — we must pass a real category id alongside the keyword filter.
# These two buckets capture lis-pendens publications: counties that file
# LPs under "Foreclosures" (4) and counties that file under generic
# "Public sales" (26).
LP_SEARCH_CATEGORIES: list[tuple[str, str]] = [
    ("4", "Foreclosures"),
    ("26", "Tax Sales"),  # Some counties group LP w/ tax-sale notices
]

# The clipped public excerpt must describe a sale, not merely refer to a
# recorded loan or appear in a newspaper foreclosure search.
SALE = re.compile(
    r"\b(?:notice of (?:mortgage )?foreclosure|foreclosure (?:sale|notice)|"
    r"(?:substitute |successor )?trustee(?:'s|s|')?\s+(?:notice of )?sale|"
    r"sale (?:by|under) (?:the )?(?:substitute )?trustee)\b", re.I)
CIVIC = re.compile(r"^\s*(?:public notice\s*[-:]?\s*)?notice of (?:public hearings?|zoning amendment)\b", re.I)


def publication_stage(text):
    normalized = (text or "").replace("’", "'").replace("�", "'")
    if PARTITION_ACTION_RE.search(normalized):
        return "partition_action"
    normalized = re.sub(r"(?i)(trustee)[?`´]s", r"\1's", normalized)
    if CIVIC.search(normalized) or "PUBLIC HEARING FOR ORDINANCE" in normalized.upper():
        return "public_hearing"
    if SALE.search(normalized):
        return "foreclosure_sale_publication"
    return "excerpt_unconfirmed"


class TnLisPendensBot(TnPublicNoticeBot):
    name = "tn_lis_pendens"
    description = (
        "TN Public Notice statewide aggregator — keyword-driven search "
        "for newspaper foreclosure sale notices"
    )
    # LP filings are less frequent than foreclosure notices, so widen
    # the look-back window. Most LP-to-trustee-sale gaps are 60-120
    # days, which means a 60-day window catches the freshest cohort
    # without rehashing leads we already converted or DNC'd.
    date_range_days = int(os.getenv("FALCO_LP_DATE_RANGE_DAYS", "60"))
    rows_per_page = 50
    max_pages_per_category = int(os.getenv("FALCO_LP_MAX_PAGES", "8"))
    expected_min_yield = 1  # rare; even 1 hit a week is signal

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_ids: set[str] = set()

        self.logger.info(
            f'keyword="{LP_KEYWORD}" date_range={self.date_range_days}d '
            f'categories={[c[1] for c in LP_SEARCH_CATEGORIES]}'
        )

        # tnpublicnotice's search REQUIRES a category id (an empty
        # ddlPopularSearches returns 0 rows even with a keyword), so we
        # iterate the buckets that historically carry lis-pendens
        # filings. The keyword filter narrows each bucket's results to
        # actual LP notices.
        for cat_id, cat_label in LP_SEARCH_CATEGORIES:
            session_state = self._init_search_session()
            if session_state is None:
                self.logger.warning(f"category {cat_label}: session init failed")
                continue
            search_url, state, cookies = session_state

            page_state = self._submit_keyword_search(
                search_url, state, cookies, LP_KEYWORD, cat_id
            )
            if page_state is None:
                self.logger.warning(
                    f"category {cat_label}: keyword search returned no state"
                )
                continue

            for page_idx in range(self.max_pages_per_category):
                rows = self._extract_rows(page_state["html"])
                self.logger.info(
                    f"category {cat_label} page {page_idx + 1}: {len(rows)} rows"
                )
                if not rows:
                    break
                for row in rows:
                    notice_id = row["notice_id"]
                    if notice_id in seen_ids:
                        continue
                    seen_ids.add(notice_id)
                    stage = publication_stage(row.get("excerpt"))
                    if stage not in ("foreclosure_sale_publication", "partition_action"):
                        continue
                    lead = self._build_lead(row, "PRE_FORECLOSURE", cat_label)
                    if lead is not None:
                        lead.raw_payload["publication_stage"] = stage
                        leads.append(lead)
                if page_idx + 1 >= self.max_pages_per_category:
                    break
                next_state = self._next_page(search_url, page_state, cookies)
                if next_state is None:
                    break
                page_state = next_state

        return leads

    def _write_staging(self, leads):
        # An identical newspaper URL in the primary feed is the same
        # publication. Keep supplemental coverage if its type differs.
        from ._base import _supabase
        client = _supabase()
        if client is None:
            return super()._write_staging(leads)
        keep = []
        for start in range(0, len(leads), 50):
            batch = leads[start:start + 50]
            rows = (client.table("homeowner_requests_staging")
                    .select("source_url,distress_type")
                    .eq("bot_source", "tn_public_notice").in_("staging_status", ["pending", "verified"])
                    .in_("source_url", [lead.source_url for lead in batch]).execute().data or [])
            known = {(row["source_url"], row["distress_type"]) for row in rows}
            keep.extend(lead for lead in batch if (lead.source_url, lead.distress_type) not in known)
        staged, duplicates = super()._write_staging(keep)
        return staged, duplicates + len(leads) - len(keep)

    def _submit_keyword_search(
        self,
        search_url: str,
        state: Dict[str, str],
        cookies: dict,
        keyword: str,
        category_id: str,
    ) -> Optional[Dict]:
        """Same POST mechanics as tn_public_notice's _submit_initial_search,
        but layers a keyword filter on top of a real category. Empty
        ddlPopularSearches doesn't actually search — must pass a real
        id. rdoType=0 = "all words" matching, fine for "lis pendens"
        which we want to match anywhere in the body."""
        data = {
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$as1$btnGo1",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": state.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": state.get("__EVENTVALIDATION", ""),
            "ctl00$ContentPlaceHolder1$as1$ddlPopularSearches": category_id,
            "ctl00$ContentPlaceHolder1$as1$txtSearch": keyword,
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
        return {"html": res.text, "state": self._extract_form_state(res.text)}


def run() -> dict:
    """Entry point for the new-staging runner registry."""
    bot = TnLisPendensBot()
    return bot.run()


if __name__ == "__main__":
    import sys
    result = run()
    print(result)
    sys.exit(0 if result.get("status") != "failed" else 1)
