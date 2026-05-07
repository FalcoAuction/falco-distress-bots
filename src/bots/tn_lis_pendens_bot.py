"""TN Foreclosure-LP — keyword-driven supplement to tn_public_notice.

What this captures (honest scope):
  Substitute trustee notices and foreclosure notices that reference
  "lis pendens" in their body text. tnpublicnotice.com indexes legal
  notices published in member newspapers — it does NOT carry standalone
  lis pendens recordings (those live at county Registers of Deeds,
  scraping which is a separate build).

  In practice this surfaces ~100 additional foreclosure notices per
  cron pass that the dedicated single-paper scrapers (nashville_ledger,
  memphis_daily_news, hamilton_county_herald) miss because they're
  published in smaller member papers we don't scrape directly.

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
from typing import Dict, List, Optional, Tuple

from ._base import LeadPayload
from .tn_public_notice_bot import (
    TnPublicNoticeBot,
    TNPN_BASE,
    TNPN_SEARCH,
    CATEGORY_TO_DISTRESS,
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

# tn_public_notice's _build_lead does
#   next(k for k, v in CATEGORY_TO_DISTRESS.items() if v[1] == category_label)
# which raises StopIteration for any label not in the parent dict.
# Inject benign synonyms so the keyword-mode label resolves. The
# parent's own scrape() doesn't iterate these labels so we don't
# change its behavior.
_LP_SUPPLEMENT_LABEL = "Lis Pendens"
if _LP_SUPPLEMENT_LABEL not in {v[1] for v in CATEGORY_TO_DISTRESS.values()}:
    CATEGORY_TO_DISTRESS["__lp_supplement"] = (
        "PRE_FORECLOSURE",
        _LP_SUPPLEMENT_LABEL,
    )


class TnLisPendensBot(TnPublicNoticeBot):
    name = "tn_lis_pendens"
    description = (
        "TN Public Notice statewide aggregator — keyword-driven search "
        "for lis pendens notices (lawsuit-stage distress, pre-foreclosure)"
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
                    # Tag as PRE_FORECLOSURE — these are
                    # near-trustee-sale notices that mention LP, not
                    # standalone lis-pendens recordings. The synonym
                    # label keeps the parent's category lookup happy.
                    lead = self._build_lead(row, "PRE_FORECLOSURE", "Lis Pendens")
                    if lead is not None:
                        leads.append(lead)
                if page_idx + 1 >= self.max_pages_per_category:
                    break
                next_state = self._next_page(search_url, page_state, cookies)
                if next_state is None:
                    break
                page_state = next_state

        return leads

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
