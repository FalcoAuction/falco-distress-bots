"""Brock & Scott PLLC — substitute trustee firm with significant TN
foreclosure volume.

Why this matters: TN law (TCA § 35-5-101) requires three consecutive
weekly newspaper publications before a substitute trustee sale. By the
time the notice hits the Nashville Ledger or TN Public Notice, the
homeowner has ~20-30 days. The trustee firms themselves publish their
case lists earlier than that — Brock & Scott has 27+ TN sales live on
brockandscott.com with sale dates often 4-6 weeks out, which gives us
roughly 10-25 additional days of conversion runway over the paper feed.

Source: https://www.brockandscott.com/foreclosure-sales/?_sft_foreclosure_state=tn

Page structure (verified 2026-05-12):
  <article class="foreclosure_search ... foreclosure_county-X foreclosure_state-tn">
    <div class="continfo">
      <div class="record">
        <div class="forecol"><p>County:</p><p> Davidson</p></div>
        <div class="forecol"><p>Sale Date:</p><p> 05/12/2026 - 10:00:00 AM</p></div>
        <div class="forecol"><p>State:</p><p> TN</p></div>
        <div class="forecol"><p>Court SP #:</p><p> </p></div>
        <div class="forecol"><p>Case #:</p><p> 25-27397-FC01</p></div>
        <div class="forecol"><p>Address:</p><p> 5740 Stone Brook Dr  Brentwood, Tennessee 37027</p></div>
        <div class="forecol"><p>Opening Bid Amount:</p><p> 208467.44</p></div>
        <div class="forecol"><p>Book Page:</p><p></p></div>
      </div>
    </div>
  </article>

Pagination: ?_sft_foreclosure_state=tn&sf_paged=N — Next > link visible
when more pages exist.

Borrower names are NOT exposed on the listing pages — Brock & Scott
withholds them. Owner enrichment happens downstream via the assessor
bots (davidson_assessor, williamson_assessor, etc.) which look up the
owner by address against county parcel databases. Leads sit in
staging until that fires.

Distress type: TRUSTEE_NOTICE (substitute trustee sale notice — same
mechanic as Nashville Ledger / TN Public Notice trustee sales).
"""
from __future__ import annotations

import re
import hashlib
from datetime import datetime
from typing import List, Optional

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload


BS_INDEX = "https://www.brockandscott.com/foreclosure-sales/"
BS_TN_PARAMS = {"_sft_foreclosure_state": "tn"}

# "05/12/2026 - 10:00:00 AM" — also tolerate variants without time
SALE_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
# Opening bid: numeric string like "208467.44"; tolerate $ and commas
BID_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d{1,2})?)")

# Field labels Brock & Scott uses (in order in the HTML; order isn't
# guaranteed cross-listing so we parse by label).
FIELD_LABELS = {
    "county", "sale date", "state", "court sp #", "case #",
    "address", "opening bid amount", "book page",
}


class BrockScottTrusteeBot(BotBase):
    name = "brock_scott_trustee"
    description = (
        "Brock & Scott PLLC TN substitute trustee sales — "
        "10-25 days earlier than newspaper notice."
    )
    throttle_seconds = 2.0  # polite — small firm site
    expected_min_yield = 5  # they normally have 25+ TN listings live

    # Walk at most this many paginated pages before stopping.
    # 27 listings at ~25/page = 2 pages typical; cap at 5 for safety.
    max_pages = 5

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_keys: set[str] = set()

        for page in range(1, self.max_pages + 1):
            params = dict(BS_TN_PARAMS)
            if page > 1:
                params["sf_paged"] = str(page)
            res = self.fetch(BS_INDEX, params=params)
            if res is None:
                self.logger.warning(f"page {page}: fetch failed")
                break
            if res.status_code != 200:
                self.logger.warning(f"page {page}: HTTP {res.status_code}")
                break

            soup = BeautifulSoup(res.text, "html.parser")
            articles = soup.find_all("article", class_=re.compile(r"foreclosure_search"))
            if not articles:
                self.logger.info(f"page {page}: no listings (end of pagination)")
                break

            self.logger.info(f"page {page}: {len(articles)} listing articles")
            new_this_page = 0
            for art in articles:
                lead = self._parse_article(art)
                if lead is None:
                    continue
                if lead.pipeline_lead_key in seen_keys:
                    continue
                seen_keys.add(lead.pipeline_lead_key)
                leads.append(lead)
                new_this_page += 1

            # If no new rows came out of this page (all duplicates of
            # earlier pages), the pagination probably looped — stop.
            if new_this_page == 0:
                self.logger.info(f"page {page}: 0 new — stopping pagination")
                break

        self.logger.info(f"parsed {len(leads)} TN listings across {page} page(s)")
        return leads

    # ── Parsing ─────────────────────────────────────────────────────────────

    def _parse_article(self, art) -> Optional[LeadPayload]:
        """Extract one lead from an <article class="foreclosure_search ...">."""
        fields = {}
        for col in art.find_all("div", class_="forecol"):
            paras = col.find_all("p")
            if len(paras) < 2:
                continue
            label = paras[0].get_text(strip=True).rstrip(":").lower()
            value = paras[1].get_text(strip=True)
            if label in FIELD_LABELS:
                fields[label] = value

        # State filter at URL level should already restrict to TN, but
        # double-check in case Brock & Scott adds multi-state pages later.
        state = (fields.get("state") or "").upper()
        if state and state != "TN":
            return None

        address = fields.get("address", "").strip()
        if not address:
            return None
        county = fields.get("county", "").strip()

        # Case # is the stable identifier per case. Fall back to a
        # sha40 of address+sale_date if case# missing (rare).
        case_num = fields.get("case #", "").strip()
        sale_date_raw = fields.get("sale date", "").strip()
        sale_date_iso = self._parse_sale_date(sale_date_raw)

        if case_num:
            lead_key = self.make_lead_key("brock_scott", case_num)
        else:
            ident = f"{address}|{sale_date_raw}"
            lead_key = self.make_lead_key("brock_scott", ident)

        # Compose admin_notes with the metadata we have. Owner name is
        # not published; the assessor bots will fill it post-staging.
        notes_parts = ["bot_source=brock_scott_trustee"]
        if case_num:
            notes_parts.append(f"case#={case_num}")
        court_sp = fields.get("court sp #", "").strip()
        if court_sp:
            notes_parts.append(f"court_sp#={court_sp}")
        bid_raw = fields.get("opening bid amount", "").strip()
        bid_clean = self._parse_bid(bid_raw)
        if bid_clean:
            notes_parts.append(f"opening_bid=${bid_clean:,.2f}")
        book_page = fields.get("book page", "").strip()
        if book_page:
            notes_parts.append(f"book_page={book_page}")

        return LeadPayload(
            bot_source="brock_scott_trustee",
            pipeline_lead_key=lead_key,
            property_address=address,
            county=county or None,
            distress_type="TRUSTEE_NOTICE",
            trustee_sale_date=sale_date_iso,
            admin_notes=" · ".join(notes_parts),
            raw_payload={
                "fields": fields,
                "scraped_at": datetime.utcnow().isoformat() + "Z",
            },
            source_url=BS_INDEX + "?_sft_foreclosure_state=tn",
        )

    @staticmethod
    def _parse_sale_date(raw: str) -> Optional[str]:
        """'05/12/2026 - 10:00:00 AM' → '2026-05-12'."""
        if not raw:
            return None
        m = SALE_DATE_RE.search(raw)
        if not m:
            return None
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return None

    @staticmethod
    def _parse_bid(raw: str) -> Optional[float]:
        """'208467.44' or '$208,467.44' → 208467.44."""
        if not raw:
            return None
        m = BID_RE.search(raw)
        if not m:
            return None
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            return None


def run() -> dict:
    return BrockScottTrusteeBot().run()


if __name__ == "__main__":
    print(run())
