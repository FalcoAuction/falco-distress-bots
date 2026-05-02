"""
Craigslist Tennessee — real estate by owner scraper.

Source: each major TN metro has its own Craigslist subdomain. Public
HTML, server-rendered, no auth, no JS required.

Why this is a great lead source: people posting their house on
Craigslist are explicitly motivated to sell themselves (avoiding
agent commission). Often distressed financial situations or older
owners who don't want to deal with traditional listings.

Distress type: FSBO (For Sale By Owner)
"""

from __future__ import annotations

import re
from typing import List, Optional
from urllib.parse import urljoin

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

from ._base import BotBase, LeadPayload


# Craigslist regional subdomains for Tennessee. RFS = real estate, FSBO.
TN_REGIONS = [
    ("nashville", "Davidson"),
    ("memphis", "Shelby"),
    ("knoxville", "Knox"),
    ("chattanooga", "Hamilton"),
    ("tricities", "Sullivan/Washington/Carter"),
    ("clarksville", "Montgomery"),
    ("jackson", "Madison"),
    ("cookeville", "Putnam"),
]

# Path: real-estate-by-owner section
REO_PATH = "/d/real-estate-by-owner/search/reo"


class CraigslistTnBot(BotBase):
    name = "craigslist_tn"
    description = "Craigslist Tennessee real estate by-owner (FSBO) across major metros"
    throttle_seconds = 2.0  # craigslist is sensitive to fast-fetching
    expected_min_yield = 20

    def scrape(self) -> List[LeadPayload]:
        if BeautifulSoup is None:
            self.logger.error("beautifulsoup4 not installed; pip install beautifulsoup4")
            return []

        leads: List[LeadPayload] = []
        for region, county in TN_REGIONS:
            base = f"https://{region}.craigslist.org"
            url = base + REO_PATH
            self.logger.info(f"fetching {region}")
            res = self.fetch(url)
            if res is None or res.status_code != 200:
                self.logger.warning(f"  {region}: failed {res.status_code if res else 'no-response'}")
                continue
            soup = BeautifulSoup(res.text, "html.parser")

            # Craigslist's listing structure varies; try multiple selectors
            cards = (
                soup.select("li.cl-static-search-result")  # static-render style
                or soup.select("li.result-row")             # legacy
                or soup.select("a.cl-app-anchor")           # newer SPA fallback
            )
            if not cards:
                # Fallback: any anchor pointing to a posting (URL pattern .../d/...)
                cards = [a for a in soup.select("a") if a.get("href") and "/d/" in a["href"]]

            self.logger.info(f"  {region}: {len(cards)} cards found")

            for card in cards:
                lead = self._parse_card(card, base, region, county)
                if lead is not None:
                    leads.append(lead)

        self.logger.info(f"total leads: {len(leads)}")
        return leads

    def _parse_card(self, card, base_url: str, region: str, county: str) -> Optional[LeadPayload]:
        # Title anchor
        a = card if card.name == "a" else card.find("a")
        if not a or not a.get("href"):
            return None
        link = urljoin(base_url, a["href"])
        # Listing IDs are usually a numeric segment in the URL
        m = re.search(r"/(\d{8,})\.html", link)
        if not m:
            return None
        listing_id = m.group(1)

        title = a.get_text(strip=True) or ""
        if not title:
            return None

        # Price extraction (best-effort across structures)
        price_str = ""
        price_el = card.select_one(".result-price, .price, span.priceinfo")
        if price_el:
            price_str = price_el.get_text(strip=True)
        else:
            # Sometimes inline in the title text
            pm = re.search(r"\$([\d,]+)", title)
            if pm:
                price_str = "$" + pm.group(1)
        property_value: Optional[float] = None
        if price_str:
            digits = re.sub(r"[^\d]", "", price_str)
            if digits:
                try:
                    property_value = float(digits)
                except ValueError:
                    pass

        # Location hint
        loc_el = card.select_one(".result-hood, .hood, .nearby")
        location_hint = loc_el.get_text(strip=True).strip("()") if loc_el else region.title()

        # Constructed pseudo-address (real address often only in the
        # detail page, which we'd need a separate fetch for)
        address = f"{title[:100]} ({location_hint}, TN)"

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, listing_id),
            property_address=address,
            county=f"{county} County" if county and "/" not in county else None,
            distress_type="FSBO",
            property_value=property_value,
            admin_notes=f"craigslist {region} · listing {listing_id}{' · ' + price_str if price_str else ''}",
            source_url=link,
            raw_payload={"craigslist_region": region, "title": title, "price": price_str},
        )


def run() -> dict:
    bot = CraigslistTnBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
