"""
USDA Rural Housing Service (RHS) foreclosure scraper.

Source: USDA's Single Family Housing Real Estate Owned Property Search
at properties.sc.egov.usda.gov. These are properties USDA foreclosed
on under their rural housing loan programs — heavy in East TN.

NOTE: this is a v1 best-guess implementation. The site requires a
session cookie + the actual API endpoint shape needs verification via
browser DevTools. Marked staging_status='pending' regardless. If this
bot returns zero_yield for 3 consecutive runs, the framework will alert
and Patrick can debug.

Distress type: REO
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


USDA_BASE = "https://properties.sc.egov.usda.gov"
USDA_LANDING = f"{USDA_BASE}/resales/public/home"
# Likely search endpoint patterns based on agent research; subject to verify
USDA_SEARCH_CANDIDATES = [
    f"{USDA_BASE}/resales/public/propertySearch?state=TN",
    f"{USDA_BASE}/resales/public/searchSFH?state=TN",
    f"{USDA_BASE}/resales/public/api/property/search?state=TN",
    f"{USDA_BASE}/resales/public/getProperties?state=TN",
]


class UsdaRhsBot(BotBase):
    name = "usda_rhs"
    description = "USDA RHS foreclosed properties — TN (rural housing REO)"
    throttle_seconds = 2.0
    expected_min_yield = 5  # USDA RHS is a smaller pool; East TN focused

    def scrape(self) -> List[LeadPayload]:
        if BeautifulSoup is None:
            self.logger.error("beautifulsoup4 not installed")
            return []

        # First, hit the landing to establish session + cookies
        self.logger.info(f"establishing session at {USDA_LANDING}")
        landing = self.fetch(USDA_LANDING)
        if landing is None:
            self.logger.warning("landing page fetch failed; site may be down or relocated")
            return []

        # Try search candidate URLs in order
        leads: List[LeadPayload] = []
        for url in USDA_SEARCH_CANDIDATES:
            self.logger.info(f"trying search endpoint: {url}")
            res = self.fetch(url)
            if res is None or res.status_code != 200:
                continue

            content_type = res.headers.get("Content-Type", "")
            if "json" in content_type.lower():
                # JSON response — parse as data
                try:
                    data = res.json()
                except Exception:
                    continue
                leads.extend(self._parse_json_response(data, url))
                if leads:
                    break
            elif "html" in content_type.lower() or res.text.strip().startswith("<"):
                # HTML — try scraping cards
                soup = BeautifulSoup(res.text, "html.parser")
                leads.extend(self._parse_html_response(soup, url))
                if leads:
                    break

        # Fallback: try the resales main page itself, see if listings are inline
        if not leads:
            self.logger.info("no luck with search endpoints; trying landing page parse")
            soup = BeautifulSoup(landing.text, "html.parser")
            leads.extend(self._parse_html_response(soup, USDA_LANDING))

        self.logger.info(f"total leads built: {len(leads)}")
        return leads

    def _parse_json_response(self, data: dict, source_url: str) -> List[LeadPayload]:
        leads = []
        # Try common JSON shapes: {properties: [...]} or {results: [...]} or [...]
        candidates = (
            data.get("properties")
            if isinstance(data, dict)
            else None
        )
        if candidates is None:
            candidates = data.get("results") if isinstance(data, dict) else None
        if candidates is None and isinstance(data, list):
            candidates = data
        if not candidates:
            return []

        for prop in candidates:
            if not isinstance(prop, dict):
                continue
            address = (
                prop.get("address")
                or prop.get("street_address")
                or prop.get("propertyAddress")
                or ""
            ).strip()
            if not address:
                continue
            city = (prop.get("city") or "").strip()
            zip_code = str(prop.get("zip") or prop.get("postalCode") or "").strip()
            full_addr = address
            if city:
                full_addr = f"{full_addr}, {city}, TN"
                if zip_code:
                    full_addr = f"{full_addr} {zip_code}"
            prop_id = (
                prop.get("propertyId")
                or prop.get("id")
                or prop.get("listingId")
                or address
            )
            list_price = prop.get("listPrice") or prop.get("price")
            if isinstance(list_price, str):
                try:
                    list_price = float(re.sub(r"[^\d.]", "", list_price))
                except Exception:
                    list_price = None
            leads.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=self.make_lead_key(self.name, str(prop_id)),
                property_address=full_addr,
                distress_type="REO",
                property_value=float(list_price) if list_price else None,
                admin_notes=f"USDA RHS · prop_id={prop_id}",
                source_url=source_url,
                raw_payload={"usda_rhs": prop},
            ))
        return leads

    def _parse_html_response(self, soup, source_url: str) -> List[LeadPayload]:
        # Best-effort HTML scrape; structure varies wildly
        leads = []
        # Look for listing-like containers
        candidates = (
            soup.select(".property-card, .listing-card, .property-listing, .reo-listing, tr.listing-row")
            or soup.select("[class*='property'], [class*='listing']")
        )
        for el in candidates[:200]:
            text = el.get_text(" ", strip=True)
            # Look for TN address pattern in text
            m = re.search(r"(\d+\s+[\w\s.]+?(?:St|Ave|Rd|Dr|Ln|Blvd|Ct|Cir|Pl|Way|Hwy|Pkwy|Trl|Trail|Court|Drive|Lane|Boulevard|Avenue|Street|Road|Highway|Parkway))[\s,]+([\w\s]+?),?\s+TN\s+(\d{5})", text, re.IGNORECASE)
            if not m:
                continue
            street, city, zip_code = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            full_addr = f"{street}, {city}, TN {zip_code}"
            link_el = el.find("a", href=True)
            link = urljoin(source_url, link_el["href"]) if link_el else source_url
            leads.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=self.make_lead_key(self.name, full_addr),
                property_address=full_addr,
                distress_type="REO",
                admin_notes=f"USDA RHS · scraped from {source_url}",
                source_url=link,
                raw_payload={"usda_rhs_text": text[:500]},
            ))
        return leads


def run() -> dict:
    bot = UsdaRhsBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
