# src/bots/tn_foreclosure_notices_bot.py

import re
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

from ..utils import fetch, make_lead_key
from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale, score_v2, label

BASE_URL = "https://tnforeclosurenotices.com/"
SEARCH_URL = urljoin(BASE_URL, "search/")

MAX_COUNTIES_CAP = 150  # safety cap


def _parse_date_flex(s: str):
    if not s:
        return None
    s = s.strip()
    for fmt in ["%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"]:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except:
            continue
    return None


def _extract_county_links(html):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        if "/results/counties/" in a["href"]:
            full = urljoin(BASE_URL, a["href"])
            links.append(full)
    return list(set(links))


def _parse_county_page(county_url):
    html = fetch(county_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    leads = []

    blocks = soup.find_all(text=re.compile("Sale Date", re.I))

    for block in blocks:
        parent = block.find_parent()
        if not parent:
            continue

        text = parent.get_text(" ", strip=True)

        # Address
        address_match = re.search(r"\d{1,6}\s.+?(?=Sale Date|Original)", text)
        address = address_match.group(0).strip() if address_match else None

        # Sale date
        date_match = re.search(r"Sale Date[:\s]+([A-Za-z0-9,\/\s]+)", text)
        sale_date = _parse_date_flex(date_match.group(1)) if date_match else None

        # Trustee/Firm
        firm_match = re.search(r"(Trustee|Firm|Attorney)[:\s]+(.+?)(?=Sale|Location|$)", text, re.I)
        firm = firm_match.group(2).strip() if firm_match else None

        if not sale_date or not address:
            continue

        leads.append(
            {
                "address": address,
                "sale_date": sale_date,
                "firm": firm,
                "county_url": county_url,
                "raw_text": text,
            }
        )

    return leads


def run():
    print("TNForeclosureNoticeBot starting...")

    search_html = fetch(SEARCH_URL)
    if not search_html:
        print("Failed to fetch search page.")
        return

    county_links = _extract_county_links(search_html)
    county_links = county_links[:MAX_COUNTIES_CAP]

    total_written = 0
    created = 0
    updated = 0
    skipped_expired = 0

    for county_url in county_links:
        county_name = county_url.rstrip("/").split("/")[-1].replace("-", " ").title()

        leads = _parse_county_page(county_url)

        for lead in leads:
            dts = days_to_sale(lead["sale_date"])
            if dts is None or dts < 0:
                skipped_expired += 1
                continue

            falco_score = score_v2(dts, flags=[])
            status = label(falco_score)

            lead_key = make_lead_key(
                distress_type="Foreclosure",
                county=county_name,
                sale_date=lead["sale_date"],
                address=lead["address"],
                trustee=lead["firm"],
                notice_url=county_url,
            )

            props = build_properties(
                property_name=lead["address"],
                source="TNForeclosureNotices",
                county=county_name,
                distress_type="Foreclosure",
                address=lead["address"],
                sale_date=lead["sale_date"],
                trustee=lead["firm"],
                status=status,
                falco_score=falco_score,
                raw_snippet=lead["raw_text"],
                url=county_url,
                lead_key=lead_key,
                days_to_sale=dts,
            )

            existing_id = find_existing_by_lead_key(lead_key)

            if existing_id:
                update_lead(existing_id, props)
                updated += 1
            else:
                create_lead(props)
                created += 1

            total_written += 1

    print(
        f"TNForeclosureNoticeBot complete: "
        f"total_written={total_written} "
        f"created={created} "
        f"updated={updated} "
        f"skipped_expired={skipped_expired}"
    )
