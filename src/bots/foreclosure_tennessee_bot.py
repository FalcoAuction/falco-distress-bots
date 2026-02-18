# src/bots/foreclosure_tennessee_bot.py

from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

from ..config import TARGET_COUNTIES
from ..utils import fetch, make_lead_key
from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale


BASE_URL = "https://foreclosuretennessee.com/"


def _parse_date_flex(s: str):
    if not s:
        return None
    s = s.strip()
    if not s or s.lower() in {"tbd", "unknown", "n/a", "-"}:
        return None

    fmts = ["%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _normalize_county(name: str):
    if not name:
        return None
    n = name.strip()
    # Site returns "Davidson" etc.
    if n.lower().endswith("county"):
        return n
    return f"{n} County"


def _status_from_dts(dts: int | None):
    if dts is None:
        return None
    if dts < 0:
        return "EXPIRED"
    if dts <= 6:
        return "URGENT"
    if 7 <= dts <= 13:
        return "HOT"
    if dts >= 14:
        return "GREEN"
    return None


def run():
    print(f"[ForeclosureTNBot] seed={BASE_URL}")

    try:
        html = fetch(BASE_URL)
    except Exception as e:
        print("[ForeclosureTNBot] fetch failed:", e)
        return

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    print(f"[ForeclosureTNBot] rows_found={len(rows)}")

    total_written = 0
    green = 0
    hot = 0
    urgent = 0
    monitor = 0

    created = 0
    updated = 0

    skipped_out_of_geo = 0
    skipped_no_date = 0
    skipped_expired = 0
    skipped_kill = 0
    skipped_bad_row = 0
    skipped_no_link = 0

    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]

        # Expect at least 9 columns per your debug output
        # [Sale Date, Continuance Date, City, Address, Zip, County, Firm/Trustee, Listing Link, Hidden]
        if len(cols) < 8:
            skipped_bad_row += 1
            continue

        # Row 0 is pagination garbage (has one merged cell)
        if len(cols) == 1 and "items in" in cols[0].lower():
            skipped_bad_row += 1
            continue

        sale_date_str = cols[0]
        cont_date_str = cols[1]
        city = cols[2]
        address = cols[3]
        zip_code = cols[4]
        county_raw = cols[5]
        firm_trustee = cols[6]

        county = _normalize_county(county_raw)

        # Statewide mode if TARGET_COUNTIES empty
        if TARGET_COUNTIES and county not in TARGET_COUNTIES:
            skipped_out_of_geo += 1
            continue

        # Use continuance date if present; else sale date
        sale_date_iso = _parse_date_flex(cont_date_str) or _parse_date_flex(sale_date_str)
        if not sale_date_iso:
            skipped_no_date += 1
            continue

        dts = days_to_sale(sale_date_iso)
        if dts is not None and dts < 0:
            skipped_expired += 1
            continue

        status = _status_from_dts(dts)
        if status in (None, "EXPIRED"):
            skipped_kill += 1
            continue

        if status == "GREEN":
            green += 1
        elif status == "HOT":
            hot += 1
        elif status == "URGENT":
            urgent += 1
        else:
            monitor += 1

        # Pull actual listing URL from the anchor tag
        a = row.select_one('a[href*="Foreclosure-Listing"]')
        if not a or not a.get("href"):
            skipped_no_link += 1
            continue
        listing_url = urljoin(BASE_URL, a["href"])

        distress_type = "Foreclosure"
        title = f"{distress_type} ({status}) ({county_raw})"
        address_full = f"{address}, {city}, TN {zip_code}"

        # Stable Lead Key: based on listing url + county + date + address
        lead_key = make_lead_key(
            "FORECLOSURETN",
            listing_url,
            county,
            sale_date_iso,
            address_full,
        )

        props = build_properties(
            title=title,
            source="ForeclosureTennessee",
            distress_type=distress_type,
            county=county,  # Notion select expects "Davidson County" etc
            address=address_full,
            sale_date_iso=sale_date_iso,
            trustee_attorney=firm_trustee,
            contact_info=firm_trustee,
            raw_snippet=f"orig_sale={sale_date_str} cont={cont_date_str}",
            url=listing_url,
            score=0,
            status=status,
            lead_key=lead_key,
            days_to_sale_num=dts,
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
        "[ForeclosureTNBot] summary "
        f"total_written={total_written} green={green} hot={hot} urgent={urgent} monitor={monitor} "
        f"created={created} updated={updated} "
        f"skipped_out_of_geo={skipped_out_of_geo} skipped_no_date={skipped_no_date} "
        f"skipped_expired={skipped_expired} skipped_kill={skipped_kill} "
        f"skipped_bad_row={skipped_bad_row} skipped_no_link={skipped_no_link}"
    )
    print("[ForeclosureTNBot] Done.")
