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


def normalize_county(name: str):
    if not name:
        return None
    name = name.strip()
    if not name.lower().endswith("county"):
        name = f"{name} County"
    return name


def determine_status(dts: int | None):
    if dts is None:
        return None
    if dts <= 6:
        return "URGENT"
    if 7 <= dts <= 13:
        return "HOT"
    if dts >= 14:
        return "GREEN"
    return None


def run():

    print(f"[ForeclosureTNBot] seed={BASE_URL}")

    page = 1
    max_pages = 15

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

    while page <= max_pages:

        url = BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"

        try:
            html = fetch(url)
        except Exception:
            break

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tbody tr")

        if not rows:
            break

        print(f"[ForeclosureTNBot] page={page} rows={len(rows)}")

        for row in rows:

            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) < 6:
                continue

            county_raw = cols[0]
            city = cols[1]
            zip_code = cols[2]
            sale_date_str = cols[3]
            trustee = cols[4]
            continuance_str = cols[5]

            county = normalize_county(county_raw)

            # Geo filter
            if TARGET_COUNTIES and county not in TARGET_COUNTIES:
                skipped_out_of_geo += 1
                continue

            # Parse sale date
            sale_date_iso = None
            if sale_date_str:
                try:
                    dt = datetime.strptime(sale_date_str, "%m/%d/%Y")
                    sale_date_iso = dt.date().isoformat()
                except Exception:
                    pass

            # Override with continuance if present
            if continuance_str:
                try:
                    dt = datetime.strptime(continuance_str, "%m/%d/%Y")
                    sale_date_iso = dt.date().isoformat()
                except Exception:
                    pass

            if not sale_date_iso:
                skipped_no_date += 1
                continue

            dts = days_to_sale(sale_date_iso)

            if dts is None:
                skipped_no_date += 1
                continue

            if dts < 0:
                skipped_expired += 1
                continue

            status = determine_status(dts)

            if not status:
                skipped_kill += 1
                continue

            if status == "GREEN":
                green += 1
            elif status == "HOT":
                hot += 1
            elif status == "URGENT":
                urgent += 1
            elif status == "MONITOR":
                monitor += 1

            distress_type = "Trustee Sale"

            title = f"{distress_type} ({status}) ({county})"

            listing_url = BASE_URL

            address = f"{city} TN {zip_code}"

            lead_key = make_lead_key(
                distress_type,
                county,
                sale_date_iso,
                address,
                trustee,
                listing_url,
            )

            props = build_properties(
                title=title,
                source="ForeclosureTennessee",
                distress_type=distress_type,
                county=county,
                address=address,
                sale_date_iso=sale_date_iso,
                trustee_attorney=trustee,
                contact_info=trustee or "",
                raw_snippet=f"City={city} Zip={zip_code}",
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

        page += 1

    print(
        f"[ForeclosureTNBot] summary "
        f"total_written={total_written} "
        f"green={green} hot={hot} urgent={urgent} monitor={monitor} "
        f"created={created} updated={updated} "
        f"skipped_out_of_geo={skipped_out_of_geo} "
        f"skipped_no_date={skipped_no_date} "
        f"skipped_expired={skipped_expired} "
        f"skipped_kill={skipped_kill}"
    )

    print("[ForeclosureTNBot] Done.")
