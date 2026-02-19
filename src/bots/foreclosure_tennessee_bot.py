import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..utils import fetch, make_lead_key
from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale
from ..settings import (
    get_dts_window,
    is_allowed_county,
    within_target_counties,
    normalize_county_full,
)

BASE_URL = "https://foreclosuretennessee.com/"
MAX_PAGES_CAP = 25

_DTS_MIN, _DTS_MAX = get_dts_window("FORECLOSURE_TN")


def _parse_date_flex(s: str):
    if not s:
        return None
    s = s.strip()
    fmts = ["%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _extract_rows(soup: BeautifulSoup):
    return soup.select("table tbody tr")


def run():
    print(
        f"[ForeclosureTNBot] seed={BASE_URL} "
        f"dts_window=[{_DTS_MIN},{_DTS_MAX}]"
    )

    html = fetch(BASE_URL)
    if not html:
        print("[ForeclosureTNBot] fetch failed")
        return

    soup = BeautifulSoup(html, "html.parser")

    fetched_rows = 0
    parsed_rows = 0
    filtered_in = 0
    created = 0
    updated = 0

    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_no_date = 0
    skipped_expired = 0
    skipped_dup_in_run = 0
    skipped_bad_row = 0
    skipped_no_link = 0

    seen_in_run = set()
    sample_kept = []

    rows = _extract_rows(soup)
    fetched_rows = len(rows)

    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) < 8:
            skipped_bad_row += 1
            continue

        parsed_rows += 1

        sale_date_str = cols[0]
        cont_date_str = cols[1]
        city = cols[2]
        address = cols[3]
        zip_code = cols[4]
        county_raw = cols[5]
        trustee = cols[6]

        county_full = normalize_county_full(county_raw)

        if not is_allowed_county(county_full):
            skipped_out_of_geo += 1
            continue

        if not within_target_counties(county_full):
            skipped_out_of_geo += 1
            continue

        sale_date_iso = _parse_date_flex(cont_date_str) or _parse_date_flex(
            sale_date_str
        )
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

        if not (_DTS_MIN <= dts <= _DTS_MAX):
            skipped_outside_window += 1
            continue

        address_full = f"{address}, {city}, TN {zip_code}"

        a = row.select_one('a[href*="Foreclosure-Listing"]')
        if not a or not a.get("href"):
            skipped_no_link += 1
            continue

        listing_url = urljoin(BASE_URL, a["href"])

        lead_key = make_lead_key(
            "FORECLOSURE_TN",
            listing_url,
            county_full,
            sale_date_iso,
            address_full,
        )

        if lead_key in seen_in_run:
            skipped_dup_in_run += 1
            continue

        seen_in_run.add(lead_key)

        payload = {
            "title": address_full,
            "source": "ForeclosureTennessee",
            "distress_type": "Foreclosure",
            "county": county_full,
            "address": address_full,
            "sale_date_iso": sale_date_iso,
            "trustee_attorney": trustee,
            "contact_info": trustee,
            "raw_snippet": f"sale={sale_date_str} cont={cont_date_str}",
            "url": listing_url,
            "lead_key": lead_key,
            "days_to_sale": dts,
        }

        props = build_properties(payload)

        existing = find_existing_by_lead_key(lead_key)
        if existing:
            update_lead(existing, props)
            updated += 1
        else:
            create_lead(props)
            created += 1

        filtered_in += 1

        if len(sample_kept) < 5:
            sample_kept.append(
                f"county={county_full} sale={sale_date_iso} dts={dts} addr={address_full}"
            )

    print(
        "[ForeclosureTNBot] summary "
        f"fetched_rows={fetched_rows} parsed_rows={parsed_rows} "
        f"filtered_in={filtered_in} created={created} updated={updated} "
        f"skipped_out_of_geo={skipped_out_of_geo} "
        f"skipped_outside_window={skipped_outside_window} "
        f"skipped_no_date={skipped_no_date} skipped_expired={skipped_expired} "
        f"skipped_bad_row={skipped_bad_row} skipped_no_link={skipped_no_link} "
        f"skipped_dup_in_run={skipped_dup_in_run} "
        f"sample_kept={sample_kept}"
    )
    print("[ForeclosureTNBot] Done.")
