# src/bots/foreclosure_tennessee_bot.py

import re
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
MAX_PAGES_CAP = 25  # safety cap


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


def _falco_score_from_status(dts: int | None, status: str | None) -> int:
    """
    Simple, stable score for this source:
    - URGENT: 90–100
    - HOT:    75–89
    - GREEN:  55–74 (closer = higher)
    """
    if dts is None or not status:
        return 0

    if status == "URGENT":
        # dts 0..6 => 100..90
        return max(90, min(100, 100 - (dts * 2)))
    if status == "HOT":
        # dts 7..13 => 89..77
        return max(75, min(89, 89 - ((dts - 7) * 2)))
    if status == "GREEN":
        # dts 14..60+ => 74 downwards, cap at 55
        return max(55, min(74, 74 - int((max(14, dts) - 14) / 2)))
    return 0


def _extract_total_pages(soup: BeautifulSoup) -> int:
    text = soup.get_text(" ", strip=True)
    m = re.search(r"in\s*(\d+)\s*pages", text, flags=re.IGNORECASE)
    if m:
        try:
            return max(1, int(m.group(1)))
        except Exception:
            return 1
    return 1


def _page_url(page: int) -> str:
    return BASE_URL if page == 1 else f"{BASE_URL}page/{page}/"


def run():
    print(f"[ForeclosureTNBot] seed={BASE_URL}")

    try:
        html1 = fetch(_page_url(1))
    except Exception as e:
        print("[ForeclosureTNBot] fetch failed:", e)
        return

    soup1 = BeautifulSoup(html1, "html.parser")
    total_pages = min(_extract_total_pages(soup1), MAX_PAGES_CAP)
    print(f"[ForeclosureTNBot] detected_pages={total_pages}")

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

    for page in range(1, total_pages + 1):
        url = _page_url(page)

        try:
            html = html1 if page == 1 else fetch(url)
        except Exception as e:
            print(f"[ForeclosureTNBot] fetch failed page={page} url={url}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tbody tr")
        print(f"[ForeclosureTNBot] page={page} rows={len(rows)}")

        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]

            if len(cols) < 8:
                skipped_bad_row += 1
                continue

            if len(cols) == 1 and "items in" in cols[0].lower():
                skipped_bad_row += 1
                continue

            # Verified column order:
            sale_date_str = cols[0]
            cont_date_str = cols[1]
            city = cols[2]
            address = cols[3]
            zip_code = cols[4]
            county_raw = cols[5]
            firm_trustee = cols[6]

            county = _normalize_county(county_raw)

            if TARGET_COUNTIES and county not in TARGET_COUNTIES:
                skipped_out_of_geo += 1
                continue

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

            a = row.select_one('a[href*="Foreclosure-Listing"]')
            if not a or not a.get("href"):
                skipped_no_link += 1
                continue
            listing_url = urljoin(BASE_URL, a["href"])

            distress_type = "Foreclosure"
            title = f"{distress_type} ({status}) ({county_raw})"
            address_full = f"{address}, {city}, TN {zip_code}"

            lead_key = make_lead_key(
                "FORECLOSURETN",
                listing_url,
                county,
                sale_date_iso,
                address_full,
            )

            existing_id = find_existing_by_lead_key(lead_key)

            # ✅ Score ONLY on create; NEVER overwrite on update
            score_for_create = _falco_score_from_status(dts, status)
            score_for_update = None

            props = build_properties(
                title=title,
                source="ForeclosureTennessee",
                distress_type=distress_type,
                county=county,
                address=address_full,
                sale_date_iso=sale_date_iso,
                trustee_attorney=firm_trustee,
                contact_info=firm_trustee,
                raw_snippet=f"orig_sale={sale_date_str} cont={cont_date_str} page={page}",
                url=listing_url,
                score=(score_for_update if existing_id else score_for_create),
                status=status,
                lead_key=lead_key,
                days_to_sale_num=dts,
            )

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
