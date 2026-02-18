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
    if dts is None or not status:
        return 0
    if status == "URGENT":
        return max(90, min(100, 100 - (dts * 2)))
    if status == "HOT":
        return max(75, min(89, 89 - ((dts - 7) * 2)))
    if status == "GREEN":
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


def _extract_rows(soup: BeautifulSoup):
    return soup.select("table tbody tr")


def _has_real_rows(soup: BeautifulSoup) -> bool:
    rows = _extract_rows(soup)
    if not rows:
        return False
    # ignore pagination junk row if present
    for r in rows:
        cols = [c.get_text(strip=True) for c in r.find_all("td")]
        if len(cols) >= 8:
            return True
    return False


def _get_page_html(url: str) -> str | None:
    try:
        return fetch(url)
    except Exception:
        return None


def _candidate_page_urls(page_num: int, page_size: int = 20):
    """
    Common non-WP paging patterns. We'll probe these and pick the first that works.
    page_num is 1-indexed.
    """
    # DataTables style (start/length)
    start = (page_num - 1) * page_size
    return [
        # Query param paging
        f"{BASE_URL}?page={page_num}",
        f"{BASE_URL}?paged={page_num}",
        f"{BASE_URL}?pg={page_num}",
        # Offset paging
        f"{BASE_URL}?start={start}&length={page_size}",
        f"{BASE_URL}?offset={start}&limit={page_size}",
        # Another common pattern
        f"{BASE_URL}?p={page_num}",
    ]


def _detect_paging_url_builder(first_page_soup: BeautifulSoup, total_pages: int):
    """
    Try to find a working paging scheme by probing page 2.
    Returns a function(page_num)->url, or None if no paging supported.
    """
    if total_pages <= 1:
        return lambda n: BASE_URL

    # Probe for the page size from the "Page size" select if present, else assume 20
    page_size = 20
    # not strictly needed; keep simple

    probe_urls = _candidate_page_urls(2, page_size=page_size)

    for u in probe_urls:
        html = _get_page_html(u)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        if _has_real_rows(soup):
            print(f"[ForeclosureTNBot] pagination_detected url_pattern_example={u}")
            # Determine which pattern matched and build closure
            if "?page=" in u:
                return lambda n: f"{BASE_URL}?page={n}"
            if "?paged=" in u:
                return lambda n: f"{BASE_URL}?paged={n}"
            if "?pg=" in u:
                return lambda n: f"{BASE_URL}?pg={n}"
            if "?start=" in u and "&length=" in u:
                return lambda n: f"{BASE_URL}?start={(n-1)*page_size}&length={page_size}"
            if "?offset=" in u and "&limit=" in u:
                return lambda n: f"{BASE_URL}?offset={(n-1)*page_size}&limit={page_size}"
            if "?p=" in u:
                return lambda n: f"{BASE_URL}?p={n}"

    # If nothing worked, site may only show first page without server-side paging
    return None


def run():
    print(f"[ForeclosureTNBot] seed={BASE_URL}")

    html1 = _get_page_html(BASE_URL)
    if not html1:
        print("[ForeclosureTNBot] fetch failed on base url")
        return

    soup1 = BeautifulSoup(html1, "html.parser")
    total_pages = min(_extract_total_pages(soup1), MAX_PAGES_CAP)
    print(f"[ForeclosureTNBot] detected_pages={total_pages}")

    url_builder = _detect_paging_url_builder(soup1, total_pages)
    if url_builder is None:
        print("[ForeclosureTNBot] pagination_not_supported_server_side -> processing only page 1")
        url_builder = lambda n: BASE_URL
        total_pages = 1

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
        url = url_builder(page)

        html = html1 if page == 1 and url == BASE_URL else _get_page_html(url)
        if not html:
            print(f"[ForeclosureTNBot] fetch failed page={page} url={url}")
            break

        soup = BeautifulSoup(html, "html.parser")
        rows = _extract_rows(soup)
        print(f"[ForeclosureTNBot] page={page} rows={len(rows)} url={url}")

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

            score_for_create = _falco_score_from_status(dts, status)

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
                score=(None if existing_id else score_for_create),  # ✅ create-only score
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
