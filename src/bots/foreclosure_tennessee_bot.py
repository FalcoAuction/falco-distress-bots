import os
import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..config import (
    TARGET_COUNTIES,
    ALLOWED_COUNTIES_BASE,
    DTS_WINDOW_MIN,
    DTS_WINDOW_MAX,
)
from ..notion_client import build_properties, create_lead, find_existing_by_lead_key, update_lead
from ..scoring import days_to_sale
from ..utils import fetch, make_lead_key

BASE_URL = "https://foreclosuretennessee.com/"
MAX_PAGES_CAP = 25


def _env_csv(name: str) -> set[str]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return set()
    return {c.strip() for c in raw.split(",") if c.strip()}


def _effective_allowed_counties() -> set[str]:
    env = _env_csv("FALCO_ALLOWED_COUNTIES")
    if env:
        return env
    return {c.strip() for c in (ALLOWED_COUNTIES_BASE or []) if c and c.strip()}


def _effective_dts_window() -> tuple[int, int]:
    mn = os.getenv("FALCO_DTS_MIN")
    mx = os.getenv("FALCO_DTS_MAX")
    if mn and mx:
        try:
            return int(mn), int(mx)
        except Exception:
            pass
    return int(DTS_WINDOW_MIN), int(DTS_WINDOW_MAX)


_ALLOWED_COUNTIES_BASE = _effective_allowed_counties()
_DTS_MIN, _DTS_MAX = _effective_dts_window()


def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = " ".join(name.strip().split())
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n


def _is_allowed_county(county: str | None) -> bool:
    base = _county_base(county)
    if not base:
        return False
    if not _ALLOWED_COUNTIES_BASE:
        return True
    return base in _ALLOWED_COUNTIES_BASE


def _clean(s: str | None) -> str:
    if not s:
        return ""
    s = s.replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split())
    return s.strip()


def _normalize_city(city_raw: str) -> str:
    s = _clean(city_raw)
    if not s:
        return ""
    s = re.sub(r",\s*TN\b.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r",\s*Tennessee\b.*$", "", s, flags=re.IGNORECASE).strip()
    return s


def _address_has_state(addr: str) -> bool:
    s = _clean(addr).lower()
    if not s:
        return False
    return bool(re.search(r"\bTN\b", addr)) or (" tennessee" in s)


def _address_has_zip(addr: str) -> bool:
    s = _clean(addr)
    return bool(re.search(r"\b\d{5}(?:-\d{4})?\b", s))


def _normalize_state_tokens(addr: str) -> str:
    s = _clean(addr)
    if not s:
        return s
    # Normalize ", Tennessee" -> ", TN"
    s = re.sub(r",\s*Tennessee\b", ", TN", s, flags=re.IGNORECASE)
    return _clean(s)


def _compose_address(address_raw: str, city_raw: str, zip_raw: str) -> str:
    address = _normalize_state_tokens(address_raw)
    zip_code = _clean(zip_raw)

    # If address already contains state (TN/Tennessee), do NOT append city again.
    if _address_has_state(address):
        if zip_code and not _address_has_zip(address):
            # Add zip to the end (ensure one space before zip)
            return _clean(f"{address} {zip_code}")
        return address

    # Otherwise build "address, city, TN zip"
    city = _normalize_city(city_raw)
    parts_left = [p for p in [address, city] if p]
    left = ", ".join(parts_left) if parts_left else ""
    right_parts = [p for p in ["TN", zip_code] if p]
    right = " ".join(right_parts) if right_parts else ""

    if left and right:
        return _clean(f"{left}, {right}")
    return _clean(left or right)


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
    start = (page_num - 1) * page_size
    return [
        f"{BASE_URL}?page={page_num}",
        f"{BASE_URL}?paged={page_num}",
        f"{BASE_URL}?pg={page_num}",
        f"{BASE_URL}?start={start}&length={page_size}",
        f"{BASE_URL}?offset={start}&limit={page_size}",
        f"{BASE_URL}?p={page_num}",
    ]


def _detect_paging_url_builder(total_pages: int):
    if total_pages <= 1:
        return lambda n: BASE_URL

    page_size = 20
    probe_urls = _candidate_page_urls(2, page_size=page_size)

    for u in probe_urls:
        html = _get_page_html(u)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        if _has_real_rows(soup):
            print(f"[ForeclosureTNBot] pagination_detected url_pattern_example={u}")
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

    return None


def run():
    print(
        f"[ForeclosureTNBot] seed={BASE_URL} "
        f"allowed_counties={sorted(_ALLOWED_COUNTIES_BASE)} dts_window=[{_DTS_MIN},{_DTS_MAX}]"
    )

    html1 = _get_page_html(BASE_URL)
    if not html1:
        print("[ForeclosureTNBot] fetch failed on base url")
        return

    soup1 = BeautifulSoup(html1, "html.parser")
    total_pages = min(_extract_total_pages(soup1), MAX_PAGES_CAP)
    print(f"[ForeclosureTNBot] detected_pages={total_pages}")

    url_builder = _detect_paging_url_builder(total_pages)
    if url_builder is None:
        print("[ForeclosureTNBot] pagination_not_supported_server_side -> processing only page 1")
        url_builder = lambda n: BASE_URL
        total_pages = 1

    fetched_rows = 0
    parsed_rows = 0
    filtered_in = 0
    created = 0
    updated = 0

    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_no_date = 0
    skipped_expired = 0
    skipped_kill = 0
    skipped_bad_row = 0
    skipped_no_link = 0
    skipped_dup_in_run = 0

    sample_kept: list[str] = []
    seen_lead_keys: set[str] = set()

    for page in range(1, total_pages + 1):
        url = url_builder(page)
        html = html1 if page == 1 and url == BASE_URL else _get_page_html(url)
        if not html:
            print(f"[ForeclosureTNBot] fetch failed page={page} url={url}")
            break

        soup = BeautifulSoup(html, "html.parser")
        rows = _extract_rows(soup)
        fetched_rows += len(rows)
        print(f"[ForeclosureTNBot] page={page} rows={len(rows)} url={url}")

        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]

            if len(cols) < 8:
                skipped_bad_row += 1
                continue
            if len(cols) == 1 and "items in" in cols[0].lower():
                skipped_bad_row += 1
                continue

            parsed_rows += 1

            sale_date_str = cols[0]
            cont_date_str = cols[1]
            city_raw = cols[2]
            address_raw = cols[3]
            zip_raw = cols[4]
            county_raw = _clean(cols[5])
            firm_trustee = _clean(cols[6])

            county = _normalize_county(county_raw)

            if not _is_allowed_county(county):
                skipped_out_of_geo += 1
                continue
            if TARGET_COUNTIES and county not in TARGET_COUNTIES:
                skipped_out_of_geo += 1
                continue

            sale_date_iso = _parse_date_flex(cont_date_str) or _parse_date_flex(sale_date_str)
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

            status = _status_from_dts(dts)
            if status in (None, "EXPIRED"):
                skipped_kill += 1
                continue

            a = row.select_one('a[href*="Foreclosure-Listing"]')
            if not a or not a.get("href"):
                skipped_no_link += 1
                continue
            listing_url = urljoin(BASE_URL, a["href"])

            distress_type = "Foreclosure"
            address_full = _compose_address(address_raw, city_raw, zip_raw)
            title = f"{distress_type} ({status}) ({county_raw})"

            lead_key = make_lead_key(
                "FORECLOSURETN",
                listing_url,
                county,
                sale_date_iso,
                address_full,
            )

            if lead_key in seen_lead_keys:
                skipped_dup_in_run += 1
                continue
            seen_lead_keys.add(lead_key)

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
                raw_snippet=_clean(f"orig_sale={sale_date_str} cont={cont_date_str} page={page}"),
                url=listing_url,
                score=(None if existing_id else score_for_create),
                status=status,
                lead_key=lead_key,
                days_to_sale=dts,
            )

            if existing_id:
                update_lead(existing_id, props)
                updated += 1
            else:
                create_lead(props)
                created += 1

            filtered_in += 1

            if len(sample_kept) < 5:
                sample_kept.append(f"county={county_raw} sale={sale_date_iso} dts={dts} addr={address_full}")

    print(
        "[ForeclosureTNBot] summary "
        f"fetched_rows={fetched_rows} parsed_rows={parsed_rows} filtered_in={filtered_in} "
        f"created={created} updated={updated} "
        f"skipped_out_of_geo={skipped_out_of_geo} skipped_outside_window={skipped_outside_window} "
        f"skipped_no_date={skipped_no_date} skipped_expired={skipped_expired} "
        f"skipped_kill={skipped_kill} skipped_bad_row={skipped_bad_row} skipped_no_link={skipped_no_link} "
        f"skipped_dup_in_run={skipped_dup_in_run} "
        f"sample_kept={sample_kept}"
    )
    print("[ForeclosureTNBot] Done.")
