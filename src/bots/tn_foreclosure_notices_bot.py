# src/bots/tn_foreclosure_notices_bot.py

import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale, score_v2, label
from ..utils import make_lead_key

BASE_URL = "https://tnforeclosurenotices.com/"
COUNTY_URL_FMT = urljoin(BASE_URL, "results/counties/{slug}/")

COUNTY_NAMES = [
    "Anderson","Bedford","Benton","Bledsoe","Blount","Bradley","Campbell","Cannon","Carroll","Carter",
    "Cheatham","Chester","Claiborne","Clay","Cocke","Coffee","Crockett","Cumberland","Davidson","Decatur",
    "DeKalb","Dickson","Dyer","Fayette","Fentress","Franklin","Gibson","Giles","Grainger","Greene",
    "Grundy","Hamblen","Hamilton","Hancock","Hardeman","Hardin","Hawkins","Haywood","Henderson","Henry",
    "Hickman","Houston","Humphreys","Jackson","Jefferson","Johnson","Knox","Lake","Lauderdale","Lawrence",
    "Lewis","Lincoln","Loudon","Macon","Madison","Marion","Marshall","Maury","McMinn","McNairy",
    "Meigs","Monroe","Montgomery","Moore","Morgan","Obion","Overton","Perry","Pickett","Polk",
    "Putnam","Rhea","Roane","Robertson","Rutherford","Scott","Sequatchie","Sevier","Shelby","Smith",
    "Stewart","Sullivan","Sumner","Tipton","Trousdale","Unicoi","Union","Van Buren","Warren","Washington",
    "Wayne","Weakley","White","Williamson","Wilson",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _slugify_county(name: str) -> str:
    s = name.strip().lower()
    s = s.replace(".", "")
    s = re.sub(r"\s+", "-", s)
    return s


def _get(url: str, session: requests.Session, timeout: int = 25):
    try:
        r = session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, r.text
    except Exception:
        return None, None


def _parse_date_tnfn(s: str):
    if not s:
        return None
    s = s.strip().strip("()")
    for fmt in ("%a %d, %b %Y", "%a %d, %B %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _extract_field(text: str, start_label: str, end_label: str | None):
    start_idx = text.find(start_label)
    if start_idx == -1:
        return None
    start_idx += len(start_label)
    if end_label:
        end_idx = text.find(end_label, start_idx)
        if end_idx == -1:
            return text[start_idx:].strip()
        return text[start_idx:end_idx].strip()
    return text[start_idx:].strip()


def _parse_notice_line(line: str):
    if "Sale Notice:" not in line or "TNFN#" not in line:
        return None

    notice_id = None
    m = re.search(r"(TNFN#\d+)", line)
    if m:
        notice_id = m.group(1)

    county = _extract_field(line, "County:", "Original Sale Date:")
    orig_sale_raw = _extract_field(line, "Original Sale Date:", "Address:")
    address = _extract_field(line, "Address:", "Firm:")

    firm = _extract_field(line, "Firm:", "PP Sale Date:")
    if firm is None:
        firm = _extract_field(line, "Firm:", "Current Sale Date:")
    if firm is None:
        firm = _extract_field(line, "Firm:", "Sale Location:")

    pp_sale_raw = _extract_field(line, "PP Sale Date:", "Sale Location:")
    curr_sale_raw = _extract_field(line, "Current Sale Date:", "Sale Location:")

    sale_location = _extract_field(line, "Sale Location:", "Sale Time:")
    sale_time = _extract_field(line, "Sale Time:", "Auction Vendor:")
    auction_vendor = _extract_field(line, "Auction Vendor:", None)

    sale_date_iso = None
    if pp_sale_raw:
        mm = re.search(r"([A-Za-z]{3}\s+\d{1,2},\s+[A-Za-z]{3,9}\s+\d{4})", pp_sale_raw)
        sale_date_iso = _parse_date_tnfn(mm.group(1)) if mm else _parse_date_tnfn(pp_sale_raw)
    if not sale_date_iso and curr_sale_raw:
        sale_date_iso = _parse_date_tnfn(curr_sale_raw)
    if not sale_date_iso and orig_sale_raw:
        sale_date_iso = _parse_date_tnfn(orig_sale_raw)

    if not county or not address or not sale_date_iso:
        return None

    return {
        "notice_id": notice_id,
        "county": county.strip(),
        "sale_date": sale_date_iso,
        "address": address.strip(),
        "firm": firm.strip() if firm else None,
        "sale_location": sale_location.strip() if sale_location else None,
        "sale_time": sale_time.strip() if sale_time else None,
        "auction_vendor": auction_vendor.strip() if auction_vendor else None,
        "raw_text": line.strip(),
    }


def _parse_county_html(html: str):
    if not html:
        return []

    if "No results found" in html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [ln for ln in text.split("\n") if "Sale Notice:" in ln and "TNFN#" in ln]

    leads = []
    for ln in lines:
        parsed = _parse_notice_line(ln)
        if parsed:
            leads.append(parsed)
    return leads


def run():
    print("TNForeclosureNoticeBot starting...")

    total_written = 0
    created = 0
    updated = 0
    skipped_expired = 0
    parsed_ok = 0
    counties_hit = 0

    # lightweight diagnostics (single line at end)
    http_ok_pages = 0
    http_403 = 0
    http_other = 0

    session = requests.Session()

    for county_name in COUNTY_NAMES:
        slug = _slugify_county(county_name)
        county_url = COUNTY_URL_FMT.format(slug=slug)

        status, html = _get(county_url, session=session)
        if status == 200:
            http_ok_pages += 1
        elif status == 403:
            http_403 += 1
            continue
        elif status is None:
            http_other += 1
            continue
        else:
            http_other += 1
            continue

        leads = _parse_county_html(html)
        if not leads:
            continue

        counties_hit += 1

        for lead in leads:
            parsed_ok += 1

            dts = days_to_sale(lead["sale_date"])
            if dts is None or dts < 0:
                skipped_expired += 1
                continue

            falco_score = score_v2(dts, flags=[])
            status_label = label(falco_score)

            lead_key = make_lead_key(
                distress_type="Foreclosure",
                county=lead["county"],
                sale_date=lead["sale_date"],
                address=lead["address"],
                trustee=lead["firm"],
                notice_url=f"{county_url}#{lead.get('notice_id') or ''}",
            )

            props = build_properties(
                property_name=lead["address"],
                source="TNForeclosureNotices",
                county=lead["county"],
                distress_type="Foreclosure",
                address=lead["address"],
                sale_date=lead["sale_date"],
                trustee=lead["firm"],
                contact_info=None,
                status=status_label,
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
        "TNForeclosureNoticeBot complete: "
        f"total_written={total_written} created={created} updated={updated} "
        f"skipped_expired={skipped_expired} parsed_ok={parsed_ok} counties_hit={counties_hit} "
        f"http_ok_pages={http_ok_pages} http_403={http_403} http_other={http_other}"
    )
