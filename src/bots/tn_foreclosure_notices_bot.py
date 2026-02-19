# src/bots/tn_foreclosure_notices_bot.py

import re
import hashlib
import inspect
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
from ..scoring import days_to_sale

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
    m = re.search(r"([A-Za-z]{3}\s+\d{1,2},\s+[A-Za-z]{3,9}\s+\d{4})", s)
    if m:
        return _parse_date_tnfn(m.group(1))
    return None


def _extract_field(text: str, start_label: str, end_labels: list[str] | None = None):
    idx = text.find(start_label)
    if idx == -1:
        return None
    idx += len(start_label)
    tail = text[idx:]

    if not end_labels:
        return tail.strip()

    end_positions = []
    for lab in end_labels:
        p = tail.find(lab)
        if p != -1:
            end_positions.append(p)
    if not end_positions:
        return tail.strip()

    end = min(end_positions)
    return tail[:end].strip()


def _pick_sale_date_iso(text: str):
    pp = _extract_field(
        text,
        "PP Sale Date:",
        end_labels=["Sale Location:", "Sale Time:", "Auction Vendor:", "Address:", "Firm:", "County:"],
    )
    cur = _extract_field(
        text,
        "Current Sale Date:",
        end_labels=["Sale Location:", "Sale Time:", "Auction Vendor:", "Address:", "Firm:", "County:"],
    )
    orig = _extract_field(
        text,
        "Original Sale Date:",
        end_labels=["Sale Location:", "Sale Time:", "Auction Vendor:", "Address:", "Firm:", "County:"],
    )

    for candidate in (pp, cur, orig):
        iso = _parse_date_tnfn(candidate) if candidate else None
        if iso:
            return iso
    return None


def _triage_and_score(dts: int):
    # URGENT 0–7, HOT 8–14, GREEN 15+
    if dts <= 7:
        return "URGENT", 95
    if dts <= 14:
        return "HOT", 80
    return "GREEN", 65


def _make_lead_key(distress_type: str, county: str, sale_date: str, address: str, trustee: str | None, notice_url: str):
    parts = [
        (distress_type or "").strip().lower(),
        (county or "").strip().lower(),
        (sale_date or "").strip().lower(),
        (address or "").strip().lower(),
        (trustee or "").strip().lower(),
        (notice_url or "").strip().lower(),
    ]
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _parse_notice_container_text(container_text: str):
    m = re.search(r"(TNFN#\d+)", container_text)
    if not m:
        return None
    notice_id = m.group(1)

    county = _extract_field(
        container_text,
        "County:",
        end_labels=["Original Sale Date:", "Current Sale Date:", "PP Sale Date:", "Address:", "Firm:"],
    )
    address = _extract_field(
        container_text,
        "Address:",
        end_labels=["Firm:", "County:", "Original Sale Date:", "Current Sale Date:", "PP Sale Date:"],
    )
    firm = _extract_field(
        container_text,
        "Firm:",
        end_labels=["PP Sale Date:", "Current Sale Date:", "Sale Location:", "Sale Time:", "Auction Vendor:", "County:"],
    )

    sale_date_iso = _pick_sale_date_iso(container_text)

    if not county or not address or not sale_date_iso:
        return None

    return {
        "notice_id": notice_id,
        "county": county.strip(),
        "sale_date": sale_date_iso,
        "address": address.strip(),
        "firm": firm.strip() if firm else None,
        "raw_text": container_text.strip(),
    }


def _parse_county_html(html: str):
    if not html:
        return []
    if "No results found" in html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    hits = soup.find_all(string=re.compile(r"TNFN#\d+"))
    leads = []
    seen_ids = set()

    for node in hits:
        container = node.parent
        for _ in range(8):
            if container is None:
                break
            if container.name in ("div", "li", "section", "article", "tr", "tbody", "table"):
                break
            container = container.parent

        if container is None:
            continue

        container_text = container.get_text(" ", strip=True)
        parsed = _parse_notice_container_text(container_text)
        if not parsed:
            continue

        nid = parsed["notice_id"]
        if nid in seen_ids:
            continue
        seen_ids.add(nid)
        leads.append(parsed)

    return leads


def _call_build_properties(data: dict):
    """
    Adapts to build_properties signature without knowing it in advance.
    Tries:
      1) build_properties(data) if single-arg function
      2) build_properties(**filtered_kwargs) where keys match signature
      3) build_properties(*positional) in common order if signature is positional-only
    """
    sig = inspect.signature(build_properties)
    params = list(sig.parameters.values())

    # Case 1: single positional parameter (often `data` or `lead`)
    if len(params) == 1 and params[0].kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
        return build_properties(data)

    # Case 2: supports **kwargs
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
        return build_properties(**data)

    # Case 3: strict named parameters — filter to what it accepts
    accepted = {p.name for p in params if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)}
    filtered = {k: v for k, v in data.items() if k in accepted}
    if filtered:
        return build_properties(**filtered)

    # Case 4: positional-only / unknown — try common order
    # (title/name, source, county, distress_type, address, sale_date, trustee, contact_info, status, falco_score, raw_snippet, url, lead_key, days_to_sale)
    common_order = [
        data.get("title") or data.get("name") or data.get("property_name"),
        data.get("source"),
        data.get("county"),
        data.get("distress_type"),
        data.get("address"),
        data.get("sale_date"),
        data.get("trustee"),
        data.get("contact_info"),
        data.get("status"),
        data.get("falco_score"),
        data.get("raw_snippet"),
        data.get("url"),
        data.get("lead_key"),
        data.get("days_to_sale"),
    ]
    return build_properties(*common_order)


def run():
    print("TNForeclosureNoticeBot starting...")

    total_written = 0
    created = 0
    updated = 0
    skipped_expired = 0
    parsed_ok = 0
    counties_hit = 0

    http_ok_pages = 0
    http_403 = 0
    http_other = 0

    session = requests.Session()

    for county_name in COUNTY_NAMES:
        slug = _slugify_county(county_name)
        county_url = COUNTY_URL_FMT.format(slug=slug)

        http_status, html = _get(county_url, session=session)
        if http_status == 200:
            http_ok_pages += 1
        elif http_status == 403:
            http_403 += 1
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

            status_label, falco_score = _triage_and_score(dts)

            lead_key = _make_lead_key(
                distress_type="Foreclosure",
                county=lead["county"],
                sale_date=lead["sale_date"],
                address=lead["address"],
                trustee=lead["firm"],
                notice_url=f"{county_url}#{lead.get('notice_id') or ''}",
            )

            # Provide multiple aliases for "title" so build_properties can map it.
            data = {
                "title": lead["address"],
                "name": lead["address"],
                "property_name": lead["address"],
                "source": "TNForeclosureNotices",
                "county": lead["county"],
                "distress_type": "Foreclosure",
                "address": lead["address"],
                "sale_date": lead["sale_date"],
                "trustee": lead["firm"],
                "contact_info": None,
                "status": status_label,
                "falco_score": falco_score,
                "raw_snippet": lead["raw_text"],
                "url": county_url,
                "lead_key": lead_key,
                "days_to_sale": dts,
            }

            props = _call_build_properties(data)

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
