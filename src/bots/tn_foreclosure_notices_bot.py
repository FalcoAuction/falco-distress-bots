import json
import re
import hashlib
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
    NOTION_WRITE_ENABLED,
)
from ..gating.convertibility import apply_convertibility_gate
from ..storage import sqlite_store as _store
from ..scoring.days_to_sale import days_to_sale
from ..settings import (
    get_dts_window,
    is_allowed_county,
    within_target_counties,
    normalize_county_full,
    clip_raw_snippet,
)

BASE_URL = "https://tnforeclosurenotices.com/"
COUNTY_URL_FMT = urljoin(BASE_URL, "results/counties/{slug}/")

# You can keep this list full; we only fetch allowed counties.
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

_DTS_MIN, _DTS_MAX = get_dts_window("TN_FORECLOSURE_NOTICES")


# Phone pattern: formatted US numbers (no separators needed, but must look like
# NXX-NXX-XXXX, (NXX) NXX-XXXX, NXX.NXX.XXXX, or ten raw digits near "phone"/"tel")
_PHONE_FMT_RX = re.compile(
    r"(?:\+?1[\s\-\.])?(?:\(\d{3}\)|\d{3})[\s\-\.]?\d{3}[\s\-\.]\d{4}"
)


def _find_phone(text: str) -> str | None:
    """
    Return the first plausible US phone number from text, normalized to NXX-NXX-XXXX,
    or None if not found.  Rejects year-like sequences and all-same-digit runs.
    """
    for m in _PHONE_FMT_RX.finditer(text):
        raw = m.group(0).strip()
        digits = re.sub(r"\D", "", raw)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            continue
        # Reject year-like prefixes (date/id noise common in notice text)
        if digits.startswith("19") or digits.startswith("20"):
            continue
        if digits[-4:] == "0000":
            continue
        area, exch = digits[:3], digits[3:6]
        if area[0] in ("0", "1") or exch[0] in ("0", "1"):
            continue
        if len(set(digits)) == 1:  # all same digit
            continue
        return f"{area}-{exch}-{digits[6:]}"
    return None


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


def _parse_date(s: str):
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
        return _parse_date(m.group(1))
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
    # Prefer current / postponed, then original
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
        iso = _parse_date(candidate) if candidate else None
        if iso:
            return iso
    return None


def _triage(dts: int):
    if dts <= 7:
        return "URGENT", 95
    if dts <= 14:
        return "HOT", 80
    return "GREEN", 65


def _make_lead_key(
    distress_type: str,
    county: str,
    address: str,
    trustee: str | None,
    notice_id: str | None,
    notice_url: str,
):
    parts = [
        (distress_type or "").strip().lower(),
        (county or "").strip().lower(),
        (address or "").strip().lower(),
        (trustee or "").strip().lower(),
        (notice_id or "").strip().lower(),
        (notice_url or "").strip().lower(),
    ]
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _parse_notice_container_text(container_text: str):
    # Find TNFN ID
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
    trustee_attorney = _extract_field(
        container_text,
        "Firm:",
        end_labels=["PP Sale Date:", "Current Sale Date:", "Sale Location:", "Sale Time:", "Auction Vendor:", "County:"],
    )

    sale_date_iso = _pick_sale_date_iso(container_text)

    sale_location = _extract_field(
        container_text,
        "Sale Location:",
        end_labels=["Sale Time:", "Auction Vendor:", "County:", "Firm:", "Notice:", "Original Sale Date:", "Current Sale Date:", "PP Sale Date:"],
    )

    phone = _find_phone(container_text)

    if not county or not address or not sale_date_iso:
        return None

    return {
        "notice_id": notice_id,
        "county_raw": county.strip(),
        "sale_date_iso": sale_date_iso,
        "address_raw": address.strip(),
        "trustee_attorney": trustee_attorney.strip() if trustee_attorney else None,
        "sale_location": sale_location.strip() if sale_location else None,
        "phone": phone,
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
        # climb to a sane container
        for _ in range(10):
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


def run():
    print(
        "TNForeclosureNoticeBot starting... "
        f"dts_window=[{_DTS_MIN},{_DTS_MAX}]"
    )

    fetched_notices = 0
    parsed_ok = 0
    filtered_in = 0
    created = 0
    updated = 0
    would_create = 0
    would_update = 0

    skipped_out_of_geo = 0
    skipped_expired = 0
    skipped_outside_window = 0
    skipped_dup_in_run = 0
    stored_leads = 0
    stored_ingests = 0

    counties_hit = 0
    http_ok_pages = 0
    http_403 = 0
    http_other = 0

    seen_in_run = set()
    sample_kept = []

    session = requests.Session()

    for county_name in COUNTY_NAMES:
        # only hit allowed base counties (e.g., Davidson)
        county_full = normalize_county_full(county_name)
        if not is_allowed_county(county_full):
            continue
        if not within_target_counties(county_full):
            continue

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
            fetched_notices += 1

            county_full = normalize_county_full(lead.get("county_raw"))
            if not is_allowed_county(county_full):
                skipped_out_of_geo += 1
                continue
            if not within_target_counties(county_full):
                skipped_out_of_geo += 1
                continue

            # Normalize address lightly (don’t overthink)
            address = " ".join((lead.get("address_raw") or "").split())

            dts = days_to_sale(lead["sale_date_iso"])
            if dts is None or dts < 0:
                skipped_expired += 1
                continue
            if not (_DTS_MIN <= dts <= _DTS_MAX):
                skipped_outside_window += 1
                continue

            parsed_ok += 1

            status_label, score = _triage(dts)

            notice_url = f"{county_url}#{lead.get('notice_id') or ''}"

            lead_key = _make_lead_key(
                distress_type="Foreclosure",
                county=county_full,
                address=address,
                trustee=lead.get("trustee_attorney"),
                notice_id=lead.get("notice_id"),
                notice_url=notice_url,
            )

            if lead_key in seen_in_run:
                skipped_dup_in_run += 1
                continue
            seen_in_run.add(lead_key)

            if _store.upsert_lead(lead_key, {"address": address, "state": "TN"}, county_full, distress_type="FORECLOSURE"):
                stored_leads += 1
            _tn_raw = {k: v for k, v in {
                "trustee_attorney": lead.get("trustee_attorney"),
                "sale_location":    lead.get("sale_location"),
                "phone":            lead.get("phone"),
            }.items() if v}
            if _store.insert_ingest_event(
                lead_key,
                "TNForeclosureNotices",
                notice_url,
                lead["sale_date_iso"],
                json.dumps(_tn_raw, ensure_ascii=False) if _tn_raw else None,
            ):
                stored_ingests += 1

            payload = {
                "title": address or f"Foreclosure ({county_full})",
                "source": "TNForeclosureNotices",
                "distress_type": "Foreclosure",
                "county": county_full,
                "address": address,
                "sale_date_iso": lead["sale_date_iso"],
                "trustee_attorney": lead.get("trustee_attorney") or "",
                "contact_info": lead.get("trustee_attorney") or "",
                "raw_snippet": clip_raw_snippet(lead.get("raw_text") or ""),
                "url": county_url,
                "lead_key": lead_key,
                "days_to_sale": dts,
                "status": status_label,
                "score": score,
            }

            payload = apply_convertibility_gate(payload)
            props = build_properties(payload)

            existing_id = find_existing_by_lead_key(lead_key)
            if existing_id:
                update_lead(existing_id, props)
                if NOTION_WRITE_ENABLED:
                    updated += 1
                else:
                    would_update += 1
            else:
                create_lead(props)
                if NOTION_WRITE_ENABLED:
                    created += 1
                else:
                    would_create += 1

            filtered_in += 1

            if len(sample_kept) < 5:
                sample_kept.append(
                    f"county={county_full} sale={lead['sale_date_iso']} dts={dts} addr={address}"
                )

    print(
        "TNForeclosureNoticeBot summary: "
        f"fetched_notices={fetched_notices} parsed_ok={parsed_ok} filtered_in={filtered_in} "
        f"created={created} updated={updated} would_create={would_create} would_update={would_update} "
        f"skipped_out_of_geo={skipped_out_of_geo} skipped_expired={skipped_expired} "
        f"skipped_outside_window={skipped_outside_window} skipped_dup_in_run={skipped_dup_in_run} "
        f"counties_hit={counties_hit} http_ok_pages={http_ok_pages} http_403={http_403} http_other={http_other} "
        f"stored_leads={stored_leads} stored_ingests={stored_ingests} "
        f"sample_kept={sample_kept}"
    )
    print("=== DONE: TNForeclosureNoticesBot ===")
