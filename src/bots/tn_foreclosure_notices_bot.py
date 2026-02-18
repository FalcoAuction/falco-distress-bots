# src/bots/tn_foreclosure_notices_bot.py

import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale, score_v2, label
from ..utils import fetch, make_lead_key

BASE_URL = "https://tnforeclosurenotices.com/"
SEARCH_URL = urljoin(BASE_URL, "search/")
MAX_COUNTIES_CAP = 150  # safety cap


def _parse_date_tnfn(s: str):
    if not s:
        return None
    s = s.strip()
    s = s.strip("()")
    # examples:
    # "Tue 24, Mar 2026"
    # "Tue 03, Mar 2026"
    for fmt in ("%a %d, %b %Y", "%a %d, %B %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _extract_county_links(html: str):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/results/counties/" in href:
            links.add(urljoin(BASE_URL, href))
    return list(links)


def _extract_field(text: str, start_label: str, end_label: str | None):
    """
    Extracts value after `start_label` up to `end_label` (or end of string if None).
    Labels are literal substrings like "Address:".
    """
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
    # Expect lines like:
    # "Sale Notice: TNFN#13606  County: Sullivan  Original Sale Date: Tue 24, Mar 2026  Address: ...  Firm: ...  PP Sale Date: (Tue 24, Mar 2026) Sale Location: ... Sale Time: ... Auction Vendor: ..."
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

    # Prefer PP Sale Date (current) when present, else Current Sale Date, else Original Sale Date
    pp_sale_raw = _extract_field(line, "PP Sale Date:", "Sale Location:")
    curr_sale_raw = _extract_field(line, "Current Sale Date:", "Sale Location:")

    sale_location = _extract_field(line, "Sale Location:", "Sale Time:")
    sale_time = _extract_field(line, "Sale Time:", "Auction Vendor:")
    auction_vendor = _extract_field(line, "Auction Vendor:", None)

    sale_date_iso = None
    if pp_sale_raw:
        # usually "(Tue 03, Mar 2026)"
        # grab the first date-like token inside
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
        "original_sale_raw": orig_sale_raw,
        "address": address,
        "firm": firm,
        "sale_location": sale_location,
        "sale_time": sale_time,
        "auction_vendor": auction_vendor,
        "raw_text": line.strip(),
    }


def _parse_county_page(county_url: str):
    html = fetch(county_url)
    if not html:
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

    search_html = fetch(SEARCH_URL)
    if not search_html:
        print("TNForeclosureNoticeBot: failed to fetch search page.")
        return

    county_links = _extract_county_links(search_html)
    county_links = county_links[:MAX_COUNTIES_CAP]

    total_written = 0
    created = 0
    updated = 0
    skipped_expired = 0
    parsed_ok = 0
    parsed_bad = 0

    for county_url in county_links:
        leads = _parse_county_page(county_url)

        for lead in leads:
            parsed_ok += 1

            dts = days_to_sale(lead["sale_date"])
            if dts is None or dts < 0:
                skipped_expired += 1
                continue

            falco_score = score_v2(dts, flags=[])
            status = label(falco_score)

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

        # if a county page has “Sale Notice” but we parsed none, count as bad parse
        # (helps debug yield without spamming logs)
        if not leads:
            # quick check if page likely had no results
            # (site shows "No results found..." on empty counties)
            html = fetch(county_url) or ""
            if "No results found" not in html:
                parsed_bad += 1

    print(
        "TNForeclosureNoticeBot complete: "
        f"total_written={total_written} created={created} updated={updated} "
        f"skipped_expired={skipped_expired} parsed_ok={parsed_ok} parsed_bad_pages={parsed_bad}"
    )
