import base64
import os
import re
import json
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..utils import fetch, make_lead_key
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
)

BASE_URL = "https://foreclosuretennessee.com/"
MAX_PAGES_CAP = 25

_DTS_MIN, _DTS_MAX = get_dts_window("FORECLOSURE_TN")


# --- Institutional counterparty guard (minimal, defensive, cheap) ---
# NOTE: ForeclosureTennessee table column "Trustee" is sometimes a bank/beneficiary style string.
# We only have THIS surface right now, so we apply a conservative exclusion for obvious institutions.
_FIRMISH_RX = re.compile(
    r"\b(PLLC|P\.?L\.?L\.?C\.?|LLC|P\.?C\.?|LLP|LAW\s+GROUP|ATTORNEYS?|ASSOCIATES|COUNSEL|FIRM)\b",
    re.IGNORECASE,
)

_INSTITUTIONAL_RX = re.compile(
    r"\b("
    r"U\.?\s*S\.?\s*BANK|WELLS\s+FARGO|JPMORGAN|CHASE|CITIBANK|CITI|BANK\s+OF\s+AMERICA|BANA|PNC|TRUIST|"
    r"FANNIE\s+MAE|FEDERAL\s+NATIONAL|FREDDIE\s+MAC|FEDERAL\s+HOME\s+LOAN|"
    r"NATIONSTAR|MR\.?\s*COOPER|OCWEN|SHELLPOINT|PENNYMAC|NEWREZ|RUSHMORE|SPS|"
    r"MORTGAGE|SERVICING|NATIONAL\s+ASSOCIATION|N\.?A\.?|TRUST\s+COMPANY"
    r")\b",
    re.IGNORECASE,
)

def _is_institutional_trustee_or_beneficiary(s: str | None) -> tuple[bool, str]:
    """
    Returns (is_institutional, reason_code).
    We do NOT want to exclude common trustee/attorney law firms.
    We DO want to exclude obvious banks/servicers if they appear in this column.
    """
    if not s:
        return (False, "")
    t = s.strip()
    if not t:
        return (False, "")

    # If it clearly looks like a law firm/attorney line, don't treat as institutional here.
    if _FIRMISH_RX.search(t):
        return (False, "")

    if _INSTITUTIONAL_RX.search(t):
        return (True, "INSTITUTIONAL_TRUSTEE_OR_BENEFICIARY")

    return (False, "")


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
    would_create = 0
    would_update = 0

    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_no_date = 0
    skipped_expired = 0
    skipped_dup_in_run = 0
    skipped_bad_row = 0
    skipped_no_link = 0
    skipped_institutional = 0

    stored_leads = 0
    stored_ingests = 0
    stored_html_artifacts = 0
    stored_pdf_artifacts = 0

    seen_in_run = set()
    sample_kept = []
    sample_skipped_institutional = []

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

        # --- Institutional exclusion (cheap, but prevents obviously dead inventory) ---
        is_inst, inst_reason = _is_institutional_trustee_or_beneficiary(trustee)
        if is_inst:
            skipped_institutional += 1
            if len(sample_skipped_institutional) < 5:
                sample_skipped_institutional.append(trustee)
            continue

        county_full = normalize_county_full(county_raw)

        if not is_allowed_county(county_full):
            skipped_out_of_geo += 1
            continue

        if not within_target_counties(county_full):
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

        # Persist an ingest evidence blob (for audit + later PDF stamping)
        ingest_raw = json.dumps(
            {
                "source": "ForeclosureTennessee",
                "distress_type": "FORECLOSURE",
                "sale_date_str": sale_date_str,
                "cont_date_str": cont_date_str,
                "sale_date_iso": sale_date_iso,
                "dts_days": dts,
                "city": city,
                "address": address,
                "zip": zip_code,
                "county_raw": county_raw,
                "county_full": county_full,
                "trustee_col": trustee,
                "institutional_check": {
                    "is_institutional": False,
                    "reason": inst_reason or None,
                },
                "listing_url": listing_url,
            },
            ensure_ascii=False,
        )

        if _store.upsert_lead(lead_key, {"address": address_full, "state": "TN"}, county_full, distress_type="FORECLOSURE"):
            stored_leads += 1
        if _store.insert_ingest_event(lead_key, "ForeclosureTennessee", listing_url, sale_date_iso, ingest_raw):
            stored_ingests += 1

        # Fetch listing detail page; store HTML + embedded notice PDF
        try:
            _retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            _detail_html = fetch(listing_url)
            if _detail_html:
                # Store raw HTML
                _ok, _ = _store.insert_raw_artifact(
                    lead_key, "NOTICE_HTML", listing_url, _retrieved_at,
                    "text/html",
                    payload_text=_detail_html,
                    notes="ForeclosureTennessee listing HTML",
                )
                if _ok:
                    stored_html_artifacts += 1

                # Extract and store embedded notice PDF if present
                _detail_soup = BeautifulSoup(_detail_html, "html.parser")
                _dl_a = _detail_soup.select_one("a.downloadLink[href]")
                if _dl_a:
                    _href = str(_dl_a.get("href") or "")
                    if _href.startswith("data:") and "base64," in _href:
                        _pdf_bytes = base64.b64decode(_href.split("base64,", 1)[1])
                        _pdf_dir = os.path.join("out", "notices")
                        os.makedirs(_pdf_dir, exist_ok=True)
                        _pdf_path = os.path.join(_pdf_dir, f"{lead_key}.pdf")
                        with open(_pdf_path, "wb") as _fh:
                            _fh.write(_pdf_bytes)
                        _ok, _ = _store.insert_raw_artifact(
                            lead_key, "NOTICE_PDF", listing_url, _retrieved_at,
                            "application/pdf",
                            payload_bytes=_pdf_bytes,
                            file_path=_pdf_path,
                            notes="ForeclosureTennessee embedded notice PDF",
                        )
                        if _ok:
                            stored_pdf_artifacts += 1
        except Exception:
            pass  # artifact writes never block ingestion

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

        payload = apply_convertibility_gate(payload)
        props = build_properties(payload)

        existing = find_existing_by_lead_key(lead_key)
        if existing:
            update_lead(existing, props)
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
                f"county={county_full} sale={sale_date_iso} dts={dts} addr={address_full}"
            )

    print(
        "[ForeclosureTNBot] summary "
        f"fetched_rows={fetched_rows} parsed_rows={parsed_rows} "
        f"filtered_in={filtered_in} created={created} updated={updated} would_create={would_create} would_update={would_update} "
        f"skipped_out_of_geo={skipped_out_of_geo} "
        f"skipped_outside_window={skipped_outside_window} "
        f"skipped_no_date={skipped_no_date} skipped_expired={skipped_expired} "
        f"skipped_bad_row={skipped_bad_row} skipped_no_link={skipped_no_link} "
        f"skipped_dup_in_run={skipped_dup_in_run} "
        f"skipped_institutional={skipped_institutional} "
        f"stored_leads={stored_leads} stored_ingests={stored_ingests} "
        f"stored_html_artifacts={stored_html_artifacts} stored_pdf_artifacts={stored_pdf_artifacts} "
        f"sample_kept={sample_kept} "
        f"sample_skipped_institutional={sample_skipped_institutional}"
    )
    print("[ForeclosureTNBot] Done.")
