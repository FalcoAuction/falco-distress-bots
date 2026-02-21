import os
from typing import Dict, Any

from ..notion_client import query_database, extract_page_fields, build_extra_properties, update_lead
from ..settings import get_dts_window
from .pdf_builder import build_pdf_packet
from .drive_uploader import upload_pdf, have_drive_creds
from ..gating.convertibility import is_institutional


def run() -> Dict[str, int]:
    dts_min, dts_max = get_dts_window("PACKAGER")
    max_packets = int(os.getenv("FALCO_MAX_PACKETS_PER_RUN", "10"))

    filter_obj = {
        "and": [
            {"property": "Days to Sale", "number": {"greater_than_or_equal_to": dts_min}},
            {"property": "Days to Sale", "number": {"less_than_or_equal_to": dts_max}},
        ]
    }

    pages = query_database(
        filter_obj,
        page_size=50,
        sorts=[{"property": "Sale Date", "direction": "ascending"}],
        max_pages=10,
    )

    packaged = 0
    skipped_missing = 0
    skipped_already = 0
    upload_failures = 0
    skipped_upload_missing_creds = 0
    skipped_institutional = 0

    out_dir = os.path.join(os.getcwd(), "out_packets")
    drive_enabled = have_drive_creds()

    for page in pages:
        if packaged >= max_packets:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        if not page_id:
            continue

        if is_institutional(fields):
            skipped_institutional += 1
            continue

        # Already has URL
        if (fields.get("packet_pdf_url") or "").strip():
            skipped_already += 1
            continue

        # Required inputs for a packet
        if not fields.get("address") or fields.get("value_band_low") is None or fields.get("value_band_high") is None or not fields.get("grade"):
            skipped_missing += 1
            continue

        try:
            local_path = build_pdf_packet(fields, out_dir=out_dir)
        except Exception:
            skipped_missing += 1
            continue

        # Upload optional
        pdf_url = None
        if drive_enabled:
            pdf_url = upload_pdf(local_pdf_path=local_path, filename=os.path.basename(local_path))
            if not pdf_url:
                upload_failures += 1
                # Still count as packaged locally
        else:
            skipped_upload_missing_creds += 1

        # Write back only if URL exists (non-destructive)
        if pdf_url:
            update_lead(page_id, build_extra_properties({"packet_pdf_url": pdf_url}))

        packaged += 1

    # Keep original summary keys + add one more (safe)
    summary = {
        "packaged_count": packaged,
        "skipped_packaging_missing_fields": skipped_missing,
        "skipped_packaging_already_done": skipped_already,
        "upload_failures": upload_failures,
        "skipped_packaging_institutional": skipped_institutional,
    }

    if not drive_enabled:
        summary["skipped_upload_missing_creds"] = skipped_upload_missing_creds

    return summary
