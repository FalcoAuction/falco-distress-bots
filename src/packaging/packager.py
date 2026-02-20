# src/packaging/packager.py
import os
from datetime import datetime
from typing import Any, Dict

from ..notion_client import (
    query_database,
    extract_page_fields,
    build_extra_properties,
    update_lead,
)
from ..settings import get_dts_window
from .pdf_builder import build_packet_pdf
from .drive_uploader import upload_pdf

DEBUG = os.getenv("FALCO_PDF_DEBUG", "").strip() not in ("", "0", "false", "False")


def run() -> Dict[str, int]:
    dts_min, dts_max = get_dts_window("PACKET")
    max_packets = int(os.getenv("FALCO_MAX_PACKETS_PER_RUN", "10"))

    drive_folder_id = os.getenv("FALCO_DRIVE_FOLDER_ID", "").strip() or None
    drive_public = os.getenv("FALCO_DRIVE_PUBLIC", "1").strip() not in ("0", "false", "False")

    out_dir = os.getenv("FALCO_PACKET_OUT_DIR", "/tmp/falco_packets").strip() or "/tmp/falco_packets"

    filter_obj = {
        "and": [
            {"property": "Days to Sale", "number": {"greater_than_or_equal_to": dts_min}},
            {"property": "Days to Sale", "number": {"less_than_or_equal_to": dts_max}},
            {"property": "Address", "rich_text": {"is_not_empty": True}},
        ]
    }
    pages = query_database(
        filter_obj,
        page_size=50,
        sorts=[{"property": "Sale Date", "direction": "ascending"}],
        max_pages=10,
    )

    packaged = 0
    skipped_missing_fields = 0
    skipped_already = 0
    upload_failures = 0

    for page in pages:
        if packaged >= max_packets:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        if not page_id:
            continue

        # already packaged?
        if (fields.get("packet_pdf_url") or "").strip():
            skipped_already += 1
            continue

        # require grade and value band/estimate
        if not (fields.get("grade") or "").strip():
            skipped_missing_fields += 1
            continue
        if fields.get("value_band_low") is None and fields.get("value_band_high") is None and fields.get("estimated_value_low") is None and fields.get("estimated_value_high") is None:
            skipped_missing_fields += 1
            continue

        try:
            paths = build_packet_pdf(fields, out_dir=out_dir)
            link = upload_pdf(paths.pdf_path, folder_id=drive_folder_id, make_public=drive_public)

            if not link:
                upload_failures += 1
                # Still mark packet_built_at so you can inspect local artifacts when running manually.
                props = build_extra_properties({"packet_built_at": datetime.utcnow().date().isoformat()})
                update_lead(page_id, props)
                continue

            props = build_extra_properties({
                "packet_pdf_url": link,
                "packet_built_at": datetime.utcnow().date().isoformat(),
            })
            update_lead(page_id, props)
            packaged += 1

            if DEBUG:
                print(f"[Packager] packaged page_id={page_id} link={link}")

        except Exception as e:
            upload_failures += 1
            print(f"[Packager] ERROR packaging page_id={page_id}: {type(e).__name__}: {e}")

    summary = {
        "packaged_count": packaged,
        "skipped_packaging_missing_fields": skipped_missing_fields,
        "skipped_packaging_already_done": skipped_already,
        "upload_failures": upload_failures,
    }
    print(f"[Packager] summary {summary}")
    return summary
