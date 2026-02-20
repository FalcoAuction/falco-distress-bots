import os
from typing import Dict, Any, Optional
from datetime import datetime

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas


def build_pdf_packet(fields: Dict[str, Any], out_dir: str) -> str:
    """
    Builds a simple one-page deal packet PDF locally and returns the local file path.
    This MUST succeed even if Drive upload is disabled.
    """
    os.makedirs(out_dir, exist_ok=True)

    addr = (fields.get("address") or "unknown_address").replace("/", "-")
    lead_key = (fields.get("lead_key") or "").strip()
    suffix = lead_key[-8:] if lead_key else datetime.utcnow().strftime("%Y%m%d%H%M%S")

    filename = f"falco_packet_{suffix}.pdf"
    path = os.path.join(out_dir, filename)

    c = canvas.Canvas(path, pagesize=LETTER)
    width, height = LETTER

    y = height - 72
    line = 14

    def write(label: str, value: Any):
        nonlocal y
        c.setFont("Helvetica-Bold", 11)
        c.drawString(72, y, f"{label}:")
        c.setFont("Helvetica", 11)
        c.drawString(170, y, str(value)[:120])
        y -= line

    c.setFont("Helvetica-Bold", 16)
    c.drawString(72, y, "FALCO Deal Packet")
    y -= 2 * line

    write("Address", fields.get("address", ""))
    write("County", fields.get("county", ""))
    write("Sale Date", fields.get("sale_date", ""))
    write("Days to Sale", fields.get("days_to_sale", ""))

    y -= line
    write("Value Band Low", fields.get("value_band_low", ""))
    write("Value Band High", fields.get("value_band_high", ""))
    write("Liquidity Score", fields.get("liquidity_score", ""))
    write("Grade", fields.get("grade", ""))
    write("Grade Score", fields.get("grade_score", ""))
    write("Status Flag", fields.get("status_flag", ""))

    y -= line
    write("Source URL", fields.get("url", ""))

    c.showPage()
    c.save()
    return path
