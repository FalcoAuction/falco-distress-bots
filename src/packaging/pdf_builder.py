# src/packaging/pdf_builder.py
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

# We intentionally use reportlab because generating a valid multi-page PDF robustly with stdlib is brittle.
# reportlab is lightweight and stable in CI when pinned in requirements.txt
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

PDF_DEBUG = os.getenv("FALCO_PDF_DEBUG", "").strip() not in ("", "0", "false", "False")


@dataclass
class PacketPaths:
    pdf_path: str


def _money(x: Any) -> str:
    try:
        if x is None:
            return ""
        v = float(x)
        if v <= 0:
            return ""
        return f"${v:,.0f}"
    except Exception:
        return ""


def _safe(s: Any) -> str:
    return ("" if s is None else str(s)).strip()


def build_packet_pdf(fields: Dict[str, Any], out_dir: str) -> PacketPaths:
    os.makedirs(out_dir, exist_ok=True)

    lead_key = _safe(fields.get("lead_key")) or _safe(fields.get("page_id")) or "packet"
    filename = f"falco_packet_{lead_key[:16]}.pdf"
    pdf_path = os.path.join(out_dir, filename)

    c = canvas.Canvas(pdf_path, pagesize=LETTER)
    w, h = LETTER

    def header(title: str):
        c.setFont("Helvetica-Bold", 16)
        c.drawString(0.75 * inch, h - 0.85 * inch, title)
        c.setFont("Helvetica", 10)
        c.drawString(0.75 * inch, h - 1.05 * inch, f"Generated: {datetime.utcnow().isoformat()} UTC")

    def label_value(y: float, label: str, value: str):
        c.setFont("Helvetica-Bold", 10)
        c.drawString(0.75 * inch, y, label)
        c.setFont("Helvetica", 10)
        c.drawString(2.4 * inch, y, value)

    # ---------------- Page 1 ----------------
    address = _safe(fields.get("address")) or _safe(fields.get("property_name")) or "Unknown Address"
    header("FALCO — Auction Deal Packet")

    y = h - 1.55 * inch
    label_value(y, "Address:", address); y -= 0.22 * inch
    label_value(y, "County:", _safe(fields.get("county"))); y -= 0.22 * inch
    label_value(y, "Sale Date:", _safe(fields.get("sale_date"))); y -= 0.22 * inch
    label_value(y, "Days to Sale:", _safe(fields.get("days_to_sale"))); y -= 0.22 * inch

    trustee = _safe(fields.get("trustee_attorney"))
    if trustee:
        label_value(y, "Trustee/Attorney:", trustee[:80]); y -= 0.22 * inch

    band_low = fields.get("value_band_low") or fields.get("estimated_value_low")
    band_high = fields.get("value_band_high") or fields.get("estimated_value_high")
    label_value(y, "Value Band:", f"{_money(band_low)} – {_money(band_high)}".strip()); y -= 0.22 * inch

    grade = _safe(fields.get("grade"))
    grade_score = _safe(fields.get("grade_score"))
    status_flag = _safe(fields.get("status_flag"))
    label_value(y, "Grade / Score:", f"{grade} / {grade_score}"); y -= 0.22 * inch
    label_value(y, "Status Flag:", status_flag); y -= 0.22 * inch

    # Flags row
    flags: List[str] = []
    if fields.get("absentee_flag"):
        flags.append("ABSENTEE")
    if _safe(fields.get("loan_indicators")):
        flags.append("LOAN")
    if _safe(fields.get("grade_reasons")):
        flags.append("NOTES")

    label_value(y, "Key Flags:", ", ".join(flags) if flags else "—"); y -= 0.3 * inch

    # Risk notes
    c.setFont("Helvetica-Bold", 11)
    c.drawString(0.75 * inch, y, "Risk / Notes"); y -= 0.18 * inch
    c.setFont("Helvetica", 10)
    reasons = _safe(fields.get("grade_reasons")) or "—"
    # wrap simple
    wrap_width = 95
    for i in range(0, len(reasons), wrap_width):
        c.drawString(0.85 * inch, y, reasons[i:i+wrap_width])
        y -= 0.16 * inch
        if y < 1.0 * inch:
            break

    # Footer
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(0.75 * inch, 0.65 * inch, "Falco Distress Bots — automation-first origination engine")

    c.showPage()

    # ---------------- Page 2 ----------------
    header("Comps + Next Steps")

    y = h - 1.55 * inch
    c.setFont("Helvetica-Bold", 11)
    c.drawString(0.75 * inch, y, "Comps Summary"); y -= 0.22 * inch
    c.setFont("Helvetica", 10)
    comps_summary = _safe(fields.get("comps_summary")) or "—"
    for i in range(0, len(comps_summary), 95):
        c.drawString(0.85 * inch, y, comps_summary[i:i+95])
        y -= 0.16 * inch
        if y < 1.3 * inch:
            break

    y -= 0.1 * inch
    c.setFont("Helvetica-Bold", 11)
    c.drawString(0.75 * inch, y, "What To Do Next (Checklist)"); y -= 0.24 * inch
    c.setFont("Helvetica", 10)
    checklist = [
        "Confirm sale location + bidding procedure with trustee/auctioneer",
        "Pull deed / legal description (county register) + verify parcel",
        "Verify occupancy (drive-by / photos) + note visible condition",
        "Estimate opening bid strategy + buyer list fit",
        "Prepare outreach brief (why this deal, who will buy, timeline)",
    ]
    for item in checklist:
        c.drawString(0.85 * inch, y, f"☐ {item}")
        y -= 0.18 * inch
        if y < 1.0 * inch:
            break

    c.setFont("Helvetica-Oblique", 9)
    c.drawString(0.75 * inch, 0.65 * inch, "Generated packet is informational; verify all facts before auction marketing.")

    c.save()

    if PDF_DEBUG:
        print(f"[PDFBuilder] built {pdf_path}")

    return PacketPaths(pdf_path=pdf_path)
