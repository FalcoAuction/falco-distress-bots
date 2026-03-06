# src/packaging/pdf_builder.py
#
# FALCO Auction Intelligence Brief — premium 5-page PDF
# AI narrative via OpenAI when FALCO_OPENAI_API_KEY is set; otherwise deterministic templates.
# No ATTOM calls. No new dependencies beyond reportlab (already installed).

import json
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.intelligence.brief_generator import generate_brief
from src.enrichment.streetview import get_streetview_image_path

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas as rl_canvas

_TEMPLATE_VERSION = "v1.0"

# ─── Colour palette ───────────────────────────────────────────────────────────
_NAVY      = colors.HexColor("#1C2B4B")
_SLATE     = colors.HexColor("#2C3E50")
_GRAY      = colors.HexColor("#6B7280")
_LGRAY     = colors.HexColor("#F3F4F6")
_MGRAY     = colors.HexColor("#E5E7EB")
_GREEN     = colors.HexColor("#166534")
_AMBER     = colors.HexColor("#92400E")
_RED       = colors.HexColor("#991B1B")
_BLUE_BAR  = colors.HexColor("#BFDBFE")
_NAVY_BAR  = colors.HexColor("#1E40AF")
_LINE      = colors.HexColor("#D1D5DB")
_WHITE     = colors.white
_TILE_BG   = colors.HexColor("#F8F9FA")
_TILE_BRD  = colors.HexColor("#E2E4E8")
_PILL_GRN  = colors.HexColor("#D1FAE5")
_PILL_AMB  = colors.HexColor("#FEF3C7")
_PILL_RED  = colors.HexColor("#FEE2E2")
_TXT_GRN   = colors.HexColor("#065F46")

# ─── Layout constants ─────────────────────────────────────────────────────────
PAGE_W, PAGE_H = LETTER          # 612 × 792 pt
ML = 0.75 * inch                 # left margin  (~54 pt)
MR = 0.75 * inch                 # right margin
MT = 0.85 * inch                 # top margin
MB = 0.70 * inch                 # bottom margin
CW = PAGE_W - ML - MR            # content width ≈ 504 pt


# ─── Text utilities ───────────────────────────────────────────────────────────

def _wrap(text: str, font: str, size: float, max_w: float) -> List[str]:
    words = str(text or "").split()
    lines: List[str] = []
    cur: List[str] = []
    for w in words:
        test = " ".join(cur + [w])
        if stringWidth(test, font, size) <= max_w:
            cur.append(w)
        else:
            if cur:
                lines.append(" ".join(cur))
            cur = [w]
    if cur:
        lines.append(" ".join(cur))
    return lines or [""]


def _fmt_cur(v: Optional[float]) -> str:
    if v is None:
        return "Unavailable"
    return f"${v:,.0f}"


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "N/A"
    return f"{v * 100:.1f}%"


def _val(v: Any, fallback: str = "Unavailable") -> str:
    if v is None:
        return fallback
    s = str(v).strip()
    return fallback if s in ("", "None", "null") else s


# ─── PDF document wrapper ─────────────────────────────────────────────────────

class _Doc:
    """Thin canvas wrapper: page management, typography helpers."""

    def __init__(self, path: str):
        self.c   = rl_canvas.Canvas(path, pagesize=LETTER)
        self.y   = PAGE_H - MT
        self._pn = 1

    # ── page control ──────────────────────────────────────────────────────────

    def new_page(self) -> None:
        self._draw_footer()
        self.c.showPage()
        self._pn += 1
        self.y = PAGE_H - MT

    def save(self) -> None:
        self._draw_footer()
        self.c.save()

    def gap(self, pts: float = 8) -> None:
        self.y -= pts

    def space_left(self) -> float:
        return self.y - MB

    # ── footer ────────────────────────────────────────────────────────────────

    def _draw_footer(self) -> None:
        c  = self.c
        fy = MB - 6
        c.setStrokeColor(_LINE)
        c.setLineWidth(0.4)
        c.line(ML, fy, PAGE_W - MR, fy)
        c.setFont("Helvetica", 6.5)
        c.setFillColor(_GRAY)
        run_id   = os.getenv("FALCO_RUN_ID", "unknown_run")
        gen_date = datetime.utcnow().date().isoformat()
        footer_left = (
            f"FALCO Diamond Acquisition Dossier | Confidential — For Professional Auction Partners | "
            f"Template {_TEMPLATE_VERSION} | Run {run_id} | Generated {gen_date}"
        )
        c.drawString(ML, fy - 11, footer_left)
        c.drawRightString(PAGE_W - MR, fy - 11, f"Page {self._pn}")

    # ── page header band ──────────────────────────────────────────────────────

    def page_break(self) -> None:
        """Start a new PDF page."""
        self.c.showPage()
        self._pn = getattr(self, "_pn", 1) + 1
        # Reset cursor near top (safe constant, no margin attrs)
        self.y = PAGE_H - 72

	
    def page_header(self, title: str, subtitle: str = "") -> None:
        c = self.c
        bh = 38
        c.setFillColor(_NAVY)
        c.rect(0, PAGE_H - bh - 2, PAGE_W, bh + 2, fill=1, stroke=0)
        c.setFont("Helvetica-Bold", 14)
        c.setFillColor(_WHITE)
        c.drawString(ML, PAGE_H - bh + 9, title)
        if subtitle:
            c.setFont("Helvetica", 8)
            c.setFillColor(colors.HexColor("#A5B4C8"))
            c.drawRightString(PAGE_W - MR, PAGE_H - bh + 9, subtitle)
        self.y = PAGE_H - bh - 18

    def cover_header(self, address: str, location: str) -> None:
        """Page-1 header band: address + county/state + 'Diamond Acquisition Dossier' badge."""
        c  = self.c
        bh = 58
        c.setFillColor(_NAVY)
        c.rect(0, PAGE_H - bh, PAGE_W, bh, fill=1, stroke=0)
        # Badge — top-right
        c.setFont("Helvetica-Bold", 8)
        c.setFillColor(colors.HexColor("#A5B4C8"))
        c.drawRightString(PAGE_W - MR, PAGE_H - 16, "Diamond Acquisition Dossier")
        # Address — main title
        addr_display = address[:72]
        c.setFont("Helvetica-Bold", 15)
        c.setFillColor(_WHITE)
        c.drawString(ML, PAGE_H - 28, addr_display)
        # County / State — subtitle line
        c.setFont("Helvetica", 9)
        c.setFillColor(colors.HexColor("#A5B4C8"))
        c.drawString(ML, PAGE_H - 44, location)
        self.y = PAGE_H - bh - 12

    # ── section heading ───────────────────────────────────────────────────────

    def section(self, label: str) -> None:
        self.gap(10)
        c = self.c
        c.setFont("Helvetica-Bold", 8.5)
        c.setFillColor(_NAVY)
        c.drawString(ML, self.y, label.upper())
        self.y -= 3
        c.setStrokeColor(_NAVY)
        c.setLineWidth(0.8)
        c.line(ML, self.y, PAGE_W - MR, self.y)
        self.y -= 9

    # ── typography ────────────────────────────────────────────────────────────

    def body(
        self,
        text: str,
        size: float = 9,
        color=None,
        indent: float = 0,
        leading: float = 13,
    ) -> None:
        c = self.c
        c.setFont("Helvetica", size)
        c.setFillColor(color or _SLATE)
        x = ML + indent
        for line in _wrap(text, "Helvetica", size, CW - indent):
            c.drawString(x, self.y, line)
            self.y -= leading
        self.gap(2)

    def kv(
        self,
        label: str,
        value: str,
        lw: float = 145,
        vc=None,
        bold_v: bool = False,
    ) -> None:
        v = str(value).strip()
        if not v or v in ("None", "null", "Unavailable"):
            return
        c = self.c
        c.setFont("Helvetica-Bold", 8.5)
        c.setFillColor(_GRAY)
        c.drawString(ML, self.y, label)
        c.setFont("Helvetica-Bold" if bold_v else "Helvetica", 8.5)
        c.setFillColor(vc or _SLATE)
        c.drawString(ML + lw, self.y, v[:90])
        self.y -= 13

    def bullet(self, text: str, color=None) -> None:
        c = self.c
        c.setFillColor(color or _SLATE)
        c.setFont("Helvetica", 8.5)
        c.drawString(ML + 6, self.y, "-")
        for i, line in enumerate(_wrap(text, "Helvetica", 8.5, CW - 16)):
            c.drawString(ML + 16, self.y, line)
            self.y -= 12
        self.gap(1)

    def hline(self, color=None) -> None:
        c = self.c
        c.setStrokeColor(color or _LINE)
        c.setLineWidth(0.4)
        c.line(ML, self.y, PAGE_W - MR, self.y)
        self.gap(6)

    def two_col(self, pairs: List[Tuple[str, str]], lw: float = 110) -> None:
        """Two-column key-value grid."""
        c    = self.c
        cw   = CW / 2
        half = math.ceil(len(pairs) / 2)
        left  = pairs[:half]
        right = pairs[half:]
        for i in range(half):
            base = self.y
            if i < len(left):
                lb, vb = left[i]
                vb = str(vb).strip()
                if vb and vb not in ("None", "null", "Unavailable"):
                    c.setFont("Helvetica-Bold", 8)
                    c.setFillColor(_GRAY)
                    c.drawString(ML, base, lb)
                    c.setFont("Helvetica", 8)
                    c.setFillColor(_SLATE)
                    c.drawString(ML + lw, base, vb[:38])
            if i < len(right):
                lb, vb = right[i]
                vb = str(vb).strip()
                if vb and vb not in ("None", "null", "Unavailable"):
                    c.setFont("Helvetica-Bold", 8)
                    c.setFillColor(_GRAY)
                    c.drawString(ML + cw, base, lb)
                    c.setFont("Helvetica", 8)
                    c.setFillColor(_SLATE)
                    c.drawString(ML + cw + lw, base, vb[:38])
            self.y -= 13


# ─── Valuation bar ────────────────────────────────────────────────────────────

def _draw_val_bar(doc: _Doc, low: float, mid: float, high: float) -> None:
    c  = doc.c
    bx = ML
    by = doc.y - 26
    bw = CW
    bh = 18
    spread = max(high - low, 1.0)

    # Track background
    c.setFillColor(_MGRAY)
    c.roundRect(bx, by, bw, bh, 4, fill=1, stroke=0)

    # Filled range (light blue)
    c.setFillColor(_BLUE_BAR)
    c.roundRect(bx, by, bw, bh, 4, fill=1, stroke=0)

    # Mid-point marker
    mid_frac = max(0.0, min(1.0, (mid - low) / spread))
    mid_x    = bx + mid_frac * bw
    c.setFillColor(_NAVY_BAR)
    c.rect(mid_x - 2, by - 3, 4, bh + 6, fill=1, stroke=0)

    # Labels
    c.setFont("Helvetica-Bold", 7.5)
    c.setFillColor(_SLATE)
    c.drawString(bx,              by - 13, f"LOW  {_fmt_cur(low)}")
    c.drawCentredString(mid_x,    by - 13, f"MID  {_fmt_cur(mid)}")
    c.drawRightString(bx + bw,    by - 13, f"HIGH  {_fmt_cur(high)}")

    doc.y = by - 28


# ─── KPI tile row ─────────────────────────────────────────────────────────────

_READINESS_LABELS: Dict[str, str] = {
    "GREEN":     "GREEN",
    "YELLOW":    "PARTIAL",
    "RED":       "RED",
    "UW_READY":  "UNDERWRITTEN",
    "NOT_READY": "NEEDS UW",
    "NOT READY": "NEEDS UW",
    "UPSTREAM":  "UPSTREAM",
}


def _readiness_label(r: str) -> str:
    return _READINESS_LABELS.get(r.upper(), r)


def _draw_kpi_tiles(doc: _Doc, fields: Dict[str, Any]) -> None:
    c         = doc.c
    readiness = (fields.get("auction_readiness") or "UNKNOWN").upper()
    diamond   = bool(fields.get("diamond_proxy"))
    rc        = {"GREEN": _GREEN, "YELLOW": _AMBER, "RED": _RED}.get(readiness, _GRAY)
    tiles = [
        ("Falco Score",  _val(fields.get("falco_score_internal"), "—"), _SLATE),
        ("Days to Sale", _val(fields.get("dts_days"), "—"),             _SLATE),
        ("Readiness",    _readiness_label(readiness),                   rc),
        ("AVM Low",      _fmt_cur(fields.get("value_anchor_low")),      _SLATE),
        ("Diamond",      "PASS" if diamond else "FAIL",                 _GREEN if diamond else _RED),
    ]
    n       = len(tiles)
    gap_pts = 6
    tw      = (CW - gap_pts * (n - 1)) / n
    th      = 44
    tx      = ML
    ty      = doc.y - th
    for label, value, vc in tiles:
        c.setFillColor(_TILE_BG)
        c.setStrokeColor(_TILE_BRD)
        c.setLineWidth(0.5)
        c.roundRect(tx, ty, tw, th, 4, fill=1, stroke=1)
        c.setFont("Helvetica", 7)
        c.setFillColor(_GRAY)
        c.drawCentredString(tx + tw / 2, ty + th - 13, label)
        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(vc)
        c.drawCentredString(tx + tw / 2, ty + 9, str(value)[:14])
        tx += tw + gap_pts
    doc.y = ty - 10


# ─── Hero image placeholder ───────────────────────────────────────────────────

def _draw_hero(
    doc: _Doc,
    img_path: Optional[str] = None,
    imagery_date: Optional[str] = None,
) -> None:
    c  = doc.c
    bh = 1.8 * inch
    by = doc.y - bh

    if img_path:
        # Border only (rounded); image drawn inside fills the rect area
        c.setStrokeColor(_LINE)
        c.setLineWidth(0.6)
        c.roundRect(ML, by, CW, bh, 6, fill=0, stroke=1)
        try:
            c.drawImage(img_path, ML, by, CW, bh, preserveAspectRatio=True, anchor="c")
        except Exception:
            # Image unreadable — fall back to placeholder silently
            c.setFillColor(_LGRAY)
            c.roundRect(ML, by, CW, bh, 6, fill=1, stroke=1)
            c.setFont("Helvetica", 9)
            c.setFillColor(_GRAY)
            c.drawCentredString(ML + CW / 2, by + bh / 2 - 4, "[ Property Image ]")
        # Two-line caption: source label + combined imagery date + attribution
        date_str = (imagery_date or "").strip()
        meta_line = (f"Imagery date: {date_str} \u2022 " if date_str else "") + "Street View imagery \u00a9 Google"
        c.setFont("Helvetica", 7)
        c.setFillColor(_GRAY)
        c.drawCentredString(ML + CW / 2, by - 9,  "Exterior image — Google Street View")
        c.drawCentredString(ML + CW / 2, by - 18, meta_line)
    else:
        c.setFillColor(_LGRAY)
        c.setStrokeColor(_LINE)
        c.setLineWidth(0.6)
        c.roundRect(ML, by, CW, bh, 6, fill=1, stroke=1)
        c.setFont("Helvetica", 9)
        c.setFillColor(_GRAY)
        c.drawCentredString(ML + CW / 2, by + bh / 2 - 4, "[ Property Image ]")
        c.setFont("Helvetica", 7)
        c.drawCentredString(ML + CW / 2, by - 9, "No street-level image available")

    doc.y = by - 28


# ─── Narrative intelligence (rule-based, page 1) ──────────────────────────────

def _narrative_intelligence(fields: Dict[str, Any]) -> List[str]:
    """Dense factual sentences derived entirely from structured fields. No fluff."""
    score       = fields.get("falco_score_internal")
    dts         = fields.get("dts_days")
    readiness   = (fields.get("auction_readiness") or "UNKNOWN").upper()
    low         = fields.get("value_anchor_low")
    mid         = fields.get("value_anchor_mid")
    high        = fields.get("value_anchor_high")
    spread_pct  = fields.get("spread_pct")
    uw_pass    = int(fields.get("uw_ready") or 0) == 1
    spread_band = (fields.get("spread_band") or "UNKNOWN").upper()
    diamond     = bool(fields.get("diamond_proxy"))
    equity_band = _val(fields.get("equity_band"), "UNKNOWN")

    score_txt = f"{score}/100" if score is not None else "unscored"
    dts_txt   = f"{dts}d"      if dts is not None   else "—"

    lines: List[str] = [
        f"Score {score_txt}.  DTS {dts_txt}.  Readiness {_readiness_label(readiness)}.  "
        f"Diamond {'PASS' if diamond else 'FAIL'}.  Equity band: {equity_band}.",
    ]

    if any(v is not None for v in (low, mid, high)):
        lines.append(
            f"AVM: low {_fmt_cur(low)} / mid {_fmt_cur(mid)} / high {_fmt_cur(high)}.  "
            f"Spread {_fmt_pct(spread_pct)} ({spread_band})."
        )

    if spread_band == "WIDE":
        lines.append("Wide AVM spread — treat low anchor as floor only; valuation confidence is low.")

    if dts is not None:
        d = int(dts)
        if d < 21:
            lines.append(f"DTS {d}d is inside the 21-day wire threshold — timeline is critical.")
        elif d > 60:
            lines.append(f"DTS {d}d exceeds 60-day window — monitor for auction slippage.")

    if low is not None and float(low) < 300_000:
        lines.append(f"AVM low {_fmt_cur(low)} is below the $300k diamond gate — verify buyer-pool depth.")

    lines.append(
        "Lien balance and equity are unknown. Do not assume positive equity without "
        "independent lien and title research."
    )
    return lines


# ─── Page-1 risk flags ────────────────────────────────────────────────────────

def _draw_p1_risk_flags(doc: _Doc, fields: Dict[str, Any]) -> None:
    flags: List[Tuple[str, str]] = []

    spread_pct = fields.get("spread_pct")
    if spread_pct is not None and spread_pct > 0.18:
        flags.append((f"AVM spread {_fmt_pct(spread_pct)} exceeds 18% — low valuation confidence.", "HIGH"))

    if not fields.get("attom_detail"):
        flags.append(("Property detail record unavailable — physical characteristics unverified.", "MED"))

    readiness = (fields.get("auction_readiness") or "UNKNOWN").upper()
    if readiness != "GREEN":
        sev = "MED" if readiness in ("YELLOW", "UW_READY") else "HIGH"
        flags.append((f"Readiness: {_readiness_label(readiness)} — diamond criteria not yet met.", sev))

    dts = fields.get("dts_days")
    if dts is None or not (21 <= int(dts) <= 60):
        dts_txt = f"{dts} days" if dts is not None else "unknown"
        flags.append((f"DTS {dts_txt} is outside optimal 21–60 day window.", "MED"))

    low = fields.get("value_anchor_low")
    if low is not None and float(low) < 300_000:
        flags.append((f"AVM low {_fmt_cur(low)} is below $300k diamond gate.", "MED"))

    doc.section("Risk Flags")
    if not flags:
        doc.bullet("No automated risk flags. Standard auction due-diligence applies.", color=_GREEN)
    else:
        sev_color = {"HIGH": _RED, "MED": _AMBER, "LOW": _GRAY}
        for flag_text, sev in flags:
            doc.bullet(f"[{sev}] {flag_text}", color=sev_color.get(sev, _SLATE))


# ─── Risk flags ───────────────────────────────────────────────────────────────

def _risk_flags(fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    flags: List[Tuple[str, str]] = []
    if not fields.get("attom_detail"):
        flags.append(("Property detail record unavailable — physical characteristics unverified.", "MED"))
    sb = (fields.get("spread_band") or "").upper()
    if sb == "WIDE":
        flags.append((
            f"AVM spread is WIDE ({_fmt_pct(fields.get('spread_pct'))}) — low valuation confidence.",
            "HIGH",
        ))
    rd = (fields.get("auction_readiness") or "").upper()
    if rd not in ("GREEN",):
        _rd_label = _readiness_label(rd) if rd else "UNKNOWN"
        flags.append((
            f"Readiness: {_rd_label} — diamond criteria not yet met.",
            "MED" if rd in ("YELLOW", "UW_READY") else "HIGH",
        ))
    low = fields.get("value_anchor_low")
    if low is not None and float(low) < 150_000:
        flags.append((f"Estimated value below $150k ({_fmt_cur(low)}) — verify buyer-pool depth.", "MED"))
    st = (fields.get("attom_status") or "").lower()
    if "partial" in st:
        flags.append((f"Enrichment status '{st}' — one data-source endpoint returned no data.", "MED"))
    if fields.get("falco_score_internal") is None:
        flags.append(("Internal Falco score absent — lead not yet scored.", "LOW"))
    return flags


# ─── Narrative generation ─────────────────────────────────────────────────────

def _deterministic_narratives(fields: Dict[str, Any]) -> Dict[str, str]:
    addr        = fields.get("address") or "this property"
    county      = (fields.get("county") or "").strip()
    loc         = f"{addr}, {county} County" if county else addr
    dts         = fields.get("dts_days")
    readiness   = (fields.get("auction_readiness") or "UNKNOWN").upper()
    score       = fields.get("falco_score_internal")
    low         = fields.get("value_anchor_low")
    high        = fields.get("value_anchor_high")
    spread_band = (fields.get("spread_band") or "UNKNOWN").upper()
    spread_pct  = fields.get("spread_pct")
    diamond     = bool(fields.get("diamond_proxy"))
    has_detail  = bool(fields.get("attom_detail"))

    score_txt    = f"{score}/100" if score is not None else "unscored"
    dts_txt      = f"{dts} days" if dts is not None else "timeline pending"
    readiness_adj = {"GREEN": "favorable", "YELLOW": "moderate", "RED": "elevated-risk", "UW_READY": "underwritten"}.get(readiness, "unclassified")
    spread_conf   = {"TIGHT": "strong", "NORMAL": "moderate", "WIDE": "limited"}.get(spread_band, "unknown")
    bid_adj       = {"GREEN": "competitive", "YELLOW": "moderate", "RED": "limited"}.get(readiness, "unknown")

    exec_summary = (
        f"Subject property at {loc} presents a {readiness_adj} auction profile "
        f"with {dts_txt} remaining to sale. "
        f"The automated valuation model (AVM) indicates a value anchor of {_fmt_cur(low)} "
        f"(range: {_fmt_cur(low)}-{_fmt_cur(high)}), reflecting a {spread_band.lower()} valuation range "
        f"({_fmt_pct(spread_pct)} spread). "
        f"Internal Falco scoring reflects {score_txt}. "
        f"Equity position cannot be determined from available data; "
        f"lien balance verification is required prior to any capital commitment."
    )

    bid_sentence = (
        "All diamond proxy criteria are satisfied — this lead meets internal screening gates for auction viability."
        if diamond else
        "This lead does not satisfy all diamond proxy thresholds; "
        "review individual gate criteria before committing capital."
    )
    valuation = (
        f"Auction pricing strategy should anchor to the AVM low of {_fmt_cur(low)}, "
        f"representing a conservative floor consistent with a conservative valuation anchor. "
        f"Expected bidder appetite is {bid_adj} given a {readiness_adj} readiness classification. "
        f"{bid_sentence} "
        f"Equity is unknown; do not assume positive equity absent independent lien and title research."
    )

    risk_items: List[str] = []
    if not has_detail:
        risk_items.append("property detail record unavailable — physical characteristics are unverified")
    if spread_band == "WIDE":
        risk_items.append(
            f"AVM spread of {_fmt_pct(spread_pct)} exceeds 18% — low valuation confidence"
        )
    if readiness not in ("GREEN",):
        risk_items.append(f"auction readiness is {_readiness_label(readiness)} — diamond criteria not yet met")
    if low is not None and float(low) < 150_000:
        risk_items.append("estimated value below $150k — verify market depth and institutional buyer pool")
    if not risk_items:
        risk_items.append("no automated flags; standard auction due-diligence requirements apply")

    risk = (
        f"Material data gaps and risk factors for {addr}: "
        + "; ".join(risk_items) + ". "
        "Lien balance, occupancy status, and physical condition are absent from this dataset "
        "and must be independently verified before committing capital. "
        "This brief is generated from automated data sources and does not constitute "
        "a legal or financial opinion."
    )

    return {"exec_summary": exec_summary, "valuation": valuation, "risk": risk}


def _ai_narratives(fields: Dict[str, Any], api_key: str, model: str) -> Dict[str, str]:
    import requests as _req

    ctx = "\n".join([
        f"Property: {fields.get('address')}, {fields.get('county')} County, {fields.get('state')}",
        f"Days to Sale: {fields.get('dts_days')}",
        f"Auction Readiness: {fields.get('auction_readiness')}",
        f"Falco Score (0-100): {fields.get('falco_score_internal')}",
        f"Equity Band: {fields.get('equity_band')}",
        f"AVM Low: {_fmt_cur(fields.get('value_anchor_low'))}",
        f"AVM Mid: {_fmt_cur(fields.get('value_anchor_mid'))}",
        f"AVM High: {_fmt_cur(fields.get('value_anchor_high'))}",
        f"Spread: {_fmt_pct(fields.get('spread_pct'))} ({fields.get('spread_band')})",
        f"Diamond Proxy: {'PASS' if fields.get('diamond_proxy') else 'FAIL'}",
        f"Property Detail Available: {'Yes' if fields.get('attom_detail') else 'No'}",
        "NOTE: Lien balance / equity is NOT known. Do not infer.",
    ])
    prompt = (
        "You are an institutional real estate underwriter. Write three short paragraphs "
        "in conservative professional tone — no hype, reference only provided data:\n"
        "1. exec_summary: executive summary for investor (mention timeline, value anchor, spread, readiness, score).\n"
        "2. valuation: how to price at auction, expected bidder appetite, risks.\n"
        "3. risk: what is missing, what to verify, key risks.\n"
        "Always note equity is unknown unless lien data is present.\n"
        "Return ONLY valid JSON: "
        '{"exec_summary": "...", "valuation": "...", "risk": "..."}\n\nDATA:\n' + ctx
    )

    resp = _req.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 600,
            "temperature": 0.3,
        },
        timeout=20,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"].strip()
    # Strip markdown code fences if present
    if content.startswith("```"):
        lines = content.split("\n")
        content = "\n".join(lines[1:])
        if content.rstrip().endswith("```"):
            content = content.rstrip()[:-3].strip()
    return json.loads(content)


def generate_narratives(fields: Dict[str, Any]) -> Dict[str, str]:
    """
    Returns {exec_summary, valuation, risk} paragraphs.
    Uses OpenAI when FALCO_OPENAI_API_KEY is set; otherwise deterministic templates.
    FALCO_OPENAI_MODEL defaults to gpt-4o-mini.
    Never raises — falls back to templates on any AI failure.
    """
    api_key = os.getenv("FALCO_OPENAI_API_KEY", "").strip()
    model   = os.getenv("FALCO_OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
    if api_key:
        try:
            return _ai_narratives(fields, api_key, model)
        except Exception as e:
            print(f"[PDF_BUILDER][WARN] AI narrative failed ({type(e).__name__}: {e}), using fallback.")
    return _deterministic_narratives(fields)


def _fetch_notice_contact(lead_key: str) -> Dict[str, str]:
    """
    Query lead_field_provenance for the latest notice contact fields for this lead.
    Returns a dict with whatever keys were found (notice_phone, notice_email,
    notice_trustee_firm, notice_trustee_name_raw, notice_trustee_address).
    Never raises; returns {} on any error (missing DB, missing table, etc.).
    """
    import sqlite3 as _sq3
    result: Dict[str, str] = {}
    try:
        _db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
        if not os.path.exists(_db):
            return result
        _con = _sq3.connect(_db)
        try:
            for _field in (
                "notice_phone",
                "notice_email",
                "notice_trustee_firm",
                "notice_trustee_name_raw",
                "notice_trustee_address",
            ):
                _row = _con.execute(
                    """
                    SELECT field_value_text FROM lead_field_provenance
                    WHERE lead_key = ? AND field_name = ?
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (lead_key, _field),
                ).fetchone()
                if _row and _row[0]:
                    result[_field] = str(_row[0])
        except Exception:
            pass
        finally:
            _con.close()
    except Exception:
        pass
    return result


def _fetch_prov_fields(lead_key: str, field_names: List[str]) -> Dict[str, str]:
    """
    Generic single-field-per-name provenance fetch (latest created_at wins).
    Same pattern as _fetch_notice_contact. Never raises; returns {} on error.
    """
    import sqlite3 as _sq3
    result: Dict[str, str] = {}
    try:
        _db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
        if not os.path.exists(_db):
            return result
        _con = _sq3.connect(_db)
        try:
            for _field in field_names:
                _row = _con.execute(
                    """
                    SELECT field_value_text FROM lead_field_provenance
                    WHERE lead_key = ? AND field_name = ?
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (lead_key, _field),
                ).fetchone()
                if _row and _row[0]:
                    result[_field] = str(_row[0])
        except Exception:
            pass
        finally:
            _con.close()
    except Exception:
        pass
    return result


def _sanitize_trustee(s: Optional[str]) -> Optional[str]:
    """
    Return s if it looks like clean text, else None.
    Rejects strings containing the Unicode replacement character (garbled decode)
    or more than 20 % non-printable / C1-control bytes.
    """
    if not s:
        return None
    if "\ufffd" in s:
        return None
    non_print = sum(1 for c in s if ord(c) < 32 or 0x7F <= ord(c) <= 0x9F)
    if non_print > len(s) * 0.20:
        return None
    return s.strip() or None


# ─── Foreclosure notice fetch & clean ─────────────────────────────────────────

_NAV_LINES = frozenset({
    "skip to content", "home", "about us", "legal compliance",
    "services", "login", "register", "browse notices",
})


def _clean_notice_html(raw: str) -> str:
    """Strip HTML → readable plain text, drop site chrome. Uses stdlib only."""
    import re as _re, html as _html
    raw = _re.sub(r"<(script|style)[^>]*>.*?</\1>", "", raw,
                  flags=_re.DOTALL | _re.IGNORECASE)
    raw = _re.sub(r"<br\s*/?>", "\n", raw, flags=_re.IGNORECASE)
    raw = _re.sub(r"</p>", "\n", raw, flags=_re.IGNORECASE)
    raw = _re.sub(
        r"</?(?:div|li|tr|td|th|h[1-6]|blockquote|section|article)[^>]*>",
        "\n", raw, flags=_re.IGNORECASE,
    )
    raw = _re.sub(r"<[^>]+>", "", raw)
    raw = _html.unescape(raw)
    # Strip, drop nav lines, collapse consecutive blank lines to at most one
    lines = [ln.strip() for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln.lower() not in _NAV_LINES]
    clean: list = []
    for ln in lines:
        if ln == "" and clean and clean[-1] == "":
            continue
        clean.append(ln)
    text = "\n".join(clean).strip()
    # Prefer to start at "Notice Text:" anchor; fall back to "Reference :" or "WHEREAS"
    for anchor in (r"Notice\s+Text\s*:", r"Reference\s*:", r"\bWHEREAS\b"):
        m = _re.search(anchor, text, _re.IGNORECASE)
        if m:
            text = text[m.start():].strip()
            break
    return text


def _fetch_lp_notice(lead_key: str):
    """
    Fetch LIS_PENDENS_HTML artifact + matching ingest_event for a lead.
    Returns (source_url, sale_date_iso, rj_dict, cleaned_text). Never raises.
    """
    import sqlite3 as _sq3
    source_url: Optional[str] = None
    sale_date:  Optional[str] = None
    rj:  Dict[str, Any] = {}
    text = ""
    try:
        _db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
        if not os.path.exists(_db):
            return source_url, sale_date, rj, text
        _con = _sq3.connect(_db)
        try:
            _row = _con.execute(
                "SELECT source_url, payload FROM raw_artifacts"
                " WHERE lead_key = ? AND channel = 'LIS_PENDENS_HTML'"
                " ORDER BY retrieved_at DESC LIMIT 1",
                (lead_key,),
            ).fetchone()
            if _row:
                source_url = _row[0]
                _pl = _row[1]
                if isinstance(_pl, (bytes, bytearray)):
                    _pl = _pl.decode("utf-8", errors="replace")
                if _pl:
                    text = _clean_notice_html(str(_pl))
            _ie = _con.execute(
                "SELECT sale_date, raw_json FROM ingest_events"
                " WHERE lead_key = ? AND source = 'LIS_PENDENS'"
                " ORDER BY ingested_at DESC LIMIT 1",
                (lead_key,),
            ).fetchone()
            if _ie:
                sale_date = _ie[0]
                try:
                    rj = json.loads(_ie[1] or "{}")
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            _con.close()
    except Exception:
        pass
    return source_url, sale_date, rj, text


def _extract_lp_contact(lead_key: str) -> Dict[str, Optional[str]]:
    """
    Derive trustee name, phone, and email for Page-1 Notice Contact from the
    LIS_PENDENS_HTML artifact + ingest_event raw_json.  Supplements
    _fetch_notice_contact (provenance table) which is often empty until notice
    fields are explicitly written.  Never raises.
    """
    import re as _re
    out: Dict[str, Optional[str]] = {"trustee": None, "phone": None, "email": None}
    try:
        _, _, rj, text = _fetch_lp_notice(lead_key)
        out["trustee"] = (rj.get("trustee") or "").strip() or None
        if text:
            _m = _re.search(r'\(?\d{3}\)?[ .\-]\d{3}[ .\-]\d{4}', text)
            if _m:
                out["phone"] = _m.group(0).strip()
            _m = _re.search(r'[\w.+\-]+@[\w.\-]+\.[a-zA-Z]{2,}', text)
            if _m:
                out["email"] = _m.group(0).strip()
    except Exception:
        pass
    return out


def _extract_lp_property_fields(lead_key: str) -> Dict[str, Optional[str]]:
    """
    Extract city, zip, and parcel_id from LP notice text so the Property
    Snapshot page can display them even when ATTOM enrichment is absent.
    Never raises.
    """
    import re as _re
    out: Dict[str, Optional[str]] = {"city": None, "zip": None, "parcel_id": None}
    try:
        _, _, _rj, text = _fetch_lp_notice(lead_key)
        if not text:
            return out
        # Parcel / Tax ID  e.g. "Parcel ID: 041 08 0 026.00" / "Tax Parcel No. 041080026"
        _m = _re.search(
            r'(?:Parcel\s+(?:ID|No\.?)|Tax\s+(?:Parcel|ID)|APN)[:\s]+([0-9A-Z][^\n,]{3,40})',
            text, _re.IGNORECASE,
        )
        if _m:
            out["parcel_id"] = _m.group(1).strip().rstrip(".")
        # City + Zip from Tennessee address pattern  "Nashville, TN 37207"
        _m = _re.search(r'([A-Za-z][A-Za-z\s]{2,28}),\s*TN\s+(\d{5})', text, _re.IGNORECASE)
        if _m:
            out["city"] = _m.group(1).strip()
            out["zip"]  = _m.group(2).strip()
    except Exception:
        pass
    return out


def _fetch_manual_uw(lead_key: str) -> Optional[Dict[str, Any]]:
    """
    Query manual_underwriting for the latest row for this lead.
    Returns a uw_json-compatible dict, or None if no row found. Never raises.
    """
    import sqlite3 as _sq3
    try:
        _db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
        if not os.path.exists(_db):
            return None
        _con = _sq3.connect(_db)
        _con.row_factory = _sq3.Row
        try:
            _row = _con.execute(
                "SELECT value_low, value_high, max_bid, occupancy, condition, strategy, notes"
                " FROM manual_underwriting WHERE lead_key = ? ORDER BY id DESC LIMIT 1",
                (lead_key,),
            ).fetchone()
            if not _row:
                return None
            _vh = _row["value_high"]
            _vl = _row["value_low"]
            _avm_conf: Optional[str] = None
            for _v in (_vh, _vl):
                if _v is not None:
                    try:
                        _avm_conf = f"${float(_v):,.0f}"
                    except Exception:
                        _avm_conf = str(_v)
                    break
            return {
                "numbers": {k: v for k, v in {
                    "avm_confidence": _avm_conf,
                    "max_bid": _row["max_bid"],
                }.items() if v is not None},
                "occupancy":     {"status": _row["occupancy"]} if _row["occupancy"] else {},
                "condition":     {"status": _row["condition"]} if _row["condition"] else {},
                "exit_strategy": _row["strategy"],
                "notes":         _row["notes"],
                "_meta":         {"source": "manual_underwriting"},
            }
        except Exception:
            return None
        finally:
            _con.close()
    except Exception:
        return None


# ─── Page renderers ───────────────────────────────────────────────────────────

def _page1_executive(
    doc: _Doc,
    fields: Dict[str, Any],
    brief: Dict[str, Any],
    img_path: Optional[str] = None,
) -> None:
    addr   = _val(fields.get("address"), "Address Unavailable")
    state  = _val(fields.get("state"), "TN")

    county_raw   = fields.get("county") or ""
    county_clean = county_raw.strip()

    if county_clean:
        if county_clean.lower().endswith("county"):
            loc = f"{county_clean}, {state}"
        else:
            loc = f"{county_clean} County, {state}"
    else:
        loc = state
    doc.cover_header(addr, loc)
    doc.page_header("Executive Summary — Acquisition Snapshot", subtitle=loc)

    # Parse UW data once — used across multiple page-1 sections
    _uw: Dict[str, Any] = {}
    try:
        _uw_raw = fields.get("uw_json") or ""
        if _uw_raw:
            _uw = json.loads(_uw_raw) if isinstance(_uw_raw, str) else (dict(_uw_raw) if isinstance(_uw_raw, dict) else {})
    except Exception:
        pass
    def _uw_obj(k: str) -> Dict[str, Any]:
        v = _uw.get(k)
        return v if isinstance(v, dict) else {}
    _uw_nums  = _uw_obj("numbers")
    _uw_occ   = _uw_obj("occupancy")
    _uw_cond  = _uw_obj("condition")
    _uw_notes = str(_uw.get("access_notes") or _uw.get("notes") or "").strip()

    # 1) Property Overview
    doc.section("Property Overview")
    doc.kv("Address",      addr)
    doc.kv("County",       _val(fields.get("county")))
    doc.kv("Property Type", _val(fields.get("property_type")))
    doc.kv("Year Built",   _val(fields.get("year_built")))
    bsqft = fields.get("building_area_sqft")
    if bsqft is not None:
        try:
            doc.kv("Living SqFt", f"{int(bsqft):,}")
        except (TypeError, ValueError):
            pass
    doc.gap(6)

    # 2) Distress & Timeline
    doc.section("Distress & Timeline")
    doc.kv("Distress Lane",     _val(fields.get("distress_lane")))
    doc.kv("Sale Date",         _val(fields.get("sale_date_iso") or fields.get("sale_date")))
    doc.kv("Days to Sale",      _val(fields.get("dts_days")), bold_v=True)
    doc.kv("Enrichment Status", _val(fields.get("attom_status")))
    doc.gap(6)

    # 3) Valuation Snapshot
    low        = fields.get("value_anchor_low")
    mid        = fields.get("value_anchor_mid")
    high       = fields.get("value_anchor_high")
    spread_pct = fields.get("spread_pct")
    doc.section("Valuation Snapshot")
    # Always show manual UW value when present — not gated on AVM being absent
    _uw_avm_conf = str(_uw_nums.get("avm_confidence") or "").strip()
    _mb_raw = _uw_nums.get("max_bid")
    try:
        _uw_bid_fmt = f"${float(_mb_raw):,.0f}" if _mb_raw is not None else ""
    except (TypeError, ValueError):
        _uw_bid_fmt = str(_mb_raw).strip() if _mb_raw is not None else ""
    if _uw_avm_conf:
        doc.kv("Manual UW Value", _uw_avm_conf, bold_v=True)
        doc.kv("Valuation Source", "Manual Underwriting")
    elif low is None and mid is None and high is None:
        doc.body("Value: NEEDS UW (no ATTOM yet)", size=8.5, color=_AMBER)
    doc.kv("AVM Low",     _fmt_cur(low))
    doc.kv("AVM Mid",     _fmt_cur(mid))
    doc.kv("AVM High",    _fmt_cur(high))
    doc.kv("AVM Spread",  _fmt_pct(spread_pct) if spread_pct is not None else "N/A")
    if spread_pct is not None:
        if spread_pct <= 0.10:
            _val_conf = "HIGH"
        elif spread_pct <= 0.18:
            _val_conf = "MODERATE"
        else:
            _val_conf = "LOW"
    else:
        _val_conf = "UNKNOWN"
    doc.kv("Valuation Confidence", _val_conf)
    doc.gap(6)

    # 4) Equity Position
    doc.section("Equity Position")
    _lien = _extract_lien_skeleton(fields)
    if _lien["equity_proxy_low"] is not None:
        doc.kv("Equity Proxy (AVM Low - Total Orig)", f"${_lien['equity_proxy_low']:,.0f}", lw=200)
    elif _lien["total_amount"] is not None:
        doc.kv("Total Orig Mortgages", f"${_lien['total_amount']:,.0f}")
        doc.body("Equity proxy unavailable — verify AVM low + title.", size=8.5, color=_AMBER)
    else:
        doc.body(
            "Mortgage position unavailable via automated sources. Independent title verification required prior to bid execution.",
            size=8.5,
            color=_AMBER,
        )
    doc.gap(6)

    # 5) Top Risk Flags
    doc.section("Top Risk Flags")
    flags = _risk_flags(fields)
    if not flags:
        doc.bullet("No automated red flags triggered.", color=_GREEN)
    else:
        sev_color = {"HIGH": _RED, "MED": _AMBER, "LOW": _GRAY}
        for flag_text, sev in flags[:3]:
            doc.bullet(f"[{sev}] {flag_text}", color=sev_color.get(sev, _SLATE))
    if _uw_occ.get("status"):
        doc.kv("Occupancy (UW)", str(_uw_occ["status"]), bold_v=True)
    if _uw_cond.get("status"):
        doc.kv("Condition (UW)", str(_uw_cond["status"]), bold_v=True)
    doc.gap(6)

    # 6) Bid Guidance
    doc.section("Bid Guidance")
    if low is not None:
        try:
            cap = float(low) * 0.85
            doc.kv(
                "Indicative Conservative Bid Cap",
                f"${cap:,.0f}  (subject to title + inspection)",
                lw=200,
            )
        except (TypeError, ValueError):
            doc.body("Bid cap unavailable — AVM low not numeric.", size=8.5, color=_AMBER)
        if _uw_bid_fmt:
            doc.kv("UW Max Bid", _uw_bid_fmt, bold_v=True)
    elif _uw_bid_fmt:
        doc.kv("Recommended Max Bid (Manual UW)", _uw_bid_fmt, lw=210, bold_v=True)
    else:
        doc.body("AVM low unavailable — bid cap cannot be computed.", size=8.5, color=_AMBER)
    doc.gap(8)

    # 7) Notice Contact (Foreclosure Notice)
    doc.section("Notice Contact (Foreclosure Notice)")
    _lead_key = fields.get("lead_key") or ""
    _nc = _fetch_notice_contact(_lead_key)
    _ft = _fetch_prov_fields(
        _lead_key,
        ["ft_trustee_firm", "ft_trustee_person", "ft_trustee_name_raw"],
    )

    # Supplement from LP notice artifact — fills phone/email/trustee when
    # the lead_field_provenance table has no notice contact rows yet.
    _lp = _extract_lp_contact(_lead_key)
    if _lp.get("phone") and not _nc.get("notice_phone"):
        _nc["notice_phone"] = _lp["phone"]
    if _lp.get("email") and not _nc.get("notice_email"):
        _nc["notice_email"] = _lp["email"]

    # Build trustee display: priority ft > notice > lp_rj, sanitize each candidate.
    _trustee_display: Optional[str] = None
    # a) ft_trustee_firm (+ " / " + ft_trustee_person if both present)
    _ft_firm   = _sanitize_trustee(_ft.get("ft_trustee_firm"))
    _ft_person = _sanitize_trustee(_ft.get("ft_trustee_person"))
    if _ft_firm:
        _trustee_display = (_ft_firm + " / " + _ft_person) if _ft_person else _ft_firm
    # b) notice_trustee_firm
    if not _trustee_display:
        _trustee_display = _sanitize_trustee(_nc.get("notice_trustee_firm"))
    # c) notice_trustee_name_raw
    if not _trustee_display:
        _trustee_display = _sanitize_trustee(_nc.get("notice_trustee_name_raw"))
    # d) LP notice artifact — bot-parsed trustee from ingest_event raw_json
    if not _trustee_display:
        _trustee_display = _sanitize_trustee(_lp.get("trustee"))

    _nc_has_any = bool(_trustee_display) or any(
        _nc.get(k) for k in ("notice_phone", "notice_email", "notice_trustee_address")
    )
    if _nc_has_any:
        if _trustee_display:
            doc.kv("Trustee / Attorney", _trustee_display, bold_v=True)
        if _nc.get("notice_trustee_address"):
            doc.kv("Mailing Address", _nc["notice_trustee_address"])
        if _nc.get("notice_email"):
            doc.kv("Notice Email", _nc["notice_email"])
        if _nc.get("notice_phone"):
            doc.kv("Notice Phone", _nc["notice_phone"], bold_v=True)
    else:
        doc.bullet("No contact found in notice artifacts yet.")
    doc.gap(6)

    # 7) Execution Notes — exit strategy + analyst notes from Manual UW
    _es_p1 = str(_uw.get("exit_strategy") or "").strip()
    if _es_p1 or _uw_notes:
        doc.section("Execution Notes")
        if _es_p1:
            _STRAT_P1 = {
                "auction_retail": "Auction / Retail",
                "wholesale":      "Wholesale",
                "fix_flip":       "Fix & Flip",
                "buy_hold":       "Buy & Hold",
                "assign":         "Assignment",
            }
            doc.kv("Exit Strategy", _STRAT_P1.get(_es_p1.lower().replace(" ", "_"), _es_p1), bold_v=True)
        if _uw_notes:
            doc.body(f"Analyst Notes: {_uw_notes[:200]}", size=8.5, color=_SLATE, leading=12)
        doc.gap(6)

    _draw_due_diligence_checklist(doc, fields, low)


def _draw_due_diligence_checklist(doc: _Doc, fields: Dict[str, Any], avm_low: Any) -> None:
    """
    Fixed partner-facing checklist micro-box (Page 1).
    Purpose: force institutional bid discipline without implying missing artifacts exist.
    """
    box_h = 98
    if doc.space_left() < (box_h + 18):
        doc.new_page()

    c = doc.c
    x = ML
    y_top = doc.y
    w = CW

    # Frame
    c.setStrokeColor(_LINE)
    c.setLineWidth(0.6)
    c.rect(x, y_top - box_h, w, box_h, fill=0, stroke=1)

    # Title band
    title_h = 16
    c.setFillColor(colors.HexColor("#F5F7FB"))
    c.rect(x, y_top - title_h, w, title_h, fill=1, stroke=0)
    c.setFont("Helvetica-Bold", 8.5)
    c.setFillColor(_NAVY)
    c.drawString(x + 8, y_top - 11.5, "DUE DILIGENCE CHECKLIST (PRE-BID)")

    # Content
    c.setFont("Helvetica", 8.25)
    c.setFillColor(_SLATE)

    lane = str(fields.get("distress_lane") or "").strip()
    sale = str(fields.get("sale_date_iso") or fields.get("sale_date") or "").strip()

    bid_cap_txt = "N/A"
    try:
        if avm_low is not None:
            bid_cap_txt = f"${(float(avm_low) * 0.85):,.0f}"
    except Exception:
        bid_cap_txt = "N/A"
    if bid_cap_txt == "N/A":
        try:
            _uw_bc = json.loads(fields.get("uw_json") or "{}") or {}
            if isinstance(_uw_bc, dict):
                _uw_bc_nums = _uw_bc.get("numbers") if isinstance(_uw_bc.get("numbers"), dict) else {}
                _mb = _uw_bc_nums.get("max_bid")
                if _mb is not None:
                    bid_cap_txt = f"${float(_mb):,.0f} (Manual UW)"
        except Exception:
            pass

    lines = [
        "\u25a1 Title + lien payoff verified (full chain, open liens, HOA if applicable)",
        "\u25a1 Occupancy verified (occupied/vacant/tenant) + eviction risk priced in",
        "\u25a1 Condition verified (exterior/interior if possible) + rehab reserve set",
        "\u25a1 Taxes verified (delinquent, redemption windows, penalties, municipal liens)",
        "\u25a1 Auction terms confirmed (deposit, buyer\u2019s premium, closing timeline)",
        f"\u25a1 Bid cap set + approved (indicative cap: {bid_cap_txt})",
    ]

    ctx = []
    if lane:
        ctx.append(lane)
    if sale:
        ctx.append(f"Sale: {sale}")
    ctx_line = " | ".join(ctx)[:95] if ctx else ""

    y = y_top - title_h - 12
    if ctx_line:
        c.setFont("Helvetica-Oblique", 7.5)
        c.setFillColor(_GRAY)
        c.drawString(x + 8, y, ctx_line)
        y -= 11
        c.setFont("Helvetica", 8.25)
        c.setFillColor(_SLATE)

    for ln in lines:
        c.drawString(x + 10, y, ln)
        y -= 12

    doc.y = y_top - box_h - 6


def _page2_valuation(doc: _Doc, fields: Dict[str, Any], brief: Dict[str, Any]) -> None:
    doc.page_header("Valuation Analysis")

    low  = fields.get("value_anchor_low")
    mid  = fields.get("value_anchor_mid")
    high = fields.get("value_anchor_high")

    doc.section("Valuation Range")
    if low is not None and mid is not None and high is not None:
        _draw_val_bar(doc, float(low), float(mid), float(high))
    else:
        # Fall back to Manual UW numbers when ATTOM AVM is absent
        _uw2: Dict[str, Any] = {}
        try:
            _uw2 = json.loads(fields.get("uw_json") or "{}") or {}
            if not isinstance(_uw2, dict):
                _uw2 = {}
        except Exception:
            pass
        _uw2_nums = _uw2.get("numbers") if isinstance(_uw2.get("numbers"), dict) else {}
        _uw2_conf = str(_uw2_nums.get("avm_confidence") or "").strip()
        _uw2_bid_r = _uw2_nums.get("max_bid")
        try:
            _uw2_bid = f"${float(_uw2_bid_r):,.0f}" if _uw2_bid_r is not None else ""
        except (TypeError, ValueError):
            _uw2_bid = ""
        if _uw2_conf or _uw2_bid:
            doc.kv("Value Basis", "Manual Underwriting", bold_v=True)
            if _uw2_conf:
                doc.kv("Manual UW Value Anchor", _uw2_conf, bold_v=True)
            if _uw2_bid:
                doc.kv("Recommended Max Bid", _uw2_bid, bold_v=True)
        else:
            doc.body("Valuation range data unavailable.", size=9, color=_AMBER)
    doc.gap(10)

    doc.section("Auction Pricing Guidance")
    doc.body(brief.get("auction_positioning", "Pricing guidance unavailable."), size=9, leading=13)
    doc.gap(10)

    doc.section("Liquidity Analysis")
    doc.body(brief.get("liquidity_analysis", "Liquidity analysis unavailable."), size=9, color=_AMBER, leading=13)
    doc.gap(10)

    doc.section("Internal Comps Proxy (SQLite)")
    comps = fields.get("internal_comps") or []
    if not comps:
        doc.body("No comparable internal auction leads currently in the FALCO database for this geography and valuation band.", size=9, color=_GRAY)
    else:
        for comp in comps[:6]:
            avm = comp.get("avm_value") or comp.get("avm_low")
            avm_str  = f"${float(avm):,.0f}" if avm is not None else "N/A"
            dts_str  = str(comp["dts"])  if comp.get("dts")       is not None else "—"
            date_str = str(comp["sale_date"]) if comp.get("sale_date")          else "—"
            addr_str = (str(comp.get("address") or "")).strip() or "—"
            doc.body(
                f"AVM {avm_str}  |  DTS {dts_str}  |  {date_str}  |  {addr_str}",
                size=8, color=_SLATE, leading=12,
            )


def _extract_owner_mortgage(fields: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Extract owner/mortgage fields from raw_merged['owner'] and raw_merged['mortgage']."""
    out: Dict[str, Optional[str]] = {
        "owner_name":       None,
        "owner_mail":       None,
        "last_sale_date":   None,
        "mortgage_lender":  None,
        "mortgage_amount":  None,
        "mortgage_date":    None,
    }
    raw_json = fields.get("attom_raw_json")
    if not raw_json:
        return out
    try:
        blob = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        if not isinstance(blob, dict):
            return out
    except Exception:
        return out

    owner_blob = blob.get("owner")
    if isinstance(owner_blob, dict):
        _ow = owner_blob.get("owner") or {}
        if isinstance(_ow, dict):
            _ow1 = _ow.get("owner1") or {}
            if isinstance(_ow1, dict):
                full = _ow1.get("fullName") or " ".join(
                    filter(None, [_ow1.get("firstName"), _ow1.get("lastName")])
                ) or None
                out["owner_name"] = full or None
            _mail = _ow.get("mailAddress") or {}
            if isinstance(_mail, dict):
                out["owner_mail"] = _mail.get("oneLine") or None
        _sale = owner_blob.get("sale") or {}
        if isinstance(_sale, dict):
            out["last_sale_date"] = _sale.get("saleTransDate") or None
            if not out["last_sale_date"]:
                hist = _sale.get("salesHistory")
                if isinstance(hist, list) and hist:
                    out["last_sale_date"] = (hist[0] or {}).get("saleRecDate") or None

    mort_blob = blob.get("mortgage")
    if isinstance(mort_blob, dict):
        _mort = mort_blob.get("mortgage") or {}
        if isinstance(_mort, dict):
            _fm = _mort.get("firstMortgage") or {}
            if isinstance(_fm, dict):
                _ldr = _fm.get("lender") or {}
                out["mortgage_lender"] = (
                    _ldr.get("institution") if isinstance(_ldr, dict) else str(_ldr)
                ) or None
                _amt = _fm.get("amount")
                if _amt is not None:
                    try:
                        out["mortgage_amount"] = f"${float(_amt):,.0f}"
                    except (TypeError, ValueError):
                        out["mortgage_amount"] = str(_amt)
                out["mortgage_date"] = _fm.get("recordingDate") or None

    return out


def _extract_lien_skeleton(fields: Dict[str, Any]) -> Dict[str, Any]:
    """Extract lien skeleton from raw_merged mortgage blob + AVM low."""
    out: Dict[str, Any] = {
        "first_lender":    None,
        "first_amount":    None,   # float or None
        "second_amount":   None,   # float or None
        "total_amount":    None,   # float or None
        "equity_proxy_low": None,  # float or None
    }
    raw_json = fields.get("attom_raw_json")
    if not raw_json:
        return out
    try:
        blob = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        if not isinstance(blob, dict):
            return out
    except Exception:
        return out

    mort_blob = blob.get("mortgage")
    if isinstance(mort_blob, dict):
        _mort = mort_blob.get("mortgage") or {}
        if isinstance(_mort, dict):
            _fm = _mort.get("firstMortgage") or {}
            if isinstance(_fm, dict):
                _ldr = _fm.get("lender") or {}
                out["first_lender"] = (
                    _ldr.get("institution") if isinstance(_ldr, dict) else str(_ldr)
                ) or None
                try:
                    out["first_amount"] = float(_fm["amount"]) if _fm.get("amount") is not None else None
                except (TypeError, ValueError):
                    pass
            _sm = _mort.get("secondMortgage") or {}
            if isinstance(_sm, dict):
                try:
                    out["second_amount"] = float(_sm["amount"]) if _sm.get("amount") is not None else None
                except (TypeError, ValueError):
                    pass

    first  = out["first_amount"]
    second = out["second_amount"]
    if first is not None or second is not None:
        out["total_amount"] = (first or 0.0) + (second or 0.0)

    # AVM low: prefer fields["estimated_value_low"], fall back to raw avm blob
    avm_low: Optional[float] = None
    if fields.get("estimated_value_low") is not None:
        try:
            avm_low = float(fields["estimated_value_low"])
        except (TypeError, ValueError):
            pass
    if avm_low is None:
        try:
            avm_low = float(
                blob.get("avm", {}).get("amount", {}).get("low")  # type: ignore[union-attr]
            )
        except (TypeError, ValueError):
            pass
    if avm_low is not None and out["total_amount"] is not None:
        out["equity_proxy_low"] = avm_low - out["total_amount"]

    return out


def _draw_lien_skeleton_section(doc: _Doc, fields: Dict[str, Any]) -> None:
    """Lien Skeleton (ATTOM Mortgage) section — max 6 lines."""
    data = _extract_lien_skeleton(fields)
    doc.section("Lien Skeleton (ATTOM Mortgage)")
    doc.kv("First Lender",        data["first_lender"]  or "Not available")
    doc.kv("First Orig Amount",
           f"${data['first_amount']:,.0f}" if data["first_amount"] is not None else "Not available")
    doc.kv("Second Orig Amount",
           f"${data['second_amount']:,.0f}" if data["second_amount"] is not None else "Not available")
    doc.kv("Total Orig Mortgages",
           f"${data['total_amount']:,.0f}" if data["total_amount"] is not None else "Not available")
    doc.kv("Equity Proxy (AVM Low - Total Orig)",
           f"${data['equity_proxy_low']:,.0f}" if data["equity_proxy_low"] is not None else "Not available",
           lw=200)


def _draw_ownership_section(doc: _Doc, fields: Dict[str, Any]) -> None:
    """Ownership & Mortgage (ATTOM) section — max 8 lines."""
    data = _extract_owner_mortgage(fields)
    doc.section("Ownership & Mortgage (ATTOM)")
    doc.kv("Owner Name",         data["owner_name"]       or "Not available")
    doc.kv("Mailing Address",    data["owner_mail"]        or "Not available")
    doc.kv("Last Transfer Date", data["last_sale_date"]    or "Not available")
    doc.kv("Mortgage Lender",    data["mortgage_lender"]   or "Not available")
    doc.kv("Orig. Amount",       data["mortgage_amount"]   or "Not available")
    doc.kv("Recording Date",     data["mortgage_date"]     or "Not available")


# ─── Property Snapshot page ────────────────────────────────────────────────────

def _page_property_snapshot(
    doc: _Doc,
    fields: Dict[str, Any],
    img_path: Optional[str] = None,
) -> None:
    """
    Page: Property Snapshot.
    Street View image (top, if available) + compact property facts grid.
    Works cleanly with or without ATTOM enrichment — only renders known fields,
    never spams 'Not available' rows.
    """
    doc.page_header("Property Snapshot")

    # ── Exterior image (only when we actually have one) ────────────────────────
    if img_path:
        _draw_hero(doc, img_path=img_path, imagery_date=fields.get("streetview_imagery_date"))
    doc.gap(4)

    # ── Compact facts grid — silently skips unknown fields ─────────────────────
    doc.section("Property Facts")

    # Address as a full-width row (too long for two_col)
    _snap_addr = _val(fields.get("address"), "")
    if _snap_addr:
        doc.kv("Address", _snap_addr, lw=80)

    pairs: List[Tuple[str, str]] = []

    def _snap_add(label: str, v: Any) -> None:
        s = _val(v)
        if s != "Unavailable":
            pairs.append((label, s))

    # Location fields first — most likely to be populated for LP leads
    _snap_add("City",        fields.get("city"))
    _snap_add("ZIP",         fields.get("zip"))
    _snap_add("County",      fields.get("county"))
    _snap_add("Parcel ID",   fields.get("property_identifier") or fields.get("parcel_id"))
    # Physical attributes (may be absent without ATTOM)
    _snap_add("Beds",        fields.get("beds"))
    _snap_add("Baths",       fields.get("baths"))
    bsqft = fields.get("building_area_sqft")
    _snap_add("Living Sqft", f"{int(bsqft):,}" if bsqft is not None else None)
    _snap_add("Year Built",  fields.get("year_built"))

    if pairs:
        doc.two_col(pairs)

    # ── Partial-data note — only when ATTOM detail is absent ───────────────────
    if not fields.get("attom_detail"):
        doc.gap(8)
        doc.body(
            "Property facts partially unavailable — verify via county assessor, MLS, or physical inspection.",
            size=8.5,
            color=_AMBER,
        )


def _page3_property_facts(doc: _Doc, fields: Dict[str, Any]) -> None:
    doc.page_header("Property Facts")

    detail = fields.get("attom_detail")
    if not detail or not isinstance(detail, dict):
        doc.section("Property Record")
        doc.body(
            "Detail unavailable — AVM only. Physical property characteristics were not returned "
            "by the available property-detail record for this address. Bidders should conduct independent "
            "physical inspection prior to any acquisition decision.",
            size=9, color=_AMBER,
        )
        doc.gap(10)
        _draw_ownership_section(doc, fields)
        doc.gap(10)
        _draw_lien_skeleton_section(doc, fields)
        return

    doc.section("Property Record")
    pairs: List[Tuple[str, str]] = []

    def _add(label: str, v: Any) -> None:
        s = _val(v)
        if s != "Unavailable":
            pairs.append((label, s))

    _add("Property Type",    fields.get("property_type"))
    _add("Land Use",         fields.get("land_use"))
    _add("Year Built",       fields.get("year_built"))
    bsqft = fields.get("building_area_sqft")
    _add("Living Area",      f"{int(bsqft):,} sqft" if bsqft is not None else None)
    _add("Lot Size",         fields.get("lot_size"))
    _add("Bedrooms",         fields.get("beds"))
    _add("Bathrooms",        fields.get("baths"))
    _add("Construction",     fields.get("construction_type"))
    _add("City",             fields.get("city"))
    _add("ZIP",              fields.get("zip"))
    _add("Property ID",      fields.get("property_identifier"))

    if pairs:
        doc.two_col(pairs)
    else:
        doc.body("Detail record present but no structured fields extracted.", size=9, color=_AMBER)

    top_keys = sorted(detail.keys())[:24]
    if top_keys:
        doc.gap(10)
        doc.section("Available Detail Keys (property record)")
        doc.body(", ".join(top_keys), size=7.5, color=_GRAY)

    doc.gap(10)
    _draw_ownership_section(doc, fields)
    doc.gap(10)
    _draw_lien_skeleton_section(doc, fields)


def _page4_timeline_risk(doc: _Doc, fields: Dict[str, Any], brief: Dict[str, Any]) -> None:
    doc.page_header("Timeline & Risk Flags")

    doc.section("Sale Timeline")
    doc.kv("Days to Sale",      _val(fields.get("dts_days")), bold_v=True)
    doc.kv("Enriched At",       _val(fields.get("enriched_at")))
    doc.kv("Enrichment Status", _val(fields.get("attom_status")))
    _notice_verified = fields.get("notice_verified")
    if _notice_verified is True:
        doc.kv("Sale Notice Verification", "MANUALLY VERIFIED")
    elif _notice_verified is False:
        doc.kv("Sale Notice Verification", "SCRAPE ONLY — NOT MANUALLY VERIFIED")
    else:
        doc.kv("Sale Notice Verification", "UNVERIFIED")
    doc.gap(6)

    doc.section("Primary Risk Drivers")
    flags = _risk_flags(fields)
    if not flags:
        doc.bullet("No automated risk triggers detected.")
    else:
        sev_color = {"HIGH": _RED, "MED": _AMBER, "LOW": _GRAY}
        for flag_text, sev in flags:
            doc.bullet(f"[{sev}] {flag_text}", color=sev_color.get(sev, _SLATE))

    _lien = _extract_lien_skeleton(fields)

    doc.gap(6)
    _draw_manual_uw_section(doc, fields)
    doc.gap(6)
    doc.section("Capital Commit Conditions")

    if _lien["equity_proxy_low"] is not None:
        doc.body(
            f"Equity proxy based on AVM low less total original mortgage: ${_lien['equity_proxy_low']:,.0f}. "
            "Title confirmation required prior to capital deployment.",
            size=9,
            leading=13,
        )
    elif _lien["total_amount"] is not None:
        doc.body(
            "Original mortgage balances detected. Independent title verification required before bid placement.",
            size=9,
            leading=13,
        )
    else:
        doc.body(
            "Mortgage data unavailable. Full title search required. Do not assume free & clear status.",
            size=9,
            leading=13,
        )


def _draw_manual_uw_section(doc: _Doc, fields: Dict[str, Any]) -> None:
    """
    Manual Underwriting block (operator-entered checklist).
    Reads:
      fields["uw_ready"] (0/1)
      fields["uw_json"] (JSON string)
    """
    uw_ready = fields.get("uw_ready")
    raw = fields.get("uw_json") or ""

    if not raw:
        doc.section("Manual Underwriting")
        doc.body("No manual underwriting notes recorded for this lead.", size=9, color=_AMBER)
        return

    try:
        uw = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(uw, dict):
            raise ValueError("uw_json not a dict")
    except Exception:
        doc.section("Manual Underwriting")
        doc.body("Manual underwriting blob present but unreadable (invalid JSON).", size=9, color=_AMBER)
        return

    doc.section("Manual Underwriting")

    _uw_is_ready = int(uw_ready or 0) == 1
    _uw_label    = "UNDERWRITTEN" if _uw_is_ready else "NEEDS UW"
    sc = _GREEN if _uw_is_ready else _AMBER
    doc.kv("UW Status", _uw_label, vc=sc, bold_v=True)

    meta = uw.get("_meta") if isinstance(uw.get("_meta"), dict) else {}
    if meta:
        doc.kv("Updated At", _val(meta.get("updated_at")))
        doc.kv("Updated By", _val(meta.get("updated_by")))
        doc.kv("Source", _val(meta.get("source")))

    # Core checklist (safe reads)
    def _fmt_obj(x):
        return x if isinstance(x, dict) else {}

    pr = uw.get("priority")
    if pr:
        doc.kv("Priority", str(pr), bold_v=True)

    title = _fmt_obj(uw.get("title_check"))
    if title:
        doc.kv("Title Check", f"{_val(title.get('status'))} ({_val(title.get('source'))})")

    occ = _fmt_obj(uw.get("occupancy"))
    if occ and occ.get("status"):
        _src = occ.get("source")
        doc.kv("Occupancy", f"{occ['status']} ({_src})" if _src else str(occ["status"]))

    cond = _fmt_obj(uw.get("condition"))
    if cond and cond.get("status"):
        _src = cond.get("source")
        doc.kv("Condition", f"{cond['status']} ({_src})" if _src else str(cond["status"]))

    exit_strat = uw.get("exit_strategy")
    if exit_strat:
        _STRAT_DISPLAY = {
            "auction_retail": "Auction / Retail",
            "wholesale":      "Wholesale",
            "fix_flip":       "Fix & Flip",
            "buy_hold":       "Buy & Hold",
            "assign":         "Assignment",
        }
        _es_key = str(exit_strat).lower().replace(" ", "_")
        doc.kv("Exit Strategy", _STRAT_DISPLAY.get(_es_key, str(exit_strat)))

    notes = uw.get("access_notes") or uw.get("notes")
    if notes:
        doc.gap(4)
        doc.body(f"Analyst Notes: {notes}", size=9, leading=13)

    nums = _fmt_obj(uw.get("numbers"))
    if nums:
        doc.gap(4)
        pairs = []
        for k, label in (
            ("avm_confidence", "UW Value"),
            ("repair_estimate", "Repair Est."),
            ("max_bid",        "Max Bid"),
        ):
            v = nums.get(k)
            if v is None:
                continue
            # avm_confidence is pre-formatted ("$X,XXX"); max_bid/repair_estimate are numeric
            if isinstance(v, str) and v.startswith("$"):
                display = v
            else:
                try:
                    display = f"${float(v):,.0f}"
                except (TypeError, ValueError):
                    display = str(v)
            pairs.append((label, display))
        if pairs:
            doc.two_col(pairs)


def _fetch_provenance_data(lead_key: str):
    """
    Fetch latest lead_field_provenance + raw_artifacts rows for a lead.
    Returns (prov_rows, artifact_rows). Safe no-op on any error.
    """
    import sqlite3 as _sq3
    try:
        _db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
        if not os.path.exists(_db):
            return [], []
        _con = _sq3.connect(_db)
        _con.row_factory = _sq3.Row
        try:
            _prov = _con.execute(
                """
                SELECT field_name, value_type, field_value_text, field_value_num,
                       field_value_json, units, source_channel, retrieved_at
                FROM lead_field_provenance
                WHERE lead_key = ?
                ORDER BY prov_id DESC LIMIT 12
                """,
                (lead_key,),
            ).fetchall()
        except Exception:
            _prov = []
        try:
            _arts = _con.execute(
                """
                SELECT channel, source_url, content_type, retrieved_at,
                       length(payload) AS payload_len
                FROM raw_artifacts
                WHERE lead_key = ?
                ORDER BY retrieved_at DESC LIMIT 5
                """,
                (lead_key,),
            ).fetchall()
        except Exception:
            _arts = []
        _con.close()
        return list(_prov), list(_arts)
    except Exception:
        return [], []


def _prov_display_value(row) -> str:
    """Format a lead_field_provenance row's value for display (max 36 chars)."""
    txt = row["field_value_text"]
    if txt is not None:
        return str(txt)[:36]
    num = row["field_value_num"]
    if num is not None:
        if (row["units"] or "").upper() == "USD":
            return f"${float(num):,.0f}"
        return str(round(float(num), 2))[:36]
    j = row["field_value_json"]
    if j:
        return str(j)[:36]
    return "\u2014"


def _fetch_visual_artifacts(lead_key: str) -> List[Dict[str, Any]]:
    """
    Query raw_artifacts for visually renderable payloads tied to this lead.
    Supported content types (in preference order):
      - image/jpeg
      - image/png
      - application/pdf  (payload returned as None — rasterization not supported)
    Returns list of dicts, safe no-op on any DB error.
    """
    import sqlite3 as _sq3
    out: List[Dict[str, Any]] = []
    try:
        _db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
        if not os.path.exists(_db):
            return out
        _con = _sq3.connect(_db)
        _con.row_factory = _sq3.Row
        try:
            _rows = _con.execute(
                """
                SELECT channel, source_url, content_type, retrieved_at, payload
                FROM raw_artifacts
                WHERE lead_key = ?
                  AND content_type IN ('image/jpeg', 'image/png', 'application/pdf')
                ORDER BY
                    CASE content_type
                        WHEN 'image/jpeg'       THEN 1
                        WHEN 'image/png'        THEN 2
                        WHEN 'application/pdf'  THEN 3
                    END,
                    retrieved_at DESC
                """,
                (lead_key,),
            ).fetchall()
            for _r in _rows:
                _ct = _r["content_type"] or ""
                out.append({
                    "channel":      _r["channel"],
                    "source_url":   _r["source_url"],
                    "content_type": _ct,
                    "retrieved_at": _r["retrieved_at"],
                    # Don't carry PDF bytes — rasterization not supported
                    "payload": _r["payload"] if _ct != "application/pdf" else None,
                })
        finally:
            _con.close()
    except Exception:
        pass
    return out


def _pages_notice_exhibit_images(doc: _Doc, fields: Dict[str, Any]) -> None:
    """
    Render raw_artifacts image payloads (image/jpeg, image/png) as full-page
    exhibit pages.  application/pdf artifacts render a placeholder noting
    rasterization is not supported.  No-op when no visual artifacts exist.

    Each page includes a compact muted source stamp beneath the image.
    """
    import io as _io
    from reportlab.lib.utils import ImageReader as _ImageReader

    lead_key = (fields.get("lead_key") or "").strip()
    if not lead_key:
        return

    artifacts = _fetch_visual_artifacts(lead_key)
    if not artifacts:
        return

    _sd = (
        fields.get("sale_date_iso")
        or fields.get("sale_date")
        or ""
    ).strip() or None

    total = len(artifacts)
    for idx, art in enumerate(artifacts, start=1):
        doc.new_page()
        ct      = art["content_type"] or ""
        channel = (art["channel"] or "").strip()
        src_url = (art["source_url"] or "").strip()
        payload = art["payload"]

        doc.page_header(
            f"Notice Exhibit {idx} of {total}",
            subtitle=channel,
        )

        # ── Source stamp string (only non-empty segments) ─────────────────────
        _sp: List[str] = []
        if channel:
            _sp.append(f"Source: {channel}")
        if _sd:
            _sp.append(f"Sale date: {_sd}")
        if src_url:
            _doc_lbl = (src_url.rstrip("/").split("/")[-1] or "")[:60]
            if _doc_lbl:
                _sp.append(f"Doc: {_doc_lbl}")
        _sp.append(f"Page: {idx} of {total}")
        _stamp = " \u2022 ".join(_sp)

        # ── Layout: image fills space between header and stamp ────────────────
        _img_top = doc.y                      # just below page header band
        _stamp_y = MB + 18                    # stamp sits just above footer rule
        _img_h   = max(_img_top - _stamp_y - 14, 0)   # 14 pt gap above stamp

        c = doc.c

        if ct in ("image/jpeg", "image/png") and payload:
            _raw = bytes(payload) if isinstance(payload, (bytes, bytearray, memoryview)) else b""
            if _raw:
                try:
                    _reader = _ImageReader(_io.BytesIO(_raw))
                    c.drawImage(
                        _reader, ML, _stamp_y + 14, CW, _img_h,
                        preserveAspectRatio=True, anchor="c",
                    )
                except Exception as _exc:
                    c.setFillColor(_LGRAY)
                    c.rect(ML, _stamp_y + 14, CW, _img_h, fill=1, stroke=0)
                    c.setFont("Helvetica", 9)
                    c.setFillColor(_GRAY)
                    c.drawCentredString(
                        ML + CW / 2, _stamp_y + 14 + _img_h / 2,
                        f"[ Image unreadable: {type(_exc).__name__} ]",
                    )
        elif ct == "application/pdf":
            c.setFillColor(_LGRAY)
            c.setStrokeColor(_LINE)
            c.setLineWidth(0.6)
            c.rect(ML, _stamp_y + 14, CW, _img_h, fill=1, stroke=1)
            c.setFont("Helvetica", 9)
            c.setFillColor(_GRAY)
            c.drawCentredString(
                ML + CW / 2, _stamp_y + 14 + _img_h / 2 + 8,
                "[ PDF exhibit — rasterization not yet supported ]",
            )
            if src_url:
                c.setFont("Helvetica", 7.5)
                c.drawCentredString(
                    ML + CW / 2, _stamp_y + 14 + _img_h / 2 - 8,
                    src_url[:90],
                )

        # ── Muted source stamp beneath image ──────────────────────────────────
        c.setFont("Helvetica", 7)
        c.setFillColor(_GRAY)
        c.drawCentredString(ML + CW / 2, _stamp_y, _stamp)

        doc.y = _stamp_y - 4


def _page_foreclosure_notice(doc: _Doc, fields: Dict[str, Any]) -> None:
    """One or more pages: full foreclosure notice from LIS_PENDENS_HTML artifact."""
    lead_key = (fields.get("lead_key") or "").strip()
    source_url, sale_date, rj, notice_text = _fetch_lp_notice(lead_key)

    doc.page_header("Foreclosure Notice")
    if not source_url and not notice_text:
        doc.section("Notice Text")
        doc.body("No LIS_PENDENS_HTML artifact stored for this lead.", size=9, color=_AMBER)
        return

    # ── Metadata ──────────────────────────────────────────────────────────────
    doc.section("Notice Metadata")
    if source_url:
        doc.kv("Source URL", source_url[:90])
    sd_iso = rj.get("sale_date_iso") or sale_date
    sd_raw = rj.get("sale_date_raw")
    if sd_iso:
        doc.kv("Sale Date (ISO)", sd_iso, bold_v=True)
    if sd_raw and sd_raw != sd_iso:
        doc.kv("Sale Date (Raw)", sd_raw)
    if rj.get("trustee"):
        doc.kv("Trustee",  rj["trustee"][:90])
    if rj.get("borrower"):
        doc.kv("Borrower", rj["borrower"][:90])
    doc.gap(10)

    # ── Source stamp (per-page traceability) ──────────────────────────────────
    _stamp_parts: List[str] = ["Source: LIS_PENDENS_HTML"]
    if sd_iso:
        _stamp_parts.append(f"Sale date: {sd_iso}")
    if source_url:
        _doc_label = (source_url.rstrip("/").split("/")[-1] or "")[:60]
        if _doc_label:
            _stamp_parts.append(f"Doc: {_doc_label}")
    _stamp_base = " \u2022 ".join(_stamp_parts)
    _pg = 1

    # ── Full notice text ───────────────────────────────────────────────────────
    doc.section("Notice Text")
    if not notice_text:
        doc.body("Artifact stored but payload empty.", size=9, color=_AMBER)
        return
    if _stamp_base:
        doc.body(f"{_stamp_base} \u2022 Page: {_pg}", size=7, color=_GRAY)
        doc.gap(3)
    _FSZ, _LH = 8.0, 11.5
    for _para in notice_text.split("\n"):
        _para = _para.strip()
        if not _para:
            doc.gap(4)
            continue
        for _ln in _wrap(_para, "Helvetica", _FSZ, CW):
            if doc.space_left() < _LH + 4:
                doc.new_page()
                _pg += 1
                doc.page_header("Foreclosure Notice (cont.)")
                if _stamp_base:
                    doc.body(f"{_stamp_base} \u2022 Page: {_pg}", size=7, color=_GRAY)
                    doc.gap(3)
            doc.c.setFont("Helvetica", _FSZ)
            doc.c.setFillColor(_SLATE)
            doc.c.drawString(ML, doc.y, _ln)
            doc.y -= _LH


def _page5_scoring_appendix(
    doc: _Doc,
    fields: Dict[str, Any],
    img_embedded: bool = False,
) -> None:
    doc.page_header("Internal Scoring Appendix")

    readiness = (fields.get("auction_readiness") or "UNKNOWN").upper()
    rc        = {"GREEN": _GREEN, "YELLOW": _AMBER, "RED": _RED}.get(readiness, _GRAY)

    doc.section("Scoring Snapshot")
    doc.kv("Falco Score (Internal)", _val(fields.get("falco_score_internal")), bold_v=True)
    doc.kv("Auction Readiness",      _readiness_label(readiness), vc=rc, bold_v=True)
    doc.kv("Equity Band",            _val(fields.get("equity_band")))
    doc.kv("Days to Sale",           _val(fields.get("dts_days")))
    doc.kv("Enrichment Status",      _val(fields.get("attom_status")))
    doc.kv("AVM Confidence",         _val(fields.get("confidence")))
    doc.gap(10)

    doc.section("ACQUISITION QUALIFICATION MATRIX")
    low        = fields.get("value_anchor_low")
    dts        = fields.get("dts_days")
    spread_pct = fields.get("spread_pct")
    uw_pass = int(fields.get("uw_ready") or 0) == 1
    gates: List[Tuple[str, bool]] = [
        ("Status = enriched",   (fields.get("attom_status") or "") == "enriched"),
        ("Readiness = GREEN",   readiness == "GREEN"),
        ("DTS in [21, 60]",     dts is not None and 21 <= int(dts) <= 60),
        ("AVM Low >= $300,000", low is not None and float(low) >= 300_000),
        ("Spread <= 18%",       spread_pct is not None and spread_pct <= 0.18),
        ("Manual UW Complete",  uw_pass),
    ]
    for gate_label, gate_pass in gates:
        mark = "PASS" if gate_pass else "NOT MET"
        doc.kv(gate_label, mark, vc=_GREEN if gate_pass else _RED, bold_v=True)
    doc.gap(4)
    diamond_pass = all(g for _, g in gates)
    if diamond_pass:
        doc.kv(
            "Diamond Qualification",
            "QUALIFIED — All threshold criteria satisfied.",
            vc=_GREEN,
            bold_v=True,
        )
    else:
        doc.kv(
            "Diamond Qualification",
            "CONDITIONAL — One or more threshold criteria not satisfied. Review gating components above.",
            vc=_AMBER,
            bold_v=True,
        )
    doc.gap(10)

    
    doc.section("Data Sources")
    doc.bullet("Third-party automated valuation model (AVM)")
    if fields.get("attom_detail"):
        doc.bullet("Property record attributes (if available)")
    else:
        doc.bullet("Property record attributes — not available for this lead")
    doc.bullet("FALCO internal scoring and gating")
    doc.bullet("SQLite lead store (address, county, timeline)")
    if img_embedded:
        doc.bullet("Image Source: Google Street View (static)")

    doc.gap(8)

    # ── Data Provenance (Appendix) ────────────────────────────────────────────
    try:
        _prov_rows, _art_rows = _fetch_provenance_data(fields.get("lead_key") or "")
        if _prov_rows or _art_rows:
            doc.section("Data Provenance (Appendix)")
            _c   = doc.c
            _FSZ = 7.0
            _LH  = 10.0
            if _prov_rows:
                # header
                _c.setFont("Helvetica-Bold", _FSZ)
                _c.setFillColor(_GRAY)
                _c.drawString(ML,       doc.y, "Field")
                _c.drawString(ML + 115, doc.y, "Value")
                _c.drawString(ML + 245, doc.y, "Source")
                _c.drawString(ML + 320, doc.y, "Retrieved")
                doc.y -= _LH
                doc.hline()
                for _pr in _prov_rows:
                    _c.setFont("Helvetica", _FSZ)
                    _c.setFillColor(_SLATE)
                    _c.drawString(ML,       doc.y, (_pr["field_name"] or "")[:22])
                    _c.drawString(ML + 115, doc.y, _prov_display_value(_pr))
                    _c.drawString(ML + 245, doc.y, (_pr["source_channel"] or "")[:14])
                    _c.drawString(ML + 320, doc.y, (_pr["retrieved_at"] or "")[:19])
                    doc.y -= _LH
            if _art_rows:
                doc.gap(5)
                _c.setFont("Helvetica-Bold", _FSZ)
                _c.setFillColor(_GRAY)
                _c.drawString(ML,       doc.y, "Channel")
                _c.drawString(ML + 65,  doc.y, "Content-Type")
                _c.drawString(ML + 175, doc.y, "Retrieved")
                _c.drawString(ML + 285, doc.y, "Bytes")
                _c.drawString(ML + 335, doc.y, "Source URL")
                doc.y -= _LH
                doc.hline()
                for _ar in _art_rows:
                    _c.setFont("Helvetica", _FSZ)
                    _c.setFillColor(_SLATE)
                    _abytes = str(_ar["payload_len"]) if _ar["payload_len"] is not None else ""
                    _c.drawString(ML,       doc.y, (_ar["channel"] or "")[:10])
                    _c.drawString(ML + 65,  doc.y, (_ar["content_type"] or "")[:18])
                    _c.drawString(ML + 175, doc.y, (_ar["retrieved_at"] or "")[:19])
                    _c.drawString(ML + 285, doc.y, _abytes[:8])
                    _c.drawString(ML + 335, doc.y, (_ar["source_url"] or "")[:30])
                    doc.y -= _LH
    except Exception:
        pass  # provenance section never aborts PDF build

    doc.page_break()
    doc.page_header("Methodology and Data Transparency Appendix")

    doc.body(
        "FALCO packets are produced from structured ingest, enrichment, and deterministic gating logic. "
        "Qualification outcomes reflect rule-based thresholds rather than discretionary opinion.",
        size=9,
        leading=13,
    )

    doc.gap(12)

    doc.section("Key Derived Metrics")
    doc.bullet("AVM Spread Percent = (AVM High minus AVM Low) divided by AVM Low")
    doc.bullet("Spread Classification: Tight (<=12), Normal (<=18), Wide (>18)")
    doc.bullet("Bid Cap Guidance = AVM Low multiplied by 0.85")
    doc.bullet("Diamond Window = 21 to 60 days to sale")
    doc.bullet("AVM Floor Threshold = 300000 minimum")

    doc.gap(12)

    doc.section("Diamond Qualification Logic")
    doc.bullet("Enrichment status must equal enriched")
    doc.bullet("Auction Readiness must equal GREEN")
    doc.bullet("Days-to-sale must fall within the Diamond Window")
    doc.bullet("AVM Low must exceed the AVM Floor threshold")
    doc.bullet("Spread must be less than or equal to 18 percent")
    doc.bullet("Manual underwriting must be complete (uw_ready equals 1)")

    doc.gap(16)

    doc.section("Disclaimer")
    doc.body(
        "This document is generated from automated data pipelines and is provided for informational purposes only. "
        "It does not constitute investment advice, legal counsel, or a guarantee of property value. "
        "Investors must conduct independent due diligence including title search, lien verification, "
        "physical inspection, and legal review prior to any acquisition decision.",
        size=8,
        color=_GRAY,
        leading=12,
    )


def _normalize_fields(fields: Dict[str, Any]) -> None:
    """
    Mutates fields in-place to ensure AVM anchors, spread, and diamond_proxy
    are populated before any layout code runs.
    """
    # 1. Parse AVM anchors to float; fall back to attom_raw_json if missing.
    for dest, src in (
        ("value_anchor_low",  "avm_low"),
        ("value_anchor_mid",  "avm_value"),
        ("value_anchor_high", "avm_high"),
    ):
        raw = fields.get(dest) if fields.get(dest) is not None else fields.get(src)
        if raw is not None:
            try:
                fields[dest] = float(raw)
            except (TypeError, ValueError):
                fields[dest] = None

    # 2. If anchors still absent, try attom_raw_json.
    if fields.get("value_anchor_low") is None:
        raw_json = fields.get("attom_raw_json")
        if raw_json:
            try:
                blob = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                avm_amt = (
                    blob.get("avm", {}).get("amount", {})
                    if isinstance(blob, dict) else {}
                )
                if avm_amt:
                    for dest, key in (
                        ("value_anchor_low",  "low"),
                        ("value_anchor_mid",  "value"),
                        ("value_anchor_high", "high"),
                    ):
                        v = avm_amt.get(key)
                        if v is not None:
                            try:
                                fields[dest] = float(v)
                            except (TypeError, ValueError):
                                pass
            except Exception:
                pass

    # 3. Compute spread_pct and spread_band.
    low  = fields.get("value_anchor_low")
    high = fields.get("value_anchor_high")
    if low is not None and high is not None and float(low) > 0:
        sp = (float(high) - float(low)) / float(low)
        fields["spread_pct"]  = sp
        fields["spread_band"] = "TIGHT" if sp <= 0.12 else ("NORMAL" if sp <= 0.18 else "WIDE")
    else:
        fields.setdefault("spread_pct",  None)
        fields.setdefault("spread_band", "UNKNOWN")

    # 4. Compute diamond_proxy.
    dts        = fields.get("dts_days")
    spread_pct = fields.get("spread_pct")
    low        = fields.get("value_anchor_low")
    uw_ready_flag = int(fields.get("uw_ready") or 0) == 1

    fields["diamond_proxy"] = bool(
        (fields.get("attom_status") or "") == "enriched"
        and (fields.get("auction_readiness") or "").upper() == "GREEN"
        and dts is not None and 21 <= int(dts) <= 60
        and low is not None and float(low) >= 300_000
        and spread_pct is not None and spread_pct <= 0.18
        and uw_ready_flag
    )


def build_pdf_packet(fields: Dict[str, Any], out_dir: str) -> str:
    """Builds a FALCO Auction Intelligence Brief PDF. Returns the canonical path."""
    fields = dict(fields)       # shallow copy — never mutate caller's dict
    _normalize_fields(fields)
    # Hydrate uw_json from manual_underwriting when leads table has nothing
    _mu = _fetch_manual_uw((fields.get("lead_key") or "").strip())
    if _mu is not None:
        # manual_underwriting is authoritative — always overrides leads.uw_json
        fields["uw_json"] = json.dumps(_mu, ensure_ascii=False)
        fields["uw_ready"] = 1
    if os.getenv("FALCO_PDF_DEBUG_UW", "").strip() == "1":
        _uw_preview = (fields.get("uw_json") or "")[:120]
        print(f"[PDF][UW_DEBUG] lead_key={fields.get('lead_key')} manual_uw_found={1 if _mu is not None else 0} uw_json={_uw_preview!r}")

    # Inject LP-derived property facts (city/zip/parcel) so Property Snapshot
    # page can show them even when ATTOM enrichment has not run yet.
    if (fields.get("distress_type") or "").upper() == "LIS_PENDENS":
        _lp_facts = _extract_lp_property_fields((fields.get("lead_key") or "").strip())
        for _fk, _fv in _lp_facts.items():
            if _fv and not fields.get(_fk):
                fields[_fk] = _fv

    os.makedirs(out_dir, exist_ok=True)
    lead_key = (fields.get("lead_key") or "").strip()
    filename = f"{lead_key}.pdf" if lead_key else "unknown.pdf"
    path     = os.path.join(out_dir, filename)

    try:
        narratives = generate_narratives(fields)
    except Exception as e:
        print(f"[PDF_BUILDER][WARN] narrative generation error: {type(e).__name__}: {e}")
        narratives = {"exec_summary": "", "valuation": "", "risk": ""}

    # generate_brief never raises; returns fallback dict on any failure
    brief = generate_brief(fields)

    # Sanitize generic equity sentences replaced by dynamic lien block
    _ra = brief.get("risk_analysis") or ""
    _ra = _ra.replace("Equity position is unknown.", "")
    _ra = _ra.replace(
        "Lien balance and equity position are unknown;",
        "Lien balance requires verification;",
    )
    import re as _re
    _ra = _re.sub(r" {2,}", " ", _ra).strip()
    brief["risk_analysis"] = _ra

    # Street View image — feature off by default; budget enforced per-run
    _sv_max    = int(os.getenv("FALCO_STREETVIEW_MAX_IMAGES_PER_RUN", "0") or "0")
    run_budget = {"used": 0, "max": _sv_max}
    img_path   = get_streetview_image_path(fields, run_budget)

    # Cache fallback: if budget=0 (or cap already hit), serve pre-cached image
    if not img_path:
        _lk_sv  = (fields.get("lead_key") or "").strip()
        if _lk_sv:
            _sv_dir   = os.getenv("FALCO_STREETVIEW_CACHE_DIR",
                                  os.path.join("out", "images", "streetview"))
            _sv_cache = os.path.join(_sv_dir, f"{_lk_sv}.jpg")
            if os.path.isfile(_sv_cache) and os.path.getsize(_sv_cache) > 5_120:
                img_path = _sv_cache
                _sv_sidecar = _sv_cache.replace(".jpg", ".meta.json")
                if os.path.isfile(_sv_sidecar) and not fields.get("streetview_imagery_date"):
                    try:
                        with open(_sv_sidecar) as _sf:
                            _sv_meta = json.load(_sf)
                        fields["streetview_imagery_date"] = _sv_meta.get("date")
                    except Exception:
                        pass

    doc = _Doc(path)
    _page1_executive(doc, fields, brief, img_path=img_path)
    doc.new_page()
    _page_property_snapshot(doc, fields, img_path=img_path)
    doc.new_page()
    _page2_valuation(doc, fields, brief)
    doc.new_page()
    _page3_property_facts(doc, fields)
    doc.new_page()
    _page4_timeline_risk(doc, fields, brief)
    if (fields.get("distress_type") or "").upper() == "LIS_PENDENS":
        doc.new_page()
        _page_foreclosure_notice(doc, fields)
        _pages_notice_exhibit_images(doc, fields)
    doc.new_page()
    _page5_scoring_appendix(doc, fields, img_embedded=bool(img_path))
    doc.save()
    return path


