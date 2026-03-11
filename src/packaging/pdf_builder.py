# src/packaging/pdf_builder.py
#
# FALCO Auction Intelligence Brief — premium 5-page PDF
# AI narrative via OpenAI when FALCO_OPENAI_API_KEY is set; otherwise deterministic templates.
# No ATTOM calls. No new dependencies beyond reportlab (already installed).

import json
import math
import os
import re
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
_NAVY      = colors.HexColor("#050505")
_SLATE     = colors.HexColor("#181818")
_GRAY      = colors.HexColor("#666666")
_LGRAY     = colors.HexColor("#F5F5F5")
_MGRAY     = colors.HexColor("#E6E6E6")
_GREEN     = colors.HexColor("#050505")
_AMBER     = colors.HexColor("#404040")
_RED       = colors.HexColor("#7A7A7A")
_BLUE_BAR  = colors.HexColor("#DCDCDC")
_NAVY_BAR  = colors.HexColor("#050505")
_LINE      = colors.HexColor("#D0D0D0")
_WHITE     = colors.white
_TILE_BG   = colors.HexColor("#FAFAFA")
_TILE_BRD  = colors.HexColor("#DDDDDD")
_PILL_GRN  = colors.HexColor("#EBEBEB")
_PILL_AMB  = colors.HexColor("#DCDCDC")
_PILL_RED  = colors.HexColor("#CFCFCF")
_TXT_GRN   = colors.HexColor("#050505")

# ─── Layout constants ─────────────────────────────────────────────────────────
PAGE_W, PAGE_H = LETTER          # 612 × 792 pt
ML = 0.75 * inch                 # left margin  (~54 pt)
MR = 0.75 * inch                 # right margin
MT = 0.85 * inch                 # top margin
MB = 0.70 * inch                 # bottom margin
CW = PAGE_W - ML - MR            # content width ≈ 504 pt


# ─── Text utilities ───────────────────────────────────────────────────────────

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_LOGO_CANDIDATES = [
    os.path.join(_PROJECT_ROOT, "falco-site", "public", "falco-logo.jpg"),
    os.path.join(os.getcwd(), "falco-site", "public", "falco-logo.jpg"),
]


def _get_logo_path() -> Optional[str]:
    for candidate in _LOGO_CANDIDATES:
        if os.path.isfile(candidate):
            return candidate
    return None


_LOGO_PATH = _get_logo_path()


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
    if s in ("", "None", "null"):
        return fallback
    if re.fullmatch(r"-?\d+\.0+", s):
        s = str(int(float(s)))
    return (
        s.replace("Goodletsville", "Goodlettsville")
         .replace("GOODLETSVILLE", "GOODLETTSVILLE")
         .replace("Lavergne", "La Vergne")
         .replace("LAVERGNE", "LA VERGNE")
    )


def _trim_line(text: str, font: str, size: float, max_w: float) -> str:
    s = str(text or "").strip()
    if stringWidth(s, font, size) <= max_w:
        return s
    while s and stringWidth(s + "…", font, size) > max_w:
        s = s[:-1].rstrip()
    return (s + "…") if s else "…"


# ─── PDF document wrapper ─────────────────────────────────────────────────────

def _title_case_if_all_caps(v: Any) -> Optional[str]:
    s = _val(v, None)
    if not s:
        return None
    letters = [ch for ch in s if ch.isalpha()]
    if letters and all(ch.isupper() for ch in letters):
        return s.title()
    return s


def _valuation_display(v: Any) -> Optional[str]:
    s = _val(v, None)
    if not s:
        return None
    key = s.strip().lower()
    mapping = {
        "attom": "Enterprise Third-Party API",
        "falco": "Falco Underwriting",
        "falco_underwriting": "Falco Underwriting",
    }
    return mapping.get(key, s)


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

    def gap(self, pts: float = 6) -> None:
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
            f"FALCO Auction Opportunity Brief | Confidential — For Auction Partners | "
            f"Template {_TEMPLATE_VERSION} | Run {run_id} | Generated {gen_date}"
        )
        c.drawString(ML, fy - 11, footer_left)
        c.drawRightString(PAGE_W - MR, fy - 11, f"Page {self._pn}")

    def _draw_logo_chip(self, x: float, y: float, size: float = 20) -> None:
        c = self.c
        c.setFillColor(_WHITE)
        c.roundRect(x, y, size, size, 4, fill=1, stroke=0)
        if _LOGO_PATH:
            try:
                c.drawImage(_LOGO_PATH, x + 2, y + 2, size - 4, size - 4, preserveAspectRatio=True, anchor="c")
                return
            except Exception:
                pass
        c.setFillColor(_NAVY)
        c.setFont("Helvetica-Bold", 7)
        c.drawCentredString(x + size / 2, y + size / 2 - 2, "F")

    # ── page header band ──────────────────────────────────────────────────────

    def page_break(self) -> None:
        """Start a new PDF page."""
        self.c.showPage()
        self._pn = getattr(self, "_pn", 1) + 1
        # Reset cursor near top (safe constant, no margin attrs)
        self.y = PAGE_H - 72

	
    def page_header(self, title: str, subtitle: str = "") -> None:
        c = self.c
        bh = 40
        c.setFillColor(_NAVY)
        c.rect(0, PAGE_H - bh - 2, PAGE_W, bh + 2, fill=1, stroke=0)
        self._draw_logo_chip(ML, PAGE_H - bh + 7, 18)
        c.setFont("Helvetica", 7.5)
        c.setFillColor(colors.HexColor("#CFCFCF"))
        c.drawString(ML + 26, PAGE_H - bh + 24, "FALCO AUCTION BRIEF")
        c.setFont("Helvetica-Bold", 14)
        c.setFillColor(_WHITE)
        c.drawString(ML + 26, PAGE_H - bh + 9, title)
        if subtitle:
            c.setFont("Helvetica", 8)
            c.setFillColor(colors.HexColor("#CFCFCF"))
            c.drawRightString(PAGE_W - MR, PAGE_H - bh + 10, subtitle[:48])
        self.y = PAGE_H - bh - 14

    def cover_header(self, address: str, location: str) -> None:
        """Page-1 header band: address + county/state + 'Auction Opportunity Brief' badge."""
        c  = self.c
        bh = 64
        c.setFillColor(_NAVY)
        c.rect(0, PAGE_H - bh, PAGE_W, bh, fill=1, stroke=0)
        # Badge — top-right
        self._draw_logo_chip(ML, PAGE_H - 44, 24)
        c.setFont("Helvetica", 8)
        c.setFillColor(colors.HexColor("#CFCFCF"))
        c.drawString(ML + 32, PAGE_H - 19, "FALCO")
        c.setFont("Helvetica-Bold", 8)
        c.drawRightString(PAGE_W - MR, PAGE_H - 18, "Confidential Auction Opportunity Brief")
        # Address — main title
        addr_display = address[:72]
        c.setFont("Helvetica-Bold", 16)
        c.setFillColor(_WHITE)
        c.drawString(ML, PAGE_H - 33, addr_display)
        # County / State — subtitle line
        c.setFont("Helvetica", 9)
        c.setFillColor(colors.HexColor("#CFCFCF"))
        c.drawString(ML, PAGE_H - 49, location)
        self.y = PAGE_H - bh - 12

    # ── section heading ───────────────────────────────────────────────────────

    def section(self, label: str) -> None:
        self.gap(8)
        c = self.c
        pill_h = 13
        pill_w = min(CW, stringWidth(label.upper(), "Helvetica-Bold", 7.8) + 18)
        c.setFillColor(_LGRAY)
        c.roundRect(ML, self.y - 9, pill_w, pill_h, 4, fill=1, stroke=0)
        c.setFont("Helvetica-Bold", 7.8)
        c.setFillColor(_NAVY)
        c.drawString(ML + 8, self.y - 5, label.upper())
        c.setStrokeColor(_LINE)
        c.setLineWidth(0.6)
        c.line(ML + pill_w + 8, self.y - 3, PAGE_W - MR, self.y - 3)
        self.y -= 16

    # ── typography ────────────────────────────────────────────────────────────

    def body(
        self,
        text: str,
        size: float = 9,
        color=None,
        indent: float = 0,
        leading: float = 12,
    ) -> None:
        c = self.c
        c.setFont("Helvetica", size)
        c.setFillColor(color or _SLATE)
        x = ML + indent
        for line in _wrap(text, "Helvetica", size, CW - indent):
            c.drawString(x, self.y, line)
            self.y -= leading
        self.gap(1.5)

    def kv(
        self,
        label: str,
        value: str,
        lw: float = 132,
        vc=None,
        bold_v: bool = False,
    ) -> None:
        v = str(value).strip()
        if not v or v in ("None", "null", "Unavailable"):
            return
        c = self.c
        c.setFont("Helvetica-Bold", 8.5)
        c.setFillColor(_GRAY)
        label_y = self.y
        c.drawString(ML, label_y, label)
        value_font = "Helvetica-Bold" if bold_v else "Helvetica"
        c.setFont(value_font, 8.5)
        c.setFillColor(vc or _SLATE)
        value_x = ML + lw
        max_w = PAGE_W - MR - value_x
        lines = _wrap(v, value_font, 8.5, max_w)
        if len(lines) > 2:
            lines = lines[:2]
            lines[-1] = _trim_line(" ".join(lines[-1:]), value_font, 8.5, max_w)
        for idx, line in enumerate(lines):
            c.drawString(value_x, label_y - (idx * 10), line)
        self.y -= max(13, 10 * len(lines) + 2)

    def bullet(self, text: str, color=None) -> None:
        c = self.c
        c.setFillColor(color or _SLATE)
        c.setFont("Helvetica", 8.5)
        c.drawString(ML + 6, self.y, "•")
        for line in _wrap(text, "Helvetica", 8.5, CW - 18):
            c.drawString(ML + 18, self.y, line)
            self.y -= 11
        self.gap(1)

    def hline(self, color=None) -> None:
        c = self.c
        c.setStrokeColor(color or _LINE)
        c.setLineWidth(0.4)
        c.line(ML, self.y, PAGE_W - MR, self.y)
        self.gap(6)

    def two_col(self, pairs: List[Tuple[str, str]], lw: float = 98) -> None:
        """Two-column key-value grid."""
        c    = self.c
        cw   = CW / 2
        half = math.ceil(len(pairs) / 2)
        left  = pairs[:half]
        right = pairs[half:]
        for i in range(half):
            base = self.y
            row_drop = 12
            if i < len(left):
                lb, vb = left[i]
                vb = str(vb).strip()
                if vb and vb not in ("None", "null", "Unavailable"):
                    c.setFont("Helvetica-Bold", 7.6)
                    c.setFillColor(_GRAY)
                    c.drawString(ML, base, lb)
                    c.setFont("Helvetica", 7.6)
                    c.setFillColor(_SLATE)
                    left_lines = _wrap(vb, "Helvetica", 7.6, cw - lw - 8)
                    if len(left_lines) > 2:
                        left_lines = left_lines[:2]
                        left_lines[-1] = _trim_line(left_lines[-1], "Helvetica", 7.6, cw - lw - 8)
                    for idx, line in enumerate(left_lines):
                        c.drawString(ML + lw, base - (idx * 9), line)
                    row_drop = max(row_drop, 9 * len(left_lines) + 2)
            if i < len(right):
                lb, vb = right[i]
                vb = str(vb).strip()
                if vb and vb not in ("None", "null", "Unavailable"):
                    c.setFont("Helvetica-Bold", 7.6)
                    c.setFillColor(_GRAY)
                    c.drawString(ML + cw, base, lb)
                    c.setFont("Helvetica", 7.6)
                    c.setFillColor(_SLATE)
                    right_lines = _wrap(vb, "Helvetica", 7.6, cw - lw - 8)
                    if len(right_lines) > 2:
                        right_lines = right_lines[:2]
                        right_lines[-1] = _trim_line(right_lines[-1], "Helvetica", 7.6, cw - lw - 8)
                    for idx, line in enumerate(right_lines):
                        c.drawString(ML + cw + lw, base - (idx * 9), line)
                    row_drop = max(row_drop, 9 * len(right_lines) + 2)
            self.y -= row_drop


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
        ("Days Until Scheduled Sale", _val(fields.get("dts_days"), "—"), _SLATE),
        ("Readiness",    _readiness_label(readiness),                   rc),
        ("AVM Low",      _fmt_cur(fields.get("value_anchor_low")),      _SLATE),
        ("Diamond",      "PASS" if diamond else "FAIL",                 _GREEN if diamond else _RED),
    ]
    # Clarify that this is the scheduled foreclosure sale countdown.
    if len(tiles) > 1:
        tiles[1] = ("Days Until Scheduled Sale", tiles[1][1], tiles[1][2])
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

def _auction_liquidity(fields: Dict[str, Any]) -> str:
    """
    Deterministic auction salability signal.  Returns "STRONG", "MODERATE", or "WEAK".
    Inputs used: property_type, value_anchor_low, spread_pct, dts_days.
    """
    _RES_TOKENS = {
        "sfr", "single family", "single-family", "residential",
        "townhouse", "townhome", "condo", "condominium",
        "duplex", "triplex", "quadplex", "multi-family", "multifamily",
    }
    pt = (fields.get("property_type") or "").lower().strip()
    _is_res = any(tok in pt for tok in _RES_TOKENS) if pt else False

    try:
        _low = float(fields["value_anchor_low"])
    except (TypeError, ValueError, KeyError):
        _low = None
    try:
        _sp = float(fields["spread_pct"])
    except (TypeError, ValueError, KeyError):
        _sp = None
    try:
        _dts = int(fields["dts_days"])
    except (TypeError, ValueError, KeyError):
        _dts = None

    if (
        _is_res
        and _low is not None and _low >= 300_000
        and _sp  is not None and _sp  <= 0.15
        and _dts is not None and 21 <= _dts <= 60
    ):
        return "STRONG"
    if (
        _is_res
        and _low is not None and _low >= 200_000
        and _sp  is not None and _sp  <= 0.20
    ):
        return "MODERATE"
    return "WEAK"


def _partner_verdict(fields: Dict[str, Any]) -> str:
    """
    Partner-facing routing decision: ROUTE, WATCH, or PASS.
      ROUTE — uw_ready=1, AVM low>=300k, spread<=0.18, dts 21-60, liquidity STRONG/MODERATE
      WATCH — uw_ready=1, residential, AVM low>=200k (not fully ROUTE-qualified)
      PASS  — everything else
    """
    _uw_ready = int(fields.get("uw_ready") or 0) == 1
    _liq = _auction_liquidity(fields)
    try:
        _low = float(fields["value_anchor_low"])
    except (TypeError, ValueError, KeyError):
        _low = None
    try:
        _sp = float(fields["spread_pct"])
    except (TypeError, ValueError, KeyError):
        _sp = None
    try:
        _dts = int(fields["dts_days"])
    except (TypeError, ValueError, KeyError):
        _dts = None
    _RES_TOKENS = {
        "sfr", "single family", "single-family", "residential",
        "townhouse", "townhome", "condo", "condominium",
        "duplex", "triplex", "quadplex", "multi-family", "multifamily",
    }
    pt = (fields.get("property_type") or "").lower().strip()
    _is_res = any(tok in pt for tok in _RES_TOKENS) if pt else False
    if (
        _uw_ready
        and _low is not None and _low >= 300_000
        and _sp  is not None and _sp  <= 0.18
        and _dts is not None and 21 <= _dts <= 60
        and _liq in ("STRONG", "MODERATE")
    ):
        return "ROUTE"
    if _uw_ready and _is_res and _low is not None and _low >= 200_000:
        return "WATCH"
    return "PASS"


def _sv_exterior_note(img_path: Optional[str], fields: Dict[str, Any]) -> str:
    """
    Returns a short, conservative exterior observation note for the Property Snapshot page.
    Derives text only from structured facts (imagery availability, imagery date).
    Never claims roof condition, landscaping, or visual damage.
    """
    if img_path:
        date_str = (fields.get("streetview_imagery_date") or "").strip()
        if date_str:
            return (
                f"Exterior review pending — Street View captured {date_str}. "
                "On-site inspection required to confirm physical condition."
            )
        return (
            "Street-level imagery available; exterior distress cannot be confirmed "
            "without field inspection."
        )
    return (
        "Exterior review pending — no Street View imagery retrieved. "
        "Physical condition unverified; on-site or assessor inspection recommended."
    )


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
        "1. exec_summary: property summary for auction partner (mention timeline, value anchor, spread, readiness, score).\n"
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
    Query lead_field_provenance for the latest foreclosure contact fields for this lead.
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


# Notice-title strings that look like trustees but are actually document headers.
# Matched case-insensitively after normalising apostrophe variants and whitespace.
_TRUSTEE_JUNK_TITLES = frozenset({
    "substitute trustee's sale",
    "substitute trustees sale",
    "substitute trustee sale",
    "notice of substitute trustee's sale",
    "notice of substitute trustee sale",
    "notice of trustee's sale",
    "notice of trustee sale",
    "notice of sale",
    "trustee's sale",
    "trustees sale",
    "foreclosure sale",
    "notice of foreclosure sale",
    "notice of foreclosure",
    "foreclosure notice",
    "substitute trustee",
})


def _sanitize_trustee(s: Optional[str]) -> Optional[str]:
    """
    Return s if it looks like a real trustee / firm name, else None.
    Rejects:
      - strings containing the Unicode replacement character (garbled decode)
      - more than 20 % non-printable / C1-control bytes
      - known document-title noise strings (SUBSTITUTE TRUSTEE'S SALE, etc.)
    """
    if not s:
        return None
    if "\ufffd" in s:
        return None
    non_print = sum(1 for c in s if ord(c) < 32 or 0x7F <= ord(c) <= 0x9F)
    if non_print > len(s) * 0.20:
        return None
    # Normalise apostrophe/quote variants then compare to junk-title set
    _junk_norm = s.replace("\u2019", "'").replace("\u2018", "'").strip().lower()
    _junk_norm = " ".join(_junk_norm.split())
    if _junk_norm in _TRUSTEE_JUNK_TITLES:
        return None
    return s.strip() or None


def _sanitize_phone(s: Optional[str]) -> Optional[str]:
    """
    Return s if it looks like a real US phone number, else None.
    Input is expected to already be in NXX-NXX-XXXX form from _normalize_us_phone.
    Rejects:
      - area code == exchange (e.g. 722-722-xxxx — repeated-pattern garbage)
      - all-same-digit runs
      - invalid area/exchange starting with 0 or 1
      - strings that don't resolve to exactly 10 digits
    Never raises.
    """
    if not s:
        return None
    import re as _re
    digits = _re.sub(r"\D", "", s)
    if len(digits) != 10:
        return None
    area = digits[:3]
    exch = digits[3:6]
    if area == exch:            # 722-722-xxxx, 555-555-xxxx — clear garbage
        return None
    if len(set(digits)) == 1:  # all same digit
        return None
    if area[0] in ("0", "1") or exch[0] in ("0", "1"):
        return None
    return s.strip()


# Plain-English display labels for known distress-type codes.
_DISTRESS_LABEL_MAP: Dict[str, str] = {
    "LIS_PENDENS":             "Pre-Foreclosure",
    "FORECLOSURE":             "Foreclosure",
    "FORECLOSURE_TN":          "Foreclosure",
    "SOT":                     "Trustee Sale",
    "SUBSTITUTION_OF_TRUSTEE": "Trustee Sale",
    "TAX_SALE":                "Tax Sale",
    "SHERIFF_SALE":            "Sheriff Sale",
}


def _distress_label(fields: Dict[str, Any]) -> str:
    """
    Return a plain-English distress label for the packet.
    Checks distress_lane (explicit override) first, then maps distress_type.
    Defaults to "Foreclosure" for foreclosure-family types with no label.
    """
    lane = str(fields.get("distress_lane") or "").strip()
    if lane and lane.lower() not in ("", "unknown", "none", "null"):
        return lane
    dtype = str(fields.get("distress_type") or "").upper().strip()
    if dtype in _DISTRESS_LABEL_MAP:
        return _DISTRESS_LABEL_MAP[dtype]
    if dtype:
        return dtype.replace("_", " ").title()
    return "Unknown"


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
    Derive trustee, law firm, phone, email, mailing address, sale time, and
    sale location for Page-1 Foreclosure Contact from LIS_PENDENS_HTML artifact + raw_json.
    Supplements _fetch_notice_contact when the provenance table is empty.
    Never raises.
    """
    import re as _re
    out: Dict[str, Optional[str]] = {
        "trustee":       None,
        "law_firm":      None,
        "phone":         None,
        "email":         None,
        "address":       None,
        "sale_time":     None,
        "sale_location": None,
    }
    try:
        _, _, rj, text = _fetch_lp_notice(lead_key)
        # Trustee from bot-parsed raw_json first
        out["trustee"] = (rj.get("trustee") or "").strip() or None

        if not text:
            return out

        # Phone — prefer labeled prefix, fall back to bare pattern
        _ph = _re.search(
            r'(?:Phone|Tel(?:ephone)?|Ph)\s*[:\.\-]\s*(\(?\d{3}\)?[ .\-]\d{3}[ .\-]\d{4})',
            text, _re.IGNORECASE,
        )
        if _ph:
            out["phone"] = _ph.group(1).strip()
        else:
            _ph = _re.search(r'\(?\d{3}\)?[ .\-]\d{3}[ .\-]\d{4}', text)
            if _ph:
                out["phone"] = _ph.group(0).strip()

        # Email
        _m = _re.search(r'[\w.+\-]+@[\w.\-]+\.[a-zA-Z]{2,}', text)
        if _m:
            out["email"] = _m.group(0).strip()

        # Trustee from notice text when raw_json didn't supply one
        if not out["trustee"]:
            _trustee_patterns = [
                r'Attorneys?\s+for\s+Substitute\s+Trustee[,:\s]+([A-Z][A-Za-z&.,\s]{3,80})',
                r'Attorneys?\s+for\s+(?:the\s+)?Trustee[,:\s]+([A-Z][A-Za-z&.,\s]{3,80})',
                r'(?:Substitute|Successor|Current)\s+Trustee[,:\s]+([A-Z][A-Za-z&.,\s]{3,80})',
                r'Attorney\s+for\s+(?:the\s+)?Trustee[,:\s]+([A-Z][A-Za-z&.,\s]{3,80})',
                r'\b(?:Attorney|Trustee)[,:\s]+([A-Z][A-Za-z&.,\s]{3,60})',
            ]
            for _tp_pat in _trustee_patterns:
                _tp = _re.search(_tp_pat, text)
                if _tp:
                    out["trustee"] = _tp.group(1).strip().rstrip(",.")
                    break

        # Law firm — line ending in a recognized entity-type suffix (handles SHAPIRO & INGLE, LLP etc.)
        _fm = _re.search(
            r'^([^\n]{5,90}'
            r'(?:PLLC|P\.L\.L\.C\.|LLC|L\.L\.C\.|LLP|L\.L\.P\.'
            r'|P\.C\.|P\.A\.|& Associates?|Law\s+(?:Office|Group|Firm))'
            r'\.?)\s*$',
            text,
            _re.MULTILINE | _re.IGNORECASE,
        )
        if _fm:
            out["law_firm"] = _fm.group(1).strip()

        # Mailing address — "City, TN NNNNN" terminal line + up to 2 preceding lines
        _lines = text.splitlines()
        for _i, _ln in enumerate(_lines):
            if _re.search(r'[A-Za-z][A-Za-z\s]{2,28},\s*TN\s+\d{5}', _ln):
                _pre = [
                    _lines[j].strip()
                    for j in range(max(0, _i - 2), _i)
                    if _lines[j].strip()
                ]
                out["address"] = "\n".join(_pre + [_ln.strip()])
                break

        # Sale time — "at H:MM AM/PM" anywhere in notice
        _tm = _re.search(
            r'\bat\s+(\d{1,2}:\d{2}\s*(?:A\.?M\.?|P\.?M\.?))',
            text, _re.IGNORECASE,
        )
        if _tm:
            out["sale_time"] = _tm.group(1).strip()

        # Sale location — courthouse reference or online auction URL
        _loc = _re.search(
            r'(?:'
            # online auction
            r'(?:online\s+at\s+)([\w\./:]{8,80})'
            r'|'
            # courthouse door / steps / front door / entrance — grab up to "Courthouse[,.]"
            r'(?:at\s+(?:the\s+)?)'
            r'((?:courthouse\s+(?:door|steps?)'
            r'|front\s+door\s+of\s+(?:the\s+)?[A-Za-z\s]{3,50}?Courthouse'
            r'|front\s+entrance\s+of\s+(?:the\s+)?[A-Za-z\s]{3,50}?Courthouse'
            r'|(?:north|south|east|west|main)\s+(?:door|entrance)\s+of\s+(?:the\s+)?[A-Za-z\s]{3,50}?Courthouse'
            r'|[A-Za-z\s,\.]{3,60}?Courthouse'
            r')(?=[,\.\s]|$))'
            r')',
            text, _re.IGNORECASE,
        )
        if _loc:
            out["sale_location"] = (_loc.group(1) or _loc.group(2) or "").strip().rstrip(",.")

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


def _normalize_uw_json(raw: Any) -> Dict[str, Any]:
    """
    Parse and normalize a uw_json value into the canonical wrapped schema.

    Handles three input shapes:

      A. Canonical — already has a ``"numbers"`` dict (produced by
         _fetch_manual_uw or to_uw_json_payload).  Returned with
         occupancy/condition coerced to dicts.

      B. Raw auto-UW — flat fields produced directly by auto_underwrite()
         before to_uw_json_payload() wrapping.  Detected by the presence of
         ``"manual_bid_cap"`` or ``"uw_blocker"``.  Mapped to canonical
         structure.

      C. Legacy / ad-hoc — older payloads that may contain ``"priority"``,
         ``"title_check"``, or a flat ``"max_bid"`` outside ``numbers``.
         Best-effort mapped; legacy keys preserved so existing renderers
         still display them.

    Returns {} on parse failure or empty input.  Never raises.
    """
    # ── Parse ────────────────────────────────────────────────────────────────
    if isinstance(raw, str):
        if not raw.strip():
            return {}
        try:
            data: Dict[str, Any] = json.loads(raw)
        except Exception:
            return {}
    elif isinstance(raw, dict):
        data = dict(raw)          # shallow copy — do not mutate the caller's dict
    else:
        return {}
    if not isinstance(data, dict):
        return {}

    def _to_status_dict(v: Any) -> Dict[str, Any]:
        if isinstance(v, dict):
            return v
        if v is not None and str(v).strip():
            return {"status": str(v).strip()}
        return {}

    # ── Shape A: canonical (has a "numbers" dict) ─────────────────────────
    if isinstance(data.get("numbers"), dict):
        # Coerce occupancy/condition to dicts in case they slipped in as strings
        data["occupancy"] = _to_status_dict(data.get("occupancy"))
        data["condition"] = _to_status_dict(data.get("condition"))
        return data

    # ── Shape B: raw auto_underwrite() output (flat canonical fields) ──────
    if "manual_bid_cap" in data or "uw_blocker" in data:
        nums: Dict[str, Any] = {}
        for _src, _dst in (("manual_bid_cap", "max_bid"), ("repair_estimate", "repair_estimate")):
            _v = data.get(_src)
            if _v is not None:
                try:
                    nums[_dst] = float(_v)
                except (TypeError, ValueError):
                    pass
        return {
            "numbers":      nums,
            "occupancy":    _to_status_dict(data.get("occupancy")),
            "condition":    _to_status_dict(data.get("condition")),
            "exit_strategy": data.get("exit_strategy"),
            "notes":        data.get("title_notes") or data.get("notes"),
            "access_notes": data.get("partner_action") or data.get("access_notes"),
            "_meta": {
                "source":        "auto_underwrite",
                "version":       data.get("_auto_uw_version", "v1"),
                "uw_confidence": data.get("uw_confidence"),
                "uw_blocker":    data.get("uw_blocker"),
            },
        }

    # ── Shape C: legacy / ad-hoc ──────────────────────────────────────────
    nums = {}
    for _k in ("max_bid", "repair_estimate"):
        _v = data.get(_k)
        if _v is not None:
            try:
                nums[_k] = float(_v)
            except (TypeError, ValueError):
                nums[_k] = _v
    if "avm_confidence" in data:
        nums["avm_confidence"] = data["avm_confidence"]
    return {
        "numbers":       nums or (data.get("numbers") or {}),
        "occupancy":     _to_status_dict(data.get("occupancy")),
        "condition":     _to_status_dict(data.get("condition")),
        "exit_strategy": data.get("exit_strategy"),
        "notes":         data.get("notes") or data.get("title_notes"),
        "access_notes":  data.get("access_notes"),
        "_meta":         data.get("_meta") or {"source": "legacy"},
        # Preserve legacy-only keys so existing renderers still display them
        "priority":      data.get("priority"),
        "title_check":   data.get("title_check"),
    }


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
                "SELECT value_low, value_high, max_bid, occupancy, condition, strategy, notes, analyst"
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
                "_meta": {
                    "source": (
                        "auto_underwrite"
                        if (_row["analyst"] or "") == "auto_underwrite_v1"
                        else "manual_underwriting"
                    ),
                },
            }
        except Exception:
            return None
        finally:
            _con.close()
    except Exception:
        return None


# ─── Page renderers ───────────────────────────────────────────────────────────

def _draw_auction_snapshot(doc: _Doc, fields: Dict[str, Any]) -> None:
    """
    Compact Auction Snapshot box — top of Page 1.
    Lets an auctioneer read the deal in ~5 seconds.
    All values pulled from fields already present; nothing fabricated.
    """
    c = doc.c

    # ── Gather display values ────────────────────────────────────────────────
    _addr    = _val(fields.get("address"), "Address Unavailable")
    _county  = _val(fields.get("county"), "—")
    _dist    = _distress_label(fields)
    _dts     = fields.get("dts_days")
    _dts_str = f"{_dts} days" if _dts is not None else "Unknown"

    _low  = fields.get("value_anchor_low")
    _high = fields.get("value_anchor_high")
    _mid  = fields.get("value_anchor_mid")
    if _low is not None and _high is not None:
        try:
            _val_str = f"{_fmt_cur(float(_low))} \u2013 {_fmt_cur(float(_high))}"
        except (TypeError, ValueError):
            _val_str = "Pending Review"
    elif _mid is not None:
        _val_str = _fmt_cur(_mid)
    elif _low is not None:
        _val_str = _fmt_cur(_low)
    else:
        _val_str = "Pending Review"

    # Target Bid: UW max bid → AVM Low × 85% → "Not Set"
    _uw_s      = _normalize_uw_json(fields.get("uw_json"))
    _uw_s_nums = _uw_s.get("numbers") if isinstance(_uw_s.get("numbers"), dict) else {}
    _mb_s      = _uw_s_nums.get("max_bid")
    try:
        _bid_str = f"${float(_mb_s):,.0f}" if _mb_s is not None else ""
    except (TypeError, ValueError):
        _bid_str = ""
    if not _bid_str and _low is not None:
        try:
            _bid_str = f"${float(_low) * 0.85:,.0f}"
        except (TypeError, ValueError):
            pass
    if not _bid_str:
        _bid_str = "Not Set"

    _uw_s_occ  = _uw_s.get("occupancy")  if isinstance(_uw_s.get("occupancy"),  dict) else {}
    _uw_s_cond = _uw_s.get("condition")  if isinstance(_uw_s.get("condition"),  dict) else {}
    _occ_str   = str(_uw_s_occ.get("status")  or "Not Observed")
    _cond_str  = str(_uw_s_cond.get("status") or "Not Observed")
    if _occ_str.lower() in {"unknown", "unknown (street_view)"}:
        _occ_str = "Not yet verified"
    if _cond_str.lower() in {"unknown", "unknown (street_view)"}:
        _cond_str = "Not yet verified"

    # Key Risk: first HIGH or MED flag; fall back to first flag
    _flags    = _risk_flags(fields)
    _risk_str = "No primary blockers"
    for _ft, _sv in _flags:
        if _sv in ("HIGH", "MED"):
            _risk_str = _ft[:50]
            break
    if _risk_str == "None Triggered" and _flags:
        _risk_str = _flags[0][0][:50]

    # ── Box geometry ─────────────────────────────────────────────────────────
    _HDR_H  = 17
    _ROW_H  = 14
    _PAD_X  = 9
    _PAD_Y  = 7
    _N_ROWS = 5            # address + 4 data rows
    _BOX_H  = _HDR_H + _PAD_Y + (_ROW_H * _N_ROWS) + _PAD_Y + 12   # +12 for Key Risk wrap

    if doc.space_left() < (_BOX_H + 16):
        doc.new_page()

    _btop = doc.y
    _by   = _btop - _BOX_H

    # Outer box — subtle gray fill
    c.setStrokeColor(_LINE)
    c.setLineWidth(0.7)
    c.setFillColor(_TILE_BG)
    c.roundRect(ML, _by, CW, _BOX_H, 6, fill=1, stroke=1)

    # Header band — navy
    c.setFillColor(_NAVY)
    c.roundRect(ML, _btop - _HDR_H, CW, _HDR_H, 6, fill=1, stroke=0)
    c.setFont("Helvetica-Bold", 8.5)
    c.setFillColor(_WHITE)
    c.drawString(ML + _PAD_X, _btop - _HDR_H + 5.5, "OPPORTUNITY SNAPSHOT")

    # Two-column grid
    _LBL_W = 88               # label column width
    _COL_W = CW / 2           # 252 pt each column
    _lx1   = ML + _PAD_X
    _lx2   = ML + _COL_W + _PAD_X
    _cy    = _btop - _HDR_H - _PAD_Y - 2

    def _snap_kv(
        y: float,
        lbl1: str, val1: str,
        lbl2: str = "", val2: str = "",
        bold1: bool = False,
        vc1=None,
    ) -> None:
        c.setFont("Helvetica-Bold", 7.5)
        c.setFillColor(_GRAY)
        c.drawString(_lx1, y, lbl1)
        c.setFont("Helvetica-Bold" if bold1 else "Helvetica", 7.5)
        c.setFillColor(vc1 or _SLATE)
        c.drawString(_lx1 + _LBL_W, y, str(val1)[:44])
        if lbl2:
            c.setFont("Helvetica-Bold", 7.5)
            c.setFillColor(_GRAY)
            c.drawString(_lx2, y, lbl2)
            c.setFont("Helvetica", 7.5)
            c.setFillColor(_SLATE)
            c.drawString(_lx2 + _LBL_W, y, str(val2)[:44])

    # Row 1: Address — full-width, bold navy
    c.setFont("Helvetica-Bold", 7.5)
    c.setFillColor(_GRAY)
    c.drawString(_lx1, _cy, "Address")
    c.setFont("Helvetica-Bold", 7.5)
    c.setFillColor(_NAVY)
    c.drawString(_lx1 + _LBL_W, _cy, _addr[:72])
    _cy -= _ROW_H

    # Row 2: County | Distress Type
    _snap_kv(_cy, "County", _county, "Distress Type", _dist)
    _cy -= _ROW_H

    # Row 3: Scheduled sale timing | Market Value
    _snap_kv(_cy, "Sched. Sale In", _dts_str, "Market Value", _val_str, bold1=True)
    _cy -= _ROW_H

    # Row 4: Target Bid | Occupancy
    _bid_color = _GREEN if _bid_str != "Not Set" else _AMBER
    _snap_kv(_cy, "Target Bid", _bid_str, "Occupancy", _occ_str, bold1=True, vc1=_bid_color)
    _cy -= _ROW_H

    # Row 5: Condition (left) | Key Risk (right, wraps up to 2 lines)
    c.setFont("Helvetica-Bold", 7.5)
    c.setFillColor(_GRAY)
    c.drawString(_lx1, _cy, "Condition")
    c.setFont("Helvetica", 7.5)
    c.setFillColor(_SLATE)
    c.drawString(_lx1 + _LBL_W, _cy, str(_cond_str)[:44])
    c.setFont("Helvetica-Bold", 7.5)
    c.setFillColor(_GRAY)
    c.drawString(_lx2, _cy, "Key Risk")
    c.setFont("Helvetica", 7.5)
    c.setFillColor(_SLATE)
    _rk_w = _COL_W - _PAD_X - _LBL_W - 4
    for _ri, _rl in enumerate(_wrap(_risk_str, "Helvetica", 7.5, _rk_w)[:2]):
        c.drawString(_lx2 + _LBL_W, _cy - (_ri * 10), _rl)

    doc.y -= _BOX_H
    doc.gap(10)


def _execution_reality_rows(fields: Dict[str, Any]) -> Tuple[List[Tuple[str, str]], Optional[str]]:
    packet_quality = fields.get("packet_quality") if isinstance(fields.get("packet_quality"), dict) else {}
    execution = packet_quality.get("execution_reality") if isinstance(packet_quality.get("execution_reality"), dict) else {}
    notes = packet_quality.get("execution_notes") if isinstance(packet_quality.get("execution_notes"), list) else []

    rows: List[Tuple[str, str]] = [
        ("Contact Path", _val(execution.get("contact_path_quality"), "Unknown")),
        ("Likely Control", _val(execution.get("control_party"), "Unclear")),
        ("Execution Posture", _val(execution.get("execution_posture"), "Needs More Control Clarity")),
        ("Workability", _val(execution.get("workability_band"), "Limited")),
    ]

    note = notes[0] if notes else None
    return rows, note


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
    # Seal the gap between cover_header's bottom (y≈734) and page_header's band (y≈752)
    # so the location line drawn by cover_header does not bleed through.
    doc.c.setFillColor(_NAVY)
    doc.c.rect(0, PAGE_H - 64, PAGE_W, 26, fill=1, stroke=0)
    doc.page_header("Executive Summary")

    # Auction Snapshot — compact 5-second read box for auctioneer
    _draw_auction_snapshot(doc, fields)

    # Parse UW data once — used across multiple page-1 sections
    _uw: Dict[str, Any] = _normalize_uw_json(fields.get("uw_json"))
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

    # 2) Why This Property Is in Distress
    doc.section("Auction Trigger")
    doc.kv("Distress Type",     _distress_label(fields))
    doc.kv("Scheduled Sale Date", _val(fields.get("sale_date_iso") or fields.get("sale_date")))
    doc.kv("Scheduled Sale Time", _val(fields.get("sale_time")))
    _dts_raw = fields.get("dts_days")
    doc.kv("Scheduled Sale In", f"{_dts_raw} days" if _dts_raw is not None else "Unknown", bold_v=True)
    doc.kv("Enrichment Status", _val(fields.get("attom_status")))
    doc.gap(6)

    # 3) Estimated Market Value
    low        = fields.get("value_anchor_low")
    mid        = fields.get("value_anchor_mid")
    high       = fields.get("value_anchor_high")
    spread_pct = fields.get("spread_pct")
    doc.section("Value Range")
    # Always show underwriting-derived value when present — not gated on AVM being absent
    _uw_avm_conf = str(_uw_nums.get("avm_confidence") or "").strip()
    _mb_raw = _uw_nums.get("max_bid")
    try:
        _uw_bid_fmt = f"${float(_mb_raw):,.0f}" if _mb_raw is not None else ""
    except (TypeError, ValueError):
        _uw_bid_fmt = str(_mb_raw).strip() if _mb_raw is not None else ""
    if _uw_avm_conf:
        doc.kv("Primary Valuation Input", _valuation_display(_uw_avm_conf), bold_v=True)
        doc.kv("Valuation Source", "Enterprise third-party API + Falco pricing")
    elif low is None and mid is None and high is None:
        doc.body("Value: Pending Review (no property data yet)", size=8.5, color=_AMBER)
    doc.kv("Value Low",    _fmt_cur(low))
    doc.kv("Value Mid",    _fmt_cur(mid))
    doc.kv("Value High",   _fmt_cur(high))
    doc.kv("Value Spread", _fmt_pct(spread_pct) if spread_pct is not None else "N/A")
    if spread_pct is not None:
        if spread_pct <= 0.10:
            _val_conf = "HIGH"
        elif spread_pct <= 0.18:
            _val_conf = "MODERATE"
        else:
            _val_conf = "LOW"
    else:
        _val_conf = "UNKNOWN"
    doc.kv("Value Confidence", _val_conf)
    doc.kv("Auction Demand",      _auction_liquidity(fields))
    doc.gap(6)

    # 4) Equity Position
    doc.section("Equity / Debt Context")
    _lien = _extract_lien_skeleton(fields)
    if _lien["equity_proxy_low"] is not None:
        doc.kv("Equity Proxy (AVM Low - Total Orig)", f"${_lien['equity_proxy_low']:,.0f}", lw=200)
        try:
            _avm_low_p1 = fields.get("value_anchor_low")
            if _avm_low_p1 is not None and float(_avm_low_p1) > 0:
                _eq_pct_p1 = _lien["equity_proxy_low"] / float(_avm_low_p1) * 100
                doc.kv("Equity Proxy %", f"{_eq_pct_p1:.1f}%")
        except (TypeError, ValueError):
            pass
    elif _lien["total_amount"] is not None:
        doc.kv("Total Orig Mortgages", f"${_lien['total_amount']:,.0f}")
        doc.body("Equity proxy unavailable — verify AVM low + title.", size=8.5, color=_AMBER)
    else:
        _mort_lender = _val(fields.get("mortgage_lender"), None)
        if _mort_lender:
            doc.body(
                f"Mortgage lender identified: {_mort_lender}. Title and lien payoff still require independent verification before bidding.",
                size=8.5,
                color=_AMBER,
            )
        else:
            doc.body(
                "Mortgage data not available. Verify title independently before bidding.",
                size=8.5,
                color=_AMBER,
            )
    doc.gap(6)

    # 5) Execution Reality
    doc.section("Execution Reality")
    _execution_rows, _execution_note = _execution_reality_rows(fields)
    for _label, _value in _execution_rows:
        doc.kv(_label, _value, bold_v=True)
    if _execution_note:
        _execution_color = _AMBER if "not" in _execution_note.lower() or "thin" in _execution_note.lower() else _SLATE
        doc.body(_execution_note, size=8.5, color=_execution_color)
    doc.gap(6)

    # 6) Risk Flags
    doc.section("Risk Flags")
    flags = _risk_flags(fields)
    if not flags:
        doc.bullet("No primary automated blockers identified.", color=_GREEN)
    else:
        sev_color = {"HIGH": _RED, "MED": _AMBER, "LOW": _GRAY}
        for flag_text, sev in flags[:3]:
            doc.bullet(f"[{sev}] {flag_text}", color=sev_color.get(sev, _SLATE))
    if _uw_occ.get("status"):
        _occ_status = str(_uw_occ["status"]).strip()
        if _occ_status.lower() in {"unknown", "unknown (street_view)"}:
            _occ_status = "Not yet verified"
        doc.kv("Occupancy", _occ_status, bold_v=True)
    if _uw_cond.get("status"):
        _cond_status = str(_uw_cond["status"]).strip()
        if _cond_status.lower() in {"unknown", "unknown (street_view)"}:
            _cond_status = "Not yet verified"
        doc.kv("Condition", _cond_status, bold_v=True)
    doc.gap(6)

    # 7) Bid Guidance
    doc.section("Bid Guidance")
    if low is not None:
        try:
            cap = float(low) * 0.85
            doc.kv(
                "Conservative Bid Cap",
                f"${cap:,.0f}  (subject to title + inspection)",
                lw=200,
            )
        except (TypeError, ValueError):
            doc.body("Bid cap unavailable — value data not numeric.", size=8.5, color=_AMBER)
        if _uw_bid_fmt:
            doc.kv("Recommended Max Bid", _uw_bid_fmt, bold_v=True)
    elif _uw_bid_fmt:
        doc.kv("Recommended Max Bid", _uw_bid_fmt, lw=200, bold_v=True)
    else:
        doc.body("Value data unavailable — bid cap cannot be computed.", size=8.5, color=_AMBER)
    doc.gap(8)

    # 8) Bid Range
    try:
        _bs_target: Optional[float] = None
        # Prefer UW max bid (numeric); fall back to AVM low * 0.85
        if _mb_raw is not None:
            try:
                _bs_target = float(_mb_raw)
            except (TypeError, ValueError):
                pass
        if _bs_target is None and low is not None:
            try:
                _bs_target = float(low) * 0.85
            except (TypeError, ValueError):
                pass

        if _bs_target is not None:
            _bs_target_r = round(_bs_target)
            _bs_walk     = round(_bs_target * 0.90)

            # Aggressive bid: tighter cap when market is liquid + near-term sale
            try:
                _dts_int: Optional[int] = (
                    int(fields["dts_days"]) if fields.get("dts_days") is not None else None
                )
            except (TypeError, ValueError):
                _dts_int = None
            try:
                _sp_f: Optional[float] = float(spread_pct) if spread_pct is not None else None
            except (TypeError, ValueError):
                _sp_f = None
            try:
                _avm_low_bs: Optional[float] = float(low) if low is not None else None
            except (TypeError, ValueError):
                _avm_low_bs = None

            _tight_market = (
                _sp_f is not None and _sp_f <= 0.12
                and _dts_int is not None and 21 <= _dts_int <= 60
            )
            if _avm_low_bs is not None:
                if _tight_market:
                    _bs_agg: Optional[int] = round(min(_avm_low_bs * 0.90, _bs_target * 1.08))
                else:
                    _bs_agg = round(min(_avm_low_bs * 0.87, _bs_target * 1.05))
            else:
                _bs_agg = None

            doc.section("Bid Range")
            if _bs_agg is not None:
                doc.kv("Aggressive Bid", f"${_bs_agg:,}", bold_v=True)
            doc.kv("Target Bid",    f"${_bs_target_r:,}", bold_v=True)
            doc.kv("Walk-Away Bid", f"${_bs_walk:,}")
            doc.gap(8)
    except Exception:
        pass

    # 9) Contact & Routing moved to page 2
    _lead_key = fields.get("lead_key") or ""
    _nc = _fetch_notice_contact(_lead_key)
    _ft = _fetch_prov_fields(
        _lead_key,
        ["ft_trustee_firm", "ft_trustee_person", "ft_trustee_name_raw"],
    )

    # Supplement from LP notice artifact — fills all contact fields when
    # the lead_field_provenance table has no foreclosure contact rows yet.
    _lp = _extract_lp_contact(_lead_key)
    if _lp.get("phone") and not _nc.get("notice_phone"):
        _nc["notice_phone"] = _lp["phone"]
    if _lp.get("email") and not _nc.get("notice_email"):
        _nc["notice_email"] = _lp["email"]
    if _lp.get("law_firm") and not _nc.get("notice_law_firm"):
        _nc["notice_law_firm"] = _lp["law_firm"]
    if _lp.get("address") and not _nc.get("notice_trustee_address"):
        _nc["notice_trustee_address"] = _lp["address"]
    # sale_time / sale_location live only in _lp (not in provenance table yet)
    _nc_sale_time     = _lp.get("sale_time")
    _nc_sale_location = _lp.get("sale_location")

    # Build trustee display: priority ft > notice > lp_rj > lp_text
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

    # Tier-2 / Tier-3 contact enrichment results (written by contact_enricher)
    _enrich_prov = _fetch_prov_fields(
        _lead_key,
        ["trustee_phone_public", "trustee_phone_source",
         "owner_phone_primary", "owner_phone_secondary", "owner_phone_source"],
    )
    _t2_phone  = _sanitize_phone(_enrich_prov.get("trustee_phone_public"))
    _t3_phone  = _sanitize_phone(_enrich_prov.get("owner_phone_primary"))
    _t3_phone_2 = _sanitize_phone(_enrich_prov.get("owner_phone_secondary"))
    _owner_mort = _extract_owner_mortgage(fields)

    _nc_has_any = (
        bool(_owner_mort.get("owner_name"))
        or bool(_owner_mort.get("owner_mail"))
        or bool(fields.get("property_identifier"))
        or bool(_owner_mort.get("mortgage_lender"))
        or bool(_trustee_display)
        or bool(_nc_sale_time)
        or bool(_nc_sale_location)
        or bool(_t2_phone)
        or bool(_t3_phone)
        or bool(_t3_phone_2)
        or any(
            _nc.get(k) for k in (
                "notice_phone", "notice_email", "notice_trustee_address", "notice_law_firm",
            )
        )
    )
    if False and _nc_has_any:
        _contact_pairs: List[Tuple[str, str]] = []
        if _owner_mort.get("owner_name"):
            _contact_pairs.append(("Owner Name", _owner_mort["owner_name"]))
        if _owner_mort.get("owner_mail"):
            _contact_pairs.append(("Owner Mailing", _owner_mort["owner_mail"]))
        if fields.get("property_identifier"):
            _contact_pairs.append(("Parcel / APN", _val(fields.get("property_identifier"))))
        if _owner_mort.get("mortgage_lender"):
            _contact_pairs.append(("Mortgage Lender", _owner_mort["mortgage_lender"]))
        if _trustee_display:
            _contact_pairs.append(("Trustee / Firm", _trustee_display))
        if _nc.get("notice_law_firm") and _nc["notice_law_firm"] != _trustee_display:
            _contact_pairs.append(("Law Firm", _nc["notice_law_firm"]))

        # Phone priority: notice-native > trustee firm table (T2)
        _ph_clean = _sanitize_phone(_nc.get("notice_phone"))
        if _ph_clean:
            _contact_pairs.append(("Sale Status Phone", _ph_clean))
        elif _t2_phone:
            _contact_pairs.append(("Sale Status Phone", _t2_phone))
        # Owner phone (T3) — always shown when present, separate row
        if _t3_phone:
            _contact_pairs.append(("Owner Contact", _t3_phone))
        if _t3_phone_2 and _t3_phone_2 != _t3_phone:
            _contact_pairs.append(("Owner Contact 2", _t3_phone_2))
        if _nc.get("notice_email"):
            _contact_pairs.append(("Notice Email", _nc["notice_email"]))
        if _nc.get("notice_trustee_address"):
            _contact_pairs.append(("Trustee Address", _nc["notice_trustee_address"]))
        if _nc_sale_time:
            _contact_pairs.append(("Scheduled Sale Time", _nc_sale_time))
        if _nc_sale_location:
            _contact_pairs.append(("Sale Location", _nc_sale_location))
        doc.two_col(_contact_pairs, lw=82)
    else:
        pass

    # Execution notes moved to page 2 to keep page 1 readable.

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
    c.drawString(x + 8, y_top - 11.5, "PRE-BID CHECKLIST")

    # Content
    c.setFont("Helvetica", 8.25)
    c.setFillColor(_SLATE)

    lane = _distress_label(fields)
    if lane == "Unknown":
        lane = ""
    sale = str(fields.get("sale_date_iso") or fields.get("sale_date") or "").strip()

    bid_cap_txt = "N/A"
    try:
        if avm_low is not None:
            bid_cap_txt = f"${(float(avm_low) * 0.85):,.0f}"
    except Exception:
        bid_cap_txt = "N/A"
    if bid_cap_txt == "N/A":
        _uw_bc = _normalize_uw_json(fields.get("uw_json"))
        if _uw_bc:
            _uw_bc_nums = _uw_bc.get("numbers") if isinstance(_uw_bc.get("numbers"), dict) else {}
            _mb = _uw_bc_nums.get("max_bid")
            if _mb is not None:
                try:
                    bid_cap_txt = f"${float(_mb):,.0f} (Falco underwriting)"
                except (TypeError, ValueError):
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
    doc.page_header("Value & Bid Framework")

    low  = fields.get("value_anchor_low")
    mid  = fields.get("value_anchor_mid")
    high = fields.get("value_anchor_high")

    doc.section("Estimated Market Value")
    if low is not None and mid is not None and high is not None:
        _draw_val_bar(doc, float(low), float(mid), float(high))
    else:
        # Fall back to underwriting numbers when ATTOM AVM is absent
        _uw2: Dict[str, Any] = _normalize_uw_json(fields.get("uw_json"))
        _uw2_nums = _uw2.get("numbers") if isinstance(_uw2.get("numbers"), dict) else {}
        _uw2_conf = str(_uw2_nums.get("avm_confidence") or "").strip()
        _uw2_bid_r = _uw2_nums.get("max_bid")
        try:
            _uw2_bid = f"${float(_uw2_bid_r):,.0f}" if _uw2_bid_r is not None else ""
        except (TypeError, ValueError):
            _uw2_bid = ""
        if _uw2_conf or _uw2_bid:
            doc.kv("Value Source", "Underwriting Estimate", bold_v=True)
            if _uw2_conf:
                doc.kv("Appraiser Estimate", _uw2_conf, bold_v=True)
            if _uw2_bid:
                doc.kv("Recommended Max Bid", _uw2_bid, bold_v=True)
        else:
            doc.body("Valuation range data unavailable.", size=9, color=_AMBER)
    doc.gap(10)

    doc.section("Pricing Guidance")
    doc.body(brief.get("auction_positioning", "Pricing guidance unavailable."), size=9, leading=13)
    doc.gap(10)

    doc.section("Market Liquidity")
    doc.body(brief.get("liquidity_analysis", "Liquidity analysis unavailable."), size=9, color=_AMBER, leading=13)
    doc.gap(10)

    comps = fields.get("internal_comps") or []
    if comps:
        doc.section("Comparable Properties")
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
    else:
        doc.section("Comparable Properties")
        _cty_raw = (fields.get("county") or "").strip()
        if _cty_raw.lower().endswith(" county"):
            _cty_raw = _cty_raw[:-7].rstrip()
        _cty_disp = _cty_raw or "this"
        doc.body(
            "No comparable auction records currently on file "
            f"for {_cty_disp} County in this valuation band. "
            "Independent MLS and auction-history research is required before bidding.",
            size=9, color=_GRAY, leading=13,
        )
        # Market context — derived only from verified packet fields, no fabrication
        _ctx_parts: List[str] = []
        _mc_county = (fields.get("county") or "").strip()
        _mc_ptype  = (fields.get("property_type") or "").strip()
        _mc_sqft   = fields.get("building_area_sqft")
        _mc_low    = fields.get("value_anchor_low")
        _mc_mid    = fields.get("value_anchor_mid")
        _mc_high   = fields.get("value_anchor_high")
        _mc_spread = (fields.get("spread_band") or "").upper()
        if _mc_county:
            _ctx_parts.append(f"County: {_mc_county}")
        if _mc_ptype:
            _ctx_parts.append(f"Type: {_mc_ptype}")
        if _mc_sqft is not None:
            _ctx_parts.append(f"Living area: {int(_mc_sqft):,} sqft")
        if _mc_low is not None and _mc_high is not None:
            _ctx_parts.append(f"AVM range: {_fmt_cur(_mc_low)} \u2013 {_fmt_cur(_mc_high)}")
        elif _mc_mid is not None:
            _ctx_parts.append(f"AVM mid: {_fmt_cur(_mc_mid)}")
        if _mc_spread:
            _ctx_parts.append(f"Spread: {_mc_spread}")
        if _ctx_parts:
            doc.gap(6)
            doc.body(
                "Property context:  " + "  \u2022  ".join(_ctx_parts),
                size=8, color=_SLATE, leading=12,
            )


def _render_compact_value_framework(doc: _Doc, fields: Dict[str, Any], brief: Dict[str, Any]) -> None:
    low  = fields.get("value_anchor_low")
    mid  = fields.get("value_anchor_mid")
    high = fields.get("value_anchor_high")

    doc.section("Value & Bid Framework")
    if low is not None and mid is not None and high is not None:
        _draw_val_bar(doc, float(low), float(mid), float(high))
    else:
        _uw2: Dict[str, Any] = _normalize_uw_json(fields.get("uw_json"))
        _uw2_nums = _uw2.get("numbers") if isinstance(_uw2.get("numbers"), dict) else {}
        _uw2_conf = str(_uw2_nums.get("avm_confidence") or "").strip()
        _uw2_bid_r = _uw2_nums.get("max_bid")
        try:
            _uw2_bid = f"${float(_uw2_bid_r):,.0f}" if _uw2_bid_r is not None else ""
        except (TypeError, ValueError):
            _uw2_bid = ""
        if _uw2_conf:
            doc.kv("Appraiser Estimate", _uw2_conf, bold_v=True)
        if _uw2_bid:
            doc.kv("Recommended Max Bid", _uw2_bid, bold_v=True)
        if not _uw2_conf and not _uw2_bid:
            doc.body("Valuation range data unavailable.", size=8.5, color=_AMBER, leading=12)

    doc.body(brief.get("auction_positioning", "Pricing guidance unavailable."), size=8.5, leading=12)
    doc.body(brief.get("liquidity_analysis", "Liquidity analysis unavailable."), size=8.5, color=_AMBER, leading=12)

    comps = fields.get("internal_comps") or []
    if comps:
        for comp in comps[:4]:
            avm = comp.get("avm_value") or comp.get("avm_low")
            avm_str  = f"${float(avm):,.0f}" if avm is not None else "N/A"
            dts_str  = str(comp["dts"]) if comp.get("dts") is not None else "â€”"
            date_str = str(comp["sale_date"]) if comp.get("sale_date") else "â€”"
            addr_str = (str(comp.get("address") or "")).strip() or "â€”"
            doc.body(
                f"Comp: {avm_str}  |  DTS {dts_str}  |  {date_str}  |  {addr_str}",
                size=7.6, color=_SLATE, leading=10.5,
            )


def _extract_owner_mortgage(fields: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Extract owner/mortgage fields from raw_merged['owner'] and raw_merged['mortgage']."""
    out: Dict[str, Optional[str]] = {
        "owner_name":       _val(fields.get("owner_name"), None),
        "owner_mail":       _val(fields.get("owner_mail"), None),
        "last_sale_date":   _val(fields.get("last_sale_date"), None),
        "mortgage_lender":  _val(fields.get("mortgage_lender"), None),
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
                full = (
                    _ow1.get("fullname")        # current shape (lowercase key)
                    or _ow1.get("fullName")     # old shape
                    or " ".join(filter(None, [
                        _ow1.get("firstnameandmi") or _ow1.get("firstName"),
                        _ow1.get("lastname")       or _ow1.get("lastName"),
                    ])).strip()
                    or None
                )
                if not out["owner_name"]:
                    out["owner_name"] = full or None
            if not out["owner_mail"]:
                out["owner_mail"] = (
                    _ow.get("mailingaddressoneline")                  # current shape
                    or (_ow.get("mailAddress") or {}).get("oneLine")  # old shape
                    or None
                )
        _sale = owner_blob.get("sale") or {}
        if isinstance(_sale, dict):
            if not out["last_sale_date"]:
                out["last_sale_date"] = _sale.get("saleTransDate") or None
            if not out["last_sale_date"]:
                hist = _sale.get("salesHistory")
                if isinstance(hist, list) and hist:
                    out["last_sale_date"] = (hist[0] or {}).get("saleRecDate") or None

    mort_blob = blob.get("mortgage")
    if isinstance(mort_blob, dict):
        _mort = mort_blob.get("mortgage") or {}
        if isinstance(_mort, dict):
            # Old shape: firstMortgage sub-object
            _fm = _mort.get("firstMortgage") or {}
            if isinstance(_fm, dict) and _fm:
                _ldr = _fm.get("lender") or {}
                if not out["mortgage_lender"]:
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
            # Current shape: direct fields on mortgage.mortgage
            if not out["mortgage_lender"]:
                _ldr = _mort.get("lender") or {}
                if isinstance(_ldr, dict):
                    out["mortgage_lender"] = (
                        _ldr.get("lastname") or _ldr.get("institution")
                    ) or None
                elif _ldr:
                    out["mortgage_lender"] = str(_ldr)
            if not out["mortgage_amount"]:
                _amt = _mort.get("amount")
                if _amt is not None:
                    try:
                        out["mortgage_amount"] = f"${float(_amt):,.0f}"
                    except (TypeError, ValueError):
                        out["mortgage_amount"] = str(_amt)
            if not out["mortgage_date"]:
                out["mortgage_date"] = (
                    _mort.get("date") or _mort.get("recordingDate") or None
                )

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
            # Old shape: firstMortgage sub-object
            _fm = _mort.get("firstMortgage") or {}
            if isinstance(_fm, dict) and _fm:
                _ldr = _fm.get("lender") or {}
                out["first_lender"] = (
                    _ldr.get("institution") if isinstance(_ldr, dict) else str(_ldr)
                ) or None
                try:
                    out["first_amount"] = float(_fm["amount"]) if _fm.get("amount") is not None else None
                except (TypeError, ValueError):
                    pass
            # Current shape: direct fields on mortgage.mortgage
            if out["first_lender"] is None:
                _ldr = _mort.get("lender") or {}
                if isinstance(_ldr, dict):
                    out["first_lender"] = (
                        _ldr.get("lastname") or _ldr.get("institution")
                    ) or None
                elif _ldr:
                    out["first_lender"] = str(_ldr)
            if out["first_amount"] is None:
                _da = _mort.get("amount")
                if _da is not None:
                    try:
                        out["first_amount"] = float(_da)
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
    doc.section("Recorded Debt Snapshot")
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
    try:
        _avm_low_ls = fields.get("value_anchor_low")
        if data["equity_proxy_low"] is not None and _avm_low_ls is not None and float(_avm_low_ls) > 0:
            _eq_pct_ls = data["equity_proxy_low"] / float(_avm_low_ls) * 100
            doc.kv("Equity Proxy %", f"{_eq_pct_ls:.1f}%")
    except (TypeError, ValueError):
        pass


def _draw_value_stack_section(doc: _Doc, fields: Dict[str, Any]) -> None:
    """
    Compact Value / Equity Stack summary block.
    Renders AVM range, debt basis, equity proxy, and equity %.
    Silent no-op when no AVM anchors are present.
    Additive — does not replace or modify the Lien Skeleton section.
    """
    high = fields.get("value_anchor_high")
    mid  = fields.get("value_anchor_mid")
    low  = fields.get("value_anchor_low")
    if high is None and mid is None and low is None:
        return
    _lien   = _extract_lien_skeleton(fields)
    debt    = _lien["total_amount"]
    eq_prox = _lien["equity_proxy_low"]

    doc.section("Value Stack")
    if high is not None:
        doc.kv("AVM High",     _fmt_cur(high))
    if mid is not None:
        doc.kv("AVM Mid",      _fmt_cur(mid))
    if low is not None:
        doc.kv("AVM Low",      _fmt_cur(low))
    if debt is not None:
        doc.kv("Debt Basis",   f"${debt:,.0f}")
    if eq_prox is not None:
        doc.kv("Equity Proxy", f"${eq_prox:,.0f}")
        try:
            if low is not None and float(low) > 0:
                _eq_pct = eq_prox / float(low) * 100
                doc.kv("Equity Proxy %", f"{_eq_pct:.1f}%")
        except (TypeError, ValueError):
            pass


def _draw_ownership_section(doc: _Doc, fields: Dict[str, Any]) -> None:
    """Ownership & Mortgage (ATTOM) section — max 8 lines."""
    data = _extract_owner_mortgage(fields)
    doc.section("Ownership & Mortgage")
    doc.kv("Owner Name",         data["owner_name"]       or "Not available")
    doc.kv("Mailing Address",    data["owner_mail"]        or "Not available")
    doc.kv("Last Transfer Date", data["last_sale_date"]    or "Not available")
    doc.kv("Mortgage Lender",    data["mortgage_lender"]   or "Not available")
    doc.kv("Orig. Amount",       data["mortgage_amount"]   or "Not available")
    doc.kv("Recording Date",     data["mortgage_date"]     or "Not available")


# ─── Property Snapshot page ────────────────────────────────────────────────────

def _contact_routing_pairs(fields: Dict[str, Any]) -> List[Tuple[str, str]]:
    _lead_key = fields.get("lead_key") or ""
    _nc = _fetch_notice_contact(_lead_key)
    _ft = _fetch_prov_fields(
        _lead_key,
        ["ft_trustee_firm", "ft_trustee_person", "ft_trustee_name_raw"],
    )

    _lp = _extract_lp_contact(_lead_key)
    if _lp.get("phone") and not _nc.get("notice_phone"):
        _nc["notice_phone"] = _lp["phone"]
    if _lp.get("email") and not _nc.get("notice_email"):
        _nc["notice_email"] = _lp["email"]
    if _lp.get("law_firm") and not _nc.get("notice_law_firm"):
        _nc["notice_law_firm"] = _lp["law_firm"]
    if _lp.get("address") and not _nc.get("notice_trustee_address"):
        _nc["notice_trustee_address"] = _lp["address"]
    _nc_sale_time = _lp.get("sale_time")
    _nc_sale_location = _lp.get("sale_location")

    _trustee_display: Optional[str] = None
    _ft_firm = _sanitize_trustee(_ft.get("ft_trustee_firm"))
    _ft_person = _sanitize_trustee(_ft.get("ft_trustee_person"))
    if _ft_firm:
        _trustee_display = (_ft_firm + " / " + _ft_person) if _ft_person else _ft_firm
    if not _trustee_display:
        _trustee_display = _sanitize_trustee(_nc.get("notice_trustee_firm"))
    if not _trustee_display:
        _trustee_display = _sanitize_trustee(_nc.get("notice_trustee_name_raw"))
    if not _trustee_display:
        _trustee_display = _sanitize_trustee(_lp.get("trustee"))

    _enrich_prov = _fetch_prov_fields(
        _lead_key,
        [
            "trustee_phone_public",
            "trustee_phone_source",
            "owner_phone_primary",
            "owner_phone_secondary",
            "owner_phone_source",
        ],
    )
    _t2_phone = _sanitize_phone(_enrich_prov.get("trustee_phone_public"))
    _t3_phone = _sanitize_phone(_enrich_prov.get("owner_phone_primary"))
    _t3_phone_2 = _sanitize_phone(_enrich_prov.get("owner_phone_secondary"))
    _owner_mort = _extract_owner_mortgage(fields)

    _contact_pairs: List[Tuple[str, str]] = []
    if _owner_mort.get("owner_name"):
        _contact_pairs.append(("Owner Name", _owner_mort["owner_name"]))
    if _owner_mort.get("owner_mail"):
        _contact_pairs.append(("Owner Mailing", _owner_mort["owner_mail"]))
    if fields.get("property_identifier"):
        _contact_pairs.append(("Parcel / APN", _val(fields.get("property_identifier"))))
    if _owner_mort.get("mortgage_lender"):
        _contact_pairs.append(("Mortgage Lender", _owner_mort["mortgage_lender"]))
    if _trustee_display:
        _contact_pairs.append(("Trustee / Firm", _trustee_display))
    if _nc.get("notice_law_firm") and _nc["notice_law_firm"] != _trustee_display:
        _contact_pairs.append(("Law Firm", _nc["notice_law_firm"]))

    _ph_clean = _sanitize_phone(_nc.get("notice_phone"))
    if _ph_clean:
        _contact_pairs.append(("Sale Status Phone", _ph_clean))
    elif _t2_phone:
        _contact_pairs.append(("Sale Status Phone", _t2_phone))
    if _t3_phone:
        _contact_pairs.append(("Owner Contact", _t3_phone))
    if _t3_phone_2 and _t3_phone_2 != _t3_phone:
        _contact_pairs.append(("Owner Contact 2", _t3_phone_2))
    if _nc.get("notice_email"):
        _contact_pairs.append(("Notice Email", _nc["notice_email"]))
    if _nc.get("notice_trustee_address"):
        _contact_pairs.append(("Trustee Address", _nc["notice_trustee_address"]))
    if _nc_sale_time:
        _contact_pairs.append(("Scheduled Sale Time", _nc_sale_time))
    if _nc_sale_location:
        _contact_pairs.append(("Sale Location", _nc_sale_location))

    return _contact_pairs


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
    doc.page_header("Property & Location Snapshot")

    # ── Exterior image (only when we actually have one) ────────────────────────
    if img_path:
        _draw_hero(doc, img_path=img_path, imagery_date=fields.get("streetview_imagery_date"))
    doc.gap(4)

    # ── Exterior observation note (conservative — derived from structured facts only) ──
    doc.body(_sv_exterior_note(img_path, fields), size=7.5, color=_GRAY)
    doc.gap(6)

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
    _snap_add("City",        _title_case_if_all_caps(fields.get("city")))
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

    _contact_pairs = _contact_routing_pairs(fields)
    doc.section("Contact & Routing")
    if _contact_pairs:
        for _label, _value in _contact_pairs:
            doc.kv(_label, _value, lw=110)
        doc.body(
            "Trustee contact is included for sale-status, postponement, and file-confirmation checks. "
            "Execution path may still run borrower-side, lender-side, trustee-side, or through auction channel depending on the file.",
            size=7.5,
            color=_GRAY,
            leading=11,
        )
    else:
        doc.bullet("No contact found in notice artifacts yet.")

    # ── Partial-data note — only when ATTOM detail is absent ───────────────────
    if not fields.get("attom_detail"):
        doc.gap(8)
        doc.body(
            "Property facts partially unavailable — verify via county assessor, MLS, or physical inspection.",
            size=8.5,
            color=_AMBER,
        )


def _page3_property_facts(doc: _Doc, fields: Dict[str, Any]) -> None:
    doc.page_header("Property Record & Title Context")

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
        doc.gap(10)
        _draw_value_stack_section(doc, fields)
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
    _ls = fields.get("lot_size")
    try:
        _ls_fmt = f"{int(float(_ls)):,} sqft" if _ls is not None else None
    except (TypeError, ValueError):
        _ls_fmt = str(_ls) if _ls else None
    _add("Lot Size", _ls_fmt)
    _add("Bedrooms",         fields.get("beds"))
    _add("Bathrooms",        fields.get("baths"))
    _add("Construction",     fields.get("construction_type"))
    _add("City",             _title_case_if_all_caps(fields.get("city")))
    _add("ZIP",              fields.get("zip"))
    _add("Property ID",      fields.get("property_identifier"))

    if pairs:
        doc.two_col(pairs)
    else:
        doc.body("Detail record present but no structured fields extracted.", size=9, color=_AMBER)

    doc.gap(10)
    _draw_ownership_section(doc, fields)
    doc.gap(10)
    _draw_lien_skeleton_section(doc, fields)
    doc.gap(10)
    _draw_value_stack_section(doc, fields)


def _page4_timeline_risk(doc: _Doc, fields: Dict[str, Any], brief: Dict[str, Any]) -> None:
    doc.page_header("Timing, Value & Underwriting")

    doc.section("Sale Timeline")
    doc.kv("Scheduled Sale Date", _val(fields.get("sale_date_iso") or fields.get("sale_date")), bold_v=True)
    doc.kv("Scheduled Sale Time", _val(fields.get("sale_time")))
    _dts_display = _val(fields.get("dts_days"))
    if _dts_display not in {"â€”", "Unknown"}:
        _dts_display = f"{_dts_display} days"
    doc.kv("Days Until Scheduled Sale", _dts_display)
    doc.kv("Sale Location",     _val(fields.get("sale_location")), lw=110)
    doc.kv("Sale Type",         _val(fields.get("sale_type")))
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

    doc.section("Risk Factors")
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
    _render_compact_value_framework(doc, fields, brief)
    doc.gap(6)
    doc.section("Before You Bid")

    _owner_mort = _extract_owner_mortgage(fields)
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
    elif _owner_mort.get("mortgage_lender"):
        doc.body(
            f"Mortgage lender identified: {_owner_mort['mortgage_lender']}. Full title and payoff verification remain required before bidding.",
            size=9,
            leading=13,
        )
    else:
        doc.body(
            "Mortgage position unavailable. Full title search required. Do not assume free & clear status.",
            size=9,
            leading=13,
        )


def _draw_manual_uw_section(doc: _Doc, fields: Dict[str, Any]) -> None:
    """
    Underwriting block.
    Reads:
      fields["uw_ready"] (0/1)
      fields["uw_json"] (JSON string)
    """
    uw_ready = fields.get("uw_ready")
    raw = fields.get("uw_json") or ""

    if not raw:
        doc.section("Underwriting Notes")
        doc.body("No underwriting notes recorded for this lead.", size=9, color=_AMBER)
        return

    uw = _normalize_uw_json(raw)
    if not uw:
        doc.section("Underwriting Notes")
        doc.body("Underwriting data present but unreadable (invalid JSON).", size=9, color=_AMBER)
        return

    doc.section("Underwriting Notes")

    _uw_is_ready = int(uw_ready or 0) == 1
    _uw_label    = "UNDERWRITTEN" if _uw_is_ready else "PENDING"
    sc = _GREEN if _uw_is_ready else _AMBER
    doc.kv("UW Status", _uw_label, vc=sc, bold_v=True)

    meta = uw.get("_meta") if isinstance(uw.get("_meta"), dict) else {}
    if meta:
        doc.kv("Updated At", _val(meta.get("updated_at")))

    # Core checklist (safe reads)
    def _fmt_obj(x):
        return x if isinstance(x, dict) else {}

    pr = uw.get("priority")
    if pr:
        doc.kv("Priority", str(pr), bold_v=True)

    title = _fmt_obj(uw.get("title_check"))
    if title:
        doc.kv("Title Check", _val(title.get("status")))

    occ = _fmt_obj(uw.get("occupancy"))
    if occ and occ.get("status"):
        _src = occ.get("source")
        _occ_status = str(occ["status"]).strip()
        if _occ_status.lower() in {"unknown", "unknown (street_view)"}:
            _occ_status = "Not yet verified"
        doc.kv("Occupancy", _occ_status)

    cond = _fmt_obj(uw.get("condition"))
    if cond and cond.get("status"):
        _src = cond.get("source")
        _cond_status = str(cond["status"]).strip()
        if _cond_status.lower() in {"unknown", "unknown (street_view)"}:
            _cond_status = "Not yet verified"
        doc.kv("Condition", _cond_status)

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
        if str(notes).strip().lower() not in {"initial uw shell for testing", "initial uw shell"}:
            doc.body(str(notes), size=9, leading=13)

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
    doc.page_header("Qualification Appendix")

    readiness = (fields.get("auction_readiness") or "UNKNOWN").upper()
    rc        = {"GREEN": _GREEN, "YELLOW": _AMBER, "RED": _RED}.get(readiness, _GRAY)

    doc.section("Scoring Summary")
    doc.kv("FALCO Score", _val(fields.get("falco_score_internal")), bold_v=True)
    doc.kv("Auction Readiness",      _readiness_label(readiness), vc=rc, bold_v=True)
    doc.kv("Equity Band",            _val(fields.get("equity_band")))
    doc.kv("Days Until Scheduled Sale", _val(fields.get("dts_days")))
    doc.kv("Enrichment Status",      _val(fields.get("attom_status")))
    doc.kv("AVM Confidence",         _val(fields.get("confidence")))
    doc.gap(10)

    doc.section("QUALIFICATION CHECKLIST")
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
        ("Underwriting Complete",  uw_pass),
    ]
    for gate_label, gate_pass in gates:
        mark = "PASS" if gate_pass else "NOT MET"
        doc.kv(gate_label, mark, vc=_GREEN if gate_pass else _RED, bold_v=True)
    doc.gap(4)
    diamond_pass = all(g for _, g in gates)
    if diamond_pass:
        doc.kv(
            "Full Qualification",
            "QUALIFIED — All criteria met.",
            vc=_GREEN,
            bold_v=True,
        )
    else:
        doc.kv(
            "Full Qualification",
            "NOT FULLY QUALIFIED — One or more criteria not met. See checklist above.",
            vc=_AMBER,
            bold_v=True,
        )
    doc.gap(10)
    _draw_due_diligence_checklist(doc, fields, low)
    doc.gap(10)

    doc.section("Data Sources")
    doc.bullet("Automated valuation model (AVM)")
    if fields.get("attom_detail"):
        doc.bullet("Property records (if available)")
    else:
        doc.bullet("Property records — not available for this address")
    doc.bullet("FALCO scoring and qualification")
    doc.bullet("Lead records (address, county, timeline)")
    if img_embedded:
        doc.bullet("Image Source: Google Street View (static)")

    doc.gap(8)

    # ── Data Provenance (Appendix) ────────────────────────────────────────────
    # Internal provenance remains available in storage and operator tooling rather than the partner-facing packet.

    doc.page_break()
    doc.page_header("Methodology & Disclaimer")

    doc.body(
        "This brief is produced from published property data, automated valuation models, and rule-based scoring. "
        "Qualification results reflect fixed thresholds, not personal opinion.",
        size=9,
        leading=13,
    )

    doc.gap(12)

    doc.section("How Values Are Calculated")
    doc.bullet("AVM Spread Percent = (AVM High minus AVM Low) divided by AVM Low")
    doc.bullet("Spread Classification: Tight (<=12), Normal (<=18), Wide (>18)")
    doc.bullet("Bid Cap Guidance = Value Low x 0.85")
    doc.bullet("Target bid window = 21 to 60 days before the scheduled sale")
    doc.bullet("Minimum Value Threshold = $300,000")

    doc.gap(12)

    doc.section("Full Qualification Criteria")
    doc.bullet("Property data must be enriched")
    doc.bullet("Auction readiness must be GREEN")
    doc.bullet("Days-to-sale must be 21 to 60 days")
    doc.bullet("Value Low must be at least $300,000")
    doc.bullet("Value spread must be 18 percent or less")
    doc.bullet("Falco underwriting must be complete")

    doc.gap(16)

    doc.section("Disclaimer")
    doc.body(
        "This document is prepared from public property records, automated valuation data, and internal scoring and is provided for informational purposes only. "
        "It does not constitute investment advice, legal counsel, or a guarantee of property value. "
        "Investors must conduct independent due diligence including title search, lien verification, "
        "physical inspection, and legal review prior to any acquisition decision.",
        size=8,
        color=_GRAY,
        leading=12,
    )

    doc.gap(20)
    doc.body(
        "Prepared by FALCO.  Auction partner inquiries: falco.llc",
        size=7.5,
        color=_GRAY,
        leading=11,
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

    # Inject sale_time / sale_location from the notice artifact so both the
    # Page-1 Why This Property Is in Distress and Page-4 Sale Timeline can render them.
    # Respects any value already present (e.g. from ingest_event raw_json).
    _lk_pdf = (fields.get("lead_key") or "").strip()
    if _lk_pdf and (not fields.get("sale_time") or not fields.get("sale_location")):
        _lp_sale = _extract_lp_contact(_lk_pdf)
        if not fields.get("sale_time") and _lp_sale.get("sale_time"):
            fields["sale_time"] = _lp_sale["sale_time"]
        if not fields.get("sale_location") and _lp_sale.get("sale_location"):
            fields["sale_location"] = _lp_sale["sale_location"]

    # Hydrate property fields from ATTOM detail blob when fields are missing.
    # Parse attom_detail JSON string → dict if needed.
    _ad = fields.get("attom_detail")
    if _ad and isinstance(_ad, str):
        try:
            _ad = json.loads(_ad)
            fields["attom_detail"] = _ad
        except Exception:
            _ad = None

    # Resolve the canonical detail sub-dict.
    # fields["attom_detail"] may be the detail dict directly (current pipeline),
    # a wrapper {"detail": {...}} (old shape), or absent — fall back to attom_raw_json.
    _det: Dict[str, Any] = {}
    if isinstance(_ad, dict):
        if "identifier" in _ad or "address" in _ad or "summary" in _ad:
            _det = _ad                          # attom_detail IS the detail dict
        elif "detail" in _ad:
            _det = _ad.get("detail") or {}      # wrapper shape
    if not _det:
        _rj = fields.get("attom_raw_json")
        if _rj:
            try:
                _rb = json.loads(_rj) if isinstance(_rj, str) else _rj
                if isinstance(_rb, dict):
                    _det = _rb.get("detail") or {}
            except Exception:
                pass

    if _det:
        _d_id   = _det.get("identifier")  or {}
        _d_addr = _det.get("address")     or {}
        _d_sum  = _det.get("summary")     or {}
        _d_bld  = _det.get("building")    or {}
        _d_lot  = _det.get("lot")         or {}

        def _hyd(key: str, val: Any) -> None:
            if val is not None and str(val).strip() and not fields.get(key):
                fields[key] = val

        # APN wins unconditionally — ATTOM internal Id must not appear as parcel ID
        _apn = _d_id.get("apn")
        if _apn:
            fields["property_identifier"] = _apn
            fields["parcel_id"]           = _apn

        _hyd("city",         _d_addr.get("locality"))
        _hyd("zip",          _d_addr.get("postal1"))
        _hyd("year_built",   _d_sum.get("yearbuilt"))
        _hyd("property_type",
             _d_sum.get("propertyType") or _d_sum.get("proptype") or _d_sum.get("propclass"))
        _hyd("land_use",     _d_sum.get("propLandUse"))
        _hyd("lot_size",     (_d_lot.get("lotsize2") if isinstance(_d_lot, dict) else None))

        _bld_sz  = (_d_bld.get("size")         or {}) if isinstance(_d_bld, dict) else {}
        _bld_rm  = (_d_bld.get("rooms")        or {}) if isinstance(_d_bld, dict) else {}
        _bld_con = (_d_bld.get("construction") or {}) if isinstance(_d_bld, dict) else {}
        _bld_sum = (_d_bld.get("summary")      or {}) if isinstance(_d_bld, dict) else {}

        if not fields.get("building_area_sqft"):
            for _szk in ("livingsize", "bldgsize", "grosssizeadjusted", "universalsize"):
                _szv = _bld_sz.get(_szk)
                if _szv is not None:
                    try:
                        fields["building_area_sqft"] = float(_szv)
                        break
                    except (TypeError, ValueError):
                        pass

        _hyd("construction_type",
             _bld_con.get("constructiontype") or _bld_con.get("frameType"))
        _hyd("stories",
             _bld_sum.get("stories") or _bld_sz.get("stories") or _bld_sum.get("levels"))

        # Beds/baths: set when genuinely present; otherwise infer and label as estimated
        _hyd("beds",  _bld_rm.get("beds")  or _bld_sz.get("bdrms"))
        _hyd("baths", _bld_rm.get("bathstotal") or _bld_rm.get("baths")
                      or _bld_sz.get("bathstotal"))

        # Bathroom inference from fixture count when baths still missing
        if not fields.get("baths"):
            try:
                _fixtures = int(_bld_rm.get("bathfixtures") or 0)
                if _fixtures > 0:
                    _bath_est = (
                        1 if _fixtures <= 6  else
                        2 if _fixtures <= 10 else
                        3 if _fixtures <= 14 else
                        4
                    )
                    fields["baths"] = f"{_bath_est} (est.)"
            except (TypeError, ValueError):
                pass

        # Bedroom inference from living sqft when beds still missing
        if not fields.get("beds"):
            _sqft = fields.get("building_area_sqft")
            if _sqft is not None:
                try:
                    _sqft_f = float(_sqft)
                    _bed_est = (
                        2 if _sqft_f < 900  else
                        3 if _sqft_f < 2200 else
                        4 if _sqft_f < 3000 else
                        5
                    )
                    fields["beds"] = f"{_bed_est} (est.)"
                except (TypeError, ValueError):
                    pass

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
    _page4_timeline_risk(doc, fields, brief)
    doc.new_page()
    _page3_property_facts(doc, fields)
    if (fields.get("distress_type") or "").upper() == "LIS_PENDENS":
        doc.new_page()
        _page_foreclosure_notice(doc, fields)
        _pages_notice_exhibit_images(doc, fields)
    doc.new_page()
    _page5_scoring_appendix(doc, fields, img_embedded=bool(img_path))
    doc.save()
    return path


