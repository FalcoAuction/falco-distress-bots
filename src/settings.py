# src/settings.py
"""Shared configuration + normalization helpers used by all bots.

Hard goals:
- Keep signatures stable across bot imports (back/forward compat).
- Centralize: allowed counties, county normalization, target-county filtering,
  distress time window (days-to-sale), and raw snippet clipping.

Env vars (backwards compatible):
- FALCO_ALLOWED_COUNTIES: CSV of county base names (e.g. Davidson,Williamson)
- FALCO_DTS_MIN, FALCO_DTS_MAX: default days-to-sale window
- FALCO_PUBLIC_DTS_MIN, FALCO_PUBLIC_DTS_MAX: PublicNoticesBot override window
- FALCO_MAX_RAW_SNIPPET_CHARS: default snippet length (1200)
"""

from __future__ import annotations

import os
from typing import Optional, Tuple


# ============================================================
# COUNTY HELPERS
# ============================================================

def _norm_ws(s: str) -> str:
    return " ".join(str(s).strip().split())


def county_base(name: Optional[str]) -> Optional[str]:
    """Return base county name without 'County' suffix, normalized whitespace."""
    if not name:
        return None
    n = _norm_ws(name)
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n or None


def normalize_county(name: Optional[str]) -> Optional[str]:
    """Return 'X County' canonical form (or None)."""
    b = county_base(name)
    if not b:
        return None
    return f"{b} County"


# Back-compat alias (some bots import this)
def normalize_county_full(name: Optional[str]) -> Optional[str]:
    return normalize_county(name)


def get_allowed_counties_base() -> set[str]:
    """Read allowlist from env and return a set of base names."""
    raw = os.getenv("FALCO_ALLOWED_COUNTIES", "Davidson,Williamson,Rutherford,Wilson,Sumner")
    vals: list[str] = []
    for part in raw.split(","):
        p = part.strip()
        if p:
            vals.append(p)
    return {county_base(v) for v in vals if county_base(v)}


def is_allowed_county(county_name: Optional[str]) -> bool:
    """Accepts 'X' or 'X County' and checks against env allowlist."""
    b = county_base(county_name)
    if not b:
        return False
    return b in get_allowed_counties_base()


def _get_config_target_counties_fallback() -> list[str]:
    """If a bot calls within_target_counties(county) with no 2nd arg, try src.config.TARGET_COUNTIES."""
    try:
        from .config import TARGET_COUNTIES  # type: ignore
        if isinstance(TARGET_COUNTIES, list):
            return TARGET_COUNTIES
        return []
    except Exception:
        return []


def within_target_counties(county_name: Optional[str], target_counties: Optional[list[str]] = None) -> bool:
    """BACKWARD + FORWARD COMPAT target-county filter.

    - Old call: within_target_counties(county)
    - New call: within_target_counties(county, TARGET_COUNTIES)

    If target_counties is None, we fallback to src.config.TARGET_COUNTIES if present.
    If still empty/None => allow.
    """
    if target_counties is None:
        target_counties = _get_config_target_counties_fallback()

    if not target_counties:
        return True

    b = county_base(county_name)
    if not b:
        return False

    targets_base = {county_base(t).lower() for t in target_counties if county_base(t)}
    return b.lower() in targets_base


# ============================================================
# DISTRESS WINDOW CONTROL
# ============================================================

def get_dts_window(source: Optional[str] = None, *args, **kwargs) -> Tuple[int, int]:
    """Return (min_days, max_days) days-to-sale window.

    Stable signature: accepts optional `source` plus *args/**kwargs so older/newer callers won't crash.

    Default env vars:
      FALCO_DTS_MIN (default 21)
      FALCO_DTS_MAX (default 90)

    PublicNoticesBot override (if source indicates public notices):
      FALCO_PUBLIC_DTS_MIN (default falls back to FALCO_DTS_MIN or 0)
      FALCO_PUBLIC_DTS_MAX (default falls back to FALCO_DTS_MAX or 120)
    """
    src = (source or "").upper()

    if "PUBLIC" in src:
        # More permissive defaults to avoid silent skips.
        try:
            dts_min = int(os.getenv("FALCO_PUBLIC_DTS_MIN", os.getenv("FALCO_DTS_MIN", "0")))
        except Exception:
            dts_min = 0
        try:
            dts_max = int(os.getenv("FALCO_PUBLIC_DTS_MAX", os.getenv("FALCO_DTS_MAX", "120")))
        except Exception:
            dts_max = 120
        return dts_min, dts_max

    try:
        dts_min = int(os.getenv("FALCO_DTS_MIN", "21"))
    except Exception:
        dts_min = 21

    try:
        dts_max = int(os.getenv("FALCO_DTS_MAX", "90"))
    except Exception:
        dts_max = 90

    return dts_min, dts_max


# ============================================================
# RAW SNIPPET CONTROL
# ============================================================

def clip_raw_snippet(text: Optional[str], max_chars: Optional[int] = None) -> str:
    """Clip + normalize Raw Snippet so Notion doesn't store full notice bodies.

    Env:
      FALCO_MAX_RAW_SNIPPET_CHARS (default 1200)
    """
    if text is None:
        return ""

    s = str(text).strip()
    if not s:
        return ""

    if max_chars is None:
        try:
            max_chars = int(os.getenv("FALCO_MAX_RAW_SNIPPET_CHARS", "1200"))
        except Exception:
            max_chars = 1200

    if max_chars <= 0:
        return ""

    s = " ".join(s.split())
    if len(s) <= max_chars:
        return s

    return s[: max_chars - 3].rstrip() + "..."
