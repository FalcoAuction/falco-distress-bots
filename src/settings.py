# src/settings.py
"""
Shared settings/helpers for Falco distress bots.

Purpose:
- Centralize env/config parsing so bots don't duplicate logic.
- Provide stable helper functions that bots can import:
    - get_allowed_counties()
    - get_dts_window()
    - within_target_counties()
    - clip_raw_snippet()
    - county_base()
    - normalize_county()

Conventions:
- "Allowed counties" are enforced first (from env or config fallback).
- "Target counties" is optional (from config.py). If empty => statewide allowed.
- County comparisons are done on the BASE name (e.g., "Davidson" matches "Davidson County").
"""

from __future__ import annotations

import os
import re
from typing import Iterable, Tuple, Optional


# -----------------------------
# County normalization
# -----------------------------

def county_base(name: str | None) -> str | None:
    """
    Converts "Davidson County" -> "Davidson"
    Converts " davidson   county " -> "davidson" (base returned in original casing? we return normalized casing)
    """
    if not name:
        return None
    n = " ".join(str(name).strip().split())
    if not n:
        return None
    n = re.sub(r"\s+", " ", n).strip()
    # remove trailing "County" (case-insensitive)
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n if n else None


def normalize_county(name: str | None) -> str | None:
    """
    Ensures county is stored as "X County".
    - "Davidson" -> "Davidson County"
    - "Davidson County" -> "Davidson County"
    """
    b = county_base(name)
    if not b:
        return None
    return f"{b} County"


# -----------------------------
# Allowed Counties / DTS window
# -----------------------------

_DEFAULT_ALLOWED = "Davidson,Williamson,Rutherford,Wilson,Sumner"
_DEFAULT_DTS_MIN = 21
_DEFAULT_DTS_MAX = 90

def get_allowed_counties() -> set[str]:
    """
    Returns allowed county BASE names (e.g., {"Davidson","Wilson"}).

    Priority:
    1) env var FALCO_ALLOWED_COUNTIES = "Davidson,Wilson,..."
    2) default list in this module

    NOTE: This returns BASE names only (no "County" suffix).
    """
    raw = os.getenv("FALCO_ALLOWED_COUNTIES", _DEFAULT_ALLOWED)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    bases = set()
    for p in parts:
        b = county_base(p)
        if b:
            bases.add(b)
    return bases


def get_dts_window() -> Tuple[int, int]:
    """
    Returns (min_days, max_days) until sale.

    Priority:
    1) env vars FALCO_DTS_MIN / FALCO_DTS_MAX
    2) defaults in this module
    """
    try:
        dmin = int(os.getenv("FALCO_DTS_MIN", str(_DEFAULT_DTS_MIN)))
    except Exception:
        dmin = _DEFAULT_DTS_MIN

    try:
        dmax = int(os.getenv("FALCO_DTS_MAX", str(_DEFAULT_DTS_MAX)))
    except Exception:
        dmax = _DEFAULT_DTS_MAX

    # safety
    if dmin < 0:
        dmin = 0
    if dmax < dmin:
        dmax = dmin

    return dmin, dmax


# -----------------------------
# Target counties (optional strict filter)
# -----------------------------

def within_target_counties(county_name: str | None, target_counties: list[str] | None) -> bool:
    """
    Enforces optional TARGET_COUNTIES from config.py.

    - If target_counties is empty/None => allow everything (True)
    - Otherwise county must match one of target_counties (case-insensitive),
      with or without the "County" suffix.
    """
    if not target_counties:
        return True

    b = county_base(county_name)
    if not b:
        return False

    targets_base = set()
    for t in target_counties:
        tb = county_base(t)
        if tb:
            targets_base.add(tb.lower())

    return b.lower() in targets_base


# -----------------------------
# Raw snippet clipping
# -----------------------------

def clip_raw_snippet(text: str | None, limit: int | None = None) -> str:
    """
    Clips large raw notice text so Notion doesn't get flooded.

    Defaults:
    - limit from env FALCO_RAW_SNIPPET_MAX (int) OR 1200 chars.

    Behavior:
    - Normalize whitespace
    - Clip to limit with a clear suffix
    """
    if not text:
        return ""

    try:
        max_chars = int(os.getenv("FALCO_RAW_SNIPPET_MAX", "1200"))
    except Exception:
        max_chars = 1200

    if limit is not None:
        try:
            max_chars = int(limit)
        except Exception:
            pass

    # normalize whitespace
    cleaned = re.sub(r"\s+", " ", str(text)).strip()

    if max_chars <= 0:
        return cleaned

    if len(cleaned) <= max_chars:
        return cleaned

    return cleaned[: max_chars - 40].rstrip() + " ... [clipped]"


# -----------------------------
# Convenience checks (optional)
# -----------------------------

def is_allowed_county(county_name: str | None, allowed_bases: Optional[set[str]] = None) -> bool:
    """
    Checks whether the given county is in allowed list.
    Uses base matching ("Davidson" matches "Davidson County").
    """
    allowed_bases = allowed_bases or get_allowed_counties()
    b = county_base(county_name)
    if not b:
        return False
    return b in allowed_bases

# Back-compat alias (some bots import this name)
def normalize_county_full(name: str | None) -> str | None:
    return normalize_county(name)

