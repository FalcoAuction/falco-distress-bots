# src/settings.py
"""
Centralized runtime settings + small helpers shared by bots.

Bots should read:
- Allowed counties (base names) from env FALCO_ALLOWED_COUNTIES
- Days-to-sale window from env FALCO_DTS_MIN / FALCO_DTS_MAX
- Optional raw snippet clipping limit from env FALCO_MAX_RAW_SNIPPET_CHARS
"""

from __future__ import annotations

import os
from typing import Iterable


# ----------------------------
# Counties
# ----------------------------

DEFAULT_ALLOWED_COUNTIES = ["Davidson", "Williamson", "Rutherford", "Wilson", "Sumner"]


def get_allowed_counties_base() -> set[str]:
    """
    Returns base county names like {"Davidson","Sumner"} (no 'County' suffix).
    Uses env var FALCO_ALLOWED_COUNTIES (comma-separated). Falls back to defaults.
    """
    raw = os.getenv("FALCO_ALLOWED_COUNTIES", ",".join(DEFAULT_ALLOWED_COUNTIES))
    items = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        # normalize: strip trailing "County" if user included it
        if p.lower().endswith(" county"):
            p = p[:-7].strip()
        items.append(p)

    return set(items) if items else set(DEFAULT_ALLOWED_COUNTIES)


def county_base(name: str | None) -> str | None:
    """
    "Davidson County" -> "Davidson"
    "Davidson" -> "Davidson"
    """
    if not name:
        return None
    n = " ".join(str(name).strip().split())
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n or None


def normalize_county(name: str | None) -> str | None:
    """
    Ensures 'X County' format.
    """
    b = county_base(name)
    if not b:
        return None
    return f"{b} County"


def is_allowed_county(county_name: str | None, allowed_bases: set[str] | None = None) -> bool:
    """
    True if county base exists in allowed bases.
    """
    allowed = allowed_bases or get_allowed_counties_base()
    b = county_base(county_name)
    if not b:
        return False
    return b in allowed


# ----------------------------
# DTS (days-to-sale) window
# ----------------------------

def get_dts_window(default_min: int = 21, default_max: int = 90) -> tuple[int, int]:
    """
    Reads FALCO_DTS_MIN/FALCO_DTS_MAX, returns (min,max) ints.
    """
    try:
        dmin = int(os.getenv("FALCO_DTS_MIN", str(default_min)))
    except Exception:
        dmin = default_min

    try:
        dmax = int(os.getenv("FALCO_DTS_MAX", str(default_max)))
    except Exception:
        dmax = default_max

    # safety normalize
    if dmin < 0:
        dmin = 0
    if dmax < dmin:
        dmax = dmin

    return dmin, dmax


# ----------------------------
# Raw snippet clipping
# ----------------------------

def clip_raw_snippet(text: str, max_chars: int | None = None) -> str:
    """
    Hard-cap raw notice text so Notion doesn't get flooded.
    Default cap can be set via env var FALCO_MAX_RAW_SNIPPET_CHARS.
    """
    if text is None:
        return ""

    s = str(text).strip()
    if not s:
        return ""

    # Prefer explicit arg, else env var, else safe default
    if max_chars is None:
        try:
            max_chars = int(os.getenv("FALCO_MAX_RAW_SNIPPET_CHARS", "1200"))
        except Exception:
            max_chars = 1200

    if max_chars <= 0:
        return ""

    # normalize whitespace
    s = " ".join(s.split())

    if len(s) <= max_chars:
        return s

    return s[: max_chars - 3].rstrip() + "..."


# ----------------------------
# Minor shared helpers
# ----------------------------

def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default
