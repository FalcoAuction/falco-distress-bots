# src/settings.py

import os

# ============================================================
# COUNTY HELPERS
# ============================================================

def county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = " ".join(str(name).strip().split())
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n


def normalize_county(name: str | None) -> str | None:
    b = county_base(name)
    if not b:
        return None
    return f"{b} County"


# Back-compat alias (some bots import this name)
def normalize_county_full(name: str | None) -> str | None:
    return normalize_county(name)


def within_target_counties(county_name: str | None, target_counties: list[str] | None) -> bool:
    if not target_counties:
        return True

    b = county_base(county_name)
    if not b:
        return False

    targets_base = {
        county_base(t).lower()
        for t in target_counties
        if county_base(t)
    }

    return b.lower() in targets_base


# ============================================================
# DISTRESS WINDOW CONTROL
# ============================================================

def get_dts_window(source: str | None = None):
    """
    Returns (min_days, max_days) window for a source.

    Uses environment variables:
      FALCO_DTS_MIN
      FALCO_DTS_MAX

    If not set:
      default = 21 to 90 days
    """

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

def clip_raw_snippet(text: str, max_chars: int | None = None) -> str:
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
