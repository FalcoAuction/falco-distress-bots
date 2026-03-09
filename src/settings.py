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


# Back-compat alias (some bots import this)
def normalize_county_full(name: str | None) -> str | None:
    return normalize_county(name)


def get_allowed_counties_base() -> set[str]:
    """
    Reads allowed counties from env:
      FALCO_ALLOWED_COUNTIES="Davidson,Williamson,..."
    Returns a set of base county names (no 'County' suffix).
    """
    # WAR-PLAN DEFAULTS (hard): only these counties unless explicitly overridden by env
    raw = os.getenv("FALCO_ALLOWED_COUNTIES", "Davidson,Williamson,Rutherford,Sumner,Wilson,Maury")
    vals: list[str] = []
    for part in raw.split(","):
        p = part.strip()
        if p:
            vals.append(p)
    return {county_base(v) for v in vals if county_base(v)}


def get_allowed_counties_list() -> list[str]:
    """Convenience: stable ordered list of base counties."""
    return sorted(get_allowed_counties_base())


def is_allowed_county(county_name: str | None) -> bool:
    """
    Back-compat function used by multiple bots.
    Accepts 'Davidson' or 'Davidson County' and checks against env allowlist.
    """
    b = county_base(county_name)
    if not b:
        return False
    allowed = get_allowed_counties_base()
    return b in allowed


def _get_config_target_counties_fallback() -> list[str]:
    """
    If a bot calls within_target_counties(county) with no 2nd arg,
    we try to pull TARGET_COUNTIES from src.config if it exists.
    """
    try:
        from .config import TARGET_COUNTIES  # type: ignore
        if isinstance(TARGET_COUNTIES, list):
            return TARGET_COUNTIES
        return []
    except Exception:
        return []


def within_target_counties(county_name: str | None, target_counties: list[str] | None = None) -> bool:
    """
    BACKWARD + FORWARD COMPAT:

    - Old call style: within_target_counties(county)
    - New call style: within_target_counties(county, TARGET_COUNTIES)

    If target_counties is None, we fallback to src.config.TARGET_COUNTIES if present.
    If still empty/None => we default to the allowed counties (WAR-PLAN hard).
    """
    if target_counties is None:
        target_counties = _get_config_target_counties_fallback()

    if not target_counties:
        # Hard default: restrict to allowed counties even if bots didn't pass TARGET_COUNTIES
        target_counties = get_allowed_counties_list()

    b = county_base(county_name)
    if not b:
        return False

    targets_base = {county_base(t).lower() for t in target_counties if county_base(t)}
    return b.lower() in targets_base


# ============================================================
# DISTRESS WINDOW CONTROL
# ============================================================

def get_dts_window(source: str | None = None) -> tuple[int, int]:
    """
    Returns (min_days, max_days) window for filtering.

    Uses env vars:
      FALCO_DTS_MIN
      FALCO_DTS_MAX

    Defaults: 21..90
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
    """
    Keep Notion 'Raw Snippet' short so it doesn't become the full notice.
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

    # collapse whitespace (but preserve some readability by keeping single spaces)
    s = " ".join(s.split())

    if len(s) <= max_chars:
        return s

    return s[: max_chars - 3].rstrip() + "..."
