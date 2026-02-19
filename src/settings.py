# src/settings.py

import os
from typing import Tuple, List, Optional

from .config import (
    TARGET_COUNTIES,
    GLOBAL_DTS_WINDOW,
    DEFAULT_ALLOWED_COUNTIES_BASE,
    PUBLIC_NOTICES_DTS_WINDOW,
    FORECLOSURE_TN_DTS_WINDOW,
    TNFN_DTS_WINDOW,
    TAXPAGES_DTS_WINDOW,
)

BOTKEY_TO_CONFIG_WINDOW = {
    "PUBLIC_NOTICES": PUBLIC_NOTICES_DTS_WINDOW,
    "FORECLOSURE_TN": FORECLOSURE_TN_DTS_WINDOW,
    "TNFN": TNFN_DTS_WINDOW,
    "TAXPAGES": TAXPAGES_DTS_WINDOW,
}


def get_allowed_counties_base() -> List[str]:
    """
    Returns base county names like ["Davidson","Sumner"].

    Precedence:
    1) Env: FALCO_ALLOWED_COUNTIES="Davidson,Sumner"
    2) config.DEFAULT_ALLOWED_COUNTIES_BASE
    """
    raw = os.getenv("FALCO_ALLOWED_COUNTIES", "").strip()
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return parts
    return list(DEFAULT_ALLOWED_COUNTIES_BASE)


def normalize_county_full(name: Optional[str]) -> Optional[str]:
    """
    Turns "Davidson" -> "Davidson County"
    Leaves "Davidson County" as-is.
    """
    if not name:
        return None
    n = " ".join(name.strip().split())
    if n.lower().endswith(" county"):
        return n
    return f"{n} County"


def county_base(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    n = " ".join(name.strip().split())
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n


def is_allowed_county(county_full_or_base: Optional[str]) -> bool:
    """
    Accepts "Davidson" or "Davidson County".
    Compares against allowed base list.
    """
    base = county_base(county_full_or_base)
    if not base:
        return False
    return base in set(get_allowed_counties_base())


def within_target_counties(county_full: Optional[str]) -> bool:
    """
    Enforces config.TARGET_COUNTIES if it's non-empty.
    TARGET_COUNTIES must be full format: "Davidson County".
    """
    if not TARGET_COUNTIES:
        return True
    if not county_full:
        return False
    return county_full in set(TARGET_COUNTIES)


def get_dts_window(botkey: str) -> Tuple[int, int]:
    """
    Future-proof DTS resolution.

    Precedence (highest -> lowest):
    1) Env per-bot:  FALCO_<BOTKEY>_DTS_MIN/MAX
    2) Config per-bot window (from BOTKEY_TO_CONFIG_WINDOW)
    3) Env global:   FALCO_DTS_MIN/MAX
    4) Config global GLOBAL_DTS_WINDOW
    """
    bk = botkey.strip().upper()

    # 1) env per-bot
    env_min = os.getenv(f"FALCO_{bk}_DTS_MIN")
    env_max = os.getenv(f"FALCO_{bk}_DTS_MAX")
    if env_min and env_max:
        return int(env_min), int(env_max)

    # 2) config per-bot
    if bk in BOTKEY_TO_CONFIG_WINDOW:
        w = BOTKEY_TO_CONFIG_WINDOW[bk]
        if w and len(w) == 2:
            return int(w[0]), int(w[1])

    # 3) env global
    gmin = os.getenv("FALCO_DTS_MIN")
    gmax = os.getenv("FALCO_DTS_MAX")
    if gmin and gmax:
        return int(gmin), int(gmax)

    # 4) config global
    return int(GLOBAL_DTS_WINDOW[0]), int(GLOBAL_DTS_WINDOW[1])
