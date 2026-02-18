from datetime import date, datetime
from typing import Dict, Any

# Early-stage liquidity / focus. Tune later.
FOCUS_COUNTIES = {
    "Davidson", "Williamson", "Rutherford", "Sumner", "Wilson", "Maury",
    "Montgomery", "Robertson", "Dickson", "Bedford", "Putnam"
}

COUNTY_LIQUIDITY = {  # 1 (low) -> 5 (high)
    "Davidson": 5,
    "Williamson": 5,
    "Rutherford": 4,
    "Sumner": 4,
    "Wilson": 3,
    "Maury": 3,
    "Montgomery": 3,
    "Robertson": 3,
    "Dickson": 3,
    "Bedford": 2,
    "Putnam": 2,
}

# Risk keyword flags (best-effort)
BANKRUPTCY_WORDS = ["bankruptcy", "chapter 7", "chapter 11", "chapter 13", "bk"]
PROBATE_WORDS = ["estate of", "probate", "executor", "administrator", "letters testamentary", "letters of administration"]
HOA_WORDS = ["hoa", "homeowners association", "condominium", "condo", "horizontal property regime"]

def _parse_iso_date(iso: str) -> date | None:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso).date()
    except Exception:
        return None

def days_to_sale(sale_date_iso: str) -> int | None:
    d = _parse_iso_date(sale_date_iso)
    if not d:
        return None
    return (d - date.today()).days

def detect_risk_flags(text: str) -> Dict[str, bool]:
    t = (text or "").lower()
    return {
        "bankruptcy": any(w in t for w in BANKRUPTCY_WORDS),
        "probate": any(w in t for w in PROBATE_WORDS),
        "hoa_condo": any(w in t for w in HOA_WORDS),
    }

def triage(dts: int | None, flags: Dict[str, bool]) -> tuple[str, str]:
    """
    Strategic triage:
    - Past sale dates => KILL (historical / irrelevant)
    - <30 days to sale => MONITOR (still potentially actionable)
    - Bankruptcy / HOA-condo / probate authority => KILL
    """
    if flags.get("bankruptcy"):
        return "KILL", "KILL: bankruptcy flag"
    if flags.get("hoa_condo"):
        return "KILL", "KILL: HOA/condo flag"
    if flags.get("probate"):
        return "KILL", "KILL: probate authority risk"

    if dts is None:
        return "MONITOR", "MONITOR: sale date missing"
    if dts < 0:
        return "KILL", "KILL: past sale date"
    if dts < 30:
        return "MONITOR", "MONITOR: sale < 30 days"

    return "", ""

def score_v2(distress_type: str, county: str, dts: int | None, has_contact: bool) -> int:
    # Score answers: "likelihood to close into auction commission within timeline"
    score = 0

    # Distress type weight
    dt = (distress_type or "").lower()
    if "trustee" in dt or "foreclosure" in dt:
        score += 35
    elif dt == "tax":
        score += 28
    elif dt == "estate":
        score += 10
    else:
        score += 5

    # Timeline comfort (strategic)
    if dts is None:
        score += 0
    elif dts >= 60:
        score += 35
    elif dts >= 28:
        score += 25
    elif dts >= 0:
        score += 10
    else:
        score -= 40  # past date should be killed

    # County liquidity (light early)
    if county:
        liq = COUNTY_LIQUIDITY.get(county, 2)
        score += (liq - 1) * 3  # 0..12

    # Contact ability
    if has_contact:
        score += 10
    else:
        score -= 10

    return max(0, min(100, score))

def label(distress_type: str, county: str, dts: int | None, flags: Dict[str, bool], score: int, has_contact: bool) -> str:
    is_focus = (county in FOCUS_COUNTIES) if county else False
    dt = (distress_type or "").lower()
    is_primary_dt = ("trustee" in dt) or (dt == "tax")

    if (dts is not None and dts >= 28) and is_primary_dt and is_focus and (not any(flags.values())) and has_contact and score >= 70:
        return "GREEN"

    return "MONITOR"
