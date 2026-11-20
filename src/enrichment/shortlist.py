"""Which ranked leads earn a paid county lien check.

Tier 1 (every lead) is free: owner, county appraisal, sale history, the
distress notice. Tier 2 is a Register of Deeds search for every open loan and
lien, which costs a subscription and minutes per lead, so only leads that
could become an auction listing get it:

  - in Tennessee (statewide since 2026-11-20)
  - on today's ranked list (lead_opportunities already applies the value
    floor, the no_equity rule and the auctionable-lead rules)
  - not about to sell: a foreclosure sale fewer than MIN_DAYS_TO_SALE days out
    leaves no time to run an auction
  - not checked at the county in the last RECHECK_DAYS days

Each queued lead also gets the county system its Register uses, so the
subscription cost per county is visible before anything is bought.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional, Tuple

MIDDLE_TN = {
    "bedford", "cannon", "cheatham", "clay", "coffee", "davidson", "dekalb", "dickson", "fentress",
    "franklin", "giles", "grundy", "hickman", "houston", "humphreys", "jackson", "lawrence", "lewis",
    "lincoln", "macon", "marshall", "maury", "montgomery", "moore", "overton", "perry", "pickett",
    "putnam", "robertson", "rutherford", "sequatchie", "smith", "stewart", "sumner", "trousdale",
    "van buren", "warren", "wayne", "white", "williamson", "wilson",
}
# Statewide since 2026-11-20: the ranked list decides which leads are in play.
from ..bots._regions import TN_COUNTIES as IN_SCOPE  # noqa: E402

# Register of Deeds access per county (2026-11 research; prices in the
# pilot budget). Counties not listed default to TitleSearcher, which carries
# most outer Middle Tennessee counties at $40 to $45 a month plus $20 for each
# extra county.
REGISTER_SYSTEM = {
    "davidson": "davidson_portal",        # davidsonportal.com, $50/mo
    "williamson": "williamson_proaccess",  # ProAccess, $50/mo or $100 unlimited
    "rutherford": "ustitlesearch",         # existing account, unlimited
    "sumner": "ustitlesearch",             # existing account, unlimited
    "montgomery": "ustitlesearch",         # same system, day passes on the existing account
    "wilson": "wilsondeeds",               # subscription, price not confirmed
    "maury": "titlesearcher",              # $45/mo
}
DEFAULT_SYSTEM = "titlesearcher"

MIN_DAYS_TO_SALE = 21
TIGHT_DAYS_TO_SALE = 45
RECHECK_DAYS = 30


def county_key(county: Optional[str]) -> str:
    return (county or "").lower().replace(" county", "").strip()


def register_system(county: Optional[str]) -> str:
    return REGISTER_SYSTEM.get(county_key(county), DEFAULT_SYSTEM)


def _as_date(v: Any) -> Optional[date]:
    if not v:
        return None
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def screen(opportunity: Dict[str, Any], raw_payload: Optional[Dict[str, Any]], today: date) -> Tuple[bool, Dict[str, Any]]:
    """(eligible, detail). `opportunity` is a lead_opportunities row; the
    detail says why a lead was left out, or how urgent it is."""
    county = county_key(opportunity.get("county"))
    if county not in IN_SCOPE:
        return False, {"reason": "outside_tennessee"}
    sale = _as_date(opportunity.get("sale_date"))
    days = (sale - today).days if sale else None
    if days is not None and 0 <= days < MIN_DAYS_TO_SALE:
        return False, {"reason": "sale_too_soon", "days_to_sale": days}
    check = (raw_payload or {}).get("lien_check") or {}
    checked = _as_date(check.get("checked_at"))
    if checked and (today - checked).days < RECHECK_DAYS:
        return False, {"reason": "checked_recently", "checked_at": checked.isoformat()}
    if check.get("status") == "queued" and not checked:
        return False, {"reason": "already_queued"}
    return True, {
        "county": county,
        "register_system": register_system(county),
        "days_to_sale": days,
        "tight": days is not None and days < TIGHT_DAYS_TO_SALE,
        "expected_fee": opportunity.get("expected_fee"),
    }
