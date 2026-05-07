"""Shared code-violation auctionability filter.

Patrick's directive (2026-05-07): we should only ingest CODE_VIOLATION
leads that could plausibly be auction deals — chronic vacant, structural,
substandard, dangerous, demolition. Lawn-only leads (high weeds, tall
grass) are not auction deals; the owner pays $200 and mows. Sending an
auction text to those owners is a goodwill burn, not a pipeline win.

Used by every CV scraper (nashville_codes, memphis_codes, etc.) to
filter at ingestion time. Backfill SQL handles existing leads that
predate this filter.

Decision logic:
  1. Split the violation field on common separators (`,`, ` · `, `;`)
     so multi-violation strings like "HIGH WEEDS,EXTERIOR REPAIR,..." are
     evaluated cite-by-cite, not as a blob.
  2. If ANY cited code matches DEALABLE_PATTERNS → keep the lead.
     (One real code is enough; lawn citations alongside a structural
     code still qualify because the structural code is the deal driver.)
  3. If ALL cited codes match LAWN_ONLY_PATTERNS → reject.
  4. If the violation field is empty / unparseable → keep (we'd rather
     review false positives than drop a real one we can't classify).
"""
from __future__ import annotations

import re
from typing import List, Tuple


# Codes that signal a maintenance / structural / vacancy / dangerous-
# building issue. Property is unlikely to be casually fixed by an
# owner-occupier; signals motivated absentee owner or genuine distress.
DEALABLE_PATTERNS = (
    # Building / structural
    "STRUCTURAL", "ROOF", "FOUNDATION", "EXTERIOR OF BLDGS",
    "EXTERIOR REPAIR", "EXTERIOR PAINT", "WINDOWS BROKEN", "WALL",
    "SIDING", "CEILING", "FLOOR", "STAIRS",
    # Vacancy / dangerous
    "UNFIT FOR HABITATION", "DEMOLITION", "OPEN VACANT BUILDING",
    "VACANT/SUBSTANDARD", "VACANT BUILDING", "BOARDED", "ABANDON",
    "DANGEROUS BUILDING", "CONDEMN", "UNSAFE",
    # Substandard living conditions
    "BLDG MAINTENANCE", "PROPERTY MAINTENANCE", "MAJOR SECTIONS",
    "STANDING WATER", "MOLD", "INFEST", "RAT", "RODENT",
    # Chronic / accumulation
    "ACCUMULATION OF DEBRIS", "OPEN STORAGE", "JUNK", "TRASH",
    "DEBRIS", "RUBBISH",
    # Vehicles (chronic absentee owner signal)
    "INOP", "UNLIC", "ABANDONED VEHICLE", "JUNK VEHICLE",
    # Plumbing / electrical / mechanical (substandard)
    "PLUMBING", "ELECTRICAL", "MECHANICAL", "SEWER", "GAS LEAK",
    # Hearing / case escalations
    "HEARING", "ENVIRONMENTAL COURT",
)

# Codes that on their own signal a lawn-only / casual citation.
# Owner mows the lawn, pays $200, done. Not your deal UNLESS another
# dealable code is also cited.
LAWN_ONLY_PATTERNS = (
    "HIGH WEEDS", "TALL GRASS", "WEED/GRASS", "WEEDS", "GRASS",
    "MOW", "OVERGROWN", "PARKING ON GRASS",
)


def _split_codes(violation_field: str) -> List[str]:
    """Split a violation field into individual citation codes.
    Handles comma, semicolon, middot, and pipe separators."""
    if not violation_field:
        return []
    parts = re.split(r"\s*[,;·|]+\s*", violation_field)
    return [p.strip().upper() for p in parts if p.strip()]


def _matches_any(code: str, patterns: Tuple[str, ...]) -> bool:
    return any(p in code for p in patterns)


def is_auctionable_cv(violation_field: str) -> Tuple[bool, str]:
    """True if the lead has at least one dealable citation OR no codes
    can be parsed (review-required default).

    Returns (is_auctionable, reason) so callers can log the decision.
    """
    codes = _split_codes(violation_field or "")
    if not codes:
        return True, "no_codes_parsed"

    dealable_codes = [c for c in codes if _matches_any(c, DEALABLE_PATTERNS)]
    if dealable_codes:
        return True, f"dealable: {dealable_codes[0]}"

    # All codes are non-dealable. Check if they're all lawn-only — if so,
    # explicit reject. If they're something else (rare), keep with a flag.
    lawn_codes = [c for c in codes if _matches_any(c, LAWN_ONLY_PATTERNS)]
    if lawn_codes and len(lawn_codes) == len(codes):
        return False, f"lawn_only: {','.join(lawn_codes[:3])}"

    # Codes present but none match either bucket — keep with review flag.
    # Better to ingest false positives than drop a real one we can't
    # classify; admin can audit.
    return True, f"unclassified_codes: {','.join(codes[:3])}"
