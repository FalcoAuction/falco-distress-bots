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


# Codes that on their own signal a real auctionable deal — vacant
# building, dangerous structure, demolition order, condemned, unfit
# for habitation. Owner is either gone or the building is unsalvageable
# without major capital. These are the gold standard for CV leads.
SEVERE_PATTERNS = (
    "UNFIT FOR HABITATION", "DEMOLITION", "OPEN VACANT BUILDING",
    "VACANT/SUBSTANDARD", "VACANT BUILDING", "BOARDED", "ABANDON",
    "DANGEROUS BUILDING", "CONDEMN", "UNSAFE STRUCTURE",
    "ENVIRONMENTAL COURT",  # case escalated to court = serious
)

# Codes signaling structural / building-envelope work that a casual
# owner-occupier doesn't fix on a $200 ticket. Single citation = real
# deal candidate.
STRUCTURAL_PATTERNS = (
    "STRUCTURAL", "ROOF", "FOUNDATION", "EXTERIOR REPAIR",
    "WINDOWS BROKEN", "WALL", "SIDING", "CEILING", "FLOOR", "STAIRS",
    "MAJOR SECTIONS", "STANDING WATER", "MOLD", "INFEST", "RAT", "RODENT",
    "PLUMBING", "ELECTRICAL", "MECHANICAL", "SEWER", "GAS LEAK",
    # Substandard living
    "BLDG MAINTENANCE", "PROPERTY MAINTENANCE",
)

# Codes that are real but not dispositive on their own. Open storage +
# junk/trash + abandoned vehicles signal SOMETHING but a single
# citation alone is borderline — the owner might just clean it up and
# pay the $200 fine. We KEEP these only when stacked with another code
# (3+ violations cited) since chronic accumulation is a real distress
# signal but a one-off isn't.
CHRONIC_PATTERNS = (
    "ACCUMULATION OF DEBRIS", "OPEN STORAGE", "JUNK", "TRASH",
    "DEBRIS", "RUBBISH",
    "INOP", "UNLIC", "ABANDONED VEHICLE", "JUNK VEHICLE",
    "EXTERIOR PAINT", "EXTERIOR OF BLDGS",
    "HEARING",
)

# Combined dealable set (used by legacy is_auctionable_cv only).
DEALABLE_PATTERNS = SEVERE_PATTERNS + STRUCTURAL_PATTERNS + CHRONIC_PATTERNS

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
    """STRICT filter — only ingests CV leads that are genuinely auction-
    grade. Reject everything else, including unclassified codes (the old
    default-keep was filling the dialer with sign-permit and paving
    citations).

    Tier-1 keep rules:
      - SEVERE pattern (vacant/dangerous/unfit/condemn/etc.) — single
        citation is enough; the building is the deal.
      - STRUCTURAL pattern (roof/foundation/exterior repair/etc.) —
        single citation is enough; not a $200-fine fix.
      - CHRONIC pattern (open storage/junk/debris/inop vehicles) ONLY
        when 3+ violations cited — chronic accumulation across multiple
        codes signals real distress, single citations don't.

    Reject:
      - Lawn-only (high weeds, grass)
      - Single chronic code (one open-storage citation)
      - Unclassified codes (paving, sign permits, building permit
        required, certificate of compliance — these are paperwork, not
        property condition)
      - Empty / unparseable violation field

    Returns (is_auctionable, reason).
    """
    codes = _split_codes(violation_field or "")
    if not codes:
        # No parseable codes = reject (old behavior was keep; the
        # backlog showed unparseable strings are almost always paving /
        # sign / paperwork citations, not real building issues).
        return False, "no_codes_parsed"

    severe = [c for c in codes if _matches_any(c, SEVERE_PATTERNS)]
    if severe:
        return True, f"severe: {severe[0]}"

    structural = [c for c in codes if _matches_any(c, STRUCTURAL_PATTERNS)]
    if structural:
        return True, f"structural: {structural[0]}"

    chronic = [c for c in codes if _matches_any(c, CHRONIC_PATTERNS)]
    # Chronic accumulation only counts when stacked: 3+ codes total
    # OR 2+ chronic codes cited at once.
    if (len(codes) >= 3 and chronic) or len(chronic) >= 2:
        return True, f"chronic_stack: {len(chronic)}/{len(codes)}"

    lawn = [c for c in codes if _matches_any(c, LAWN_ONLY_PATTERNS)]
    if lawn and len(lawn) == len(codes):
        return False, f"lawn_only: {','.join(lawn[:3])}"

    # Anything else — paperwork (paving, sign permit, building permit
    # required), single chronic, single unclassified code — reject.
    return False, f"low_grade: {','.join(codes[:3])}"
