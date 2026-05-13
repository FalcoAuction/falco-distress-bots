"""Address normalization for FALCO scrapers.

The audit caught real garbage in our active foreclosure list:

    "98 Randy Road,\\r\\nMadison, TN, Madison, TN 37115"
    "713 Garland Drive, Old Hickory, Tennessee 37138, Old Hickory, TN 37138"
    "02 73.00 Commonly Property Address: 3074 Richmond Hill Dr., Nashville, TN 37207"
    "0 Brooksboro Place Nashville, TN 37217"
    "5032 BONNAMEADE DR, Hermitage, TN 37076"

These all came from notice PDFs / docket scrapes where parsers grabbed
extra context, kept embedded CRLFs, or surfaced parcel-only references
the assessor uses in lieu of a real street address.

Downstream cost:
  - BatchData / RentCast / Davidson assessor can't AVM "0 Brooksboro"
    because there's no real street number — value never gets filled
  - "98 Randy Road,\\r\\nMadison, TN, Madison, TN 37115" fails address
    geocoding entirely, which kills HMDA tract matching
  - "Property Address: 3074 Richmond Hill" with the prefix attached
    fails fuzzy-match dedup, leaving a duplicate lead

This module normalizes at the LeadPayload boundary — called from
`LeadPayload.as_db_row()` so every scraper benefits without each one
having to remember to call it.

Result is a small dataclass with:
  - normalized:        the cleaned address string, or None if unsalvageable
  - needs_resolution:  True for parcel-only / no-street-number cases;
                       callers can skip enrichment until human review
  - changes:           list of what was fixed (audit / logging)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class NormalizedAddress:
    """Result of normalize_address(). `needs_resolution=True` means the
    address parsed but won't enrich (parcel-only / missing street #)."""
    normalized: Optional[str]
    needs_resolution: bool = False
    changes: List[str] = field(default_factory=list)


# Common prefix-junk that leaks in from notice PDFs.
_PREFIX_PATTERNS = [
    # "02 73.00 Commonly Property Address: 3074 Richmond Hill Dr." style:
    # legal description / map+parcel prefix followed by the real address.
    re.compile(r"^.*?\b(?:commonly[\s,]+(?:known\s+as[\s,]+)?)?property\s+address[:\-]\s*", re.IGNORECASE),
    # "ALSO KNOWN AS:" / "AKA:" prefixes
    re.compile(r"^.*?\baka[:\-]?\s+", re.IGNORECASE),
    re.compile(r"^.*?\balso\s+known\s+as[:\-]?\s+", re.IGNORECASE),
]

# "Tennessee" → "TN" so duplicate-city detection works on a single form.
_STATE_LONG = re.compile(r"\bTennessee\b", re.IGNORECASE)

# Catches "City, TN, City, TN ZIP" — same city duplicated. Captures
# the city + zip from the (typically) last occurrence. The optional
# `(?:\d{5})?` between the two TNs handles the case where both halves
# carry a zip ("Old Hickory, TN 37138, Old Hickory, TN 37138").
_DUP_CITY_STATE = re.compile(
    r",\s*([A-Za-z][A-Za-z\s\.'\-]+?)\s*,\s*TN\s*(?:\d{5})?\s*,?\s*\1\s*,?\s*TN\s*(\d{5})?",
    re.IGNORECASE,
)

# Catches CRLF, LF, tab runs.
_WHITESPACE_NOISE = re.compile(r"[\r\n\t]+")
_MULTI_SPACE = re.compile(r"\s{2,}")
_MULTI_COMMA = re.compile(r",(\s*,)+")

# Parcel-only detector: street number "0", "00", "000" etc. with no
# real address. The assessor uses "0 Knight Drive" for unimproved lots
# or where the parcel doesn't have a USPS-deliverable address. We can't
# AVM these — flag for human review.
_PARCEL_ONLY = re.compile(r"^0+\s+", re.IGNORECASE)

# Detects leading legal-description noise (map+parcel like "MAP 080 PARCEL 003" or
# "Tax Map 12 Parcel 5") followed by an address. We want to strip it.
_LEGAL_DESC_PREFIX = re.compile(
    r"^\s*(?:tax\s+)?map\s+\d+[\w\.\-]*\s+parcel\s+\d+[\w\.\-]*[,\s]+",
    re.IGNORECASE,
)


def normalize_address(raw: Optional[str]) -> NormalizedAddress:
    """Clean a property address string. Pure function; no I/O.

    Returns NormalizedAddress with:
      - normalized: the cleaned string, or None if the input was empty/junk
      - needs_resolution: True if the address is parcel-only (no street #)
      - changes: list of normalization steps applied (for logging)

    Conservative — when in doubt, return the input unchanged with a note.
    """
    if not raw or not str(raw).strip():
        return NormalizedAddress(normalized=None)

    s = str(raw).strip()
    changes: List[str] = []

    # 1. Replace CRLF / tab runs with single space (NOT comma — that
    #    over-commas the result). Then collapse repeated whitespace.
    if _WHITESPACE_NOISE.search(s):
        s = _WHITESPACE_NOISE.sub(" ", s)
        changes.append("stripped_crlf")
    if _MULTI_SPACE.search(s):
        s = _MULTI_SPACE.sub(" ", s)
        changes.append("collapsed_whitespace")

    # 2. Strip legal-description prefix ("MAP 080 PARCEL 003, 123 Main St...")
    if _LEGAL_DESC_PREFIX.search(s):
        s = _LEGAL_DESC_PREFIX.sub("", s)
        changes.append("stripped_legal_desc")

    # 3. Strip "Property Address:" / "AKA:" prefix garbage. Run in a loop
    #    so we eat "02 73.00 Commonly Property Address: 3074 Richmond..."
    for _ in range(3):
        for pat in _PREFIX_PATTERNS:
            new = pat.sub("", s)
            if new != s:
                s = new
                changes.append("stripped_prefix_junk")
                break
        else:
            break

    # 4. Normalize "Tennessee" → "TN" so duplicate-city pattern matches.
    if _STATE_LONG.search(s):
        s = _STATE_LONG.sub("TN", s)
        changes.append("tennessee_to_tn")

    # 5. Collapse "City, TN, City, TN ZIP" — keep one canonical
    #    "City, TN ZIP". The original first city + state is dropped;
    #    we trust the second occurrence because that's typically the
    #    canonical one the parser tacked on.
    m = _DUP_CITY_STATE.search(s)
    if m:
        city = m.group(1).strip()
        zip_code = m.group(2) or ""
        replacement = f", {city}, TN" + (f" {zip_code}" if zip_code else "")
        s = s[: m.start()] + replacement + s[m.end() :]
        changes.append("deduped_city_state")

    # 6. Clean stray comma runs ", ,," from any of the above.
    if _MULTI_COMMA.search(s):
        s = _MULTI_COMMA.sub(",", s)
        changes.append("collapsed_commas")

    s = s.strip().strip(",").strip()
    if not s:
        return NormalizedAddress(normalized=None, changes=changes)

    # 7. Parcel-only detection. We still return the cleaned address
    #    (other downstream consumers want it for display), but flag
    #    that AVM/skip-trace won't work without manual resolution.
    needs_resolution = bool(_PARCEL_ONLY.match(s))
    if needs_resolution:
        changes.append("parcel_only_address")

    return NormalizedAddress(
        normalized=s,
        needs_resolution=needs_resolution,
        changes=changes,
    )


# ───────────────────────── Owner-name helpers ──────────────────────────

# Business / non-natural-person markers. If any token matches as a
# whole word (case-insensitive), the owner is NOT a natural person.
# Kept in sync with the LLC regex used in route_high_probability.sql
# and admin staging filters.
_BUSINESS_TOKENS = (
    r"LLC", r"L\.L\.C", r"INC", r"CORP", r"TRUST", r"HOLDINGS",
    r"PROPERTIES", r"COMPANY", r"GROUP", r"PARTNERS", r"CONSTRUCTION",
    r"BUILDERS", r"DEMOLITION", r"CONTRACTING", r"WOODWORKS",
    r"CAPITAL", r"AGRICULTURAL", r"ORGANIZATION", r"ENTERPRISES",
    r"MANAGEMENT", r"DEVELOPMENT", r"HOMES", r"RENOVATION",
    r"CONTRACTOR", r"BUILDING", r"REALTY", r"INVESTMENT", r"SOLUTIONS",
    r"CO\.", r"LP", r"LLP", r"LTD", r"FOUNDATION", r"CHURCH",
    r"MINISTRIES", r"ASSOCIATION", r"ESTATES",
)
_BUSINESS_RE = re.compile(
    r"(?<![A-Za-z])(?:" + r"|".join(_BUSINESS_TOKENS) + r")(?![A-Za-z])",
    re.IGNORECASE,
)


def is_natural_person(owner: Optional[str]) -> bool:
    """Return True if `owner` looks like a real person (not LLC/Trust/etc).

    Conservative: returns False on empty/None (we'd rather skip than
    accidentally text a business). Matches words as tokens — won't
    false-positive on names containing "Inc" as a substring of a
    surname, etc.

    Examples that return False (rejected):
      "QUALITY CLEAN CONSTRUCTION, LLC"
      "Jebra Home Contractors LLC (Jessica Samborski)"
      "Smith Family Revocable Trust"
      "Acme Properties Inc"

    Examples that return True (allowed):
      "Patrick Armour"
      "Drew Brownlow"
      "David Hall et Al"  (multi-owner but still individuals)
      "Jonathan St. Clair"
    """
    if not owner or not str(owner).strip():
        return False
    return _BUSINESS_RE.search(str(owner)) is None
