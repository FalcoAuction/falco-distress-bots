"""
Match Tennessee bankruptcy debtors to the homes they own, using the
Comptroller's statewide parcel index (86 counties, owner names included).

A bankruptcy docket gives the debtor's name and the court, never an
address. The court narrows the search to its district's counties; the
rest is name evidence, ranked from strongest to weakest:

  joint          two debtors filed together (usually spouses) and one parcel
                 names both of them. Near certain.
  middle_initial one debtor; exactly one owner in the district has the same
                 first and last name AND the same middle initial.
  name_only      one debtor, unique first + last name in the district, but no
                 middle initial to confirm. Kept as a candidate only; the
                 enricher does not set an address from it.

Anything that fits more than one owner is ambiguous and returns None: a
wrong address is worse than no address.

The Comptroller does not distribute the nine counties that run their own
systems (Davidson, Shelby, Knox, Hamilton, Williamson, Rutherford,
Montgomery, and two more), so a debtor who owns only there is not found
here; bankruptcy_property_enricher covers Davidson and Williamson.

Pure functions over a sqlite connection; no network.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ._comptroller import _record, county_key, lead_first_last, lead_middle_initial, name_tokens, owner_names_person

# Counties served by each Tennessee bankruptcy court (28 U.S.C. 123).
TNMB_COUNTIES = {
    # Nashville division
    "CANNON", "CHEATHAM", "DAVIDSON", "DICKSON", "HOUSTON", "HUMPHREYS", "MONTGOMERY", "ROBERTSON",
    "RUTHERFORD", "STEWART", "SUMNER", "TROUSDALE", "WILLIAMSON", "WILSON",
    # Northeastern (Cookeville) division
    "CLAY", "CUMBERLAND", "DEKALB", "FENTRESS", "JACKSON", "MACON", "OVERTON", "PICKETT", "PUTNAM",
    "SMITH", "WHITE",
    # Columbia division
    "GILES", "HICKMAN", "LAWRENCE", "LEWIS", "MARSHALL", "MAURY", "WAYNE",
}
TNWB_COUNTIES = {
    "BENTON", "CARROLL", "CHESTER", "CROCKETT", "DECATUR", "DYER", "FAYETTE", "GIBSON", "HARDEMAN",
    "HARDIN", "HAYWOOD", "HENDERSON", "HENRY", "LAKE", "LAUDERDALE", "MCNAIRY", "MADISON", "OBION",
    "PERRY", "SHELBY", "TIPTON", "WEAKLEY",
}

# Words in assessor owner strings that are not names: "AND WIFE DORENA SELKE".
_NOT_NAME = {"AND", "WIFE", "HUSBAND", "ETUX", "ETVIR", "SPOUSE", "HIS", "HER", "MRS", "MR"}


def district_counties(court_id: Optional[str], available: Iterable[str]) -> List[str]:
    """Index counties a debtor in this court's district could own in."""
    avail = {county_key(c) or c for c in available}
    court = (court_id or "").lower()
    if court == "tnmb":
        want = TNMB_COUNTIES
    elif court == "tnwb":
        want = TNWB_COUNTIES
    elif court == "tneb":
        want = {c for c in avail if c not in TNMB_COUNTIES and c not in TNWB_COUNTIES}
    else:
        return []
    return sorted(c for c in avail if c in want)


def person(name: Optional[str]) -> Optional[Tuple[str, str, Optional[str]]]:
    """(first, last, middle initial) for a debtor name, or None when it is
    not a single person or the parts are too short to match on."""
    fl = lead_first_last(name)
    if not fl:
        return None
    first, last = fl
    if len(first) < 2 or len(last) < 3:
        return None
    return first, last, lead_middle_initial(name)


def names_person_loosely(text: str, first: str, last: str) -> bool:
    """A second owner on the parcel, in any order: 'AND WIFE DORENA SELKE'."""
    toks = {t for t in name_tokens(text, 1) if t not in _NOT_NAME}
    return first in toks and last in toks


def middle_confirmed(owner: str, last: str, first: str, middle: Optional[str]) -> bool:
    """The owner string carries a middle initial for this person and it agrees."""
    if not middle:
        return False
    for seg in re.split(r"\s*(?:&|\bAND\b|\bETUX\b|\bETVIR\b)\s*", (owner or "").upper()):
        toks = name_tokens(seg, 1)
        if toks and toks[0] == last:
            toks = toks[1:]
        if len(toks) >= 2 and toks[0] == first and toks[1][0] == middle:
            return True
    return False


def situs_address(row: sqlite3.Row) -> Optional[str]:
    """'DUNCAN LN 419' + city -> '419 DUNCAN LN, ANDERSONVILLE, TN'."""
    raw = re.sub(r"\s+", " ", (row["address"] or "")).strip().upper()
    if not raw:
        return None
    m = re.match(r"^(.*?)\s+(\d+[A-Z]?)$", raw)
    street = f"{m.group(2)} {m.group(1)}" if m else raw
    if not re.match(r"^\d", street):
        return None  # no house number: land or a parcel with no situs
    city, zip5 = row["city"], None
    # Owner lives there: the mailing address is the property, with city and ZIP.
    mail = re.sub(r"\s+", " ", (row["mailaddr"] or "")).strip().upper()
    if mail and mail == street and (row["state"] or "").upper() == "TN":
        city, zip5 = city or (row["mailcity"] or "").strip().upper() or None, (row["zip"] or "")[:5] or None
    tail = ", ".join(x for x in (city, " ".join(y for y in ("TN", zip5) if y)) if x)
    return f"{street}, {tail}"


def _residential(row: sqlite3.Row) -> bool:
    return "RESIDENTIAL" in (row["class"] or "").upper() or "HOUSEHOLD" in (row["landuse"] or "").upper()


def _rows_for_last(con: sqlite3.Connection, counties: Sequence[str], last: str) -> List[sqlite3.Row]:
    """Parcels whose owner fields could name someone with this last name, one
    row per parcel (the index carries a row per tax year; the newest wins)."""
    marks = ",".join("?" * len(counties))
    # Two queries, not one OR: an OR across the two columns defeats both indexes.
    out = con.execute(f"SELECT * FROM parcels WHERE county IN ({marks}) AND owner_first=?", (*counties, last)).fetchall()
    out += con.execute(f"SELECT * FROM parcels WHERE county IN ({marks}) AND owner2_first=?", (*counties, last)).fetchall()
    # A spouse in OWNER2 ("AND WIFE DORENA SELKE") is reached through the first
    # owner's last name and checked by names_person_loosely; a spouse with a
    # different last name than the first owner is not searched (full scan).
    newest: Dict[Tuple[str, str], sqlite3.Row] = {}
    for r in out:
        k = (r["county"], _parcel_base(r["parcelid"]))
        if k not in newest or (r["parcelid"] or "") > (newest[k]["parcelid"] or ""):
            newest[k] = r
    return list(newest.values())


def _parcel_base(parcelid: Optional[str]) -> str:
    """'001 003    00100 000 2027' -> '001 003 00100 000' (drop the tax year)."""
    p = re.sub(r"\s+", " ", (parcelid or "")).strip()
    return re.sub(r"\s(19|20)\d{2}$", "", p)


def _owner_key(row: sqlite3.Row) -> Tuple[str, str]:
    """Who owns the parcel. Assessor owner strings drift ('SELKE ALAN' vs
    'SELKE ALAN C') but one owner keeps one mailing address, so group by it;
    fall back to the owner string when there is no mailing address."""
    mail = re.sub(r"[^A-Z0-9]", "", (row["mailaddr"] or "").upper())
    zip5 = (row["zip"] or "")[:5]
    return (row["county"], f"{mail}|{zip5}" if mail else (row["owner"] or ""))


def middle_conflicts(owner: str, last: str, first: str, middle: Optional[str]) -> bool:
    """The owner string gives this person a different middle initial."""
    if not middle:
        return False
    for seg in re.split(r"\s*(?:&|\bAND\b|\bETUX\b|\bETVIR\b)\s*", (owner or "").upper()):
        toks = [t for t in name_tokens(seg, 1) if t not in _NOT_NAME]
        if last in toks:
            toks = [t for t in toks if t != last]
        if len(toks) >= 2 and toks[0] == first and toks[1][0] != middle:
            return True
    return False


def _names(row: sqlite3.Row, p: Tuple[str, str, Optional[str]]) -> bool:
    first, last, middle = p
    text = f"{row['owner']} & {row['owner2']}"
    if middle_conflicts(text, last, first, middle):
        return False
    return (owner_names_person(row["owner"], last, first, middle)
            or owner_names_person(row["owner2"], last, first, middle)
            or names_person_loosely(text, first, last))


def _pick(hits: List[sqlite3.Row]) -> sqlite3.Row:
    """The home among a debtor's parcels: residential first, then value."""
    return sorted(hits, key=lambda r: (not _residential(r), -(r["appraisal"] or 0)))[0]


def match_debtors(con: sqlite3.Connection, names: Sequence[str], counties: Sequence[str],
                  max_parcels: int = 8) -> Optional[Dict[str, Any]]:
    """Best parcel for these debtors in these counties, or None."""
    people = [p for p in (person(n) for n in names[:2]) if p]
    if not people or not counties:
        return None
    p1 = people[0]
    rows = _rows_for_last(con, counties, p1[1])
    hits1 = [r for r in rows if _names(r, p1)]

    if len(people) == 2 and people[1][0] != p1[0]:
        p2 = people[1]
        if p2[1] != p1[1]:   # different last names: the spouse may be the first owner
            rows2 = _rows_for_last(con, counties, p2[1])
            pool = {(r["county"], r["parcelid"]): r for r in hits1 + [r for r in rows2 if _names(r, p1)]}
            hits1 = list(pool.values())
        both = [r for r in hits1 if _names(r, p2)]
        owners = {_owner_key(r) for r in both}
        if both and len(owners) == 1 and len(both) <= max_parcels:
            return _result(both, "joint")

    owners = {_owner_key(r) for r in hits1}
    if len(owners) != 1 or not (1 <= len(hits1) <= max_parcels):
        return None
    first, last, middle = p1
    confirmed = any(middle_confirmed(f"{r['owner']} & {r['owner2']}", last, first, middle) for r in hits1)
    return _result(hits1, "middle_initial" if confirmed else "name_only")


def _result(hits: List[sqlite3.Row], confidence: str) -> Dict[str, Any]:
    home = _pick(hits)
    rec = _record(home)
    rec.update({
        "method": "debtor_name", "confidence": confidence, "county": home["county"],
        "situs_address": situs_address(home), "parcel_count": len(hits),
        "parcels": [_record(h) for h in hits] if len(hits) > 1 else None,
    })
    return rec
