"""Tennessee Comptroller parcel + assessment data (86 counties) as a local
SQLite index, and the address / owner-name matching against it.

The Comptroller's Division of Property Assessments publishes one zip per
county, refreshed weekly, free and cleared for commercial use. Each holds
ASSESSMENT_DATA_##.dbf: one row per parcel with owner, owner mailing,
property class, land use, appraisal, acreage and last sale. Not covered
(the county runs its own system): Davidson, Shelby, Knox, Hamilton,
Rutherford, Williamson, Montgomery, Chester, Hickman.

    build_index(data_dir, db_path)   # once per weekly refresh, a few minutes
    idx = ParcelIndex(db_path)
    idx.match_address("1078 Magnolia Springs Rd, Portland, TN", "sumner")
    idx.match_owner("John A Smith", "sumner")

Quirk: the ADDRESS field puts the house number after the street
("MAGNOLIA SPRINGS RD 1078"). Matching compares the number plus the
street tokens as a sorted set, so "31W HWY 6110" matches "6110 Hwy 31W".
"""
from __future__ import annotations

import glob
import os
import re
import sqlite3
import struct
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

FIELDS = ("PARCELID", "ADDRESS", "CITY", "OWNER", "OWNER2", "MAILADDR", "MAILCITY", "STATE", "ZIP",
          "CLASS", "LANDUSE", "APPRAISAL", "IMPVAL", "CALCAC", "BLDGS", "SALEDATE", "PRICE", "ZONING", "IMP")

_SUFFIX = {
    "STREET": "ST", "STR": "ST", "AVENUE": "AVE", "AV": "AVE", "ROAD": "RD", "DRIVE": "DR", "DRV": "DR",
    "LANE": "LN", "COURT": "CT", "CIRCLE": "CIR", "CIRCL": "CIR", "BOULEVARD": "BLVD", "PLACE": "PL",
    "TERRACE": "TER", "TRAIL": "TRL", "PARKWAY": "PKWY", "PKY": "PKWY", "HIGHWAY": "HWY", "HIWAY": "HWY",
    "PIKE": "PIKE", "COVE": "CV", "POINT": "PT", "CROSSING": "XING", "SQUARE": "SQ", "ALLEY": "ALY",
    "LOOP": "LOOP", "RIDGE": "RDG", "HOLLOW": "HOLW", "BRANCH": "BR", "CREEK": "CRK", "MOUNTAIN": "MTN",
    "VALLEY": "VLY", "HEIGHTS": "HTS", "SPRINGS": "SPGS", "SPRING": "SPG", "EXTENSION": "EXT",
    "STATE": "ST", "US": "US",
}
_DIR = {"NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W", "NORTHEAST": "NE", "NORTHWEST": "NW",
        "SOUTHEAST": "SE", "SOUTHWEST": "SW"}
_UNIT_RE = re.compile(r"\b(APT|APARTMENT|UNIT|STE|SUITE|BLDG|LOT|SPC|SPACE|TRLR|#)\b.*$")
_NUM_RE = re.compile(r"^(\d+)[A-Z]?(?:-\d+)?$")
_NAME_NOISE = re.compile(
    r"\b(ESTATE OF|EST OF|THE ESTATE|DECEASED|DECD|DEC'D|ETAL|ET AL|ETUX|ET UX|ETVIR|ET VIR|JR|SR|II|III|IV|"
    r"TRUSTEE|TRUSTEES|TR|TRS|LIFE ESTATE|L/E|REVOCABLE|LIVING|TRUST|IN RE|AKA|A/K/A|MR|MRS|MS|DR)\b")


# ── DBF ─────────────────────────────────────────────────────────────────────

def read_dbf(path: str, want: Sequence[str]) -> Iterator[Dict[str, str]]:
    """Minimal dBASE III reader: character and numeric fields as stripped text."""
    with open(path, "rb") as f:
        head = f.read(32)
        n = struct.unpack("<I", head[4:8])[0]
        hlen, rlen = struct.unpack("<HH", head[8:12])
        fields: List[Tuple[str, int]] = []
        while True:
            d = f.read(32)
            if not d or d[0] == 0x0D:
                break
            fields.append((d[:11].split(b"\0")[0].decode("ascii", "replace"), d[16]))
        offs, pos = [], 1
        for name, ln in fields:
            if name in want:
                offs.append((name, pos, ln))
            pos += ln
        f.seek(hlen)
        chunk = 5000
        for start in range(0, n, chunk):
            block = f.read(rlen * min(chunk, n - start))
            for i in range(0, len(block), rlen):
                rec = block[i:i + rlen]
                if len(rec) < rlen or rec[:1] == b"*":
                    continue
                yield {name: rec[o:o + ln].decode("latin-1").strip() for name, o, ln in offs}


# ── normalization ───────────────────────────────────────────────────────────

def county_key(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    k = re.sub(r"\bCOUNTY\b|\bCO\b|[^A-Z]", "", str(name).upper())
    return k or None


def _street_tokens(street: str) -> List[str]:
    out = []
    for t in re.sub(r"[^A-Z0-9 ]", " ", street.upper()).split():
        t = _SUFFIX.get(t, _DIR.get(t, t))
        out.append(t)
    return out


def street_key_from_lead(address: Optional[str]) -> Optional[str]:
    """'1078 Magnolia Springs Rd, Portland, TN' -> '1078|MAGNOLIA RD SPGS'."""
    if not address:
        return None
    street = str(address).split(",")[0].upper().strip()
    street = _UNIT_RE.sub("", street).strip()
    toks = street.split()
    if len(toks) < 2:
        return None
    m = _NUM_RE.match(toks[0])
    if not m:
        return None
    rest = _street_tokens(" ".join(toks[1:]))
    return f"{m.group(1)}|{' '.join(sorted(rest))}" if rest else None


def street_key_from_comptroller(address: Optional[str]) -> Optional[str]:
    """'MAGNOLIA SPRINGS RD 1078' -> '1078|MAGNOLIA RD SPGS'. Also accepts number-first."""
    if not address:
        return None
    toks = _UNIT_RE.sub("", str(address).upper()).split()
    if len(toks) < 2:
        return None
    # The Comptroller puts the number last; a leading token like "31W" is a
    # route name, so a trailing plain number wins.
    m_last, m_first = _NUM_RE.match(toks[-1]), _NUM_RE.match(toks[0])
    if m_last and re.match(r"^\d+(?:-\d+)?$", toks[-1]):
        num, rest = m_last.group(1), toks[:-1]
    elif m_last and not m_first:
        num, rest = m_last.group(1), toks[:-1]
    elif m_first:
        num, rest = m_first.group(1), toks[1:]
    else:
        return None
    rest = _street_tokens(" ".join(rest))
    return f"{num}|{' '.join(sorted(rest))}" if rest else None


def name_tokens(name: Optional[str], min_len: int = 2) -> List[str]:
    s = _NAME_NOISE.sub(" ", re.sub(r"[^A-Z,' ]", " ", str(name or "").upper()))
    return [t for t in re.sub(r"[,']", " ", s).split() if len(t) >= min_len]


def lead_first_last(name: Optional[str]) -> Optional[Tuple[str, str]]:
    """(first, last) from 'John A Smith', 'Smith, John A' or 'SMITH JOHN A' (assessor style)."""
    raw = str(name or "").upper()
    if not raw.strip() or " AND " in raw or "&" in raw:
        return None
    if "," in raw:
        last_part, first_part = raw.split(",", 1)
        last, first = name_tokens(last_part), name_tokens(first_part)
        if first and last:
            return first[0], last[-1]
        raw = last_part if last else first_part   # "Estate of Mary Jones, Deceased"
    toks = name_tokens(raw)
    if len(toks) < 2:
        return None
    return toks[0], toks[-1]


def lead_middle_initial(name: Optional[str]) -> Optional[str]:
    """'Mary Elizabeth Baker' -> 'E'; 'Smith, John A' -> 'A'; None when absent."""
    raw = str(name or "").upper()
    if "," in raw:
        last_part, first_part = raw.split(",", 1)
        toks = name_tokens(first_part, 1) or name_tokens(last_part, 1)[:-1]
        mids = toks[1:]
    else:
        mids = name_tokens(raw, 1)[1:-1]
    return mids[0][0] if mids else None


_SPOUSE_SPLIT = re.compile(r"\s*(?:&|\bAND\b|\bETUX\b|\bET UX\b|\bETVIR\b|\bET VIR\b|\bC/O\b)\s*")


def owner_names_person(owner: Optional[str], last: str, first: str, middle: Optional[str]) -> bool:
    """True when an assessor owner string ('LAST FIRST M & SPOUSE M') names this
    person: the first name must sit right after the last name, or open a
    spouse segment ('CURRY J B & VIRGIE MAE', 'AEBERSOLD MONICA ETVIR MARK C').
    A middle initial on both sides must agree. A first name that only appears
    as someone else's middle name ('GRAVES AMANDA CAROL') does not count."""
    segs = [s for s in _SPOUSE_SPLIT.split(str(owner or "").upper()) if s.strip()]
    for i, seg in enumerate(segs):
        toks = name_tokens(seg, 1)   # keep single-letter middle initials
        if toks and toks[0] == last:
            toks = toks[1:]
        elif i == 0:
            continue                 # first segment must start with the last name
        if not toks or toks[0] != first:
            continue
        if middle and len(toks) > 1 and toks[1][0] != middle:
            continue
        return True
    return False


def _city_name(city_field: str) -> Optional[str]:
    """'584 PORTLAND' -> 'PORTLAND'; '000' -> None."""
    t = re.sub(r"^\d+\s*", "", city_field or "").strip().upper()
    return t or None


def _num(v: Any) -> Optional[float]:
    try:
        n = float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return n


# ── index ───────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE parcels (
  county TEXT, parcelid TEXT, street_key TEXT, city TEXT, owner TEXT, owner2 TEXT, owner_first TEXT,
  owner2_first TEXT,
  mailaddr TEXT, mailcity TEXT, state TEXT, zip TEXT, class TEXT, landuse TEXT, appraisal REAL,
  impval REAL, calcac REAL, bldgs INTEGER, saledate TEXT, price REAL, zoning TEXT, imp TEXT, address TEXT
);
"""


def build_index(data_dir: str, db_path: str) -> Dict[str, int]:
    """Rebuild the SQLite index from every county's ASSESSMENT_DATA dbf."""
    tmp = db_path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    con = sqlite3.connect(tmp)
    con.executescript(SCHEMA)
    counts: Dict[str, int] = {}
    for path in sorted(glob.glob(os.path.join(data_dir, "*", "ASSESSMENT_DATA_*.dbf"))):
        county = county_key(os.path.basename(os.path.dirname(path)))
        batch = []
        for r in read_dbf(path, FIELDS):
            owner = r["OWNER"].upper()
            first_tok = (name_tokens(owner) or [None])[0]
            first2 = (name_tokens(r["OWNER2"].upper()) or [None])[0]
            batch.append((county, r["PARCELID"], street_key_from_comptroller(r["ADDRESS"]), _city_name(r["CITY"]),
                          owner, r["OWNER2"].upper(), first_tok, first2, r["MAILADDR"], r["MAILCITY"], r["STATE"],
                          r["ZIP"], r["CLASS"], r["LANDUSE"], _num(r["APPRAISAL"]), _num(r["IMPVAL"]),
                          _num(r["CALCAC"]), int(_num(r["BLDGS"]) or 0), r["SALEDATE"], _num(r["PRICE"]),
                          r["ZONING"], r["IMP"], r["ADDRESS"]))
        con.executemany(f"INSERT INTO parcels VALUES ({','.join('?' * 23)})", batch)
        counts[county] = len(batch)
    con.execute("CREATE INDEX ix_addr ON parcels(county, street_key)")
    con.execute("CREATE INDEX ix_owner ON parcels(county, owner_first)")
    con.execute("CREATE INDEX ix_owner2 ON parcels(county, owner2_first)")
    con.commit()
    con.close()
    os.replace(tmp, db_path)
    return counts


def _record(row: sqlite3.Row) -> Dict[str, Any]:
    mailing = ", ".join(x for x in (row["mailaddr"], " ".join(y for y in (row["mailcity"], row["state"], row["zip"]) if y))
                        if x)
    return {
        "parcelid": row["parcelid"], "address": row["address"], "city": row["city"],
        "owner": " & ".join(x for x in (row["owner"], row["owner2"]) if x),
        "owner_mailing": mailing or None, "owner_state": (row["state"] or "").upper() or None,
        "class": row["class"], "land_use": row["landuse"] or None, "appraisal": row["appraisal"],
        "improvement_value": row["impval"], "acres": row["calcac"], "buildings": row["bldgs"],
        "last_sale_date": row["saledate"] or None, "last_sale_price": row["price"], "zoning": row["zoning"] or None,
        "improvement": row["imp"] or None,
    }


class ParcelIndex:
    def __init__(self, db_path: str):
        self.con = sqlite3.connect(db_path)
        self.con.row_factory = sqlite3.Row
        self.counties = {r[0] for r in self.con.execute("SELECT DISTINCT county FROM parcels")}
        # Postal city -> county, only where the city name sits in a single county.
        rows = self.con.execute("SELECT city, county, COUNT(*) FROM parcels WHERE city IS NOT NULL "
                                "GROUP BY city, county").fetchall()
        by_city: Dict[str, List[Tuple[str, int]]] = {}
        for city, county, n in rows:
            by_city.setdefault(city, []).append((county, n))
        self.city_county = {c: v[0][0] for c, v in by_city.items() if len(v) == 1}

    def resolve_county(self, county: Optional[str], address: Optional[str]) -> Optional[str]:
        k = county_key(county)
        if k in self.counties:
            return k
        if k:                        # a county we don't hold (Davidson, Shelby, ...)
            return None
        parts = [p.strip().upper() for p in str(address or "").split(",")]
        return self.city_county.get(parts[1]) if len(parts) >= 2 else None

    @staticmethod
    def _candidate_keys(address: str) -> List[str]:
        """The lead's key, then fallbacks: the city glued onto the street with no
        comma ('1508 Cardinal Ln Mount Juliet, TN') and assessor-style
        addresses ('E ROGERS ST 110') some sources store."""
        street = str(address).split(",")[0].strip()
        keys = [street_key_from_lead(address)]
        toks = street.split()
        for drop in (1, 2, 3):
            if len(toks) - drop >= 2:
                keys.append(street_key_from_lead(" ".join(toks[:-drop])))
        keys.append(street_key_from_comptroller(street))
        out: List[str] = []
        for k in keys:
            if k and k not in out:
                out.append(k)
        return out

    def match_address(self, address: Optional[str], county: Optional[str]) -> Optional[Dict[str, Any]]:
        ck = self.resolve_county(county, address)
        if not ck or not address:
            return None
        rows: List[sqlite3.Row] = []
        for key in self._candidate_keys(address):
            rows = self.con.execute("SELECT * FROM parcels WHERE county=? AND street_key=?", (ck, key)).fetchall()
            if rows:
                break
        if len(rows) > 1:            # same number + street in two towns: use the city
            parts = [p.strip().upper() for p in str(address).split(",")]
            city = parts[1] if len(parts) >= 2 else None
            same_city = [r for r in rows if r["city"] == city]
            owners = {r["owner"] for r in (same_city or rows)}
            if same_city and len(owners) == 1:
                rows = same_city
            elif len(owners) == 1:
                rows = rows[:1]
            else:
                return None
        if not rows:
            return None
        rec = _record(rows[0])
        rec.update({"method": "address", "county": ck})
        return rec

    def match_owner(self, name: Optional[str], county: Optional[str], max_parcels: int = 5) -> Optional[Dict[str, Any]]:
        """Strict: assessor OWNER (or OWNER2) must start with the lead's last name and
        contain the first name, and exactly one distinct owner string may match."""
        ck = county_key(county)
        fl = lead_first_last(name)
        if ck not in self.counties or not fl:
            return None
        first, last = fl
        if len(first) < 3 or len(last) < 3:
            return None
        middle = lead_middle_initial(name)
        rows = self.con.execute("SELECT * FROM parcels WHERE county=? AND owner_first=?", (ck, last)).fetchall()
        hits = [r for r in rows if owner_names_person(r["owner"], last, first, middle)]
        seen = {r["parcelid"] for r in hits}
        rows2 = self.con.execute("SELECT * FROM parcels WHERE county=? AND owner2_first=?", (ck, last)).fetchall()
        hits += [r for r in rows2 if owner_names_person(r["owner2"], last, first, middle) and r["parcelid"] not in seen]
        owners = {r["owner"] for r in hits}
        if len(owners) != 1 or not (1 <= len(hits) <= max_parcels):
            return None
        hits = sorted(hits, key=lambda r: -(r["appraisal"] or 0))
        rec = _record(hits[0])
        rec.update({"method": "owner_name", "county": ck, "parcel_count": len(hits),
                    "parcels": [_record(h) for h in hits]})
        return rec
