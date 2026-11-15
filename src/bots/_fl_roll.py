"""
Florida statewide parcel roll: DOR NAL (real property) and NAP (tangible
personal property, i.e. business equipment), free from the Department of
Revenue data portal. One file per county, refreshed July (preliminary)
and October (final). This replaces Tennessee's per-county assessor bots
for Florida leads.

  https://floridarevenue.com/property/dataportal/  (Tax Roll Data Files)

Files are listed through the portal's public SharePoint REST endpoint and
cached under data/fl_dor/. A compact SQLite index (roll.sqlite) is rebuilt
only when a county's file changes on the portal, so a normal run costs one
small listing request per file type.

Lookups: parcel by situs street key, parcels by owner, business equipment
(NAP) by owner. Pure parsing helpers are module-level so tests need no
network.
"""

from __future__ import annotations

import csv
import io
import os
import re
import sqlite3
import zipfile
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import quote

from ._aircraft_match import ENTITY_RE, NAME_SUFFIX, clean, entity_key, parse_addr, street_key

PORTAL = "https://floridarevenue.com/property/dataportal"
FOLDER = "/property/dataportal/Documents/PTO Data Portal/Tax Roll Data Files/{kind}/{year}F"
LIST_API = PORTAL + "/_api/web/GetFolderByServerRelativeUrl('{folder}')/Files?$select=Name,Length,TimeLastModified,ServerRelativeUrl"

# DOR county numbers (CO_NO) for the counties FALCO covers in Florida.
# The portal names Miami-Dade "Dade".
TARGET_COUNTIES: Dict[str, int] = {
    "miami-dade": 23, "broward": 16, "palm beach": 60, "hillsborough": 39, "orange": 58,
    "pinellas": 62, "duval": 26, "lee": 46, "collier": 21, "sarasota": 68,
}
CO_NAME = {v: k for k, v in TARGET_COUNTIES.items()}
PORTAL_NAME = {"miami-dade": "Dade"}

DATA_DIR = os.environ.get("FALCO_FL_DOR_DIR") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "fl_dor")

# Residential DOR use codes 000-009 (single family, condo, mobile home,
# multifamily under 10 units, etc.).
RESIDENTIAL_UC = {f"{i:03d}" for i in range(10)}
OWNER_SPLIT = re.compile(r"\s*(?:&|\bAND\b|;)\s*")


def county_key(county: Optional[str]) -> Optional[str]:
    c = (county or "").lower().replace(" county", "").replace("st.", "saint").strip()
    c = {"dade": "miami-dade", "miami dade": "miami-dade"}.get(c, c)
    return c if c in TARGET_COUNTIES else None


def owner_key(name: Optional[str]) -> Optional[str]:
    """Order-insensitive key for owner names: 'SMITH JOHN A & MARY' and
    'John A Smith' both start with the same last+first pair. Entities key
    on their stripped name."""
    ek = entity_key(name)
    if ek:
        return "E|" + ek
    toks = [t for t in clean(re.split(r"\s*(?:&|\bAND\b|;)\s*", (name or "").upper())[0]).split()
            if t not in NAME_SUFFIX and len(t) > 1]
    if len(toks) < 2:
        return None
    return "P|" + "|".join(sorted(toks[:2]))


def _first_last(name: str) -> Optional[Tuple[str, str, str]]:
    """(first, last, middle initial) from 'First M Last' or 'LAST, FIRST M'."""
    n = name.upper()
    if "," in n:
        last, _, rest = n.partition(",")
        toks = [t for t in clean(rest).split() if t not in NAME_SUFFIX]
        lt = [t for t in clean(last).split() if t not in NAME_SUFFIX]
        if not toks or not lt:
            return None
        return toks[0], lt[-1], toks[1][0] if len(toks) > 1 else ""
    toks = [t for t in clean(n).split() if t not in NAME_SUFFIX]
    if len(toks) < 2:
        return None
    return toks[0], toks[-1], toks[1][0] if len(toks) > 2 else ""


def person_owner_keys(name: Optional[str]) -> List[str]:
    """Roll keys a lead name can match (roll owners are 'LAST FIRST M')."""
    n = (name or "").upper()
    if not n:
        return []
    if ENTITY_RE.search(clean(n)):
        k = owner_key(n)
        return [k] if k else []
    keys = set()
    fl = _first_last(n)
    if fl and len(fl[0]) >= 2 and len(fl[1]) >= 2:
        keys.add("P|" + "|".join(sorted(fl[:2])))
    # Suffix-less business names ("MIKE'S AUTO GLASS") key the way the roll
    # keys them; name_compatible still demands an exact match for those.
    rk = owner_key(n)
    if rk:
        keys.add(rk)
    return sorted(keys)


def name_compatible(lead_name: Optional[str], owner_name: Optional[str]) -> bool:
    """Roll owner 'HENRY JAMES W & MARY' is compatible with 'James W. Henry',
    not with 'James Edward Henry' (initial conflict) or 'Edward James'."""
    if not lead_name or not owner_name:
        return False
    if clean(lead_name) == clean(owner_name):
        return True
    ek = entity_key(lead_name)
    if ek or ENTITY_RE.search(clean(lead_name)):
        return bool(ek) and ek == entity_key(owner_name) or clean(lead_name) == clean(owner_name)
    fl = _first_last(lead_name)
    if not fl:
        return False
    first, last, mi = fl
    for piece in OWNER_SPLIT.split(owner_name.upper()):
        toks = [t for t in clean(piece).split() if t not in NAME_SUFFIX]
        if len(toks) < 2 or toks[0] != last or toks[1] != first:
            continue
        if mi and len(toks) > 2 and toks[2][0] != mi and toks[2] not in ("TRUSTEE", "LIFE", "ESTATE", "TR"):
            continue
        return True
    return False


def _f(v: Any) -> Optional[float]:
    try:
        return float(str(v).strip()) if str(v).strip() else None
    except ValueError:
        return None


def nal_record(r: Dict[str, str]) -> Optional[Tuple]:
    """One NAL CSV row -> parcel tuple, or None for rows we never use."""
    pid = (r.get("PARCEL_ID") or "").strip()
    if not pid:
        return None
    street = (r.get("PHY_ADDR1") or "").strip()
    zip5 = (r.get("PHY_ZIPCD") or "").strip()[:5]
    hmstd = (_f(r.get("JV_HMSTD")) or 0) > 0
    sale_yr, sale_mo = (r.get("SALE_YR1") or "").strip(), (r.get("SALE_MO1") or "").strip()
    return (
        int(r["CO_NO"]), pid, (r.get("DOR_UC") or "").strip(), _f(r.get("JV")), int(hmstd),
        _f(r.get("ACT_YR_BLT")), _f(r.get("EFF_YR_BLT")), _f(r.get("TOT_LVG_AREA")), _f(r.get("NO_RES_UNTS")),
        _f(r.get("SALE_PRC1")), f"{sale_yr}-{sale_mo.zfill(2)}" if sale_yr else None,
        (r.get("OWN_NAME") or "").strip(), (r.get("OWN_ADDR1") or "").strip(), (r.get("OWN_ADDR2") or "").strip(),
        (r.get("OWN_CITY") or "").strip(), (r.get("OWN_STATE") or "").strip(), (r.get("OWN_ZIPCD") or "").strip()[:5],
        street, (r.get("PHY_CITY") or "").strip(), zip5,
        street_key(street, zip5), owner_key(r.get("OWN_NAME")),
    )


def nap_record(r: Dict[str, str]) -> Optional[Tuple]:
    acct = (r.get("ACCT_ID") or "").strip()
    if not acct:
        return None
    street = (r.get("PHY_ADDR") or "").strip()
    zip5 = (r.get("PHY_ZIPCD") or "").strip()[:5]
    return (
        int(r["CO_NO"]), acct, (r.get("NAICS_CD") or "").strip(), _f(r.get("JV_TOTAL")), _f(r.get("TAX_VAL")),
        _f(r.get("PEN_RATE")), (r.get("OWN_NAM") or "").strip(), (r.get("OWN_ADDR") or "").strip(),
        (r.get("OWN_CITY") or "").strip(), (r.get("OWN_STATE") or "").strip(), (r.get("OWN_ZIPCD") or "").strip()[:5],
        street, (r.get("PHY_CITY") or "").strip(), zip5, street_key(street, zip5), owner_key(r.get("OWN_NAM")),
    )


PARCEL_COLS = ("co_no", "parcel_id", "dor_uc", "jv", "homestead", "yr_built", "eff_yr_built", "living_sqft",
               "res_units", "last_sale_price", "last_sale", "own_name", "own_addr1", "own_addr2", "own_city",
               "own_state", "own_zip", "situs", "situs_city", "situs_zip", "addr_key", "owner_key")
TPP_COLS = ("co_no", "acct_id", "naics", "jv_total", "tax_val", "pen_rate", "own_name", "own_addr", "own_city",
            "own_state", "own_zip", "situs", "situs_city", "situs_zip", "addr_key", "owner_key")

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS parcel ({", ".join(PARCEL_COLS)});
CREATE TABLE IF NOT EXISTS tpp ({", ".join(TPP_COLS)});
CREATE TABLE IF NOT EXISTS source (kind TEXT, co_no INTEGER, name TEXT, modified TEXT, PRIMARY KEY (kind, co_no));
CREATE INDEX IF NOT EXISTS parcel_addr ON parcel (co_no, addr_key);
CREATE INDEX IF NOT EXISTS parcel_owner ON parcel (owner_key);
CREATE INDEX IF NOT EXISTS parcel_pid ON parcel (co_no, parcel_id);
CREATE INDEX IF NOT EXISTS tpp_owner ON tpp (owner_key);
CREATE INDEX IF NOT EXISTS tpp_addr ON tpp (co_no, addr_key);
"""


def iter_csv_from_zip(zf: zipfile.ZipFile) -> Iterator[Dict[str, str]]:
    name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    with zf.open(name) as fh:
        yield from csv.DictReader(io.TextIOWrapper(fh, encoding="latin-1", newline=""))


def load_rows(db: sqlite3.Connection, kind: str, co_no: int, rows: Iterable[Dict[str, str]]) -> int:
    table, cols, fn = (("parcel", PARCEL_COLS, nal_record) if kind == "NAL" else ("tpp", TPP_COLS, nap_record))
    db.execute(f"DELETE FROM {table} WHERE co_no = ?", (co_no,))
    batch, n = [], 0
    for r in rows:
        rec = fn(r)
        if rec is None:
            continue
        batch.append(rec)
        if len(batch) >= 20_000:
            db.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(cols))})", batch)
            n += len(batch)
            batch = []
    if batch:
        db.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(cols))})", batch)
        n += len(batch)
    return n


class FlRoll:
    """Read side of the cached roll."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self.db.row_factory = sqlite3.Row

    def counts(self) -> Dict[str, int]:
        return {t: self.db.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in ("parcel", "tpp")}

    def parcel_at(self, county: Optional[str], address: Optional[str]) -> Optional[Dict[str, Any]]:
        ck = county_key(county)
        street, _, zip5 = parse_addr(address)
        key = street_key(street, zip5)
        if not key:
            return None
        # Always bind co_no so the (co_no, addr_key) index is used; with no
        # county, probe each FALCO county (an unbound addr_key scans 5M rows).
        cos = [TARGET_COUNTIES[ck]] if ck else list(TARGET_COUNTIES.values())
        rows = [r for co in cos
                for r in self.db.execute("SELECT * FROM parcel WHERE co_no = ? AND addr_key = ?", (co, key)).fetchall()]
        if not rows:
            return None
        # Condo towers share a street key; prefer the unit match, else the
        # only row. Several unmatched units -> ambiguous, return nothing.
        unit = re.search(r"(?:UNIT|APT|STE|#)\s*#?\s*([A-Z0-9\-]+)", (address or "").upper())
        if unit:
            hit = [r for r in rows if re.search(rf"(?:UNIT|APT|STE|#)\s*#?\s*{re.escape(unit.group(1))}\b",
                                                 (r["situs"] or "").upper())]
            if len(hit) == 1:
                return dict(hit[0])
        return dict(rows[0]) if len(rows) == 1 else None

    def parcel_by_id(self, county: Optional[str], parcel_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """County folio / parcel numbers as the county publishes them; DOR
        keeps them as strings, sometimes with letters, never reformatted."""
        ck = county_key(county)
        pid = re.sub(r"[\s\-.]", "", parcel_id or "")
        if not ck or not pid:
            return None
        cands = [pid]
        if len(pid) >= 6 and pid[:6].isdigit():
            # City of Orlando writes range-township-section ('29 22 05 ...');
            # the DOR roll keeps section-township-range ('05 22 29 ...').
            cands.append(pid[4:6] + pid[2:4] + pid[0:2] + pid[6:])
        for c in cands:
            row = self.db.execute("SELECT * FROM parcel WHERE co_no = ? AND parcel_id = ?",
                                  (TARGET_COUNTIES[ck], c)).fetchone()
            if row:
                return dict(row)
        return None

    def parcels_of(self, name: Optional[str], county: Optional[str] = None, limit: int = 25) -> List[Dict[str, Any]]:
        keys = person_owner_keys(name)
        if not keys:
            return []
        ck = county_key(county)
        q = (f"SELECT * FROM parcel WHERE owner_key IN ({','.join('?' * len(keys))})"
             + (" AND co_no = ?" if ck else "") + f" ORDER BY jv DESC LIMIT {int(limit)}")
        rows = [dict(r) for r in self.db.execute(q, (*keys, TARGET_COUNTIES[ck]) if ck else keys)]
        return [r for r in rows if name_compatible(name, r["own_name"])]

    def equipment_of(self, name: Optional[str], limit: int = 25) -> List[Dict[str, Any]]:
        keys = person_owner_keys(name)
        if not keys:
            return []
        q = f"SELECT * FROM tpp WHERE owner_key IN ({','.join('?' * len(keys))}) ORDER BY jv_total DESC LIMIT {int(limit)}"
        return [dict(r) for r in self.db.execute(q, keys) if name_compatible(name, r["own_name"])]


def parcel_tag(p: Dict[str, Any]) -> Dict[str, Any]:
    """The slice of a parcel row stored on a lead (raw_payload.fl_parcel)."""
    mailing = ", ".join(x for x in (p.get("own_addr1"), p.get("own_addr2"), p.get("own_city"),
                                    " ".join(x for x in (p.get("own_state"), p.get("own_zip")) if x)) if x)
    situs_k = p.get("addr_key")
    mail_k = street_key(p.get("own_addr1"), p.get("own_zip"))
    return {
        "county": CO_NAME.get(p["co_no"], str(p["co_no"])), "parcel_id": p["parcel_id"],
        "use_code": p.get("dor_uc"), "residential": p.get("dor_uc") in RESIDENTIAL_UC,
        "just_value": p.get("jv"), "homestead": bool(p.get("homestead")),
        "year_built": int(p["yr_built"]) if p.get("yr_built") else None,
        "living_sqft": int(p["living_sqft"]) if p.get("living_sqft") else None,
        "last_sale_price": p.get("last_sale_price"), "last_sale": p.get("last_sale"),
        "owner": p.get("own_name"), "mailing_address": mailing or None,
        "situs": ", ".join(x for x in (p.get("situs"), p.get("situs_city"), p.get("situs_zip")) if x),
        "absentee": bool(situs_k and mail_k and situs_k != mail_k),
        "source": "fl_dor_nal",
    }


def tpp_tag(t: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "county": CO_NAME.get(t["co_no"], str(t["co_no"])), "account": t["acct_id"], "naics": t.get("naics"),
        "equipment_value": t.get("jv_total"), "taxable_value": t.get("tax_val"),
        "late_filing_penalty": bool(t.get("pen_rate")), "owner": t.get("own_name"),
        "situs": ", ".join(x for x in (t.get("situs"), t.get("situs_city"), t.get("situs_zip")) if x),
        "source": "fl_dor_nap",
    }


def portal_url(server_relative: str) -> str:
    return "https://floridarevenue.com" + quote(server_relative)


def list_files(session, kind: str, year: int) -> List[Dict[str, Any]]:
    folder = FOLDER.format(kind=kind, year=year)
    res = session.get(LIST_API.format(folder=quote(folder)), timeout=60,
                      headers={"Accept": "application/json;odata=nometadata"})
    if res.status_code != 200:
        return []
    return res.json().get("value", [])


def file_county(name: str) -> Optional[int]:
    """'Palm Beach 60 Final NAL 2026.zip' -> 60."""
    m = re.search(r"\s(\d{2})\s+(?:Final|Preliminary)\s", name)
    return int(m.group(1)) if m else None


def open_roll(session, log, year: int, counties: Iterable[str] = TARGET_COUNTIES,
              refresh: bool = True) -> Optional[FlRoll]:
    """Open data/fl_dor/roll.sqlite, first bringing it up to date with the
    portal when refresh=True.

    Only counties whose file changed are re-downloaded. If the portal is
    unreachable, an existing cache is used as-is (the roll changes twice a
    year, so a stale cache is still good). Pipeline bots open with
    refresh=False: a cold build takes ~15 minutes and runs in its own
    workflow step (fl_roll_refresh), not inside a bot's time cap."""
    os.makedirs(DATA_DIR, exist_ok=True)
    db = sqlite3.connect(os.path.join(DATA_DIR, "roll.sqlite"))
    db.executescript(SCHEMA)
    wanted = {TARGET_COUNTIES[c] for c in counties}
    for kind, folder_kind in (("NAL", "NAL"), ("NAP", "NAP")) if refresh else ():
        files = []
        for y in (year, year - 1):
            files = list_files(session, folder_kind, y)
            if files:
                break
        if not files:
            log.warning(f"DOR {kind}: portal listing unavailable; using cached roll")
            continue
        for f in files:
            co = file_county(f["Name"])
            if co not in wanted:
                continue
            seen = db.execute("SELECT modified FROM source WHERE kind = ? AND co_no = ?", (kind, co)).fetchone()
            if seen and seen[0] == f["TimeLastModified"]:
                continue
            path = os.path.join(DATA_DIR, f"{kind}_{co}.zip")
            res = session.get(portal_url(f["ServerRelativeUrl"]), timeout=600, stream=True)
            if res.status_code != 200:
                log.warning(f"DOR {kind} {f['Name']}: HTTP {res.status_code}")
                continue
            tmp = path + ".part"
            with open(tmp, "wb") as out:
                for chunk in res.iter_content(1 << 20):
                    out.write(chunk)
            os.replace(tmp, path)
            with zipfile.ZipFile(path) as zf:
                n = load_rows(db, kind, co, iter_csv_from_zip(zf))
            db.execute("INSERT OR REPLACE INTO source VALUES (?, ?, ?, ?)", (kind, co, f["Name"], f["TimeLastModified"]))
            db.commit()
            os.remove(path)   # the SQLite index is the cache; zips are re-fetchable
            log.info(f"DOR {kind} {f['Name']}: {n:,} rows loaded")
    roll = FlRoll(db)
    return roll if roll.counts()["parcel"] else None
