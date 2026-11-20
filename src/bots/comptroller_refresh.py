"""
Keep data/comptroller/index.sqlite current with the TN Comptroller's weekly
county parcel files (free, cleared for commercial use; robots.txt allows
all). One zip per county:

  https://comptroller.tn.gov/content/dam/cot/pa/documents/landuse/maps/parcel-maps-data/<County>.zip

Each run makes one HEAD request per county and downloads only the zips
whose Last-Modified changed (the state posts them on Mondays). A changed
county is unzipped (ASSESSMENT_DATA dbf only), its rows are replaced in the
index, and the dbf is deleted, so the cache holds just the index and a
small manifest. A cold build is ~5 GB of downloads; a warm run with no
changes is 86 HEAD requests.

Runs as its own workflow step (like fl_roll_refresh), never inside a bot's
time cap.

    python -m src.bots.comptroller_refresh
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from ._comptroller import FIELDS, SCHEMA, _city_name, _num, county_key, name_tokens, read_dbf, \
    street_key_from_comptroller

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "comptroller"
DB_PATH = DATA_DIR / "index.sqlite"
STATE_PATH = DATA_DIR / "refresh_state.json"
BASE = "https://comptroller.tn.gov/content/dam/cot/pa/documents/landuse/maps/parcel-maps-data/"
UA = "FALCO-Lead-Research/1.0 (+ops@falco.llc)"

# The 86 counties the Comptroller distributes (the other nine run their own
# assessment systems). Spelled as the state names the zips.
COUNTIES = (
    "Anderson Bedford Benton Bledsoe Blount Bradley Campbell Cannon Carroll Carter Cheatham Claiborne Clay "
    "Cocke Coffee Crockett Cumberland Decatur Dekalb Dickson Dyer Fayette Fentress Franklin Gibson Giles "
    "Grainger Greene Grundy Hamblen Hancock Hardeman Hardin Hawkins Haywood Henderson Henry Houston Humphreys "
    "Jackson Jefferson Johnson Lake Lauderdale Lawrence Lewis Lincoln Loudon McMinn McNairy Macon Madison "
    "Marion Marshall Maury Meigs Monroe Moore Morgan Obion Overton Perry Pickett Polk Putnam Rhea Roane "
    "Robertson Scott Sequatchie Sevier Smith Stewart Sullivan Tipton Trousdale Unicoi Union VanBuren Warren "
    "Washington Wayne Weakley White Wilson Sumner"
).split()

log = logging.getLogger("comptroller_refresh")


def _load_state() -> Dict[str, str]:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _save_state(state: Dict[str, str]) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
    os.replace(tmp, STATE_PATH)


def _rows(dbf_path: str, county: str) -> List[tuple]:
    out = []
    for r in read_dbf(dbf_path, FIELDS):
        owner = r["OWNER"].upper()
        out.append((county, r["PARCELID"], street_key_from_comptroller(r["ADDRESS"]), _city_name(r["CITY"]),
                    owner, r["OWNER2"].upper(), (name_tokens(owner) or [None])[0],
                    (name_tokens(r["OWNER2"].upper()) or [None])[0], r["MAILADDR"], r["MAILCITY"], r["STATE"],
                    r["ZIP"], r["CLASS"], r["LANDUSE"], _num(r["APPRAISAL"]), _num(r["IMPVAL"]), _num(r["CALCAC"]),
                    int(_num(r["BLDGS"]) or 0), r["SALEDATE"], _num(r["PRICE"]), r["ZONING"], r["IMP"], r["ADDRESS"]))
    return out


def _open_index() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.executescript(SCHEMA.replace("CREATE TABLE parcels", "CREATE TABLE IF NOT EXISTS parcels"))
    con.execute("CREATE INDEX IF NOT EXISTS ix_addr ON parcels(county, street_key)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_owner ON parcels(county, owner_first)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_owner2 ON parcels(county, owner2_first)")
    return con


def assessment_members(names: List[str]) -> List[str]:
    """The ASSESSMENT_DATA dbf inside a county zip. The state builds the zips
    on Windows, so member names use backslash separators;
    on the Linux runner os.path.basename would not split them and every county
    looked empty. Normalize before taking the base name."""
    out = []
    for m in names:
        base = m.replace("\\", "/").rsplit("/", 1)[-1]
        if base.upper().startswith("ASSESSMENT_DATA") and base.lower().endswith(".dbf"):
            out.append(m)
    return out


def _refresh_county(session: requests.Session, con: sqlite3.Connection, name: str) -> Optional[int]:
    """Download one county zip, replace its rows, return the row count."""
    url = BASE + name + ".zip"
    with tempfile.TemporaryDirectory() as tmp:
        zpath = os.path.join(tmp, f"{name}.zip")
        with session.get(url, stream=True, timeout=300) as res:
            if res.status_code != 200:
                log.warning(f"{name}: download {res.status_code}")
                return None
            if "html" in (res.headers.get("Content-Type") or "").lower():
                log.warning(f"{name}: got a web page, not a zip")
                return None
            with open(zpath, "wb") as fh:
                for chunk in res.iter_content(1 << 20):   # decodes any transfer compression
                    fh.write(chunk)
        with zipfile.ZipFile(zpath) as zf:
            members = assessment_members(zf.namelist())
            if not members:
                log.warning(f"{name}: no ASSESSMENT_DATA dbf in zip")
                return None
            dbf = zf.extract(members[0], tmp)
        county = county_key(name)
        rows = _rows(dbf, county)
    if not rows:
        return None
    with con:
        con.execute("DELETE FROM parcels WHERE county=?", (county,))
        con.executemany(f"INSERT INTO parcels VALUES ({','.join('?' * 23)})", rows)
    return len(rows)


def run(counties: Optional[List[str]] = None) -> dict:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(name)s] %(message)s")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = UA
    state = _load_state()
    con = _open_index()
    have = {r[0] for r in con.execute("SELECT DISTINCT county FROM parcels")}
    changed = failed = 0
    started = time.time()
    for name in counties or COUNTIES:
        try:
            head = session.head(BASE + name + ".zip", timeout=60, allow_redirects=True)
        except requests.RequestException as e:
            log.warning(f"{name}: HEAD failed: {e}")
            failed += 1
            continue
        stamp = head.headers.get("Last-Modified") or head.headers.get("ETag") or ""
        if head.status_code != 200:
            log.warning(f"{name}: HEAD {head.status_code}")
            failed += 1
            continue
        if stamp and state.get(name) == stamp and county_key(name) in have:
            continue
        try:
            n = _refresh_county(session, con, name)
        except (requests.RequestException, zipfile.BadZipFile, OSError) as e:
            log.warning(f"{name}: refresh failed: {e}")
            n = None
        if n is None:
            failed += 1
            continue
        state[name] = stamp
        _save_state(state)
        changed += 1
        log.info(f"{name}: {n:,} parcels")
    total = con.execute("SELECT COUNT(*), COUNT(DISTINCT county) FROM parcels").fetchone()
    con.close()
    out = {"status": "ok" if not failed else "partial", "changed": changed, "failed": failed,
           "parcels": total[0], "counties": total[1], "seconds": round(time.time() - started),
           "at": datetime.now(timezone.utc).isoformat()}
    log.info(out)
    return out


if __name__ == "__main__":
    print(run(sys.argv[1:] or None))
    sys.exit(0)
