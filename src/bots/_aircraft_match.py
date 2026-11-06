"""
Pure matching logic for the FAA aircraft overlay. No network, no DB.

Given the FAA releasable registry (MASTER / ACFTREF / DOCINDEX) and a list
of leads, find leads whose owner is the registrant of a TN aircraft that
is plausibly worth $100K+. Three ways to match, strongest first:

  address      lead property/mailing street + zip == FAA registrant street
               + zip, same unit (condo towers and office suites hold many
               unrelated registrants, so a unit mismatch is a non-match)
  entity       normalized entity name equality (BASA INC vs BASA INC)
  name         person first + last + middle initial on both sides, plus
               either same city/county, or for bankruptcies the FAA
               county sits in the filing district and the name is unique
               among TN registrants

Value bands are a model-level heuristic, not an appraisal:
  A  turbine (turboprop/turboshaft/turbofan/turbojet)
  B  piston helicopter 1995+, piston twin 1965+, piston single 1997+
  C  1965+ high-performance piston singles (Bonanza, Mooney, Baron, ...)
Gliders, balloons, light-sport, and experimental piston are dropped.
"""

from __future__ import annotations

import csv
import io
import re
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

TN_COUNTIES = """Anderson Bedford Benton Bledsoe Blount Bradley Campbell Cannon Carroll Carter Cheatham Chester
Claiborne Clay Cocke Coffee Crockett Cumberland Davidson Decatur DeKalb Dickson Dyer Fayette Fentress Franklin
Gibson Giles Grainger Greene Grundy Hamblen Hamilton Hancock Hardeman Hardin Hawkins Haywood Henderson Henry
Hickman Houston Humphreys Jackson Jefferson Johnson Knox Lake Lauderdale Lawrence Lewis Lincoln Loudon McMinn
McNairy Macon Madison Marion Marshall Maury Meigs Monroe Montgomery Moore Morgan Obion Overton Perry Pickett Polk
Putnam Rhea Roane Robertson Rutherford Scott Sequatchie Sevier Shelby Smith Stewart Sullivan Sumner Tipton
Trousdale Unicoi Union VanBuren Warren Washington Wayne Weakley White Williamson Wilson""".split()
# TN county FIPS codes are the odd numbers 001..189 in alphabetical order.
FIPS_TO_COUNTY = {f"{2 * i + 1:03d}": c.lower() for i, c in enumerate(TN_COUNTIES)}

# 28 U.S.C. 123 — bankruptcy district by county (everything else is tneb).
TNWB = set("benton carroll chester crockett decatur dyer fayette gibson hardeman hardin haywood henderson "
           "henry lake lauderdale madison mcnairy obion perry shelby tipton weakley".split())
TNMB = set("cannon cheatham clay cumberland davidson dekalb dickson fentress giles hickman houston humphreys "
           "jackson lawrence lewis macon marshall maury montgomery overton pickett putnam robertson rutherford "
           "smith stewart sumner trousdale wayne williamson wilson white".split())

SUFFIX = {"STREET": "ST", "DRIVE": "DR", "ROAD": "RD", "LANE": "LN", "COURT": "CT", "CIRCLE": "CIR",
          "AVENUE": "AVE", "BOULEVARD": "BLVD", "HIGHWAY": "HWY", "PLACE": "PL", "TRAIL": "TRL",
          "PARKWAY": "PKWY", "TERRACE": "TER", "COVE": "CV", "POINT": "PT", "HOLLOW": "HOLW"}
DIRS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW", "NORTH", "SOUTH", "EAST", "WEST"}
NAME_SUFFIX = {"JR", "SR", "II", "III", "IV", "MD", "DR", "ESQ", "DECEASED", "DEC", "ESTATE"}
ENTITY_RE = re.compile(r"\b(LLC|L L C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|PLLC|LTD|TRUST|HOLDINGS|"
                       r"PARTNERS|GROUP|ENTERPRISES|AVIATION|PROPERTIES)\b")
ENTITY_STRIP_RE = re.compile(r"\b(LLC|L L C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|PLLC|LTD|THE)\b")
# Bankruptcy "parties" and some notices list creditors / agencies — never the lead.
NOT_OWNER_RE = re.compile(r"\b(DEPT|DEPARTMENT|STATE OF|COUNTY|CITY OF|UNITED STATES|INTERNAL REVENUE|BANK|"
                          r"CREDIT UNION|TRUSTEE|BLUECROSS|INSURANCE)\b")
UNIT_RE = re.compile(r"(?:\b(?:UNIT|APT|STE|SUITE)\s*#?\s*|#\s*)([A-Z0-9\-]+)", re.I)
ESTATE_RE = re.compile(r"ESTATE OF\s+([A-Z][A-Z .'\-]{3,60}?)(?:,|\s+DOCKET|\s+WHO|\s+DECEASED|\s+NOTICE|\s+\d)",
                       re.I)
EXEC_BY_RE = re.compile(r"EXECUTED BY\s+([A-Z][A-Z .'\-]{3,60}?)(?:,|\s+TO\s)", re.I)
HIGH_PERF = re.compile(r"BONANZA|A36|G36|V35|MOONEY|M20|SR2[02]|SF50|MALIBU|MIRAGE|MERIDIAN|M350|M500|M600|"
                       r"\b210|T206|P210|COLUMBIA|CORVALIS|TTX|COMANCHE|BARON|\b58\b|SENECA|AZTEC|SARATOGA|"
                       r"DA40|DA42|DA62|KODIAK|R44|R66|182")
# More registrants than this at one street+unit = office / FBO / mail drop.
MAX_REGISTRANTS_PER_ADDRESS = 3


def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())).strip()


def district_of(county: Optional[str]) -> Optional[str]:
    c = (county or "").replace(" ", "").lower()
    if not c:
        return None
    return "tnwb" if c in TNWB else "tnmb" if c in TNMB else "tneb"


def units_of(s: Optional[str]) -> frozenset:
    return frozenset(m.upper() for m in UNIT_RE.findall(s or ""))


def street_key(street: Optional[str], zip5: Optional[str]) -> Optional[str]:
    s = clean(UNIT_RE.sub(" ", street or ""))
    if not s or not zip5:
        return None
    toks = s.split()
    if not toks[0].isdigit():
        return None
    rest = [t for t in toks[1:] if t not in DIRS]
    if not rest:
        return None
    return f"{toks[0]}|{SUFFIX.get(rest[0], rest[0])}|{zip5}"


def parse_addr(a: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """'207 S IDLEWILD ST, Memphis, TN 38104' / '235 ROXBURY CIR, HIXSON TN37343' -> street, city, zip5."""
    if not a:
        return None, None, None
    m = re.search(r"(\d{5})(?:-\d{4})?\s*$", a.strip())
    zip5 = m.group(1) if m else None
    parts = [p.strip() for p in a.split(",")]
    # 'Unit 509' sometimes lands in its own comma part; keep it with the street.
    if len(parts) > 2 and UNIT_RE.search(parts[1]):
        parts = [parts[0] + " " + parts[1]] + parts[2:]
    city = None
    if len(parts) >= 2:
        city = clean(re.sub(r"\bTN\b.*$", "", parts[1].upper())) or None
    return parts[0], city, zip5


def person_keys(name: Optional[str]) -> List[Tuple[str, str, str]]:
    """(last, first, middle_initial) candidates; tries 'First M Last' and 'LAST FIRST M'."""
    n = (name or "").upper()
    if not n or ENTITY_RE.search(clean(n)):
        return []
    out = []
    for piece in re.split(r"\s*(?:&|\bAND\b|;)\s*", n):
        if "," in piece:
            last, _, rest = piece.partition(",")
            toks = [t for t in clean(rest).split() if t not in NAME_SUFFIX]
            if toks and clean(last):
                out.append((clean(last).split()[-1], toks[0], toks[1][0] if len(toks) > 1 else ""))
            continue
        toks = [t for t in clean(piece).split() if t not in NAME_SUFFIX]
        if len(toks) < 2:
            continue
        out.append((toks[-1], toks[0], toks[1][0] if len(toks) > 2 else ""))
        out.append((toks[0], toks[1], toks[2][0] if len(toks) > 2 else ""))
    return out


def faa_person_key(name: str) -> Optional[Tuple[str, str, str]]:
    toks = [t for t in clean(name).split() if t not in NAME_SUFFIX]
    if len(toks) < 2:
        return None
    return toks[0], toks[1], toks[2][0] if len(toks) > 2 else ""


def entity_key(name: Optional[str]) -> Optional[str]:
    s = clean(name)
    if not ENTITY_RE.search(s):
        return None
    s = re.sub(r"\s+", " ", ENTITY_STRIP_RE.sub(" ", s)).strip()
    return s if len(s) >= 4 else None


def value_band(ref: Dict[str, str], a: Dict[str, Any]) -> Optional[str]:
    te, ta, cert = a["type_eng"], a["type_acft"], a["cert"]
    yr = int(a["year"]) if a["year"].isdigit() else 0
    if ta in ("1", "2", "3", "7", "8", "9", "H", "O"):
        return None
    if te in ("2", "3", "4", "5"):
        return "A"
    if cert.startswith("4"):
        return None
    eng_s = ref.get("eng") or "1"
    eng = int(eng_s) if eng_s.isdigit() else 1
    if ta == "6":
        return "B" if yr >= 1995 else None
    if eng >= 2 and yr >= 1965:
        return "B"
    if yr >= 1997:
        return "B"
    if yr >= 1965 and HIGH_PERF.search(f'{ref.get("mfr", "")} {ref.get("model", "")}'):
        return "C"
    return None


BAND_LABEL = {"A": "turbine", "B": "late-model piston / twin / helicopter", "C": "high-performance piston single"}


def _rows(text_lines: Iterable[str]) -> Iterable[List[str]]:
    rd = csv.reader(text_lines)
    next(rd, None)
    yield from rd


def load_ref(lines: Iterable[str]) -> Dict[str, Dict[str, str]]:
    ref = {}
    for r in _rows(lines):
        if len(r) >= 9:
            ref[r[0].strip()] = {"mfr": r[1].strip(), "model": r[2].strip(), "eng": r[7].strip()}
    return ref


def load_docs(lines: Iterable[str]) -> Dict[str, List[str]]:
    docs: Dict[str, List[str]] = defaultdict(list)
    for r in _rows(lines):
        if len(r) >= 10 and r[0].strip() == "1":
            docs[r[1].strip()].append(f"{r[9].strip()} {r[4].strip()}")
    return docs


def load_aircraft(lines: Iterable[str], ref: Dict[str, Dict[str, str]]) -> List[Dict[str, Any]]:
    out = []
    for r in _rows(lines):
        if len(r) < 30 or r[10].strip() != "TN":
            continue
        a = {"n": r[0].strip(), "serial": r[1].strip(), "mdl": r[2].strip(), "year": r[4].strip(),
             "name": r[6].strip(), "street": r[7].strip(), "street2": r[8].strip(), "city": r[9].strip(),
             "zip": r[11].strip()[:5], "county_fips": r[13].strip(), "cert": r[17].strip(),
             "type_acft": r[18].strip(), "type_eng": r[19].strip(), "status": r[20].strip(),
             "others": [x.strip() for x in r[24:29] if x.strip()], "exp": r[29].strip()}
        m = ref.get(a["mdl"], {})
        band = value_band(m, a)
        if not band:
            continue
        a["band"] = band
        a["aircraft"] = f'{a["year"]} {m.get("mfr", "")} {m.get("model", "")}'.strip()
        a["county"] = FIPS_TO_COUNTY.get(a["county_fips"], "")
        out.append(a)
    return out


class AircraftIndex:
    def __init__(self, aircraft: List[Dict[str, Any]], docs: Optional[Dict[str, List[str]]] = None):
        self.docs = docs or {}
        self.by_addr: Dict[str, List[Tuple[Dict[str, Any], frozenset]]] = defaultdict(list)
        self.by_person: Dict[Tuple[str, str], List[Tuple[Dict[str, Any], str]]] = defaultdict(list)
        self.by_entity: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.person_count: Counter = Counter()
        regs_at: Dict[Tuple[str, frozenset], Set[str]] = defaultdict(set)
        for a in aircraft:
            full = f'{a["street"]} {a["street2"]}'
            k = street_key(a["street"], a["zip"]) or street_key(a["street2"], a["zip"])
            if k:
                u = units_of(full)
                self.by_addr[k].append((a, u))
                regs_at[(k, u)].add(clean(a["name"]))
            for nm in [a["name"]] + a["others"]:
                if not ENTITY_RE.search(clean(nm)):
                    pk = faa_person_key(nm)
                    if pk:
                        self.by_person[pk[:2]].append((a, pk[2]))
                        self.person_count[pk[:2]] += 1
                ek = entity_key(nm)
                if ek:
                    self.by_entity[ek].append(a)
        self.crowded = {key for key, names in regs_at.items() if len(names) > MAX_REGISTRANTS_PER_ADDRESS}

    def match(self, lead: Dict[str, Any]) -> List[Dict[str, Any]]:
        """lead: id, distress_type, full_name, owner_name_records, property_address, county,
        and raw_* fields flattened from raw_payload (subject, owner_name_1, owner_name_2, case_name,
        mailing_address, city, court_id, excerpt)."""
        dt = (lead.get("distress_type") or "").upper()
        is_bk = dt == "BANKRUPTCY"
        if is_bk:
            base = [lead.get("full_name"), lead.get("owner_name_records"), lead.get("case_name")]
            names = {p for n in base if n for p in re.split(r"\s+and\s+", n, flags=re.I)}
        else:
            names = {x for x in [lead.get("full_name"), lead.get("owner_name_records"), lead.get("subject"),
                                 lead.get("owner_name_1"), lead.get("owner_name_2")] if x}
        ex = lead.get("excerpt") or ""
        m = ESTATE_RE.search(ex)
        if m:
            names.add(m.group(1))
        if dt in ("PRE_FORECLOSURE", "TRUSTEE_NOTICE"):
            m = EXEC_BY_RE.search(ex)
            if m:
                names.add(m.group(1))
        names = {n for n in names if not NOT_OWNER_RE.search(clean(n))}

        addrs = [x for x in [lead.get("property_address"), lead.get("mailing_address")] if x]
        cities = {clean(lead["city"])} if lead.get("city") else set()
        counties = set()
        if lead.get("county"):
            counties.add(lead["county"].lower().replace(" county", "").replace(" ", "").strip())

        hits: Dict[str, Tuple[Dict[str, Any], str, str]] = {}
        for ad in addrs:
            st, city, z = parse_addr(ad)
            if city:
                cities.add(city)
            k = street_key(st, z)
            if not k:
                continue
            lu = units_of(st)
            if (k, lu) in self.crowded:
                continue
            for a, fu in self.by_addr.get(k, []):
                if fu == lu:
                    hits[a["n"]] = (a, "address", "high")
        for nm in names:
            ek = entity_key(nm)
            if ek:
                for a in self.by_entity.get(ek, []):
                    hits.setdefault(a["n"], (a, "entity_name", "high"))
            for last, first, mi in person_keys(nm):
                if not mi or len(first) < 2:
                    continue
                for a, fmi in self.by_person.get((last, first), []):
                    if fmi != mi:
                        continue
                    geo = clean(a["city"]) in cities or a["county"].replace(" ", "") in counties
                    if geo:
                        hits.setdefault(a["n"], (a, "name_middle_geo", "medium"))
                    elif is_bk and self.person_count[(last, first)] == 1 \
                            and district_of(a["county"]) == lead.get("court_id"):
                        hits.setdefault(a["n"], (a, "name_middle_bk_district", "medium"))
        return [self._tag(a, how, conf) for a, how, conf in hits.values()]

    def _tag(self, a: Dict[str, Any], how: str, conf: str) -> Dict[str, Any]:
        return {
            "n_number": "N" + a["n"],
            "aircraft": a["aircraft"],
            "serial": a["serial"],
            "value_band": a["band"],
            "value_band_label": BAND_LABEL[a["band"]],
            "registrant": a["name"],
            "co_owners": a["others"],
            "registrant_address": re.sub(r"\s+", " ", f'{a["street"]} {a["street2"]}, {a["city"]} TN {a["zip"]}'),
            "registrant_county": a["county"],
            "registration_status": a["status"],
            "registration_expires": a["exp"],
            "recent_faa_documents": self.docs.get(a["n"], [])[:10],
            "match_type": how,
            "confidence": conf,
        }


def build_index_from_zip(zf) -> AircraftIndex:
    """zf: zipfile.ZipFile of ReleasableAircraft.zip."""
    def lines(name):
        return io.TextIOWrapper(zf.open(name), encoding="utf-8-sig", errors="ignore", newline="")
    ref = load_ref(lines("ACFTREF.txt"))
    docs = load_docs(lines("DOCINDEX.txt")) if "DOCINDEX.txt" in zf.namelist() else {}
    return AircraftIndex(load_aircraft(lines("MASTER.txt"), ref), docs)
