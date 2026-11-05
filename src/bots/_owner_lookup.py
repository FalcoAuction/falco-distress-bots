"""Owner-name property lookup for the counties TPAD does not serve.

TPAD (the Comptroller's statewide system) and the state's own parcel viewer
both exclude Davidson, Hamilton, Knox, Montgomery, Rutherford, Shelby,
Williamson, Chester and Hickman; those counties run their own systems. This
module searches four of them by owner name, for matching a probate
decedent (or a debtor) to the property they own:

  montgomery  County GIS parcels (gis.mcgtn.org), Owner1/Owner2
  rutherford  County GIS parcels (Parcel_Data FeatureServer layer 1),
              Owner1-3: the assessor-listed owners, not the historical
              Grantee field
  williamson  County Inigo property search, owner_name parameter
  shelby      Assessor of Property site (assessor.shelbycountytn.gov),
              owner search form. OFF by default: the site answers 403 to
              this bot's self-identifying user agent and only serves
              browser-like ones, and FALCO does not disguise its bots to get
              past a block. Enable with FALCO_SHELBY_OWNER_LOOKUP=1 once the
              Assessor's office has agreed to automated access. The county's
              ReGIS GIS states it is for ReGIS participants only and the City
              of Memphis parcel layer requires a token; neither is used.

Matching is deliberately strict, because a wrong house poisons the data:
  - the owner name must start with the decedent's LAST then FIRST name
    (co-owner segments split on "&", ETUX, ETAL, AND are each checked);
  - when both sides carry a middle initial, they must agree;
  - a generational suffix (JR, SR, II, III, IV) must agree when either
    side has one, so "TAGG WILLIAM T JR" never matches William T. Tagg;
  - trust, LLC and other entity owners are never matched to a person;
  - several parcels under one person are one estate with several
    properties; two different people means ambiguous, and nothing is
    written.

Pure matching functions plus small adapters that take the caller's fetch
callable (so the caller's throttling, retries and session apply).
"""

from __future__ import annotations

import html as _html
import os
import re
import ssl
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from requests.adapters import HTTPAdapter

SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}
_ENTITY_RE = re.compile(r"\b(TRUST|TRUSTEE|TRS|LLC|L L C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|PARTNERSHIP|ESTATE|BANK|CHURCH|ASSOCIATION|ASSN|PROPERTIES|HOLDINGS|INVESTMENTS)\b")
_SEGMENT_SPLIT_RE = re.compile(r"\s*&\s*|\s+(?:AND|ETUX|ETVIR|ETAL|ET UX|ET VIR|ET AL)\b\s*")

_ALWAYS_ON = ("montgomery", "rutherford", "williamson")


def supported_counties() -> tuple:
    if os.environ.get("FALCO_SHELBY_OWNER_LOOKUP", "").strip() == "1":
        return _ALWAYS_ON + ("shelby",)
    return _ALWAYS_ON


SUPPORTED_COUNTIES = _ALWAYS_ON  # static default; use supported_counties() at call time


@dataclass(frozen=True)
class PersonName:
    first: str
    last: str
    middle_initial: Optional[str] = None
    suffix: Optional[str] = None


def _tokens(value: str) -> List[str]:
    value = _html.unescape(value or "").upper().replace("’", "'")
    value = re.sub(r"[.,]", " ", value)
    return [t for t in re.split(r"\s+", value) if t]


def parse_person(name: str) -> Optional[PersonName]:
    """'Raymond Gerstell Swadley Jr.' -> RAYMOND G SWADLEY JR.
    Accepts 'Last, First Middle' too. None when there is no first+last."""
    if not name or not name.strip():
        return None
    raw = name.strip()
    if raw.count(",") == 1 and not re.search(r",\s*(jr|sr|ii|iii|iv)\.?$", raw, re.I):
        last_part, rest = raw.split(",", 1)
        raw = f"{rest.strip()} {last_part.strip()}"
    toks = _tokens(raw)
    suffix = None
    while toks and toks[-1] in SUFFIXES:
        suffix = suffix or toks[-1]
        toks.pop()
    if len(toks) < 2:
        return None
    first, last = toks[0], toks[-1]
    middle = toks[1:-1]
    mi = middle[0][0] if middle else None
    return PersonName(first=first, last=last, middle_initial=mi, suffix=suffix)


def owner_segments(owner: str) -> List[List[str]]:
    """'SMITH HEATHER MARIE & SMITH JOHN MARTIN JR' -> [[SMITH,HEATHER,MARIE],[SMITH,JOHN,MARTIN,JR]]"""
    text = " ".join(_tokens(owner))
    return [_tokens(seg) for seg in _SEGMENT_SPLIT_RE.split(text) if seg.strip()]


def segment_person(person: PersonName, seg: List[str]) -> Optional[tuple]:
    """(last, first, middle_initial, suffix) if the owner segment names this
    person, else None."""
    if len(seg) < 2 or seg[0] != person.last or seg[1] != person.first:
        return None
    if _ENTITY_RE.search(" ".join(seg)):
        return None
    rest = seg[2:]
    seg_suffix = next((t for t in rest if t in SUFFIXES), None)
    middle = [t for t in rest if t not in SUFFIXES]
    seg_mi = middle[0][0] if middle else None
    if person.middle_initial and seg_mi and person.middle_initial != seg_mi:
        return None
    if (person.suffix or seg_suffix) and person.suffix != seg_suffix:
        return None
    return (seg[0], seg[1], seg_mi, seg_suffix)


@dataclass
class Candidate:
    owner: str
    address: Optional[str]
    parcel: Optional[str]
    value: Optional[float] = None
    acres: Optional[float] = None
    property_class: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MatchResult:
    status: str  # matched | ambiguous | not_found
    parcels: List[Candidate] = field(default_factory=list)
    owner_keys: List[str] = field(default_factory=list)
    confidence: Optional[str] = None  # middle_initial | name_only

    @property
    def primary(self) -> Optional[Candidate]:
        if not self.parcels:
            return None
        return max(self.parcels, key=lambda c: c.value or 0)


# Name-only matches (no middle initial to confirm) are accepted for at most
# this many parcels. Two is a plausible estate; six "BROWN CHARLES" records
# are several different people.
MAX_NAME_ONLY_PARCELS = 2


def choose(person: PersonName, candidates: List[Candidate]) -> MatchResult:
    """Pick the decedent's parcels, or refuse.

    1. If the notice gives a middle initial and some records carry that same
       initial, only those records count (confidence 'middle_initial').
    2. Otherwise records without a conflicting initial count, but only when
       they are few (confidence 'name_only'); a common name spread over many
       parcels, or two different initials, is ambiguous and nothing is written.
    """
    hits: List[tuple] = []
    for cand in candidates:
        people = {h for h in (segment_person(person, seg) for seg in owner_segments(cand.owner)) if h}
        if people:
            hits.append((cand, people))
    if not hits:
        return MatchResult("not_found")
    keys = sorted({" ".join(x for x in h if x) for _, people in hits for h in people})
    if person.middle_initial:
        confirmed = [cand for cand, people in hits if any(h[2] == person.middle_initial for h in people)]
        if confirmed:
            return MatchResult("matched", confirmed, keys, confidence="middle_initial")
    initials = {h[2] for _, people in hits for h in people if h[2]}
    if len(initials) > 1:
        return MatchResult("ambiguous", [c for c, _ in hits], keys)
    matched = [c for c, _ in hits]
    if len(matched) > MAX_NAME_ONLY_PARCELS:
        return MatchResult("ambiguous", matched, keys)
    return MatchResult("matched", matched, keys, confidence="name_only")


# ── Adapters ──────────────────────────────────────────────────────────────

Fetch = Callable[..., Any]

MONTGOMERY_URL = "https://gis.mcgtn.org/arcgis/rest/services/Parcels/MapServer/0/query"
RUTHERFORD_URL = "https://services5.arcgis.com/A5C0MR9xfkxVRwat/arcgis/rest/services/Parcel_Data/FeatureServer/1/query"
WILLIAMSON_BASE = "https://inigo.williamson-tn.org/property_search"
SHELBY_BASE = "https://www.assessor.shelbycountytn.gov"


class LegacyTLSAdapter(HTTPAdapter):
    """The Shelby Assessor's server needs TLS legacy renegotiation, which
    OpenSSL 3 disables by default. Certificate verification stays on."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


def _money(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    digits = re.sub(r"[^\d.]", "", str(value))
    try:
        v = float(digits) if digits else None
    except ValueError:
        return None
    return v if v and v > 0 else None


def _sql_like(value: str) -> str:
    return value.replace("'", "''")


def _arcgis(fetch: Fetch, url: str, where: str, fields: str) -> Optional[List[Dict[str, Any]]]:
    res = fetch(url, params={"where": where, "outFields": fields, "returnGeometry": "false",
                             "f": "json", "resultRecordCount": 50})
    if res is None or res.status_code != 200:
        return None
    try:
        data = res.json()
    except Exception:
        return None
    if data.get("error"):
        return None
    return [f.get("attributes") or {} for f in data.get("features") or []]


def search_montgomery(fetch: Fetch, person: PersonName) -> Optional[List[Candidate]]:
    key = _sql_like(f"{person.last} {person.first}")
    rows = _arcgis(fetch, MONTGOMERY_URL,
                   f"UPPER(Owner1) LIKE '%{key}%' OR UPPER(Owner2) LIKE '%{key}%'",
                   "ParcelID,Owner1,Owner2,PropertyAddress,AppraisedValue,AssessmentClass,CalcAcreage")
    if rows is None:
        return None
    return [Candidate(owner=" & ".join(x for x in (r.get("Owner1"), r.get("Owner2")) if x and str(x).strip()),
                      address=(r.get("PropertyAddress") or "").strip() or None, parcel=r.get("ParcelID"),
                      value=_money(r.get("AppraisedValue")), acres=r.get("CalcAcreage"),
                      property_class=r.get("AssessmentClass")) for r in rows]


def search_rutherford(fetch: Fetch, person: PersonName) -> Optional[List[Candidate]]:
    key = _sql_like(f"{person.last} {person.first}")
    where = " OR ".join(f"UPPER(Owner{i}) LIKE '%{key}%'" for i in (1, 2, 3))
    rows = _arcgis(fetch, RUTHERFORD_URL, where,
                   "ParcelID,Owner1,Owner2,Owner3,FormattedLocation,LocationCity,TotalValue,CALCACRES")
    if rows is None:
        return None
    out = []
    for r in rows:
        address = (r.get("FormattedLocation") or "").strip()
        if address and r.get("LocationCity"):
            address = f"{address}, {r['LocationCity'].strip()}"
        out.append(Candidate(owner=" & ".join(x for x in (r.get("Owner1"), r.get("Owner2"), r.get("Owner3")) if x and str(x).strip()),
                             address=address or None, parcel=r.get("ParcelID"), value=_money(r.get("TotalValue")),
                             acres=r.get("CALCACRES")))
    return out


_CSRF_RE = re.compile(r'name="csrf_token"[^>]+value="([^"]+)"')
_WILLIAMSON_TOTAL_RE = re.compile(r"Total\s+(?:Market\s+)?Appraisal[^$]*\$\s*([\d,]+)", re.I)


def search_williamson(fetch: Fetch, person: PersonName) -> Optional[List[Candidate]]:
    home = fetch(WILLIAMSON_BASE + "/")
    token = _CSRF_RE.search(home.text) if home is not None and home.status_code == 200 else None
    if not token:
        return None
    res = fetch(WILLIAMSON_BASE + "/json/search", params={
        "csrf_token": token.group(1), "owner_name": f"{person.last} {person.first}", "property_address": "",
        "parcel": "", "map_number": "", "lot": "", "subdivision": "", "city": "",
        "sales_date_start": "", "sales_date_end": ""})
    if res is None or res.status_code != 200:
        return None
    try:
        items = (res.json() or {}).get("data") or []
    except Exception:
        return None
    out = []
    for it in items:
        if it.get("Status") not in (None, "", "A"):
            continue  # inactive / retired parcels
        address = (it.get("Property Address") or "").strip()
        if address and it.get("Property City"):
            address = f"{address}, {it['Property City'].strip()}"
        out.append(Candidate(owner=it.get("Owner") or "", address=address or None, parcel=it.get("Parcel ID"),
                             extra={"lrsn": it.get("lrsn") or it.get("DT_RowId"), "csrf": token.group(1)}))
    return out


def williamson_value(fetch: Fetch, cand: Candidate) -> Optional[float]:
    lrsn, csrf = cand.extra.get("lrsn"), cand.extra.get("csrf")
    if not lrsn:
        return None
    res = fetch(f"{WILLIAMSON_BASE}/parcel/{lrsn}", params={"csrf": csrf})
    if res is None or res.status_code != 200:
        return None
    text = re.sub(r"<[^>]+>", " ", res.text)
    m = _WILLIAMSON_TOTAL_RE.search(_html.unescape(text))
    return _money(m.group(1)) if m else None


def _cells(row_html: str) -> List[str]:
    # Strip only: Shelby parcel IDs carry significant internal spaces
    # ("011111  00001") that the detail URL needs verbatim.
    return [_html.unescape(re.sub(r"<[^>]+>", "", c)).strip()
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.S | re.I)]


def _squash(value: Optional[str]) -> Optional[str]:
    return re.sub(r"\s+", " ", value).strip() if value else value


def parse_shelby_detail(page: str) -> Dict[str, Any]:
    fields: Dict[str, str] = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S | re.I):
        cells = _cells(row)
        if len(cells) >= 2 and cells[0].endswith(":"):
            fields[cells[0].rstrip(": ").strip().lower()] = cells[1]
    address = fields.get("property address")
    return {"parcel": fields.get("parcel id"), "owner": _squash(fields.get("owner name")),
            "address": re.sub(r"\s+", " ", address).strip() if address else None,
            "value": _money(fields.get("total appraisal")), "acres": _money(fields.get("acres")),
            "class": fields.get("class")}


def parse_shelby_results(page: str) -> List[Candidate]:
    """The owner search returns either a results table (Parcel ID, Owner
    Name, Property Location) or, for a single hit, the detail page itself."""
    if re.search(r"Total Appraisal", page, re.I):
        d = parse_shelby_detail(page)
        return [Candidate(owner=d["owner"] or "", address=d["address"], parcel=d["parcel"], value=d["value"],
                          acres=d["acres"], property_class=d["class"], extra={"detail_loaded": True})] if d["owner"] else []
    out = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S | re.I):
        cells = _cells(row)
        if len(cells) >= 3 and cells[0] and not cells[0].lower().startswith("parcel"):
            out.append(Candidate(owner=_squash(cells[1]) or "", address=_squash(cells[2]) or None, parcel=cells[0]))
    return out


def search_shelby(fetch: Fetch, person: PersonName) -> Optional[List[Candidate]]:
    res = fetch(SHELBY_BASE + "/OwnerSubmit", method="POST",
                data={"firstName": person.first, "lastName": person.last, "page": "property"})
    if res is None or res.status_code != 200:
        return None
    return parse_shelby_results(res.text)


def shelby_value(fetch: Fetch, cand: Candidate) -> Optional[float]:
    if cand.extra.get("detail_loaded") or not cand.parcel:
        return cand.value
    res = fetch(SHELBY_BASE + "/propertyDetails", params={"parcelid": cand.parcel, "IR": "true"})
    if res is None or res.status_code != 200:
        return None
    detail = parse_shelby_detail(res.text)
    cand.acres = cand.acres or detail["acres"]
    cand.property_class = cand.property_class or detail["class"]
    return detail["value"]


SEARCHERS = {
    "montgomery": search_montgomery,
    "rutherford": search_rutherford,
    "williamson": search_williamson,
    "shelby": search_shelby,
}
VALUE_FETCHERS = {"williamson": williamson_value, "shelby": shelby_value}


def lookup(fetch: Fetch, county: str, decedent: str) -> Dict[str, Any]:
    """Search, match and (for matched parcels lacking one) fetch values.
    Returns {'status', 'query', 'parcels': [...], 'primary': {...} | None}.
    status is one of matched / ambiguous / not_found / source_error /
    unparseable_name / unsupported_county."""
    county = (county or "").lower().replace(" county", "").strip()
    if county not in SEARCHERS or county not in supported_counties():
        return {"status": "unsupported_county"}
    person = parse_person(decedent)
    if person is None:
        return {"status": "unparseable_name"}
    query = f"{person.last} {person.first}"
    candidates = SEARCHERS[county](fetch, person)
    if candidates is None:
        return {"status": "source_error", "query": query}
    result = choose(person, candidates)
    if result.status == "matched" and county in VALUE_FETCHERS:
        for cand in result.parcels:
            if cand.value is None:
                cand.value = VALUE_FETCHERS[county](fetch, cand)
    primary = result.primary if result.status == "matched" else None
    as_dict = lambda c: {"owner": c.owner, "address": c.address, "parcel": c.parcel, "value": c.value,
                         "acres": c.acres, "class": c.property_class}
    return {"status": result.status, "query": query, "owner_keys": result.owner_keys, "confidence": result.confidence,
            "parcels": [as_dict(c) for c in result.parcels], "primary": as_dict(primary) if primary else None}
