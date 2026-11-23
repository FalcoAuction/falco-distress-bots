"""
Check a Memphis Daily News foreclosure notice against the sale list its own
substitute trustee publishes.

A newspaper notice keeps its printed date after a sale is moved or pulled.
The trustee's own list is the independent, dated source for whether the sale
still stands. Lists read here:

  mackie_wolf_trustee   Mackie Wolf Zientz & Mann weekly Tennessee sale report,
                        already collected into staging by its own bot
  brock_scott_trustee   Brock & Scott Tennessee sale table, same
  rubin_lublin_list     Rubin Lublin Tennessee property listings (one page,
                        read by rubin_lublin_list.py when enabled)

None of these lists prints a borrower, an instrument number or a parcel. The
tie is therefore: the notice names this firm as substitute trustee, and the
firm's own list carries the same house number, street name, street suffix,
direction, unit and ZIP. A different unit, a different suffix ("Dr" against
"Cv"), two listings for one street or a different ZIP is never a match; it is
stored as the reason the notice stays in research.

Outcomes (check_notice):

  confirmed       on the list today with the advertised date
  postponed       on the list today with a later date (both dates kept)
  dropped         was on the list, is not on today's read
  not_listed      the list was read and does not carry the street
  identity_hold   the list has the house number in that ZIP, not the exact address
  date_conflict   the list shows an earlier or already passed date

Pure functions: no network, no database.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

LIST_KEY = "trustee_list_check"
# A staged listing not seen within this many hours of the source's latest
# successful read has left the list (same rule as the direct sale cards).
OFF_LIST_AFTER_HOURS = 36
# A list not read for this long cannot confirm or deny anything.
LIST_STALE_DAYS = 4

FIRMS = (
    (re.compile(r"\bmackie\s+wolf\b", re.I), "mackie_wolf_trustee", "Mackie Wolf Zientz & Mann"),
    (re.compile(r"\bbrock\s*(?:&|and)\s*scott\b", re.I), "brock_scott_trustee", "Brock & Scott"),
    (re.compile(r"\brubin\s+lublin\b", re.I), "rubin_lublin_list", "Rubin Lublin"),
)
FIRM_LABEL = {key: label for _, key, label in FIRMS}

_SUFFIX = {
    "ALLEY": "ALY", "AVENUE": "AVE", "AV": "AVE", "BOULEVARD": "BLVD", "CIRCLE": "CIR", "COURT": "CT", "COVE": "CV",
    "CROSSING": "XING", "DRIVE": "DR", "EXTENDED": "EXT", "HIGHWAY": "HWY", "LANE": "LN", "PARKWAY": "PKWY",
    "PLACE": "PL", "POINT": "PT", "ROAD": "RD", "SQUARE": "SQ", "STREET": "ST", "TERRACE": "TER", "TRAIL": "TRL",
    "TRACE": "TRCE", "WAY": "WAY",
}
_DIRECTION = {"NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W"}
_UNIT = re.compile(r"(?:\b(?:UNIT|APT|APARTMENT|SUITE|STE|LOT|BLDG|BUILDING)\b\.?\s*#?\s*|#\s*)([A-Z0-9-]+)")
# Places a Shelby address is written under. A list that prints street and
# city with no comma between them is split on one of these.
SHELBY_PLACES = ("MEMPHIS", "CORDOVA", "BARTLETT", "GERMANTOWN", "COLLIERVILLE", "ARLINGTON", "MILLINGTON", "LAKELAND",
                 "EADS")


def firm_source(firm: Any) -> Optional[str]:
    """Which readable list belongs to the substitute trustee named in a notice."""
    text = str(firm or "")
    for pattern, key, _ in FIRMS:
        if pattern.search(text):
            return key
    return None


def street_identity(text: Any) -> Optional[Dict[str, Any]]:
    """House number, street words (direction and suffix normalised) and unit.
    None when the text does not start with a house number."""
    head = re.sub(r"[.,]", " ", str(text or "").upper())
    unit = None
    found = _UNIT.search(head)
    if found:
        unit = found.group(1)
        head = (head[:found.start()] + " " + head[found.end():])
    tokens = head.split()
    if len(tokens) < 2 or not re.fullmatch(r"\d+[A-Z]?(?:-\d+)?", tokens[0]):
        return None
    words = [_DIRECTION.get(t, _SUFFIX.get(t, t)) for t in tokens[1:]]
    return {"number": tokens[0], "street": " ".join(words), "unit": unit}


def split_address(address: Any) -> Dict[str, Any]:
    """Street identity and ZIP from 'street, city, TN zip' or from a staged
    listing printed as 'street city, TN zip' (no comma before the city)."""
    text = re.sub(r"\s+", " ", str(address or "")).strip()
    zipcode = re.search(r"\b(?:TN|TENNESSEE)\s+(\d{5})(?:-\d{4})?\s*$", text, re.I)
    parts = [p.strip() for p in text.split(",")]
    street = parts[0]
    if len(parts) == 2:     # "761 Woodland Levee Way Cordova, TN 38018"
        upper = street.upper()
        for place in SHELBY_PLACES:
            if upper.endswith(" " + place):
                street = street[:-len(place)].strip()
                break
        else:
            street = ""     # city cannot be separated from the street: no identity
    return {"identity": street_identity(street), "zip": zipcode.group(1) if zipcode else None}


def _stamp(value: Any) -> Optional[datetime]:
    """Naive UTC datetime from an ISO string. None when unreadable."""
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return stamp if stamp.tzinfo is None else stamp.astimezone(timezone.utc).replace(tzinfo=None)


def staged_book(source: str, rows: Iterable[Dict[str, Any]], now: datetime) -> Optional[Dict[str, Any]]:
    """The current list of a firm whose bot stages its sale list
    (mackie_wolf_trustee, brock_scott_trustee), rebuilt from the staged rows
    and their last sighting. None when the firm's list has not been read
    recently enough to say what is on it."""
    now = _stamp(now.isoformat())
    rows = [r for r in rows if r.get("bot_source") == source]
    seen_at: Dict[str, Optional[datetime]] = {}
    last_read = None
    for r in rows:
        tracking = r.get("notice_tracking") if isinstance(r.get("notice_tracking"), dict) else {}
        stamp = _stamp(tracking.get("last_seen_at")) if tracking.get("last_seen_by") == source else None
        seen_at[r["id"]] = stamp
        if stamp and (last_read is None or stamp > last_read):
            last_read = stamp
    shelby = [r for r in rows if str(r.get("county") or "").strip().lower() == "shelby"]
    return _book(source, shelby, seen_at, last_read, now)


def _book(source, rows, seen_at, last_read, now):
    if last_read is None or (now - last_read).days >= LIST_STALE_DAYS:
        return None
    out = []
    for r in rows:
        tracking = r.get("notice_tracking") if isinstance(r.get("notice_tracking"), dict) else {}
        where = split_address(r.get("property_address"))
        stamp = seen_at.get(r["id"])
        if not where["identity"] or not where["zip"] or not r.get("trustee_sale_date"):
            continue
        out.append({**where, "date": str(r["trustee_sale_date"])[:10], "file": tracking.get("case_number"),
                    "address": r.get("property_address"), "url": tracking.get("report_url") or r.get("source_url"),
                    "on_list": bool(r.get("sale_status") is None and stamp
                                    and (last_read - stamp) <= timedelta(hours=OFF_LIST_AFTER_HOURS)),
                    "seen_on": stamp.date().isoformat() if stamp else None, "staging_id": r.get("id")})
    return {"source": source, "read_on": last_read.date().isoformat(), "rows": out, "url": None}


def all_staged_books(rows: Iterable[Dict[str, Any]], now: datetime) -> Dict[str, Optional[Dict[str, Any]]]:
    rows = list(rows)
    return {source: staged_book(source, rows, now) for source in ("mackie_wolf_trustee", "brock_scott_trustee")}


def page_book(source: str, listings: List[Dict[str, Any]], read_on: str, url: str) -> Dict[str, Any]:
    """A list read whole from one page today: every row on it is on the list."""
    out = []
    for item in listings:
        identity = street_identity(item.get("street"))
        if identity and item.get("zip") and item.get("date"):
            out.append({"identity": identity, "zip": item["zip"], "date": item["date"], "file": item.get("file"),
                        "address": f"{item['street']}, {item.get('city') or ''} {item['zip']}".replace(" ,", ","),
                        "url": url, "on_list": True, "seen_on": read_on})
    return {"source": source, "read_on": read_on, "rows": out, "url": url}


def check_notice(notice: Dict[str, Any], books: Dict[str, Optional[Dict[str, Any]]], today: str) -> Optional[Dict[str, Any]]:
    """Evidence for one notice row, or None when its trustee has no list read
    here or the notice has no usable address.

    notice: firm, property_address, trustee_sale_date, notice_id, list_check
    (the evidence stored by an earlier run). When the firm's list could not
    be read today the earlier evidence is returned unchanged; it stops
    counting once it is LIST_STALE_DAYS old (see sale_state).
    """
    source = firm_source(notice.get("firm"))
    if not source:
        return None
    prior = notice.get("list_check") if isinstance(notice.get("list_check"), dict) else None
    if prior and prior.get("source") != source:
        prior = None
    book = books.get(source)
    if book is None:
        return prior
    where = split_address(notice.get("property_address"))
    target, zipcode = where["identity"], where["zip"]
    advertised = str(notice.get("trustee_sale_date") or "")[:10] or None
    if not target or not zipcode or not advertised:
        return None
    base = {"source": source, "firm": FIRM_LABEL[source], "checked_on": book["read_on"], "notice_id": notice.get("notice_id"),
            "notice_date": advertised, "list_url": book.get("url")}
    near = [r for r in book["rows"] if r["zip"] == zipcode and r["identity"]["number"] == target["number"]]
    exact = [r for r in near if r["identity"] == target]
    current = [r for r in exact if r["on_list"]]

    def shown(r):
        return {"date": r["date"], "file": r.get("file"), "listed_address": r.get("address"),
                "list_url": r.get("url") or book.get("url"), "last_listed_on": r.get("seen_on")}

    if len({r["date"] for r in current}) > 1:
        return {**base, "status": "identity_hold",
                "reason": f"{base['firm']}'s sale list carries this address more than once with different dates "
                          f"({', '.join(sorted({r['date'] for r in current}))})"}
    if current:
        row = max(current, key=lambda r: r.get("seen_on") or "")
        found = {**base, **shown(row)}
        earlier = prior if prior and prior.get("status") in ("confirmed", "postponed") else None
        if row["date"] < today:
            return {**found, "status": "date_conflict",
                    "reason": f"{base['firm']}'s sale list still shows {row['date']}, a date that has passed"}
        if row["date"] == advertised:
            # After a postponement is written to the record the two dates agree;
            # keep the date it was moved from.
            if earlier and earlier.get("status") == "postponed" and earlier.get("date") == row["date"]:
                return {**found, "status": "postponed", "prior_date": earlier.get("prior_date"),
                        "notice_date": earlier.get("notice_date") or advertised}
            return {**found, "status": "confirmed"}
        if row["date"] > advertised:
            return {**found, "status": "postponed", "prior_date": advertised}
        return {**found, "status": "date_conflict",
                "reason": f"{base['firm']}'s sale list shows {row['date']}, earlier than the {advertised} in the notice"}
    if exact:
        row = max(exact, key=lambda r: r.get("seen_on") or "")
        return {**base, **shown(row), "status": "dropped"}
    if prior and prior.get("status") in ("confirmed", "postponed", "dropped") and prior.get("date"):
        return {**base, **{k: prior.get(k) for k in ("date", "file", "listed_address", "last_listed_on")},
                "list_url": prior.get("list_url") or base["list_url"], "status": "dropped"}
    if near:
        return {**base, "status": "identity_hold", "listed_address": near[0].get("address"), "date": near[0]["date"],
                "reason": f"{base['firm']}'s sale list shows {near[0].get('address')} for {near[0]['date']}, which is not "
                          "the exact street, suffix and unit in the notice"}
    return {**base, "status": "not_listed",
            "reason": f"{base['firm']}'s own sale list, read {book['read_on']}, does not carry this street"}


def check_notices(notices: Iterable[Dict[str, Any]], books: Dict[str, Optional[Dict[str, Any]]],
                  today: str) -> Dict[str, Dict[str, Any]]:
    """Evidence per notice id. When a property was advertised more than once
    only the newest notice is checked, so one listing never confirms two
    records. A notice whose date has passed is kept only when the list shows
    a later, still future date."""
    newest: Dict[Any, Dict[str, Any]] = {}
    for n in notices:
        source = firm_source(n.get("firm"))
        where = split_address(n.get("property_address"))
        if not source or not where["identity"] or not where["zip"]:
            continue
        key = (source, where["zip"], where["identity"]["number"], where["identity"]["street"], where["identity"]["unit"])
        rank = (str(n.get("publication_date") or ""), str(n.get("trustee_sale_date") or ""), str(n.get("id")))
        if key not in newest or rank > newest[key][0]:
            newest[key] = (rank, n)
    out: Dict[str, Dict[str, Any]] = {}
    for _, n in newest.values():
        if n.get("sale_status"):
            continue                                    # a manual status is never rechecked
        evidence = check_notice(n, books, today)
        if evidence is None:
            continue
        passed = str(n.get("trustee_sale_date") or "")[:10] < today
        if passed and not (evidence["status"] == "postponed" and (evidence.get("date") or "") >= today):
            continue
        out[n["id"]] = evidence
    return out


def effective_date(row: Dict[str, Any], evidence: Optional[Dict[str, Any]], today: str) -> Optional[str]:
    """The sale date a card should carry: the trustee's later date once the
    list shows a postponement, otherwise the date on the record."""
    if evidence and evidence.get("status") == "postponed" and (evidence.get("date") or "") >= today \
            and not row.get("sale_status"):
        return evidence["date"]
    return row.get("trustee_sale_date")
