"""
Shared pilot review card for the five priority Tennessee lanes.

Every lane (pre-foreclosure, delinquent tax, commercial, probate, land)
writes the same structure to raw_payload.pilot_review so the CRM can render
one card and the statewide worklist can count an owner and a property once.

The card answers five questions with dated sources:

  event         what happened, is it still active, when was it checked
  asset         which exact asset, and what ties it to the event
  decider       who can decide to sell, and what supports that role
  economics     deadline, runway, sourced value, known debt, unknowns
  availability  evidence the assignment may still be open, and the first
                thing to ask on a call

Routes:

  first_trace_review         worth buying contact info for, on this evidence
  professional_relationship  the path is counsel, a trustee or another
                             professional, not the owner
  research_hold              a real record with a named gap; says what to check
  removed                    resolved, sold, paid, conflicting or duplicate

build_review() refuses a first_trace_review card that leaves one of the five
questions unanswered or unsourced, so a lane cannot recommend spending money
on a thin record. merge_review() keeps a manual decision and never lets a
weaker automatic card replace a stronger one.

No network, no database: pure functions, used by lane bots.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

VERSION = "pilot-review-1"

LANES = ("pre_foreclosure", "tax", "cre", "probate", "land")
ROUTES = ("first_trace_review", "professional_relationship", "research_hold", "removed")
EVENT_STATUS = ("active", "postponed", "resolved", "unknown")
DEBT_KINDS = ("delinquent_tax", "mortgage_original", "mortgage_estimate", "judgment", "lien", "other")

# Higher wins when two automatic cards compete for the same row.
_ROUTE_STRENGTH = {"removed": 3, "first_trace_review": 2, "professional_relationship": 2, "research_hold": 1}

_SUFFIX = re.compile(
    r"\b(JR|SR|II|III|IV|ETUX|ET UX|ETAL|ET AL|ETVIR|ET VIR|TRUSTEE|TR|TRS|ESTATE OF|EST OF|EST|"
    r"HEIRS OF|HEIRS|DECEASED|DECD|LIFE ESTATE|LE|AKA|FKA|NKA)\b"
)
_ENTITY = re.compile(r"\b(LLC|L L C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|LTD|TRUST|PARTNERSHIP|PARTNERS|"
                     r"CHURCH|MINISTRIES|BANK|ASSOCIATION|ASSN|HOLDINGS|PROPERTIES|INVESTMENTS)\b")


def _clean(text: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", str(text or "").upper())).strip()


def _county(county: Any) -> str:
    c = re.sub(r"\s+county$", "", str(county or "").strip().lower())
    return {"de kalb": "dekalb", "vanburen": "van buren"}.get(c, c)


def owner_group_key(names: Any, county: Any, state: str = "TN") -> Optional[str]:
    """One key per owner group, order-independent, so the same people on two
    lanes count once. Accepts a string or a list of names as sourced.

    A natural person is keyed by surname plus first-name initial ("REED VIDA L"
    and "Vida Reed" agree); an entity by its cleaned legal name. Returns None
    when no usable name is given: an unknown owner is never merged with another.
    """
    if isinstance(names, str):
        names = re.split(r"\s*(?:;|&| and |/)\s*", names, flags=re.I)
    else:
        # A list entry can itself be a joint roll name ("PERCER BOBBIE E JR & DONNA M").
        names = [seg for raw in (names or []) for seg in re.split(r"\s*(?:;|&| and |/)\s*", str(raw or ""), flags=re.I)]
    parts: List[str] = []
    last_surname: Optional[str] = None
    for raw in names or []:
        text = _clean(raw)
        if not text:
            continue
        if _ENTITY.search(text):
            parts.append("E:" + _ENTITY.sub(" ", text).strip().replace("  ", " "))
            last_surname = None
            continue
        comma = "," in str(raw)
        text = _SUFFIX.sub(" ", text)
        tokens = [t for t in text.split() if len(t) > 1 or t.isalpha()]
        # County rolls and "LAST, FIRST" put the surname first; prose puts it last.
        roll_order = comma or str(raw).strip() == str(raw).strip().upper()
        # On a roll, a joint owner is often printed without the shared surname:
        # "HERIN JOHN B & TINA", "PERCER BOBBIE E JR & DONNA M". A lone given
        # name, or a given name plus an initial, takes the surname before it.
        if roll_order and last_surname and tokens and (len(tokens) == 1 or (len(tokens) == 2 and len(tokens[1]) == 1)):
            parts.append(f"P:{last_surname} {tokens[0][0]}")
            continue
        if len(tokens) < 2:
            continue
        surname, first = (tokens[0], tokens[1]) if roll_order else (tokens[-1], tokens[0])
        last_surname = surname if roll_order else None
        parts.append(f"P:{surname} {first[0]}")
    if not parts:
        return None
    return f"{state.lower()}|{_county(county)}|" + "+".join(sorted(set(parts)))


def property_key(county: Any, parcel: Any = None, address: Any = None, state: str = "TN") -> Optional[str]:
    """One key per property. Parcel wins; a normalized street address is the
    fallback. Returns None when neither identifies the property."""
    c = _county(county)
    # Comptroller roll IDs end with the roll year ("074 058    12100 000 2027");
    # the tract is the same every year, so the year is not part of the key.
    p = re.sub(r"\s+20\d{2}\s*$", "", str(parcel or "").upper())
    p = re.sub(r"[^A-Z0-9]", "", p)
    if c and len(p) >= 6:
        return f"{state.lower()}|{c}|p:{p}"
    a = _clean(address)
    a = re.sub(r"\b(TN|TENNESSEE)\b.*$", "", a).strip()
    a = re.sub(r"\b(STREET)\b", "ST", a)
    a = re.sub(r"\b(AVENUE)\b", "AVE", a)
    a = re.sub(r"\b(DRIVE)\b", "DR", a)
    a = re.sub(r"\b(ROAD)\b", "RD", a)
    a = re.sub(r"\b(LANE)\b", "LN", a)
    a = re.sub(r"\b(COURT)\b", "CT", a)
    a = re.sub(r"\b(BOULEVARD)\b", "BLVD", a)
    if c and re.match(r"^\d+ \w+", a):
        return f"{state.lower()}|{c}|a:{a}"
    return None


def _iso(d: Any) -> Optional[str]:
    if d is None or d == "":
        return None
    if isinstance(d, (date, datetime)):
        return d.isoformat()[:10]
    s = str(d)[:10]
    try:
        date.fromisoformat(s)
    except ValueError:
        raise ValueError(f"not an ISO date: {d!r}")
    return s


def source(label: str, url: Optional[str], on: Any = None) -> Dict[str, Any]:
    """A dated source. url may be None only for a paper or in-person record;
    the label must then say where it is."""
    if not label or not str(label).strip():
        raise ValueError("a source needs a label")
    return {"label": str(label).strip(), "url": (url or None), "date": _iso(on)}


def debt(kind: str, amount: Optional[float], as_of: Any, src: Dict[str, Any], note: str = "") -> Dict[str, Any]:
    """A known debt. Delinquent tax, the original mortgage and an estimate are
    kept apart: none of them is a payoff."""
    if kind not in DEBT_KINDS:
        raise ValueError(f"unknown debt kind {kind!r}")
    return {"kind": kind, "amount": (round(float(amount), 2) if amount is not None else None),
            "as_of": _iso(as_of), "source": src, "note": note}


def runway_days(deadline: Any, today: Optional[date] = None) -> Optional[int]:
    iso = _iso(deadline)
    if not iso:
        return None
    return (date.fromisoformat(iso) - (today or date.today())).days


def build_review(
    *,
    lane: str,
    route: str,
    route_reason: str,
    reviewed_by: str,
    event: Dict[str, Any],
    asset: Dict[str, Any],
    decider: Dict[str, Any],
    economics: Dict[str, Any],
    availability: Dict[str, Any],
    county: Any,
    owner_names: Any = None,
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """Validate and assemble one review card.

    event:        what, status, checked_on, source
    asset:        description, parcel, address, tie, source
    decider:      name, role, basis, verified, source
    economics:    deadline, deadline_kind, value, value_basis, value_is_estimate,
                  value_source, known_debt (list of debt()), unknowns (list)
    availability: evidence, checked_on, conflicts (list), first_call_question,
                  verify_next (list)

    Raises ValueError on a malformed card, and on a first_trace_review card
    that does not answer all five questions with a source.
    """
    if lane not in LANES:
        raise ValueError(f"unknown lane {lane!r}")
    if route not in ROUTES:
        raise ValueError(f"unknown route {route!r}")
    if not route_reason or not route_reason.strip():
        raise ValueError("route_reason is required: say in one sentence why this route")
    status = event.get("status") or "unknown"
    if status not in EVENT_STATUS:
        raise ValueError(f"unknown event status {status!r}")

    today = today or date.today()
    deadline = _iso(economics.get("deadline"))
    ev = {"what": (event.get("what") or "").strip(), "status": status,
          "checked_on": _iso(event.get("checked_on")), "source": event.get("source")}
    ast = {"description": (asset.get("description") or "").strip(), "parcel": asset.get("parcel") or None,
           "address": asset.get("address") or None, "tie": (asset.get("tie") or "").strip(),
           "source": asset.get("source")}
    dec = {"name": decider.get("name") or None, "role": (decider.get("role") or "").strip(),
           "basis": (decider.get("basis") or "").strip(), "verified": bool(decider.get("verified")),
           "source": decider.get("source")}
    eco = {"deadline": deadline, "deadline_kind": economics.get("deadline_kind") or None,
           "runway_days": runway_days(deadline, today),
           "value": economics.get("value"), "value_basis": economics.get("value_basis") or None,
           "value_is_estimate": bool(economics.get("value_is_estimate")),
           "value_source": economics.get("value_source"),
           "known_debt": list(economics.get("known_debt") or []),
           "unknowns": [u for u in (economics.get("unknowns") or []) if u]}
    av = {"evidence": (availability.get("evidence") or "").strip(),
          "checked_on": _iso(availability.get("checked_on")),
          "conflicts": [c for c in (availability.get("conflicts") or []) if c],
          "first_call_question": (availability.get("first_call_question") or "").strip(),
          "verify_next": [v for v in (availability.get("verify_next") or []) if v]}

    if route == "first_trace_review":
        gaps = first_trace_gaps(ev, ast, dec, eco, av)
        if gaps:
            raise ValueError("not ready for first trace review: " + "; ".join(gaps))
    if route == "research_hold" and not av["verify_next"]:
        raise ValueError("a research hold must say what to check next")

    names_in = owner_names if owner_names is not None else dec["name"]
    return {
        "version": VERSION, "lane": lane, "route": route, "route_reason": route_reason.strip(),
        # Names as sourced, kept so the owner key can be recomputed if the rule improves.
        "owner_names": [names_in] if isinstance(names_in, str) else [str(n) for n in (names_in or []) if n],
        "county": _county(county),
        "owner_group_key": owner_group_key(names_in, county),
        "property_key": property_key(county, ast["parcel"], ast["address"]),
        "event": ev, "asset": ast, "decider": dec, "economics": eco, "availability": av,
        "reviewed_by": reviewed_by, "reviewed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def rekey(card: Dict[str, Any], county: Any = None) -> Dict[str, Any]:
    """Recompute both keys from what the card stores, so cards written under
    an older key rule still dedupe. Uses owner_names when the card has them,
    else the decider name. Returns a copy; the card itself is not changed."""
    c = card.get("county") or county
    names = card.get("owner_names") or (card.get("decider") or {}).get("name")
    asset = card.get("asset") or {}
    return {**card, "owner_group_key": owner_group_key(names, c) if c else card.get("owner_group_key"),
            "property_key": (property_key(c, asset.get("parcel"), asset.get("address")) if c else None)
            or card.get("property_key")}


def first_trace_gaps(ev, ast, dec, eco, av) -> List[str]:
    """Why a card cannot yet be a first-trace recommendation. Empty = ready.

    Payoff, seller intent and a phone number may be unknown; they are listed
    as unknowns. What may not be missing: a current, sourced event; an exact
    asset tied to it; a named decider with a basis; a sourced value; and a
    first question to ask.
    """
    gaps: List[str] = []
    if not ev["what"] or not ev["source"]:
        gaps.append("event needs a description and a source")
    if ev["status"] not in ("active", "postponed"):
        gaps.append(f"event is {ev['status']}, not active")
    if not ev["checked_on"]:
        gaps.append("event needs a checked_on date")
    if not ast["description"] or not (ast["parcel"] or ast["address"]):
        gaps.append("asset needs a description and a parcel or street address")
    if not ast["tie"]:
        gaps.append("asset needs the fact that ties it to the event")
    if not dec["name"] or not dec["role"] or not dec["basis"]:
        gaps.append("decider needs a name, a role and the basis for that role")
    if eco["value"] is None or not eco["value_basis"] or not eco["value_source"]:
        gaps.append("economics needs a sourced value and its basis")
    if eco["runway_days"] is not None and eco["runway_days"] < 0:
        gaps.append("deadline has passed")
    if av["conflicts"]:
        gaps.append("availability has an unresolved conflict: " + av["conflicts"][0])
    if not av["evidence"] or not av["first_call_question"]:
        gaps.append("availability needs evidence and a first-call question")
    return gaps


def merge_review(existing: Optional[Dict[str, Any]], new: Dict[str, Any]) -> Dict[str, Any]:
    """Decide which card a row keeps.

    - A manual card (existing["manual"] is true) always stays.
    - A re-run by the same reviewer replaces its own card (fresh check date).
    - Between two reviewers, "removed" beats everything, a recommendation
      beats a research hold, and a tie keeps the existing card.
    """
    if not existing or not isinstance(existing, dict):
        return new
    if existing.get("manual"):
        return existing
    if existing.get("reviewed_by") == new.get("reviewed_by"):
        return new
    if _ROUTE_STRENGTH.get(new.get("route"), 0) > _ROUTE_STRENGTH.get(existing.get("route"), 0):
        return new
    return existing


def dedupe_worklist(cards: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse first-trace cards to one row per owner group, keeping every
    separately identified property under it. A card with no owner key stands
    alone. Cards for the same property across lanes merge into one asset entry
    that lists every lane that reached it."""
    groups: Dict[str, Dict[str, Any]] = {}
    out: List[Dict[str, Any]] = []
    for n, card in enumerate(cards):
        if card.get("route") != "first_trace_review":
            continue
        key = card.get("owner_group_key") or f"__solo_{n}"
        g = groups.get(key)
        if g is None:
            g = {"owner_group_key": card.get("owner_group_key"), "assets": {}, "lanes": []}
            groups[key] = g
            out.append(g)
        pkey = card.get("property_key") or f"__asset_{n}"
        slot = g["assets"].setdefault(pkey, {"property_key": card.get("property_key"), "cards": []})
        slot["cards"].append(card)
        if card["lane"] not in g["lanes"]:
            g["lanes"].append(card["lane"])
    for g in out:
        g["assets"] = list(g["assets"].values())
    return out
