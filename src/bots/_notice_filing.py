"""
What a published notice is, who it names and which filing it belongs to,
read from the notice text and its identifiers.

A newspaper's category is a filing habit, not a fact: trustee's sales arrive
under "Delinquent Taxes", and Chancery tax sales, sheriff's execution sales
and storage-unit auctions arrive under "Foreclosures". The same notice is
printed again every week under a new notice number, often in a second paper
in another county. This module gives each notice:

  classify()         the filing type from the wording, with the basis stated.
                     Text that carries both a deed of trust sale and a tax
                     suit, or neither, is held for research with the reason.
                     It is never typed by its category alone.
  register_county()  the county whose Register holds the deed of trust. The
                     property is there, wherever the newspaper is printed.
  identifiers()      deed of trust book and page, instrument number, trustee
                     file number, docket number, every parcel printed.
  parties()          the grantor, or none with the reason. Deed wording is
                     never returned as a name.
  filing_keys()      keys that are equal for a republication of the same
                     notice and for the same sale staged by another source.
  build()            the raw_payload.filing block.
  same_filing()      how two records are tied: by instrument, by book and
                     page, by trustee file, or (weaker) by an entity grantor.

One notice is one filing. Several parcels in one notice are listed on that
filing; they do not become several leads.

Pure: no network, no database.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

from . import _court_order as court_order
from . import _trustee_notice as tn
from ._register_index import TN_COUNTIES

VERSION = "filing-1"
FORECLOSURE = "PRE_FORECLOSURE"
TAX = "TAX_LIEN"
# The column value for a notice whose type the text does not settle. It is
# not a lane: nothing ranks or routes on it.
RESEARCH = "NOTICE_RESEARCH"

_COUNTY_BY_SLUG = {re.sub(r"[^a-z]", "", name.lower()): name.lower() for name in TN_COUNTIES}

# ── Type ────────────────────────────────────────────────────────────────────

_TRUSTEE_SALE = re.compile(
    r"\b(?:substitute|successor)\s+trustee\W{0,3}s?\s+(?:notice\s+of\s+)?(?:foreclosure\s+)?sale"
    r"|\btrustee\W{0,3}s\s+(?:notice\s+of\s+)?(?:foreclosure\s+)?sale\b"
    r"|\bnotice\s+of\s+(?:substitute\s+|successor\s+)?trustee\W{0,3}s?\s+(?:foreclosure\s+)?sale"
    r"|\bforeclosure\s+(?:sale\s+)?notice\b|\bnotice\s+of\s+foreclosure\b", re.I)
_DEED_OF_TRUST = re.compile(r"\bdeed\s+of\s*trust\b", re.I)
# A deed of trust that is being enforced, not merely mentioned as an encumbrance.
_DEED_ENFORCED = re.compile(
    r"\b(?:default|executed|dated|of\s+record|recorded|secured\s+by|pursuant\s+to|by|under)\b[^.;]{0,160}"
    r"\bdeed\s+of\s*trust\b|\bdeed\s+of\s*trust\b[^.;]{0,160}\b(?:executed|dated|of\s+record|recorded|default)\b",
    re.I | re.S)
_TAX_SUIT = re.compile(
    r"\bdelinquent\s+(?:(?:real|personal)\s+)?(?:(?:property|estate)\s+)?tax\s*payers?\b|\btax\s+sale\b|\bback\s+tax(?:es)?\s+sale\b"
    r"|\bsale\s+of\s+(?:land|real\s+(?:estate|property))\s+for\s+delinquent\s+taxes\b"
    r"|\bland\s+sale\s+for\s+delinquent\s+taxes\b|\bdelinquent\s+tax\s+(?:sale|suit|lawsuit|cause|lien)\b"
    r"|\btax\s+cause\b|\b67-5-2[45]\d\d\b|\bdelinquent\s+(?:real\s+)?property\s+tax\s+(?:notice|sale|suit)\b"
    r"|\bcertifying\s+lists\s+of\s+delinquent\s+taxes\b", re.I)
_TAX_MENTION = re.compile(r"\bdelinquent\s+(?:\w+\s+){0,3}tax(?:es)?\b|\bback\s+taxes\b|\bunpaid\s+(?:property\s+)?taxes\b", re.I)
_EXECUTION_SALE = re.compile(
    r"\bsheriff\W{0,3}s\s+(?:execution\s+)?sale\b|\bexecution\s+sale\b|\blevy\s+(?:and|&)\s+sale\b"
    r"|\bholder\s+of\s+a\s+judgment\b", re.I)
_PERSONAL_PROPERTY = re.compile(r"\bsale\s+of\s+personal\s+property\b|\bpersonal\s+property\s+(?:only|sale)\b", re.I)
_HOA_SALE = re.compile(r"\bhomeowners?\W{0,2}\s+association\s+lien\b|\bassessment\s+lien\b", re.I)
_PRINT_NOISE = re.compile(r"^\W*(?:[AB]\d{1,2}\s+)?INDEX\b", re.I)
# A county or city selling land it already owns is not an owner in distress.
_GOVERNMENT_SALE = re.compile(
    r"\bsale\s+of\s+real\s+property\s+by\s+[A-Z][A-Za-z ]{2,40}\s+government\b|\breceived\s+an\s+offer\s+to\s+purchase\b"
    r"|\bproperty\s+acquired\s+by\s+[A-Z][A-Za-z ]{2,30}\s+county\s+through\b|\bsale\s+of\s+county\s+property\b", re.I)
_OTHER_STATE = re.compile(r"\bSTATE\s+OF\s+(?!TENNESSEE\b)[A-Z]+(?:\s+[A-Z]+)?\s+(?:CIRCUIT|DISTRICT|SUPERIOR|COUNTY)\s+COURT\b")


def classify(text: str, category_id: Optional[str] = None) -> Dict[str, Any]:
    """The filing type of a notice from its wording.

    Returns {"type", "basis", "signals", "hold_reason", "drop_reason"}:
      type         PRE_FORECLOSURE, TAX_LIEN, a _court_order kind, "general_tax",
                   or None when the text does not settle it
      basis        the wording that decided it (never the category)
      hold_reason  set when type is None and the notice is kept for research
      drop_reason  set when the notice is not a property filing at all
    category_id is recorded in the signals only; it never decides a type.
    """
    t = " ".join((text or "").split())
    signals: List[str] = []
    out = {"type": None, "basis": None, "signals": signals, "hold_reason": None, "hold_kind": None,
           "drop_reason": None}
    if not t:
        out["hold_reason"] = "The notice text is empty, so its type cannot be read"
        return out
    heading = _TRUSTEE_SALE.search(t)
    deed = _DEED_OF_TRUST.search(t)
    enforced = _DEED_ENFORCED.search(t)
    tax_suit = _TAX_SUIT.search(t)
    tax_mention = _TAX_MENTION.search(t)
    execution = _EXECUTION_SALE.search(t)
    if heading:
        signals.append(f"trustee sale wording: {heading.group(0)!r}")
    if deed:
        signals.append("deed of trust named" + (" as the instrument enforced" if enforced else ""))
    if tax_suit:
        signals.append(f"tax suit wording: {tax_suit.group(0)!r}")
    elif tax_mention:
        signals.append(f"taxes mentioned: {tax_mention.group(0)!r}")
    if execution:
        signals.append(f"judgment execution wording: {execution.group(0)!r}")
    foreclosure = bool(heading and deed) or bool(enforced) or bool(heading and not tax_suit and not execution)

    if foreclosure and tax_suit:
        out["hold_reason"] = ("The text carries both a trustee's sale under a deed of trust and delinquent tax suit "
                              "wording, so it is not typed until the full notice is read")
        out["hold_kind"] = "conflict"
        return out
    if foreclosure and execution and not deed:
        out["hold_reason"] = ("The text reads as a sheriff's execution sale on a judgment, not a sale under a deed of "
                              "trust. It is not a trustee foreclosure")
        return out
    if foreclosure:
        out.update(type=FORECLOSURE, basis="; ".join(signals[:2]))
        return out
    if _GOVERNMENT_SALE.search(t):
        out["drop_reason"] = "a government selling property it already owns"
        return out
    if tax_suit:
        out.update(type=TAX, basis=signals[-1] if not execution else next(s for s in signals if s.startswith("tax suit")))
        return out
    if tn.GENERAL_TAX_RE.search(t):
        out.update(type="general_tax", basis="county-wide tax announcement that names no property")
        return out
    if court_order.PARTITION_ACTION_RE.search(t):
        out.update(type="PARTITION", basis="partition suit wording")
        return out
    if execution:
        what = "personal property" if _PERSONAL_PROPERTY.search(t) else "property"
        out["hold_reason"] = (f"Sheriff's execution sale of {what} on a judgment. It is not a trustee foreclosure and "
                              "not a tax sale")
        return out
    if _HOA_SALE.search(t):
        out["hold_reason"] = "Association lien sale. It is not a deed of trust foreclosure and not a tax sale"
        return out
    if _PRINT_NOISE.search(t):
        out["drop_reason"] = "newspaper page furniture, not a notice"
        return out
    if _OTHER_STATE.search(t):
        out["drop_reason"] = "a court of another state"
        return out
    kind = court_order.kind(t)
    if kind:
        out.update(type=kind, basis="court notice wording")
        return out
    if court_order.NOT_A_LEAD_RE.search(t):
        out["drop_reason"] = "storage, towing or bid advertisement"
        return out
    if deed:
        # A deed of trust named with no sale or default wording around it.
        out.update(type=FORECLOSURE, basis="deed of trust named")
        return out
    if tax_mention:
        out["hold_reason"] = ("Taxes are mentioned but the text shows no tax suit, tax sale or deed of trust, so the "
                              "type is not settled")
        return out
    out["hold_reason"] = ("The text shown names no deed of trust, trustee, tax suit or court sale, so the type is "
                          "not settled by the notice itself")
    return out


# ── County ──────────────────────────────────────────────────────────────────

_REGISTER_COUNTY = re.compile(
    r"\bRegister(?:\W{0,3}s|\s+of\s+Deeds)?(?:\s+Of\S{0,3}ce)?(?:\s+(?:of\s+Deeds))?\s+(?:of|for|in)\s+"
    r"((?:[A-Z][A-Za-z]+\s+){1,2})County\b|"
    r"\bRegister\W{0,3}s\s+Of\S{0,3}ce,?\s+((?:[A-Z][A-Za-z]+\s+){1,2})County\b", re.I)
_SITUS_COUNTY = re.compile(
    r"\b(?:situated|located|lying|land)\s+in\s+((?:[A-Z][A-Za-z]+\s+){1,2})County,?\s+(?:Tennessee|TN)\b", re.I)


_VENUE_COUNTY = re.compile(r"\bSTATE\s+OF\s+TENNESSEE,?\s+((?:[A-Z][A-Za-z]+\s+){1,2})COUNTY\b", re.I)


def _tn_county(name: Any) -> Optional[str]:
    words = re.sub(r"[^A-Za-z ]", " ", str(name or "")).split()
    for take in (2, 1):                       # "Van Buren" before "Buren"
        slug = "".join(words[-take:]).lower()
        if len(words) >= take and slug in _COUNTY_BY_SLUG:
            return _COUNTY_BY_SLUG[slug]
    return None


def register_county(text: str) -> Dict[str, Any]:
    """The county the notice itself places the deed of trust or the land in.

    {"county", "basis"}; county is None when the text names none, or names
    more than one Register (a multi-county deed of trust is not guessed).
    """
    t = " ".join((text or "").split())
    for rx, label in ((_REGISTER_COUNTY, "Register's Office named in the notice"),
                      (_SITUS_COUNTY, "county the notice places the property in"),
                      (_VENUE_COUNTY, "venue stated in the notice")):
        found: List[str] = []
        for m in rx.finditer(t):
            county = _tn_county(next(g for g in m.groups() if g))
            if county and county not in found:
                found.append(county)
        if len(found) == 1:
            return {"county": found[0], "basis": label}
        if len(found) > 1:
            return {"county": None, "basis": "the notice names more than one county: " + ", ".join(found)}
    return {"county": None, "basis": None}


# ── Identifiers ─────────────────────────────────────────────────────────────

_TS_FILE = re.compile(r"\bTS\s*(?:#|No\.?|Number)\s*:?\s*([A-Z0-9][A-Z0-9\-]{4,})", re.I)
_LEAD_FILE = re.compile(r"^\W*(\d{7,9})\s+NOTICE\b", re.I)
_DOCKET = re.compile(
    r"\b(?:Case|Docket|Civil\s+Action|Consolidated\s+Action|Cause|Civil\s+Warrant)\s*(?:No\.?|Number|#|NO:)?\s*[:#]?\s*"
    r"(?=[A-Z0-9\-]*\d)([A-Z0-9][A-Z0-9\-]{2,})", re.I)
_PARCEL = re.compile(
    r"\b(?:Map\s*(?:and|&|/)\s*Parcel|Tax\s+Map(?:\s+and\s+Parcel)?|Parcel|Tax\s+ID)\s*(?:No\.?|Number|ID|#)?\s*[:#]?\s*"
    r"(\d{2,3}[A-Z]?[ \-]?(?:[A-Z][ \-]?)?[A-Z]?[ \-]?\d{1,3}(?:\.\d{1,2})?(?:[ \-]\d{3})?)", re.I)
_DEED_DATE = re.compile(
    r"\bdeed\s+of\s*trust\b[^.;]{0,140}?\b(?:dated|executed\s+on)\s+(?:the\s+)?"
    r"(\d{1,2}/\d{1,2}/\d{4}|[A-Za-z]+\.?\s+\d{1,2},?\s+\d{4}|\d{1,2}(?:st|nd|rd|th)?\s+day\s+of\s+[A-Za-z]+,?\s+\d{4})",
    re.I | re.S)


def _date(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    value = " ".join(text.replace(".", " ").replace(",", " ").split())
    m = re.match(r"(\d{1,2})(?:st|nd|rd|th)? day of ([A-Za-z]+) (\d{4})$", value, re.I)
    if m:
        value = f"{m.group(2)} {m.group(1)} {m.group(3)}"
    for fmt in ("%B %d %Y", "%b %d %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def identifiers(text: str) -> Dict[str, Any]:
    """Every identifier the notice prints. parcels is a list: one notice can
    cover several parcels and is still one filing."""
    t = text or ""
    flat = " ".join(t.split())
    ref = tn.deed_ref(flat)
    ts = _TS_FILE.search(flat) or _LEAD_FILE.search(flat)
    docket = _DOCKET.search(flat)
    deed_date = _DEED_DATE.search(flat)
    parcels: List[str] = []
    for m in _PARCEL.finditer(flat):
        value = re.sub(r"\s+", " ", m.group(1)).strip(" -.")
        if value not in parcels:
            parcels.append(value)
    return {"deed_book": ref["book"], "deed_page": ref["page"], "instrument": ref["instrument"],
            "trustee_file": ts.group(1).upper() if ts else None,
            "docket": docket.group(1).upper() if docket else None,
            "deed_date": _date(deed_date.group(1)) if deed_date else None,
            "sale_date": tn.sale_date(flat), "parcels": parcels}


# ── Parties ─────────────────────────────────────────────────────────────────

_TRUNCATED = re.compile(r"(?:\.\.\.|…)\s*$")


def parties(text: str) -> Dict[str, Any]:
    """The grantor of the deed of trust. {"names", "reason"}: reason says why
    names is empty. Wording from the deed is never returned as a name."""
    names = [n for n in tn.borrowers(text or "") if tn.plausible_name(n)]
    if names:
        return {"names": names, "reason": None}
    t = " ".join((text or "").split())
    if not t:
        return {"names": [], "reason": "The notice text is empty"}
    if _TRUNCATED.search(t) or len(t) < 320:
        return {"names": [], "reason": "The part of the notice shown ends before a grantor is named"}
    return {"names": [], "reason": "No grantor could be separated from the deed wording in the notice text"}


def party_name(value: Any) -> Optional[str]:
    """A stored name if it reads as a party, else None."""
    text = str(value or "").strip()
    # "X in favor of Y": X is the grantor; Y is the trustee or beneficiary.
    text = re.split(r"\s+in\s+favou?r\s+of\s+", text, flags=re.IGNORECASE)[0].strip(" ,.;")
    return text if text and tn.plausible_name(text) else None


# ── Keys ────────────────────────────────────────────────────────────────────


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", str(value or "").upper())).strip()


def grantor_key(names: Iterable[str]) -> Optional[str]:
    parts = sorted({_norm(n) for n in names or [] if _norm(n)})
    return "+".join(parts) or None


def _key(kind: str, readable: str) -> Dict[str, str]:
    """A short key safe to filter on, with the identifiers it stands for."""
    return {"kind": kind, "key": "f1-" + hashlib.sha1(readable.encode("utf-8")).hexdigest()[:24], "of": readable}


def filing_keys(county: Optional[str], ids: Dict[str, Any], names: Iterable[str]) -> List[Dict[str, str]]:
    """Keys for one filing, strongest first. Two records are the same filing
    when they share a key. A notice with no instrument, book and page, trustee
    file or named grantor has no key and is never merged with another."""
    c = (county or "").strip().lower()
    keys: List[Dict[str, str]] = []
    if not c:
        return keys
    instrument = re.sub(r"[^A-Z0-9]", "", str(ids.get("instrument") or "").upper())
    if instrument:
        keys.append(_key("instrument", f"tn|{c}|dot|i:{instrument}"))
    if ids.get("deed_book") and ids.get("deed_page"):
        book = re.sub(r"[^A-Z0-9]", "", str(ids["deed_book"]).upper())
        keys.append(_key("book_page", f"tn|{c}|dot|b:{book}-{int(ids['deed_page'])}"))
    if ids.get("trustee_file"):
        keys.append(_key("trustee_file", f"tn|{c}|ts:{ids['trustee_file']}"))
    grantors = grantor_key(names)
    if grantors and (ids.get("deed_date") or ids.get("sale_date")):
        stamp = f"d:{ids['deed_date']}" if ids.get("deed_date") else f"s:{ids['sale_date']}"
        keys.append(_key("grantor_and_date", f"tn|{c}|g:{grantors}|{stamp}"))
    return keys


def text_fingerprint(text: str) -> str:
    return hashlib.sha1(_norm(text)[:400].encode("utf-8")).hexdigest()[:16]


# ── The filing block ────────────────────────────────────────────────────────


def build(text: str, *, category: Optional[str], category_id: Optional[str], publication_county: Optional[str],
          notice_id: Optional[str] = None, publication_date: Optional[str] = None,
          newspaper: Optional[str] = None, decided: Optional[str] = None,
          decided_basis: Optional[str] = None) -> Dict[str, Any]:
    """raw_payload.filing for one notice. Nothing here replaces the stored
    excerpt: every value is derived and carries its basis.

    decided / decided_basis: a type settled by a reviewed rule outside this
    module (a county's reviewed notice list). The basis is stored as given."""
    verdict = classify(text, category_id)
    if decided and decided != verdict["type"]:
        verdict = {**verdict, "type": decided, "basis": decided_basis or "reviewed rule", "hold_reason": None,
                   "hold_kind": None, "drop_reason": None}
    ids = identifiers(text)
    who = parties(text) if verdict["type"] == FORECLOSURE else {"names": [], "reason": None}
    place = register_county(text)
    pub = (publication_county or "").strip().lower() or None
    county = place["county"] or pub
    keys = filing_keys(county, ids, who["names"]) if verdict["type"] == FORECLOSURE else []
    block: Dict[str, Any] = {
        "version": VERSION, "type": verdict["type"], "type_basis": verdict["basis"], "signals": verdict["signals"],
        "filed_under": category, "county": county,
        "county_basis": place["basis"] if place["county"] else ("county of the newspaper; the text shown names no "
                                                                "Register or property county" if pub else None),
        "county_from_text": bool(place["county"]), "publication_county": pub, "identifiers": {k: v for k, v in ids.items() if k != "parcels" and v},
        "parcels": ids["parcels"], "parties": who["names"],
        "key": keys[0]["key"] if keys else None, "keys": keys,
        "publications": [{"notice_id": str(notice_id), "date": publication_date, "newspaper": newspaper}] if notice_id else [],
    }
    if verdict["hold_reason"]:
        block["hold"] = {"route": "research", "reason": verdict["hold_reason"],
                         "kind": verdict.get("hold_kind") or "unsettled"}
    if verdict["drop_reason"]:
        block["not_a_filing"] = verdict["drop_reason"]
    if verdict["type"] == FORECLOSURE and not who["names"]:
        block["party_reason"] = who["reason"]
    if place["county"] is None and place["basis"]:
        block["county_note"] = place["basis"]
    return block


def same_filing(a: Dict[str, Any], b: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """How two filing blocks are tied, or None. The strongest shared key wins."""
    theirs = {k["key"]: k["kind"] for k in (b or {}).get("keys") or []}
    for k in (a or {}).get("keys") or []:
        if k["key"] in theirs:
            return {"kind": k["kind"], "key": k["key"]}
    return None


# ── Republications ──────────────────────────────────────────────────────────


def merge_republications(leads: List[Any], existing: Iterable[Dict[str, Any]] = ()) -> List[Any]:
    """One lead per filing.

    leads: LeadPayload-like objects with pipeline_lead_key and
    raw_payload["filing"]. existing: staged rows of the same source as
    {"pipeline_lead_key", "key", "duplicate_of"} (key = filing key).

    A notice whose filing is already staged under another notice number takes
    that record's lead key, so the writer records a sighting on it instead of
    inserting a second row. A notice already marked as a duplicate of another
    record (of this or any source) is not staged again. Within one run the
    first publication read is kept and lists the others. Leads with no filing
    key pass through untouched.
    """
    staged: Dict[str, str] = {}
    merged_away = set()
    for row in existing:
        if row.get("duplicate_of"):
            merged_away.add(row.get("pipeline_lead_key"))
        elif row.get("key"):
            staged.setdefault(row["key"], row["pipeline_lead_key"])
    out: List[Any] = []
    first: Dict[str, Any] = {}
    for lead in leads:
        filing = (lead.raw_payload or {}).get("filing") if isinstance(lead.raw_payload, dict) else None
        key = (filing or {}).get("key")
        if lead.pipeline_lead_key in merged_away:
            continue                                  # already merged into another record
        if not key:
            out.append(lead)
            continue
        if key in staged and lead.pipeline_lead_key != staged[key]:
            filing["republication_of_key"] = staged[key]
            lead.pipeline_lead_key = staged[key]
        kept = first.get(key)
        if kept is None:
            first[key] = lead
            out.append(lead)
            continue
        seen = {p.get("notice_id") for p in kept.raw_payload["filing"]["publications"]}
        for pub in filing.get("publications") or []:
            if pub.get("notice_id") not in seen:
                kept.raw_payload["filing"]["publications"].append(pub)
    return out


# ── The same sale on another record ─────────────────────────────────────────

REVIEWER = "filing_identity"
STRONG_TIES = {
    "instrument": "both cite the same deed of trust instrument number",
    "book_page": "both cite the same deed of trust book and page",
    "trustee_file": "both carry the same trustee file number",
    "grantor_and_date": "the same grantor and the same deed of trust or sale date",
}


def _iso_day(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%A, %B %d, %Y", "%B %d, %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text[:40], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def pointer_card(row: Dict[str, Any], canonical: Dict[str, Any], tie: Optional[Dict[str, str]],
                 today: date) -> Dict[str, Any]:
    """The pilot_review card for a notice that is the same sale as a record
    that already has a card.

    row: id, source_url, raw_payload (with filing, newspaper, publication_date).
    canonical: id, bot_source, pilot_review (its card).
    tie: same_filing() result, or None for an entity-name match with no shared
    identifier.

    A tie on an instrument, book and page, trustee file or grantor and date
    removes the notice as a duplicate and points at the card to work. A match
    on the borrower company alone is not proof: the notice is held for
    research, still pointing at that card. Either way the card carries the
    existing card's lane, county and owner names, so it is the same owner
    group and never a second one.
    """
    from ._pilot_review import build_review, source

    card = canonical["pilot_review"]
    day = today.isoformat()
    raw = row.get("raw_payload") or {}
    filing = raw.get("filing") or {}
    pubs = filing.get("publications") or []
    notice_id = (pubs[0].get("notice_id") if pubs else None) or raw.get("notice_id") or ""
    printed = _iso_day(raw.get("publication_date"))
    paper = raw.get("newspaper") or "newspaper"
    where = f"record {canonical['id']} ({canonical.get('bot_source')})"
    deadline = (card.get("economics") or {}).get("deadline")
    filed = filing.get("filed_under")
    what = (f"Foreclosure notice printed in {paper}" + (f" on {printed}" if printed else "")
            + f" (TN Public Notice {notice_id})"
            + (f", which the paper filed under {filed!r}" if filed and filed != "Foreclosures" else "")
            + f". It is the same sale as {where}" + (f", set for {deadline}" if deadline else "") + ".")
    src = source(f"TN Public Notice {notice_id}, {paper}", row.get("source_url"), printed)
    asset = dict(card.get("asset") or {})
    decider = dict(card.get("decider") or {})
    economics = {k: v for k, v in (card.get("economics") or {}).items() if k != "runway_days"}
    common = dict(lane=card["lane"], reviewed_by=REVIEWER, county=card.get("county"),
                  owner_names=card.get("owner_names") or decider.get("name"), today=today,
                  event={"what": what, "status": (card.get("event") or {}).get("status") or "unknown",
                         "checked_on": day, "source": src},
                  asset=asset, decider=decider, economics=economics)
    availability = {"evidence": f"See {where}; this notice adds no separate property or owner.",
                    "checked_on": day, "conflicts": [],
                    "first_call_question": (card.get("availability") or {}).get("first_call_question") or ""}
    if tie and tie.get("kind") in STRONG_TIES:
        of = next((k.get("of") for k in filing.get("keys") or [] if k.get("key") == tie.get("key")), None)
        return build_review(route="removed", **common, availability={**availability, "verify_next": []},
                            route_reason=(f"Duplicate of {where}: {STRONG_TIES[tie['kind']]}"
                                          + (f" ({of})" if of else "") + ". Work that card; it carries the sale date."))
    names = ", ".join(filing.get("parties") or [])
    return build_review(
        route="research_hold", **common,
        availability={**availability, "verify_next": [
            "Read the full notice and compare its deed of trust book, page or instrument with the existing record",
            f"Until then work {where} only; do not open a second owner group for {names or 'this borrower'}"]},
        route_reason=(f"Probably the same sale as {where}: the borrower company {names} is the same, but the part of "
                      "the notice shown prints no deed of trust book, page or instrument, so it is not confirmed"))
