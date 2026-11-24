"""
Final check before a paid contact lookup (skip trace) on a pilot worklist row.

A first-trace review card says a record is worth buying contact data for on
the evidence its lane had when it ran. This check asks a narrower question on
the day the money is spent: is it safe to spend today? It reads the card, the
county and register evidence already staged on the row, the CRM workflow
tables and the other cards for the same owner or parcel, and sorts every
worklist row into one of three queues:

  trace_now     worth tracing now
  verify_first  verify one fact first (the fact and how to resolve it are named)
  remove        remove from pursuit

What it rechecks, in this order:

  1. deadline and runway, and how fresh the confirmation of the date is
  2. owner and parcel against the county owner of record, and co-owners
  3. listing, contract or recent-transfer evidence (listing is never verified
     by a free source, so it is always reported as "not checked")
  4. duplicate owners across lanes, and contact or workflow holds
  5. money, as separate fields that are never added together
  6. the one missing fact and what would resolve it

Rules it never breaks: an unknown is reported as unknown; a table or source
that could not be read is "verify first", never a pass and never a removal;
an original loan amount is never shown as a payoff; a company with no verified
signer is never "trace now" for a person lookup. The check does not change a
card's route. Its result is stored beside the card (raw_payload.pretrace_check)
so a later run re-evaluates a row only when a material fact changed, when a
time rule comes due, or when the deadline is within URGENT_RECHECK_DAYS.

No network, no database: pure functions. pretrace_check_bot does the reads.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, timedelta
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._pilot_review import _ENTITY, _owner_parts, property_key, rekey, runway_days

VERSION = "pretrace-check-1"
KEY = "pretrace_check"

# Fewer days than this to a sale is not enough to reach a signed auction
# contract (first contact, meeting, title look, signature). No earlier rule
# existed in the code; lien_check only marks a sale "tight".
MIN_RUNWAY_DAYS = 10
# Every row whose deadline is this close is re-evaluated on every run.
URGENT_RECHECK_DAYS = 14
# Rows with a deadline inside this many days are listed first in the outputs.
FIRST_WINDOW_DAYS = 10
# A sale date must rest on a source read within this many days (the
# pre-foreclosure lane uses the same four days for a stale trustee source).
DATE_CONFIRM_MAX_AGE_DAYS = 4
# A row with no dated deadline: the event must have been rechecked this recently.
EVENT_CHECK_MAX_AGE_DAYS = 7
# The county owner record behind the card must be this recent.
OWNER_RECORD_MAX_AGE_DAYS = 30
# A recorded transfer this recent is checked before any spend.
RECENT_TRANSFER_DAYS = 180

TRACE, VERIFY, REMOVE = "trace_now", "verify_first", "remove"
QUEUES = (TRACE, VERIFY, REMOVE)
QUEUE_LABEL = {TRACE: "Worth tracing now", VERIFY: "Verify one fact first", REMOVE: "Remove from pursuit"}
_RANK = {REMOVE: 3, VERIFY: 2, TRACE: 1, "note": 0}

PAID_LOOKUP = "a paid property lookup"
SIGNER_TALK = "a signer conversation"
PAYOFF_REQUEST = "a payoff request"
RECORD_CHECK = "a county or court record check"
NOTHING = "nothing"
RESOLVE_BY = (PAID_LOOKUP, SIGNER_TALK, PAYOFF_REQUEST, RECORD_CHECK, NOTHING)

LISTING_NOT_CHECKED = "Listing not checked"
LISTING_FACT = ("Listing or contract status: not checked. No free source that allows automated reads covers "
                "listings, so nothing in this file says the property is unlisted.")
LISTING_HOW = "Ask on the first call, or look the address up by hand on the MLS, LoopNet or Crexi before buying contact data."

# The kind of fact each finding asks for, for counting what blocks the list most often.
FACT_KIND = {
    "workflow_hold": "Owner already in an active CRM workflow",
    "already_contacted": "Owner already contacted: outcome of the earlier contact",
    "manual_hold": "Manual hold still in force",
    "already_traced": "A phone number is already on file: does it reach the owner",
    "holds_unread": "Contact and workflow holds could not be read",
    "date_source_undated": "Date of the last confirmation is not recorded",
    "date_source_stale": "Sale date or event not confirmed recently",
    "owner_changed": "County record names a different owner",
    "co_owner_missing": "Another person is named for the property and is not on the card",
    "owner_record_missing": "No dated county owner record for the exact parcel",
    "parcel_mismatch": "County record on file is for a different parcel",
    "owner_record_stale": "County owner record not read recently",
    "company_no_signer": "Company or trust owner with no verified signer",
    "availability_conflict": "Unresolved conflict recorded on the card",
    "recent_transfer": "Recent recorded transfer",
    "sibling_card_removed": "Another record for the same parcel was removed",
    "sibling_card_research_hold": "Another record for the same parcel is on research hold",
    "value_missing": "No sourced value",
}

OPEN_WORKFLOW = ("attempting_contact", "rpc_made", "auction_booked", "listing_signed", "auction_live")
CLOSED_WORKFLOW = ("closed_lost", "closed_won")
DNC_OUTCOMES = ("do_not_call",)
DECLINED_OUTCOMES = ("not_interested",)
ENDED_SALE = ("cancelled", "ran", "reinstated")

_NOISE = {"AND", "THE", "OF", "MR", "MRS", "MS", "ETUX", "ETVIR", "ETAL", "ET", "UX", "VIR", "AL", "JR", "SR", "II",
          "III", "IV", "ESTATE", "EST", "HEIRS", "DECEASED", "DECD", "TRUSTEE", "TR", "TRS", "AKA", "FKA", "NKA",
          "LIFE", "LE", "UNKNOWN", "OR"}
_OWNER_KEYS = ("owner", "primary_owner", "roll_owner")
_PARCEL_KEYS = ("parcel_id", "parcel", "parcelid")
_DATE_KEYS = ("checked_on", "observed_on", "assessor_date", "roll_date", "as_of", "date")
_SKIP_PAYLOAD = {"pilot_review", KEY}
_CARE_OF = re.compile(r"(?:&\s*)?(?:\bATTN\b|\bATTENTION\b|\bC/O|%)", re.I)


def _d(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _day(v: Any) -> Optional[str]:
    """YYYY-MM-DD from an ISO date, an ISO timestamp or a roll date like 20231025."""
    s = str(v or "").strip()
    if re.fullmatch(r"\d{8}", s):
        s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        date.fromisoformat(s[:10])
    except ValueError:
        return None
    return s[:10]


def _age(day: Optional[str], today: date) -> Optional[int]:
    return (today - date.fromisoformat(day)).days if day else None


def _money(v: Any) -> str:
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return "amount unknown"


def _src_text(s: Any) -> str:
    s = _d(s)
    if not s:
        return ""
    return " ".join(x for x in [s.get("label") or "", f"({s['date']})" if s.get("date") else "", s.get("url") or ""] if x)


def is_entity(name: Any) -> bool:
    text = re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper())
    return bool(_ENTITY.search(text))


def name_tokens(name: Any, initials: bool = False) -> List[str]:
    toks = [t for t in re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper()).split() if t not in _NOISE]
    return [t for t in toks if initials or len(t) > 1]


def _tok_match(tok: str, pool: Iterable[str]) -> bool:
    for c in pool:
        if c == tok or (len(c) == 1 and tok.startswith(c)):
            return True
        if len(tok) >= 4 and len(c) >= 4 and SequenceMatcher(None, tok, c).ratio() >= 0.85:
            return True
    return False


def names_not_on_card(printed: Any, card_names: Iterable[Any]) -> Tuple[List[str], int]:
    """Name words in a printed owner line that no name on the card accounts
    for, and how many words did match. ("STROUD THOMAS A & GENEVA W" against a
    card naming "STROUD GENEVA W" gives (["THOMAS"], 2).) Spelling variants one
    letter apart count as the same word."""
    pool = [t for n in card_names for t in name_tokens(n, initials=True)]
    # "& ATTN MIKE DUPREE" or "C/O ..." on an owner line is a mailing contact, not an owner.
    toks = name_tokens(_CARE_OF.split(str(printed or ""))[0])
    missing = [t for t in toks if not _tok_match(t, pool)]
    return missing, len(toks) - len(missing)


def _parcel_norm(county: Any, parcel: Any) -> str:
    key = property_key(county or "x", parcel) or ""
    return key.rpartition("p:")[2] if "|p:" in key else re.sub(r"[^A-Z0-9]", "", str(parcel or "").upper())


def parcels_agree(county: Any, a: Any, b: Any) -> Optional[bool]:
    """True or False when both parcels are given, None when one is missing.
    A notice that prints only the map and group ("028042") agrees with the
    full county ID that starts with it."""
    x, y = _parcel_norm(county, a), _parcel_norm(county, b)
    if not x or not y:
        return None
    # The state parcel viewer drops the leading map digits, so its ID is the tail of the roll ID.
    return x == y or (min(len(x), len(y)) >= 6 and (x.startswith(y) or y.startswith(x) or x.endswith(y) or y.endswith(x)))


def county_records(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Every county owner record staged on the row: who the county names for
    which parcel, read when, under which payload key. Lane bots store these
    under different keys, so this looks for the owner fields wherever they are."""
    out: List[Dict[str, Any]] = []

    def visit(label: str, block: Dict[str, Any], inherit_date: Optional[str]) -> None:
        when = next((d for d in (_day(block.get(k)) for k in _DATE_KEYS) if d), None) or inherit_date
        owner = next((block[k] for k in _OWNER_KEYS if isinstance(block.get(k), str) and block[k].strip()), None)
        if owner:
            out.append({"owner": owner.strip(), "date": when, "key": label,
                        "parcel": next((block[k] for k in _PARCEL_KEYS if isinstance(block.get(k), str) and block[k].strip()), None),
                        "url": block.get("source_url") or block.get("url") or None,
                        "last_sale_date": _day(block.get("last_sale_date"))})
        for sub_key in ("roll", "primary"):
            if isinstance(block.get(sub_key), dict):
                visit(f"{label}.{sub_key}", block[sub_key], when)

    for store in ("raw_payload", "phone_metadata"):
        for key, block in sorted(_d(row.get(store)).items()):
            if key not in _SKIP_PAYLOAD and isinstance(block, dict):
                visit(key, block, None)
    return out


def other_named_parties(row: Dict[str, Any], facts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """People named for this property outside the county owner line: suit
    parties, borrowers on the notice, grantors on the recorded deed of trust,
    grantees on the latest recorded deed, and names entered by hand."""
    raw = _d(row.get("raw_payload"))
    out: List[Dict[str, Any]] = []
    pub = _d(raw.get("publication_evidence"))
    for p in pub.get("parties") or []:
        out.append({"name": str(p), "where": "the tax suit publication", "url": pub.get("url"), "date": None})
    borrowers = list(raw.get("borrowers") or []) if isinstance(raw.get("borrowers"), list) else []
    if _d(raw.get("structured")).get("borrower"):
        borrowers.append(_d(raw.get("structured"))["borrower"])
    for b in borrowers:
        if isinstance(b, str) and b.strip():
            out.append({"name": b, "where": "the foreclosure notice (borrower)", "url": row.get("source_url"), "date": None})
    docs = [d for d in _d(_d(raw.get("shelby_chain")).get("documents")).values() if isinstance(d, dict)]
    deeds = sorted((d for d in docs if str(d.get("code") or "").upper() in ("WD", "QC", "QCD", "SWD")),
                   key=lambda d: str(d.get("recorded") or ""))
    for d in docs:
        if "DEED OF TRUST" in str(d.get("doc_type") or "").upper():
            for g in d.get("grantors") or []:
                out.append({"name": str(g), "where": f"the recorded deed of trust {d.get('instrument') or ''}".strip() + " (borrower)",
                            "url": d.get("source_url"), "date": _day(d.get("file_date") or d.get("recorded"))})
    if deeds:
        for g in deeds[-1].get("grantees") or []:
            out.append({"name": str(g), "where": f"the latest recorded deed {deeds[-1].get('instrument') or ''}".strip() + " (grantee)",
                        "url": deeds[-1].get("source_url"), "date": _day(deeds[-1].get("recorded"))})
    for p in facts.get("co_owner_prints") or []:
        if isinstance(p, dict) and p.get("name"):
            out.append({"name": str(p["name"]), "where": p.get("source") or "a record read by hand",
                        "url": p.get("url"), "date": _day(p.get("date"))})
    return out


def money_fields(card: Dict[str, Any], row: Dict[str, Any], facts: Dict[str, Any]) -> Dict[str, Any]:
    """Value, recorded debt, recorded releases, tax balance and payoff as five
    separate facts. Nothing here is summed, no equity is computed, and an
    estimate of a loan balance is left out entirely. Payoff is "unknown"
    unless a dated payoff document is on file."""
    eco, raw = _d(card.get("economics")), _d(row.get("raw_payload"))
    vsrc = _d(eco.get("value_source"))
    value = {"amount": eco.get("value"), "basis": eco.get("value_basis") or None,
             "is_estimate": bool(eco.get("value_is_estimate")), "date": vsrc.get("date"),
             "source": vsrc.get("label"), "url": vsrc.get("url")}
    recorded: List[Dict[str, Any]] = []
    tax: List[Dict[str, Any]] = []
    for d in eco.get("known_debt") or []:
        if not isinstance(d, dict):
            continue
        entry = {"kind": d.get("kind"), "amount": d.get("amount"), "as_of": d.get("as_of"),
                 "source": _d(d.get("source")).get("label"), "url": _d(d.get("source")).get("url")}
        if d.get("kind") == "delinquent_tax":
            tax.append(entry)
        elif d.get("kind") == "mortgage_original":
            recorded.append({**entry, "label": "original loan amount on the recorded instrument (not a payoff)"})
        elif d.get("kind") in ("judgment", "lien", "other"):
            recorded.append({**entry, "label": f"recorded {d.get('kind')} (face amount, not a payoff)"})
    stack = _d(_d(raw.get("lien_check")).get("stack"))
    seen = {(round(float(r["amount"]), 2) if r.get("amount") is not None else None) for r in recorded}
    releases: List[Dict[str, Any]] = []
    for lien in stack.get("liens") or []:
        if not isinstance(lien, dict):
            continue
        amount = lien.get("original_amount")
        status = str(lien.get("status") or "")
        if amount is not None and round(float(amount), 2) not in seen:
            recorded.append({"kind": "mortgage_original", "amount": round(float(amount), 2), "as_of": _day(lien.get("recorded")),
                             "source": f"Register of Deeds index, instrument {lien.get('instrument') or 'not given'}", "url": None,
                             "label": "original loan amount on the recorded instrument (not a payoff)"})
        if "release" in status and not status.startswith("no_release"):
            releases.append({"instrument": lien.get("instrument"), "status": status, "as_of": _day(stack.get("as_of"))})
    if releases:
        release_note = "recorded release found"
    elif stack.get("liens"):
        release_note = f"none found in the records retrieved as of {_day(stack.get('as_of')) or 'an unrecorded date'} (partial public index)"
    else:
        release_note = "not searched"
    if not tax:
        detail = _d(raw.get("trustee_detail"))
        if detail.get("total_tax_balance") is not None:
            tax.append({"kind": "delinquent_tax", "amount": detail["total_tax_balance"], "as_of": _day(detail.get("checked_on")),
                        "source": "County Trustee parcel page, total shown", "url": None})
    doc = _d(facts.get("payoff_document")) or _d(raw.get("payoff_document"))
    if doc.get("amount") is not None and _day(doc.get("date")):
        payoff = {"status": "documented", "amount": doc["amount"], "date": _day(doc["date"]), "source": doc.get("source")}
    else:
        payoff = {"status": "unknown", "amount": None, "date": None, "source": None, "resolve_by": PAYOFF_REQUEST}
    return {"value": value, "recorded_debt": recorded, "recorded_releases": releases, "recorded_releases_note": release_note,
            "tax_balance": tax, "payoff": payoff}


def money_text(m: Dict[str, Any]) -> Dict[str, str]:
    """The money fields as five short strings for a spreadsheet row."""
    v = m["value"]
    value = "unknown" if v["amount"] is None else (
        f"{_money(v['amount'])}{' (estimate)' if v['is_estimate'] else ''}; basis: {v['basis'] or 'not stated'}; "
        f"dated {v['date'] or 'not dated'}")
    debt = "; ".join(f"{d['label']}: {_money(d['amount'])}, recorded {d['as_of'] or 'date unknown'}" for d in m["recorded_debt"])
    tax = "; ".join(f"{_money(t['amount'])} as of {t['as_of'] or 'date unknown'}" for t in m["tax_balance"])
    p = m["payoff"]
    payoff = "unknown (no dated payoff document)" if p["status"] == "unknown" else f"{_money(p['amount'])} per payoff document dated {p['date']}"
    return {"value": value, "recorded_debt": debt or "none on file", "recorded_releases": m["recorded_releases_note"],
            "tax_balance": tax or "not read", "payoff": payoff}


def row_holds(row: Dict[str, Any], ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Contact and workflow holds on one staging row. Fields read:

      homeowner_requests_staging.staging_status           rejected
      dialer_lead_workflow (listing_slug = pipeline_lead_key)
          status, next_action, last_contact_at, attempt_count, closed_lost_reason
      dialer_activities.outcome                           do_not_call, not_interested, any contact
      sms_outreach_state.status                           opted_out, paused, active, replied, exhausted
      outreach_suppressions.email                         unsubscribe, bounce, complaint, manual
      homeowner_requests (pipeline_lead_key)              phone_metadata.sale_status, last_contacted_at
      staging phone / phone_metadata.batchdata_skip_trace a phone already bought
      manual facts file                                   do_not_contact, manual_hold, contacted

    kind is one of: do_not_contact, closed, ended_sale, rejected, workflow,
    contacted, manual_hold, traced.
    """
    holds: List[Dict[str, Any]] = []

    def add(kind: str, text: str, since: Any = None) -> None:
        holds.append({"kind": kind, "text": text, "since": _day(since), "id": row.get("id")})

    if row.get("staging_status") == "rejected":
        add("rejected", "The staging record is marked rejected" + (f" ({row['rejection_reason']})" if row.get("rejection_reason") else ""))
    if row.get("mackie_notice_hold"):
        add("ended_sale", f"Trustee notice hold on the record: {row['mackie_notice_hold']}")
    wf = _d(ctx.get("workflow"))
    status = wf.get("status")
    if status in CLOSED_WORKFLOW:
        add("closed", f"CRM workflow is {status.replace('_', ' ')}" + (f" ({wf['closed_lost_reason']})" if wf.get("closed_lost_reason") else ""),
            wf.get("updated_at"))
    elif wf.get("next_action") == "drop":
        add("closed", "CRM workflow next action is Drop", wf.get("updated_at"))
    elif status in OPEN_WORKFLOW:
        add("workflow", f"Already in an active CRM workflow (status {status.replace('_', ' ')})", wf.get("last_contact_at") or wf.get("updated_at"))
    elif wf and (wf.get("last_contact_at") or (wf.get("attempt_count") or 0) > 0):
        add("contacted", f"Already contacted ({wf.get('attempt_count') or 0} attempts logged in the CRM)", wf.get("last_contact_at"))
    for act in ctx.get("activities") or []:
        outcome = _d(act).get("outcome")
        if outcome in DNC_OUTCOMES:
            add("do_not_contact", "Marked do not call in the CRM activity log", act.get("occurred_at"))
        elif outcome in DECLINED_OUTCOMES:
            add("closed", "Owner said not interested (CRM activity log)", act.get("occurred_at"))
        elif outcome and outcome != "note_only" and not any(h["kind"] in ("workflow", "contacted") for h in holds):
            add("contacted", f"Already contacted (CRM activity: {str(outcome).replace('_', ' ')})", act.get("occurred_at"))
    sms = _d(ctx.get("sms"))
    if sms.get("status") == "opted_out":
        add("do_not_contact", "Opted out of text messages (STOP)", sms.get("updated_at"))
    elif sms.get("status") == "paused":
        add("manual_hold", "Text sequence manually paused", sms.get("updated_at"))
    elif sms.get("status"):
        add("contacted", f"Already in a text sequence (status {sms['status']})", sms.get("last_sent_at") or sms.get("enrolled_at"))
    email = str(row.get("email") or "").strip().lower()
    if email and email in (ctx.get("suppressed_emails") or {}):
        add("do_not_contact", f"Email is on the suppression list ({(ctx['suppressed_emails'] or {}).get(email) or 'reason not given'})")
    live = _d(ctx.get("live"))
    flag = _d(_d(live.get("phone_metadata")).get("sale_status") or live.get("sale_status"))
    if flag.get("status") in ENDED_SALE:
        add("ended_sale", f"The live lead is flagged sale {flag['status']}", flag.get("updated_at"))
    if live.get("last_contacted_at"):
        add("contacted", "Already contacted (live lead record)", live.get("last_contacted_at"))
    batch = _d(_d(row.get("phone_metadata")).get("batchdata_skip_trace"))
    if row.get("phone") or batch.get("primary_phone") or live.get("skiptrace_fetched_at"):
        when = batch.get("resolved_at") or live.get("skiptrace_fetched_at")
        dnc = " The primary number is flagged on the do-not-call registry." if batch.get("primary_dnc") else ""
        add("traced", "A phone number is already on file for this record" + (f" (lookup dated {_day(when)})" if _day(when) else "") + "." + dnc, when)
    facts = _d(ctx.get("facts"))
    for kind in ("do_not_contact", "manual_hold", "contacted"):
        if facts.get(kind):
            f = _d(facts[kind]) or {"reason": str(facts[kind])}
            add(kind, {"do_not_contact": "Marked do not contact by hand", "manual_hold": "Manual hold",
                       "contacted": "Already contacted"}[kind] + (f": {f['reason']}" if f.get("reason") else ""), f.get("date"))
    return holds


def _finding(queue: str, code: str, reason: str, fact: str = "", by: str = NOTHING, how: str = "") -> Dict[str, Any]:
    return {"queue": queue, "code": code, "reason": reason, "fact": fact, "resolve_by": by, "resolve_how": how}


def fingerprint(material: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(material, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def material_facts(card: Dict[str, Any], row: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
    """The facts whose change forces a new evaluation: deadline, route, owner,
    parcel, value and holds. Check dates are left out on purpose, so a daily
    reread that finds the same thing does not count as a change; staleness is
    handled by recheck_on instead."""
    eco, dec, ast = _d(card.get("economics")), _d(card.get("decider")), _d(card.get("asset"))
    facts = _d(ctx.get("facts"))
    return {
        "deadline": eco.get("deadline"),
        "route": card.get("route"), "lane": card.get("lane"), "event_status": _d(card.get("event")).get("status"),
        "owner": {"card": sorted(str(n) for n in card.get("owner_names") or []), "decider": dec.get("name"),
                  "verified": bool(dec.get("verified")), "signer": dec.get("signer_tier") or dec.get("signer_verified"),
                  "county": sorted({(r["owner"], _parcel_norm(card.get("county"), r["parcel"])) for r in county_records(row)}),
                  "parties": sorted({p["name"] for p in other_named_parties(row, facts)})},
        "parcel": card.get("property_key") or ast.get("parcel") or ast.get("address"),
        "value": [eco.get("value"), eco.get("value_basis")],
        "hold": {"own": sorted((h["kind"], h["text"]) for h in row_holds(row, ctx)),
                 "group": sorted((h["kind"], h["text"], h.get("id") or "") for h in ctx.get("group_holds") or []),
                 "siblings": sorted((s.get("id") or "", s.get("route") or "") for s in ctx.get("siblings") or []),
                 "holds_read": sorted(ctx.get("unread") or []), "staging_status": row.get("staging_status"),
                 "listing": _d(facts.get("listing")).get("status")},
    }


def check_row(row: Dict[str, Any], ctx: Optional[Dict[str, Any]] = None, today: Optional[date] = None,
              previous: Any = "stored") -> Dict[str, Any]:
    """Evaluate one first-trace staging row.

    row: the staging row (id, county, raw_payload with pilot_review, and the
         phone_metadata, staging_status, phone, email columns when read).
    ctx: workflow, activities, sms, live, suppressed_emails (CRM reads for this
         row), group_holds (holds found on other records of the same owner),
         siblings (other cards for the same owner or parcel), facts (entered
         by hand), unread (names of hold tables that could not be read).
    previous: an earlier result to carry forward; by default the one stored on
         the row. Pass None to force a fresh evaluation.

    Returns the result dict. result["carried_forward"] is true when nothing
    material changed and no time rule came due, in which case the stored
    result is returned untouched apart from that flag.
    """
    ctx = ctx or {}
    today = today or date.today()
    raw = _d(row.get("raw_payload"))
    card = rekey(_d(raw.get("pilot_review")), row.get("county"))
    facts = _d(ctx.get("facts"))
    eco, dec, ev, ast, av = (_d(card.get(k)) for k in ("economics", "decider", "event", "asset", "availability"))
    county = card.get("county") or row.get("county")
    deadline = _day(eco.get("deadline"))
    days_left = runway_days(deadline, today) if deadline else None
    material = material_facts(card, row, ctx)
    fp = fingerprint(material)

    if previous == "stored":
        previous = raw.get(KEY)
    prev = _d(previous)
    urgent = days_left is not None and days_left <= URGENT_RECHECK_DAYS
    due = bool(prev.get("recheck_on")) and today.isoformat() >= prev["recheck_on"]
    if prev and prev.get("version") == VERSION and prev.get("fingerprint") == fp and not urgent and not due:
        return {**prev, "carried_forward": True}

    findings: List[Dict[str, Any]] = []
    recheck: List[date] = []
    if days_left is not None and days_left > URGENT_RECHECK_DAYS:
        recheck.append(date.fromisoformat(deadline) - timedelta(days=URGENT_RECHECK_DAYS))

    # 0. The card itself.
    if card.get("route") != "first_trace_review":
        findings.append(_finding(REMOVE, "not_first_trace", f"The review card is routed {str(card.get('route')).replace('_', ' ')}, not first trace."))

    # 4. Holds: the owner's own record, then other records of the same owner.
    own = row_holds(row, ctx)
    for h in own + [dict(g, other=True) for g in ctx.get("group_holds") or []]:
        where = " on another record of this owner" if h.get("other") else ""
        since = f" since {h['since']}" if h.get("since") and h["kind"] != "traced" else ""
        text = h["text"].rstrip(".") + where + since + "."
        if h["kind"] == "do_not_contact":
            findings.append(_finding(REMOVE, "do_not_contact", text))
        elif h["kind"] in ("closed", "ended_sale", "rejected"):
            if h.get("other") and h["kind"] != "closed":
                continue        # a sale that ended on a different property says nothing about this one
            findings.append(_finding(REMOVE, h["kind"], text))
        elif h["kind"] == "workflow":
            findings.append(_finding(VERIFY, "workflow_hold", text, "Whether the open CRM workflow already covers this owner.",
                                     NOTHING, "Work the existing CRM record. Do not buy a second lookup for an owner already in a workflow."))
        elif h["kind"] == "contacted":
            findings.append(_finding(VERIFY, "already_contacted", text, "What came of the earlier contact.",
                                     NOTHING, "Read the CRM history for this owner before paying for more contact data."))
        elif h["kind"] == "manual_hold":
            findings.append(_finding(VERIFY, "manual_hold", text, "Whether the manual hold still applies.",
                                     NOTHING, "Whoever set the hold clears it in the CRM or the facts file."))
        elif h["kind"] == "traced":
            findings.append(_finding(VERIFY, "already_traced", text, "Whether the number already on file reaches the owner.",
                                     SIGNER_TALK, "Try the number on file first. Buy a new lookup only if it is dead or wrong."))
    if ctx.get("unread"):
        findings.append(_finding(VERIFY, "holds_unread", "Contact and workflow holds could not be read today (" + ", ".join(sorted(ctx["unread"])) +
                                 "), so a hold cannot be ruled out.", "Whether this owner is under a contact or workflow hold.",
                                 NOTHING, "Run the check again when the CRM tables can be read."))

    # 1. Deadline, runway and how fresh the date is.
    checked = _day(ev.get("checked_on"))
    ev_src = _d(ev.get("source"))
    src_name = ev_src.get("label") or "the event source"
    if days_left is not None and days_left < 0:
        findings.append(_finding(REMOVE, "deadline_passed", f"The {eco.get('deadline_kind') or 'deadline'} of {deadline} passed {-days_left} day(s) ago. "
                                 "If the sale was postponed the lane review will card the new date."))
    elif days_left is not None and days_left < MIN_RUNWAY_DAYS:
        findings.append(_finding(REMOVE, "runway_too_short", f"{days_left} day(s) left before {deadline}, under the {MIN_RUNWAY_DAYS}-day minimum to reach "
                                 "a signed auction contract. It comes back if the date is postponed."))
    max_age = DATE_CONFIRM_MAX_AGE_DAYS if deadline else EVENT_CHECK_MAX_AGE_DAYS
    age = _age(checked, today)
    what = f"the {deadline} date" if deadline else "the event"
    if age is None:
        findings.append(_finding(VERIFY, "date_source_undated", f"The card does not say when {what} was last confirmed.",
                                 f"Whether {what} still stands today.", RECORD_CHECK, f"Reread {src_name}."))
    elif age > max_age:
        findings.append(_finding(VERIFY, "date_source_stale", f"{what[0].upper() + what[1:]} was last confirmed {age} days ago ({checked}, {src_name}); "
                                 f"the limit is {max_age} days.", f"Whether {what} still stands today.", RECORD_CHECK,
                                 f"Reread {src_name}" + (f" at {ev_src['url']}" if ev_src.get("url") else "") + "."))
    else:
        recheck.append(date.fromisoformat(checked) + timedelta(days=max_age + 1))

    # 2. Owner and parcel against the county record, and co-owners.
    card_names = [n for n in [*(card.get("owner_names") or []), dec.get("name")] if n]
    card_parcel = ast.get("parcel")
    records = county_records(row)
    exact, off_parcel = [], []
    for r in records:
        agree = parcels_agree(county, card_parcel, r["parcel"])
        (off_parcel if agree is False else exact).append(r)
    dec_date = _day(_d(dec.get("source")).get("date"))
    owner_date = max([r["date"] for r in exact if r["date"]] + ([dec_date] if dec_date else []), default=None)
    latest = [r for r in exact if r["date"] and r["date"] == max(x["date"] for x in exact if x["date"])] if any(r["date"] for r in exact) else exact
    record_how = "Read the current county owner record for the parcel" + (f" ({_d(dec.get('source')).get('url')})" if _d(dec.get("source")).get("url") else "") + "."
    changed = False
    for r in latest:
        missing, matched = names_not_on_card(r["owner"], card_names)
        if matched == 0 and missing:
            changed = True
            newer = not dec_date or (r["date"] or "") >= dec_date
            findings.append(_finding(REMOVE if newer and r["date"] else VERIFY, "owner_changed",
                                     f"The county record ({r['key']}, {r['date'] or 'not dated'}) names \"{r['owner']}\" for this parcel; "
                                     f"the card names \"{dec.get('name')}\".", "Who owns the parcel today.", RECORD_CHECK, record_how))
        elif missing:
            findings.append(_finding(VERIFY, "co_owner_missing", f"The county record ({r['key']}, {r['date'] or 'not dated'}) prints \"{r['owner']}\"; "
                                     f"the card does not name {', '.join(missing)}.", f"Whether {', '.join(missing)} is a living co-owner who must sign.",
                                     RECORD_CHECK, "Read the vesting deed at the Register of Deeds for every owner named, then ask on the first call."))
    if not exact and not dec_date:
        findings.append(_finding(VERIFY, "owner_record_missing", "No dated county owner record for this parcel is on file.",
                                 "The current county owner of record for the exact parcel.", RECORD_CHECK, record_how))
    elif off_parcel and not exact:
        r = off_parcel[0]
        findings.append(_finding(VERIFY, "parcel_mismatch", f"The county record on file ({r['key']}) is for parcel {r['parcel']}; the card is for {card_parcel}.",
                                 "Which parcel the event is on, and its owner.", RECORD_CHECK, record_how))
    elif not changed:
        o_age = _age(owner_date, today)
        if o_age is not None and o_age > OWNER_RECORD_MAX_AGE_DAYS:
            findings.append(_finding(VERIFY, "owner_record_stale", f"The county owner record was last read {o_age} days ago ({owner_date}); the limit is "
                                     f"{OWNER_RECORD_MAX_AGE_DAYS} days.", "The current county owner of record for the exact parcel.", RECORD_CHECK, record_how))
        elif owner_date:
            recheck.append(date.fromisoformat(owner_date) + timedelta(days=OWNER_RECORD_MAX_AGE_DAYS + 1))
    extra: Dict[str, Dict[str, Any]] = {}       # one finding per source, naming everyone it adds
    for p in other_named_parties(row, facts):
        missing, _matched = names_not_on_card(p["name"], card_names)
        if missing:
            slot = extra.setdefault(p["where"], {"names": [], "date": p.get("date")})
            if p["name"] not in slot["names"]:
                slot["names"].append(p["name"])
    on_card = ", ".join(dict.fromkeys(str(n) for n in card.get("owner_names") or [dec.get("name")]))
    for where, slot in extra.items():
        printed = ", ".join(f'"{n}"' for n in slot["names"])
        people = " and ".join(slot["names"]) if len(slot["names"]) < 3 else f"the {len(slot['names'])} other people named"
        findings.append(_finding(VERIFY, "co_owner_missing", f"{where[0].upper() + where[1:]}{' dated ' + slot['date'] if slot['date'] else ''} "
                                 f"prints {printed}; the card names \"{on_card}\" only.",
                                 f"Whether {people} hold an interest and must sign.", RECORD_CHECK,
                                 "Read the vesting deed at the Register of Deeds for every owner named, then ask on the first call."))

    # Company owner with no verified signer: never a person lookup.
    owner_is_company = any(is_entity(n) for n in card.get("owner_names") or []) or is_entity(dec.get("name"))
    signer_verified = (bool(dec.get("verified")) or dec.get("signer_verified") is True
                       or str(dec.get("signer_tier") or "").lower().startswith("verified")) and not is_entity(dec.get("name"))
    if owner_is_company and not signer_verified:
        company = next((n for n in [*(card.get("owner_names") or []), dec.get("name")] if is_entity(n)), dec.get("name"))
        findings.append(_finding(VERIFY, "company_no_signer", f"The owner is a company or trust ({company}) and no person with authority to sign is verified. "
                                 "A person lookup on a company name buys the wrong contact.", f"Who signs for {company}.", RECORD_CHECK,
                                 "Do not run a person lookup. Look the entity up by hand at the Tennessee Secretary of State (sos.tn.gov), read the "
                                 "signature block on its latest recorded deed or deed of trust, then trace the named officer or trustee."))

    # 3. Listing, contract or recent transfer.
    listing = _d(facts.get("listing"))
    l_status = str(listing.get("status") or "").lower()
    listing_status = LISTING_NOT_CHECKED
    if l_status in ("active", "pending", "under_contract", "sold"):
        listing_status = f"{l_status.replace('_', ' ')} per {listing.get('source') or 'a hand check'} ({_day(listing.get('date')) or 'not dated'})"
        findings.append(_finding(REMOVE, "listed_or_contracted", f"The property is {l_status.replace('_', ' ')} per {listing.get('source') or 'a hand check'} "
                                 f"({_day(listing.get('date')) or 'not dated'})."))
    elif l_status in ("none", "not_listed"):
        listing_status = f"not listed per {listing.get('source') or 'a hand check'} ({_day(listing.get('date')) or 'not dated'})"
    for c in av.get("conflicts") or []:
        findings.append(_finding(VERIFY, "availability_conflict", f"The card records a conflict: {c}", "Whether the property is still available.",
                                 RECORD_CHECK, "Resolve the conflict named on the card."))
    sales = sorted({r["last_sale_date"] for r in exact if r.get("last_sale_date")})
    if sales and (_age(sales[-1], today) or 0) <= RECENT_TRANSFER_DAYS and (_age(sales[-1], today) or 0) >= 0:
        findings.append(_finding(VERIFY, "recent_transfer", f"The county record shows a transfer on {sales[-1]}, within the last {RECENT_TRANSFER_DAYS} days.",
                                 "Whether the card's owner still holds title after that transfer.", RECORD_CHECK, record_how))

    # 4b. The same parcel or owner on another card.
    also = []
    for s in ctx.get("siblings") or []:
        tag = f"{s.get('lane')}: {str(s.get('route')).replace('_', ' ')}"
        why = str(s.get("route_reason") or "")
        # A record removed as a duplicate or repeat of this one says nothing against the property.
        repeat = s.get("route") == "removed" and ((len(str(row.get("id") or "")) >= 8 and str(row["id"]) in why) or re.search(r"\bduplicate\b|\brepeat publication\b", why, re.I))
        if s.get("same_property") and s.get("route") in ("removed", "research_hold") and not repeat:
            verb = "removed" if s["route"] == "removed" else "held for research"
            findings.append(_finding(VERIFY, "sibling_card_" + s["route"], f"Another record for this same parcel is {verb} ({s.get('lane')} lane): "
                                     f"{s.get('route_reason') or 'no reason stored'}", "Whether that finding also applies to this record.", RECORD_CHECK,
                                     f"Open the other record ({s.get('crm_link') or s.get('id')}) and settle the difference before spending."))
        also.append(tag + (" (same parcel)" if s.get("same_property") else " (other property)"))

    # 5. Money, as separate fields.
    money = money_fields(card, row, facts)
    if money["value"]["amount"] is None:
        findings.append(_finding(VERIFY, "value_missing", "No sourced value is on the card.", "A sourced value for the exact parcel.", PAID_LOOKUP,
                                 "The free county sources have no value for this parcel; a paid property report would give one."))

    # 6. One queue, one reason, one fact.
    findings.sort(key=lambda f: -_RANK[f["queue"]])
    if findings:
        top = findings[0]
        queue, reason, fact, by, how = top["queue"], top["reason"], top["fact"], top["resolve_by"], top["resolve_how"]
    else:
        queue = TRACE
        reason = (f"{'Deadline ' + deadline + ', ' + str(days_left) + ' days left' if deadline else 'No dated deadline'}; "
                  f"confirmed {checked} from {src_name}; the county record still names the card's owner for this parcel; no contact or workflow hold.")
        fact, by, how = LISTING_FACT, SIGNER_TALK, LISTING_HOW
    result = {
        "version": VERSION, "checked_on": today.isoformat(), "queue": queue, "queue_label": QUEUE_LABEL[queue],
        "reason": reason, "fact_to_verify": fact, "resolve_by": by, "resolve_how": how,
        "fact_kind": FACT_KIND.get(findings[0]["code"], findings[0]["code"]) if findings else "Listing or contract status not checked",
        "lane": card.get("lane"), "deadline": deadline, "days_left": days_left,
        "date_confirmed_on": checked, "date_source": _src_text(ev_src),
        "owner_record_date": owner_date, "listing_status": listing_status,
        "holds": [h["text"] for h in own] + [g["text"] + " (other record of this owner)" for g in ctx.get("group_holds") or []],
        "also_on": sorted(set(also)),
        "money": money,
        "other_findings": [{"queue": f["queue"], "code": f["code"], "reason": f["reason"]} for f in findings[1:]],
        "codes": [f["code"] for f in findings],
        "material": material, "fingerprint": fp,
        "recheck_on": min(recheck).isoformat() if recheck else None,
    }
    return result


def without_run_flags(result: Any) -> Dict[str, Any]:
    """The result as stored: the carried_forward flag belongs to one run."""
    return {k: v for k, v in _d(result).items() if k != "carried_forward"}


def link_groups(rows: List[Dict[str, Any]], all_cards: Iterable[Dict[str, Any]], crm: str = "") -> Dict[str, List[Dict[str, Any]]]:
    """For each first-trace row, the other cards (any lane, any route) that
    share its parcel or one of its owners. all_cards: [{"id", "county",
    "pilot_review"}]. Two first-trace cards for one parcel are the worklist's
    own duplicate handling and are not listed against each other."""
    by_prop: Dict[str, List[Dict[str, Any]]] = {}
    by_owner: Dict[str, List[Dict[str, Any]]] = {}
    for c in all_cards:
        card = c.get("pilot_review")
        if not isinstance(card, dict):
            continue
        card = rekey(card, c.get("county"))
        entry = {"id": c.get("id"), "lane": card.get("lane"), "route": card.get("route"), "route_reason": card.get("route_reason"),
                 "property_key": card.get("property_key"), "crm_link": (crm + str(c.get("id"))) if crm else None}
        if card.get("property_key"):
            by_prop.setdefault(card["property_key"], []).append(entry)
        for part in _owner_parts(card.get("owner_group_key")):
            by_owner.setdefault(part, []).append(entry)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        card = rekey(_d(_d(r.get("raw_payload")).get("pilot_review")), r.get("county"))
        found: Dict[str, Dict[str, Any]] = {}
        for e in by_prop.get(card.get("property_key") or "", []):
            found[e["id"]] = {**e, "same_property": True}
        for part in _owner_parts(card.get("owner_group_key")):
            for e in by_owner.get(part, []):
                found.setdefault(e["id"], {**e, "same_property": bool(e["property_key"]) and e["property_key"] == card.get("property_key")})
        found.pop(r.get("id"), None)
        out[r["id"]] = sorted((e for e in found.values()
                               if not (e["same_property"] and e["route"] == "first_trace_review")), key=lambda e: str(e["id"]))
    return out


def check_all(rows: List[Dict[str, Any]], contexts: Optional[Dict[str, Dict[str, Any]]] = None,
              all_cards: Optional[Iterable[Dict[str, Any]]] = None, today: Optional[date] = None,
              crm: str = "", fresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """Check every first-trace row. contexts maps a staging id to its CRM reads
    (see check_row). Holds found on any record of an owner are applied to every
    row of that owner, across lanes. Returns {staging id: result}."""
    today = today or date.today()
    contexts = {k: dict(v) for k, v in (contexts or {}).items()}
    siblings = link_groups(rows, all_cards if all_cards is not None else
                           [{"id": r.get("id"), "county": r.get("county"), "pilot_review": _d(r.get("raw_payload")).get("pilot_review")} for r in rows], crm)
    owner_of: Dict[str, List[str]] = {}
    for r in rows:
        card = rekey(_d(_d(r.get("raw_payload")).get("pilot_review")), r.get("county"))
        owner_of[r["id"]] = _owner_parts(card.get("owner_group_key"))
    # Holds by owner: from the first-trace rows themselves and from any other
    # record of the owner whose CRM rows were read (contexts keyed by its id).
    holds_by_part: Dict[str, List[Dict[str, Any]]] = {}
    by_id = {r["id"]: r for r in rows}
    for rid, ctx in contexts.items():
        base = by_id.get(rid) or {"id": rid, **_d(ctx.get("row"))}
        parts = owner_of.get(rid) or ctx.get("owner_parts") or []
        for h in row_holds(base, ctx):
            if h["kind"] == "traced":
                continue        # a phone on one record is that record's fact
            for part in parts:
                holds_by_part.setdefault(part, []).append(h)
    for r in rows:
        if r["id"] not in contexts:
            contexts[r["id"]] = {}
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ctx = contexts[r["id"]]
        group = {(h["kind"], h["text"], h["id"]): h for part in owner_of[r["id"]] for h in holds_by_part.get(part, []) if h["id"] != r["id"]}
        ctx = {**ctx, "siblings": siblings.get(r["id"], []), "group_holds": [group[k] for k in sorted(group, key=str)]}
        out[r["id"]] = check_row(r, ctx, today, previous=None if fresh else "stored")
    return out
