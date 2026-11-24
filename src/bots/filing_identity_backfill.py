"""
Backfill for records staged before the filing type and identity fixes.

Preview by default. Nothing is written without --write.

    python -m src.bots.filing_identity_backfill                      # preview, summary and named rows
    python -m src.bots.filing_identity_backfill --report out.json    # preview, every planned change to a file
    python -m src.bots.filing_identity_backfill --steps type,names   # preview some steps only
    python -m src.bots.filing_identity_backfill --write              # apply

Steps (all on by default, each re-reads the stored text with the same code
the scheduled bots now use):

  type           tn_public_notice: the filing type from the notice text. A
                 Chancery tax sale filed under "Foreclosures" becomes TAX_LIEN;
                 text that settles no type becomes NOTICE_RESEARCH with the
                 reason. The newspaper's category is kept as filed_under.
  names          a stored borrower that is deed wording is replaced by the
                 party the parser isolates, or cleared with the reason.
  county         the county of the Register named in the notice replaces the
                 newspaper's county; the newspaper's county is kept.
  sale_date      filled when empty and the text prints one date.
  republications the same notice staged again under a new notice number: one
                 record is kept and lists every publication; the others are
                 rejected with republication_of:<kept id>.
  cross_source   a notice that is the same sale as a record that already has a
                 pilot card gets a card pointing at it (removed, or a research
                 hold when only the borrower company matches).
  lender         nashville_ledger: a stored lender that is a sentence fragment
                 is replaced from the stored notice body, or cleared.
  register       county Register index rows: a lender printed in the grantee
                 column is no longer the substitute trustee.
  corrections    Shelby appointments: an index misspelling proved by the deed
                 of trust and the county bill is recorded beside the printed
                 name (raw_payload.name_corrections). The printed name stays.

Guards. Only pending rows change. A manual pilot card is never replaced
(merge_review). Every column write is compare-and-set on the row's source,
status and the old value of each column it changes, so a row edited since the
plan was made is skipped and counted. Raw fields are merged key by key through
apply_staging_enrichment; the stored excerpt and body are never rewritten.

Idempotent: a second run plans nothing for rows the first run changed.

Order: merge and deploy the bot change before running with --write. The old
tn_public_notice code stages a rejected republication again on its next run.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from . import _notice_filing as nf
from ._pilot_review import _clean, _ENTITY, merge_review
from ._preforeclosure_review import CORRECTION_KEY, name_corrections, same_card
from ._register_index import APPOINTMENT, IndexEntry, register_sources, split_parties

TABLE = "homeowner_requests_staging"
NOTICE_SOURCE = "tn_public_notice"
STEPS = ("type", "names", "county", "sale_date", "republications", "cross_source", "lender", "register", "corrections")
NOTICE_COLUMNS = ("id,bot_source,staging_status,staged_at,pipeline_lead_key,county,distress_type,full_name,"
                  "owner_name_records,property_address,trustee_sale_date,source_url,raw_payload")
CARD_COLUMNS = ("id,bot_source,county,full_name,owner_name_records,trustee_sale_date,staging_status,"
                "pilot_review:raw_payload->pilot_review,structured:raw_payload->structured,"
                "extracted:raw_payload->extracted,deed_of_trust:raw_payload->deed_of_trust,borrowers:raw_payload->borrowers")
LANE_OF_TYPE = {"PRE_FORECLOSURE": "pre_foreclosure", "TAX_LIEN": "tax", nf.RESEARCH: "research hold, no lane"}


# ── Pure planning ───────────────────────────────────────────────────────────


def lane_of(row: Dict[str, Any], *, card: Any = None, status: Optional[str] = None, distress: Optional[str] = None) -> str:
    """Where a record sits: its card's lane and route, else the lane its type feeds."""
    status = status or row.get("staging_status")
    if status == "rejected":
        return "none (rejected)"
    card = card if card is not None else (row.get("raw_payload") or {}).get("pilot_review")
    if isinstance(card, dict) and card.get("lane"):
        return f"{card['lane']} card: {card.get('route')}"
    kind = distress or row.get("distress_type") or ""
    return f"{LANE_OF_TYPE.get(kind, kind.lower() or 'none')} (no card)"


def _change(row: Dict[str, Any], step: str, note: str) -> Dict[str, Any]:
    return {"id": row["id"], "source": row.get("bot_source"), "step": step, "note": note, "columns": {},
            "raw_patch": {}, "guards": {"bot_source": row.get("bot_source"), "staging_status": row.get("staging_status")},
            "before": {"type": row.get("distress_type"), "lane": lane_of(row), "name": row.get("full_name"),
                       "county": row.get("county"), "status": row.get("staging_status")}}


def _set(change: Dict[str, Any], row: Dict[str, Any], column: str, value: Any) -> None:
    if row.get(column) != value:
        change["columns"][column] = value
        change["guards"][column] = row.get(column)


def _finish(change: Dict[str, Any], row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not change["columns"] and not change["raw_patch"]:
        return None
    cols = change["columns"]
    raw = {**(row.get("raw_payload") or {}), **change["raw_patch"]}
    after = {**row, **cols, "raw_payload": raw}
    change["after"] = {"type": after.get("distress_type"), "lane": lane_of(after), "name": after.get("full_name"),
                       "county": after.get("county"), "status": after.get("staging_status")}
    return change


def reread_notice(row: Dict[str, Any], bot) -> Optional[Any]:
    """The lead the current bot builds from a staged notice's stored text, or
    None when it would no longer stage it. "skip" for rows it cannot re-read."""
    from .tn_public_notice_bot import CATEGORY_TO_DISTRESS
    raw = row.get("raw_payload") or {}
    category = CATEGORY_TO_DISTRESS.get(str(raw.get("category_id") or ""))
    if not raw.get("excerpt") or not category or category[0] == "PROBATE":
        return "skip"
    notice_id = str(row.get("source_url") or "").rsplit("ID=", 1)[-1]
    filing = raw.get("filing") if isinstance(raw.get("filing"), dict) else {}
    built = bot._build_lead({"notice_id": notice_id, "paper": raw.get("newspaper") or "",
                             "pub_date": raw.get("publication_date") or "", "city": raw.get("city") or "",
                             "county": filing.get("publication_county") or raw.get("county") or row.get("county") or "",
                             "excerpt": raw["excerpt"]}, category[0], category[1])
    return built


def plan_notice(row: Dict[str, Any], bot, steps: Iterable[str]) -> Optional[Dict[str, Any]]:
    """Type, name, county and sale date for one pending tn_public_notice row."""
    steps = set(steps)
    if row.get("staging_status") != "pending" or (row.get("distress_type") or "") == "PROBATE":
        return None
    raw = row.get("raw_payload") or {}
    lead = reread_notice(row, bot)
    if lead == "skip":
        return None
    change = _change(row, "notice", "")
    notes: List[str] = []
    if lead is None:
        # The bot would not stage this text today. Only a notice that is not a
        # property filing is retyped here; the reviewed county tax rules are
        # left to their own reviews.
        filing = nf.build(raw["excerpt"], category=raw.get("category"), category_id=raw.get("category_id"),
                          publication_county=raw.get("county") or row.get("county"),
                          notice_id=str(row.get("source_url") or "").rsplit("ID=", 1)[-1],
                          publication_date=raw.get("publication_date"), newspaper=raw.get("newspaper"))
        if not filing.get("not_a_filing") or "type" not in steps:
            return None
        filing["hold"] = {"route": "research", "kind": "not_a_filing",
                          "reason": f"Not a property distress filing: {filing['not_a_filing']}"}
        _set(change, row, "distress_type", nf.RESEARCH)
        if row.get("distress_type") != nf.RESEARCH:
            change["raw_patch"]["filed_as"] = row.get("distress_type")
        notes.append(filing["hold"]["reason"])
    else:
        filing = lead.raw_payload["filing"]
        if "type" in steps and lead.distress_type != row.get("distress_type"):
            _set(change, row, "distress_type", lead.distress_type)
            change["raw_patch"]["filed_as"] = raw.get("filed_as") or row.get("distress_type")
            notes.append((filing.get("hold") or {}).get("reason") or f"type from the text: {filing.get('type_basis')}")
        if "names" in steps and lead.distress_type == "PRE_FORECLOSURE":
            names = filing.get("parties") or []
            stored_ok = bool(nf.party_name(row.get("full_name")))
            if row.get("full_name") and not stored_ok:
                _set(change, row, "full_name", lead.full_name)
                notes.append("stored name was deed wording" + ("" if lead.full_name else ": " + (filing.get("party_reason") or "")))
            elif not row.get("full_name") and lead.full_name:
                _set(change, row, "full_name", lead.full_name)
                notes.append("name filled from the text")
            owners = [n for n in str(row.get("owner_name_records") or "").split(";") if n.strip()]
            if (owners and not all(nf.party_name(n) for n in owners)) or (not owners and names):
                _set(change, row, "owner_name_records", lead.owner_name_records)
            if names and raw.get("borrowers") != names:
                change["raw_patch"]["borrowers"] = names
        if "county" in steps and lead.distress_type == "PRE_FORECLOSURE" and filing.get("county_from_text"):
            if (row.get("county") or "").strip().lower() != lead.county:
                _set(change, row, "county", lead.county)
                notes.append(f"county from the {filing.get('county_basis')}; printed in a {filing.get('publication_county')} paper")
        if "sale_date" in steps and lead.trustee_sale_date and not row.get("trustee_sale_date"):
            _set(change, row, "trustee_sale_date", lead.trustee_sale_date)
        if lead.distress_type == nf.RESEARCH and "type" in steps and row.get("property_address"):
            _set(change, row, "property_address", None)
    stored = raw.get("filing") if isinstance(raw.get("filing"), dict) else None
    keep = {k: stored[k] for k in ("publications", "duplicate_of", "same_sale_as") if stored and k in stored}
    filing = {**filing, **keep}
    if stored != filing:
        change["raw_patch"]["filing"] = filing
    change["note"] = "; ".join(n for n in notes if n) or "filing block"
    return _finish(change, row)


def _applied(row: Dict[str, Any], change: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The row as it will read once its planned change is written."""
    if not change:
        return row
    return {**row, **change["columns"], "raw_payload": {**(row.get("raw_payload") or {}), **change["raw_patch"]}}


_STATUS_RANK = {"promoted": 0, "verified": 1, "pending": 2}


def plan_republications(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One record per filing among tn_public_notice foreclosure rows.

    rows: rows as they will read after plan_notice (see _applied). Returns
    changes: the kept record lists every publication; each other pending
    record is rejected with republication_of:<kept id>.
    """
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        filing = (row.get("raw_payload") or {}).get("filing") or {}
        if (row.get("distress_type") == "PRE_FORECLOSURE" and filing.get("key")
                and row.get("staging_status") in _STATUS_RANK and not filing.get("duplicate_of")):
            groups[filing["key"]].append(row)
    out: List[Dict[str, Any]] = []
    for key, group in groups.items():
        if len(group) < 2:
            continue
        group.sort(key=lambda r: (_STATUS_RANK[r["staging_status"]],
                                  not isinstance((r.get("raw_payload") or {}).get("pilot_review"), dict),
                                  not r.get("property_address"), not r.get("trustee_sale_date"),
                                  str(r.get("staged_at") or ""), r["id"]))
        kept, extras = group[0], [r for r in group[1:] if r["staging_status"] == "pending"
                                  and not ((r.get("raw_payload") or {}).get("pilot_review") or {}).get("manual")]
        if not extras:
            continue
        pubs: List[Dict[str, Any]] = []
        for r in [kept] + extras:
            for pub in ((r.get("raw_payload") or {}).get("filing") or {}).get("publications") or []:
                if pub.get("notice_id") not in {p.get("notice_id") for p in pubs}:
                    pubs.append(pub)
        pubs.sort(key=lambda p: (nf._iso_day(p.get("date")) or "", str(p.get("notice_id"))))
        kept_filing = dict((kept.get("raw_payload") or {}).get("filing") or {})
        if kept_filing.get("publications") != pubs:
            change = _change(kept, "republications", f"kept record for {len(pubs)} publications of one notice")
            change["raw_patch"]["filing"] = {**kept_filing, "publications": pubs}
            if kept["staging_status"] == "pending":
                fill = next((r["trustee_sale_date"] for r in extras if r.get("trustee_sale_date")), None)
                if fill and not kept.get("trustee_sale_date"):
                    _set(change, kept, "trustee_sale_date", fill)
            out.append(_finish(change, kept))
        for r in extras:
            change = _change(r, "republications", f"republication of the notice kept on {kept['id']}")
            filing = dict((r.get("raw_payload") or {}).get("filing") or {})
            change["raw_patch"]["filing"] = {**filing, "duplicate_of": kept["id"]}
            _set(change, r, "staging_status", "rejected")
            change["columns"]["rejection_reason"] = f"republication_of:{kept['id']}"
            out.append(_finish(change, r))
    return [c for c in out if c]


def _entity_name(value: Any) -> Optional[str]:
    text = _clean(value)
    return _ENTITY.sub(" ", text).split() and " ".join(_ENTITY.sub(" ", text).split()) if _ENTITY.search(text) else None


def candidate_filing(card_row: Dict[str, Any], body: Optional[str] = None) -> Dict[str, Any]:
    """Filing keys of a record from another source: from its stored notice
    body when it has one, else from its structured fields."""
    county = (card_row.get("pilot_review") or {}).get("county") or card_row.get("county")
    names = card_row.get("borrowers") or [n for n in [card_row.get("full_name")] if n]
    if body:
        ids = nf.identifiers(body)
        place = nf.register_county(body)
        county = place["county"] or county
        names = nf.parties(body)["names"] or names
    else:
        structured = card_row.get("structured") if isinstance(card_row.get("structured"), dict) else {}
        ref = card_row.get("deed_of_trust") if isinstance(card_row.get("deed_of_trust"), dict) else {}
        ids = {"instrument": structured.get("instrument_no") or ref.get("instrument"), "deed_book": ref.get("book"),
               "deed_page": ref.get("page"), "trustee_file": None, "deed_date": nf._date(structured.get("trust_date")),
               "sale_date": card_row.get("trustee_sale_date")}
    return {"keys": nf.filing_keys(str(county or "").lower(), ids, names), "county": county}


def plan_cross_source(rows: List[Dict[str, Any]], cards: List[Dict[str, Any]], bodies: Dict[str, str],
                      today: date) -> List[Dict[str, Any]]:
    """Point a notice at the carded record for the same sale.

    rows: pending tn_public_notice foreclosure rows as they will read, kept
    records only. cards: carded rows of other sources (pre-foreclosure and
    commercial lanes). bodies: notice body by card row id, where stored.
    """
    by_name: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for card_row in cards:
        card = card_row.get("pilot_review") or {}
        if card_row.get("bot_source") == NOTICE_SOURCE or card.get("lane") not in ("pre_foreclosure", "cre"):
            continue
        for name in set((card.get("owner_names") or []) + [card_row.get("full_name")] + (card_row.get("borrowers") or [])):
            if name:
                by_name[_clean(name)].append(card_row)
                if _entity_name(name):
                    by_name["E:" + _entity_name(name)].append(card_row)
    out: List[Dict[str, Any]] = []
    for row in rows:
        raw = row.get("raw_payload") or {}
        filing = raw.get("filing") or {}
        if (row.get("staging_status") != "pending" or row.get("distress_type") != "PRE_FORECLOSURE"
                or filing.get("duplicate_of") or not filing.get("parties")):
            continue
        found: Dict[str, Dict[str, Any]] = {}
        entity_only: Dict[str, Dict[str, Any]] = {}
        for name in filing["parties"]:
            for cand in by_name.get(_clean(name), []) + by_name.get("E:" + (_entity_name(name) or "\0"), []):
                tie = nf.same_filing(filing, candidate_filing(cand, bodies.get(cand["id"])))
                if tie:
                    found.setdefault(cand["id"], {"row": cand, "tie": tie})
                elif _entity_name(name):
                    entity_only.setdefault(cand["id"], {"row": cand, "tie": None})
        picks = found or entity_only
        # One sale has one card to point at. The live card wins over a removed one.
        live = [p for p in picks.values() if (p["row"]["pilot_review"] or {}).get("route") != "removed"] or list(picks.values())
        groups = {(p["row"]["pilot_review"] or {}).get("owner_group_key") for p in live}
        if not live or (not found and len(groups) != 1):
            continue                      # no tie, or a company with several carded sales: nothing is guessed
        pick = sorted(live, key=lambda p: p["row"]["id"])[0]
        card = nf.pointer_card(row, pick["row"], pick["tie"], today)
        existing = raw.get("pilot_review")
        change = _change(row, "cross_source",
                         f"same sale as {pick['row']['id']} ({pick['row'].get('bot_source')}): "
                         + (nf.STRONG_TIES[pick["tie"]["kind"]] if pick["tie"] else "same borrower company only, held"))
        if merge_review(existing, card) is card and not same_card(existing, card):
            change["raw_patch"]["pilot_review"] = card
        link = {"staging_id": pick["row"]["id"], "source": pick["row"].get("bot_source"),
                "tie": pick["tie"]["kind"] if pick["tie"] else "entity_name_only",
                "owner_group_key": card.get("owner_group_key")}
        if filing.get("same_sale_as") != link:
            change["raw_patch"]["filing"] = {**filing, "same_sale_as": link}
        if pick["tie"] and (row.get("county") or "").lower() != card.get("county"):
            _set(change, row, "county", card.get("county"))
        done = _finish(change, row)
        if done:
            out.append(done)
    return out


def plan_lender(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """nashville_ledger: a stored lender that is not a name, re-read from the body."""
    from .nashville_ledger_bot import NashvilleLedgerBot, lender_name
    raw = row.get("raw_payload") or {}
    extracted = raw.get("extracted") if isinstance(raw.get("extracted"), dict) else None
    stored = (extracted or {}).get("lender")
    if not stored or lender_name(stored) or not raw.get("body"):
        return None
    fresh = NashvilleLedgerBot._extract_lender(raw["body"])
    change = _change(row, "lender", f"lender was a sentence fragment ({stored[:60]!r}); now {fresh!r}")
    change["guards"].pop("staging_status")
    change["raw_patch"]["extracted"] = {**extracted, "lender": fresh,
                                        "lender_basis": "re-read from the stored notice body" if fresh
                                        else "no lender name could be separated from the notice body"}
    notes = str(row.get("admin_notes") or "")
    piece = f" · lender={stored}"
    if piece in notes:
        change["columns"]["admin_notes"] = notes.replace(piece, f" · lender={fresh}" if fresh else "")
        change["guards"]["admin_notes"] = notes
    return _finish(change, row)


def plan_register(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Register index appointment: a lender printed in the grantee column."""
    raw = row.get("raw_payload") or {}
    if row.get("distress_type") != APPOINTMENT or raw.get("grantees_as_printed") is not None:
        return None
    printed_grantees = [t for t in (raw.get("substitute_trustees") or []) if t]
    printed_grantors = [n for n in (raw.get("lenders") or []) + (raw.get("borrowers") or []) if n]
    parties = split_parties(IndexEntry(reference=str(raw.get("book_page") or ""), recorded=str(raw.get("recorded") or ""),
                                       doc_type=str(raw.get("doc_type") or ""), first_parties=printed_grantors,
                                       second_parties=printed_grantees))
    if not parties["notes"]:
        return None
    change = _change(row, "register", parties["notes"][0])
    change["guards"].pop("staging_status")
    change["raw_patch"].update({"lenders": parties["lenders"], "substitute_trustees": parties["trustees"],
                                "party_basis": parties["basis"], "party_notes": parties["notes"],
                                "grantees_as_printed": printed_grantees, "grantors_as_printed": printed_grantors})
    notes = str(row.get("admin_notes") or "")
    piece = f"; trustee {', '.join(printed_grantees)}"
    if piece in notes and not parties["trustees"]:
        change["columns"]["admin_notes"] = notes.replace(
            piece, "; no substitute trustee printed (the grantee column repeats the lender)")
        change["guards"]["admin_notes"] = notes
    return _finish(change, row)


def plan_correction(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Shelby appointment: an index misspelling proved by two documents."""
    raw = row.get("raw_payload") or {}
    fixes = name_corrections(raw)
    if not fixes or raw.get(CORRECTION_KEY) == fixes:
        return None
    change = _change(row, "corrections", "; ".join(
        f"index prints {f['printed']}; {f['evidence'][0]['document']} and the county bill print {f['corrected']}" for f in fixes))
    change["guards"].pop("staging_status")
    change["raw_patch"][CORRECTION_KEY] = fixes
    done = _finish(change, row)
    if done:
        done["after"]["name"] = f"{row.get('full_name')} (printed, kept); corrected value {fixes[0]['corrected']} stored beside it"
    return done


def plan_all(notices: List[Dict[str, Any]], cards: List[Dict[str, Any]], bodies: Dict[str, str],
             ledger: List[Dict[str, Any]], register: List[Dict[str, Any]], appointments: List[Dict[str, Any]],
             bot, today: date, steps: Iterable[str] = STEPS) -> List[Dict[str, Any]]:
    steps = set(steps)
    changes: List[Dict[str, Any]] = []
    first: Dict[str, Dict[str, Any]] = {}
    if steps & {"type", "names", "county", "sale_date", "republications", "cross_source"}:
        for row in notices:
            change = plan_notice(row, bot, steps)
            if change:
                first[row["id"]] = change
        after = [_applied(r, first.get(r["id"])) for r in notices]
        second = plan_republications(after) if "republications" in steps else []
        merged_away = {c["id"] for c in second if c["columns"].get("staging_status") == "rejected"}
        by_id = {c["id"]: c for c in second}
        after = [_applied(r, by_id.get(r["id"])) for r in after if r["id"] not in merged_away]
        third = plan_cross_source(after, cards, bodies, today) if "cross_source" in steps else []
        changes += _fold(notices, [first, by_id, {c["id"]: c for c in third}])
    if "lender" in steps:
        changes += [c for c in (plan_lender(r) for r in ledger) if c]
    if "register" in steps:
        changes += [c for c in (plan_register(r) for r in register) if c]
    if "corrections" in steps:
        changes += [c for c in (plan_correction(r) for r in appointments) if c]
    return changes


def _fold(rows: List[Dict[str, Any]], passes: List[Dict[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """One change per row from the successive passes, guarded on the stored row."""
    out = []
    for row in rows:
        parts = [p[row["id"]] for p in passes if row["id"] in p]
        if not parts:
            continue
        change = _change(row, "+".join(dict.fromkeys(s for p in parts for s in p["step"].split("+"))),
                         "; ".join(p["note"] for p in parts if p["note"]))
        for part in parts:
            change["raw_patch"].update(part["raw_patch"])
            for column, value in part["columns"].items():
                change["columns"][column] = value
                if column != "rejection_reason":
                    change["guards"].setdefault(column, row.get(column))
        change["columns"] = {k: v for k, v in change["columns"].items() if row.get(k) != v}
        done = _finish(change, row)
        if done:
            out.append(done)
    return out


# ── Reads and writes ────────────────────────────────────────────────────────


def _page(build, size: int = 300) -> List[Dict[str, Any]]:
    rows, last = [], None
    while True:
        query = build()
        if last:
            query = query.gt("id", last)
        page = query.order("id").limit(size).execute().data or []
        rows.extend(page)
        if len(page) < size:
            return rows
        last = page[-1]["id"]


def read_all(client, steps: Iterable[str]) -> Dict[str, Any]:
    steps = set(steps)
    data: Dict[str, Any] = {"notices": [], "cards": [], "bodies": {}, "ledger": [], "register": [], "appointments": []}
    if steps & {"type", "names", "county", "sale_date", "republications", "cross_source"}:
        data["notices"] = _page(lambda: client.table(TABLE).select(NOTICE_COLUMNS).eq("bot_source", NOTICE_SOURCE)
                                .neq("distress_type", "PROBATE").in_("staging_status", ["pending", "verified", "promoted"]))
    if "cross_source" in steps:
        for route in ("first_trace_review", "professional_relationship", "research_hold", "removed"):
            data["cards"] += _page(lambda: client.table(TABLE).select(CARD_COLUMNS)
                                   .eq("raw_payload->pilot_review->>route", route))
        wanted = [c["id"] for c in data["cards"] if c.get("bot_source") == "nashville_ledger"]
        for start in range(0, len(wanted), 50):
            for r in (client.table(TABLE).select("id,body:raw_payload->>body").eq("bot_source", "nashville_ledger")
                      .in_("id", wanted[start:start + 50]).execute().data or []):
                if r.get("body"):
                    data["bodies"][r["id"]] = r["body"]
    if "lender" in steps:
        data["ledger"] = _page(lambda: client.table(TABLE).select(
            "id,bot_source,staging_status,distress_type,county,full_name,admin_notes,raw_payload")
            .eq("bot_source", "nashville_ledger").not_.is_("raw_payload->extracted->>lender", "null"), size=100)
    if "register" in steps:
        data["register"] = _page(lambda: client.table(TABLE).select(
            "id,bot_source,staging_status,distress_type,county,full_name,admin_notes,raw_payload")
            .in_("bot_source", register_sources()).eq("distress_type", APPOINTMENT))
    if "corrections" in steps:
        data["appointments"] = _page(lambda: client.table(TABLE).select(
            "id,bot_source,staging_status,distress_type,county,full_name,raw_payload")
            .eq("bot_source", "shelby_substitute_trustee").in_("staging_status", ["pending", "promoted"]))
    return data


def apply(client, changes: List[Dict[str, Any]]) -> Counter:
    """Column writes are compare-and-set; raw fields merge key by key. A row
    whose guarded values moved since the plan is skipped whole."""
    counts: Counter = Counter()
    for change in changes:
        if change["columns"]:
            query = client.table(TABLE).update(change["columns"]).eq("id", change["id"])
            for column, value in change["guards"].items():
                query = query.is_(column, "null") if value is None else query.eq(column, value)
            if not query.execute().data:
                counts["skipped_changed_since_plan"] += 1
                continue
            counts["column_rows"] += 1
        if change["raw_patch"]:
            written = client.rpc("apply_staging_enrichment",
                                 {"p_rows": [{"id": change["id"], "raw_patch": change["raw_patch"]}]}).execute().data
            counts["raw_rows" if written == 1 else "raw_write_failed"] += 1
    return counts


def line(change: Dict[str, Any]) -> str:
    b, a = change["before"], change["after"]

    def moved(key: str) -> str:
        return f"{b[key]!r}" if b[key] == a[key] else f"{b[key]!r} -> {a[key]!r}"

    return (f"{change['id']}  {change['source']}  [{change['step']}]\n"
            f"    type   {moved('type')}\n    lane   {moved('lane')}\n    name   {moved('name')}\n"
            f"    county {moved('county')}   status {moved('status')}\n    why    {change['note']}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--write", action="store_true", help="apply the plan (default: preview only)")
    parser.add_argument("--steps", default=",".join(STEPS), help="comma separated: " + ", ".join(STEPS))
    parser.add_argument("--report", help="write every planned change to this JSON file")
    parser.add_argument("--show", type=int, default=5, help="rows printed per step in the preview")
    parser.add_argument("--ids", default="", help="comma separated id prefixes always printed in full")
    parser.add_argument("--env", help="path to the .env file (default: .env in the working directory)")
    args = parser.parse_args(argv)
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = sorted(set(steps) - set(STEPS))
    if unknown:
        parser.error(f"unknown step(s): {', '.join(unknown)}")

    from dotenv import load_dotenv
    load_dotenv(args.env) if args.env else load_dotenv()
    from ._base import _supabase
    from .tn_public_notice_bot import TnPublicNoticeBot
    client = _supabase()
    if client is None:
        print("No database credentials; nothing read.")
        return 2
    today = datetime.now(timezone.utc).date()
    data = read_all(client, steps)
    changes = plan_all(data["notices"], data["cards"], data["bodies"], data["ledger"], data["register"],
                       data["appointments"], TnPublicNoticeBot(), today, steps)
    print(f"{'WRITE' if args.write else 'PREVIEW (no writes)'}: read {len(data['notices'])} notices, {len(data['cards'])} "
          f"cards, {len(data['ledger'])} ledger rows, {len(data['register'])} register rows, "
          f"{len(data['appointments'])} appointments; {len(changes)} rows to change")
    by_step: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for change in changes:
        by_step[change["step"]].append(change)
    moves = Counter((c["before"]["type"], c["after"]["type"]) for c in changes if c["before"]["type"] != c["after"]["type"])
    for (old, new), count in sorted(moves.items(), key=lambda kv: -kv[1]):
        print(f"  type {old} -> {new}: {count}")
    print(f"  names changed: {sum(1 for c in changes if c['before']['name'] != c['after']['name'])}; "
          f"county changed: {sum(1 for c in changes if c['before']['county'] != c['after']['county'])}; "
          f"rejected as republications: {sum(1 for c in changes if c['after']['status'] == 'rejected' != c['before']['status'])}; "
          f"cards written: {sum(1 for c in changes if 'pilot_review' in c['raw_patch'])}")
    wanted = tuple(p.strip() for p in args.ids.split(",") if p.strip())
    for step, group in sorted(by_step.items()):
        print(f"\n== {step}: {len(group)} rows ==")
        for change in group[:args.show]:
            print(line(change))
    if wanted:
        print("\n== requested rows ==")
        for change in changes:
            if change["id"].startswith(wanted):
                print(line(change))
    if args.report:
        with open(args.report, "w", encoding="utf-8") as handle:
            json.dump(changes, handle, indent=1, default=str)
        print(f"\nfull plan: {args.report}")
    if not args.write:
        print("\nPreview only. Re-run with --write to apply.")
        return 0
    counts = apply(client, changes)
    print("\nwritten:", dict(counts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
