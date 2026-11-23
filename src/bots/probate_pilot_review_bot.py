"""
Probate pilot review: one pilot_review card per estate with a matched parcel.

Runs after court_notices and probate_property_enricher. For each Tennessee
estate whose full notice names a fiduciary (or that carries recorded court
evidence), it rechecks the county roll for the matched parcel and writes
raw_payload.pilot_review (lane "probate") through the shared card contract:

  - decedent still the sole owner of record, tie confirmed, value sourced,
    no known listing or transfer        -> first_trace_review
  - institutional or attorney fiduciary -> professional_relationship
  - co-owner, trust or life estate on the roll, name-only tie, pending
    petition, broker listing, identity conflict, unclear signer, county roll
    not available                       -> research_hold, with the next check
  - parcel now in someone else's name, estate closed, repeat publication
                                        -> removed

Cards are rechecked every RECHECK_DAYS so a later transfer flips an existing
card. A manual card is never replaced (merge_review), rows a person has
already decided (any status but pending) are not touched, and a failed county
fetch leaves the existing card alone.

Current-owner rechecks are wired for Davidson (Assessor record card). Other
counties are held with that gap named until a free source is wired.

Distress type: N/A (reviewer; writes raw_payload only).
"""

from __future__ import annotations

import hashlib
import json
import traceback as tb
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from ._base import BotBase, _supabase
from . import _probate_control as control
from ._pilot_review import merge_review
from ._probate_staging import guard_raw

RECHECK_DAYS = 7
RECORDED_EVIDENCE_DAYS = 30
SOURCES = ("court_notices", "tn_public_notice")
STAGES = ("appointment_reported", "sale_authority_requested", "estate_inactive_reported")
COLUMNS = ("id,bot_source,distress_type,county,full_name,property_address,property_value,"
           "source_url,staging_status,raw_payload")
NO_ROLL_NOTE = {
    "montgomery": "Montgomery County GIS now requires a token and the county property search refuses "
                  "automated requests",
}


def _raw(row: Dict[str, Any]) -> Dict[str, Any]:
    return row["raw_payload"] if isinstance(row.get("raw_payload"), dict) else {}


def is_tennessee(row: Dict[str, Any]) -> bool:
    return str(_raw(row).get("state") or "TN").upper() in ("TN", "TENNESSEE")


def estate_group_key(row: Dict[str, Any]) -> Optional[str]:
    """Repeat publications of one estate share this key: court county and
    decedent name. Older excerpt rows print no docket, so the docket cannot
    be the key."""
    f = control.estate_facts(row)
    name = control._norm(f["decedent"])
    if not f["county"] or not name:
        return None
    return f"{f['county']}|{name}"


def _rank(row: Dict[str, Any]) -> tuple:
    f = control.estate_facts(row)
    return (bool(_raw(row).get(control.EVIDENCE_KEY)),
            bool(f["representatives"]),
            row.get("bot_source") == "court_notices",
            bool(control.estate_parcels(row)),
            (f["publication_date"] or date.min).isoformat(),
            str(row.get("id")))


def fingerprint(row: Dict[str, Any]) -> str:
    """Changes when the evidence a card was built from changes, so a new
    notice, parcel match or recorded court fact is reviewed without waiting
    for the weekly recheck."""
    raw = _raw(row)
    parts = {
        "notice": raw.get("notice_id"), "published": raw.get("publication_date"),
        "review": raw.get("probate_review"), "evidence": raw.get(control.EVIDENCE_KEY),
        "parcels": [(p.get("parcel"), p.get("owner")) for p in control.estate_parcels(row)],
    }
    return hashlib.sha1(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()[:16]


def needs_review(row: Dict[str, Any], today: date, role: str) -> bool:
    raw = _raw(row)
    card = raw.get("pilot_review")
    if not isinstance(card, dict):
        return True
    if card.get("manual"):
        return False
    check = raw.get(control.CHECK_KEY) if isinstance(raw.get(control.CHECK_KEY), dict) else {}
    if card.get("reviewed_by") != control.REVIEWER:
        # Another reviewer's card: look again weekly, merge_review decides.
        last = control.parse_date(str(check.get("checked_on") or "")[:10])
        return last is None or (today - last).days >= RECHECK_DAYS
    if check.get("fingerprint") != fingerprint(row) or check.get("role") != role:
        return True
    last = control.parse_date(str(check.get("checked_on") or "")[:10])
    return last is None or (today - last).days >= RECHECK_DAYS


class ProbatePilotReviewBot(BotBase):
    name = "probate_pilot_review"
    description = "Probate lane review card: estate control of a specific, available parcel"
    throttle_seconds = 1.5
    expected_min_yield = 0
    max_roll_checks = 60

    def scrape(self) -> List[Any]:
        return []

    # ── County roll ───────────────────────────────────────────────────────

    def roll_for(self, county: str, parcel: Dict[str, Any], today: date) -> Optional[Dict[str, Any]]:
        """Today's county record for one parcel. None means the fetch failed
        and the estate should be left as it is until the next run."""
        if parcel.get("recorded"):
            checked = control.parse_date(parcel.get("checked_on"))
            if checked and (today - checked).days <= RECORDED_EVIDENCE_DAYS:
                return {"status": "recorded"}
            return {"status": "unavailable",
                    "note": f"the recorded county check of {parcel.get('checked_on')} is more than "
                            f"{RECORDED_EVIDENCE_DAYS} days old"}
        if county == "davidson":
            if not parcel.get("account_id"):
                return {"status": "unavailable", "note": "the staged match has no assessor account number"}
            url = control.PADCTN_CARD.format(account=parcel["account_id"])
            res = self.fetch(url)
            if res is None or res.status_code != 200:
                return None
            card = control.parse_padctn_card(res.text)
            if not card.get("owner") or control._norm(card.get("parcel")) != control._norm(parcel.get("parcel")):
                return {"status": "unavailable", "note": "the assessor record card did not match the staged parcel"}
            return {"status": "ok", "owner": card["owner"], "parcel": card["parcel"],
                    "address": parcel.get("address") or card.get("address"), "mailing": card.get("mailing"),
                    "value": card.get("appraisal") or parcel.get("value"),
                    "value_basis": f"Davidson assessor {card.get('assessment_year') or ''} total appraisal".replace("  ", " "),
                    "last_sale_date": card.get("last_sale_date"), "last_sale_price": card.get("last_sale_price"),
                    "deed_reference": card.get("deed_reference"), "source_url": url,
                    "checked_on": today.isoformat()}
        return {"status": "unavailable",
                "note": NO_ROLL_NOTE.get(county, f"no free current-owner check is wired for {county.title()} County")}

    # ── Planning (no writes) ──────────────────────────────────────────────

    def plan(self, rows: List[Dict[str, Any]], today: date,
             roll_for: Optional[Callable[[str, Dict[str, Any], date], Optional[Dict[str, Any]]]] = None,
             only_ids: Optional[set] = None) -> List[Dict[str, Any]]:
        """Decide every card for these rows. Returns one entry per row that
        would change: {id, role, primary_id, before, after, raw_before, raw_after}."""
        roll_for = roll_for or self.roll_for
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            if row.get("staging_status") != "pending" or not is_tennessee(row):
                continue
            key = estate_group_key(row)
            if key:
                groups.setdefault(key, []).append(row)

        # One relationship per professional across every estate in view.
        professionals: Dict[str, List[str]] = {}
        counsel: Dict[str, List[str]] = {}
        primaries: Dict[str, Dict[str, Any]] = {}
        for key, members in groups.items():
            primary = max(members, key=_rank)
            primaries[key] = primary
            f = control.estate_facts(primary)
            label = f"{f['docket'] or 'no docket'} ({f['decedent']})"
            pro = control.professional_of(f)
            if pro:
                professionals.setdefault(pro["key"], []).append(label)
            for attorney in f["attorneys"]:
                counsel.setdefault(control.professional_key(attorney), []).append(label)

        out: List[Dict[str, Any]] = []
        checks = 0
        ordered = sorted(groups, key=lambda k: (isinstance(_raw(primaries[k]).get("pilot_review"), dict), k))
        for key in ordered:
            primary = primaries[key]
            members = groups[key]
            if only_ids is not None and not ({m["id"] for m in members} & only_ids):
                continue
            f = control.estate_facts(primary)
            if not (f["representatives"] or f["stage"] in STAGES or f["evidence"]):
                continue  # excerpt-only estate: no fiduciary is named, nothing to review
            if needs_review(primary, today, "primary"):
                parcels = control.estate_parcels(primary)
                roll = None
                gate_open = (f["representatives"] and f["stage"] not in ("sale_authority_requested",
                                                                         "estate_inactive_reported")
                             and "conflicting_decedent_names" not in f["hold_reasons"])
                if parcels and gate_open:
                    if checks >= self.max_roll_checks:
                        continue
                    checks += 1
                    roll = roll_for(f["county"], parcels[0], today)
                    if roll is None:
                        continue  # county source failed: keep whatever card is there
                    if roll.get("status") == "recorded":
                        roll = None
                pro = control.professional_of(f)
                new = control.review_estate(primary, roll, today,
                                            professional_estates=professionals.get(pro["key"]) if pro else None)
                if new is not None:
                    check = {"checked_on": today.isoformat(), "fingerprint": fingerprint(primary), "role": "primary",
                             "roll": roll, "duplicate_rows": sorted(m["id"] for m in members if m is not primary),
                             "counsel": [{"name": a, "key": control.professional_key(a),
                                          "estates": counsel.get(control.professional_key(a), [])}
                                         for a in f["attorneys"]]}
                    self._queue(out, primary, new, check, "primary", primary["id"])
            for member in members:
                if member is primary or not needs_review(member, today, "duplicate"):
                    continue
                if not isinstance(_raw(primary).get("pilot_review"), dict) and not any(
                        o["id"] == primary["id"] for o in out):
                    continue  # no card on the primary yet: nothing to point at
                check = {"checked_on": today.isoformat(), "fingerprint": fingerprint(member), "role": "duplicate",
                         "primary_row": primary["id"]}
                self._queue(out, member, control.duplicate_card(member, primary["id"], today), check,
                            "duplicate", primary["id"])
        if only_ids is not None:
            out = [o for o in out if o["id"] in only_ids]
        return out

    @staticmethod
    def _queue(out, row, new, check, role, primary_id) -> None:
        raw = _raw(row)
        existing = raw.get("pilot_review")
        merged = merge_review(existing, new)
        after = {**raw, "pilot_review": merged, control.CHECK_KEY: check}
        out.append({"id": row["id"], "role": role, "primary_id": primary_id,
                    "before": existing, "after": merged, "kept_existing": merged is existing,
                    "raw_before": row.get("raw_payload"), "raw_after": after})

    # ── Writing ───────────────────────────────────────────────────────────

    def apply(self, client, plan: List[Dict[str, Any]]) -> List[str]:
        """Guarded writes: the row must still be pending with the exact
        raw_payload the plan read. Returns the ids actually written."""
        written: List[str] = []
        for item in plan:
            try:
                query = (client.table("homeowner_requests_staging").update({"raw_payload": item["raw_after"]})
                         .eq("id", item["id"]).eq("staging_status", "pending"))
                if guard_raw(query, item["raw_before"]).execute().data:
                    written.append(item["id"])
            except Exception as exc:
                self.logger.warning("pilot review write deferred for %s (%s)", item["id"], type(exc).__name__)
        return written

    def load_rows(self, client) -> List[Dict[str, Any]]:
        """Estates with a full notice or recorded court evidence, plus the
        other pending notices for the same decedents (repeat publications)."""
        table = "homeowner_requests_staging"
        rows: Dict[str, Dict[str, Any]] = {}
        stage_filter = ("raw_payload->probate_review->>stage.in.(" + ",".join(STAGES) + "),"
                        f"raw_payload->{control.EVIDENCE_KEY}.not.is.null")
        for offset in range(0, 5000, 500):
            page = (client.table(table).select(COLUMNS).in_("distress_type", ["PROBATE", "COURT_SALE"])
                    .in_("bot_source", list(SOURCES)).eq("staging_status", "pending").or_(stage_filter)
                    .order("id").range(offset, offset + 499).execute().data or [])
            for row in page:
                rows[row["id"]] = row
            if len(page) < 500:
                break
        wanted = {estate_group_key(r) for r in rows.values() if is_tennessee(r)} - {None}
        counties = sorted({k.split("|", 1)[0] for k in wanted})
        extra: List[str] = []
        for county in counties:
            for offset in range(0, 5000, 1000):
                page = (client.table(table).select("id,county,full_name").eq("distress_type", "PROBATE")
                        .in_("bot_source", list(SOURCES)).eq("staging_status", "pending")
                        .ilike("county", county).not_.is_("property_address", "null")
                        .order("id").range(offset, offset + 999).execute().data or [])
                for light in page:
                    key = f"{county}|{control._norm(light.get('full_name'))}"
                    if light["id"] not in rows and key in wanted:
                        extra.append(light["id"])
                if len(page) < 1000:
                    break
        for offset in range(0, len(extra), 50):
            for row in (client.table(table).select(COLUMNS).in_("id", extra[offset:offset + 50]).execute().data or []):
                rows[row["id"]] = row
        return list(rows.values())

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            self._report_health(status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                                error_message="no_supabase_client")
            return {"name": self.name, "status": "no_supabase", "staged": 0, "duplicates": 0, "fetched": 0}
        rows: List[Dict[str, Any]] = []
        plan: List[Dict[str, Any]] = []
        written: List[str] = []
        error: Optional[str] = None
        try:
            rows = self.load_rows(client)
            plan = self.plan(rows, started.date())
            written = self.apply(client, plan)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {exc}")
        routes: Dict[str, int] = {}
        for item in plan:
            if item["id"] in written:
                routes[item["after"].get("route")] = routes.get(item["after"].get("route"), 0) + 1
        status = "failed" if error else ("ok" if written else "all_dupes" if rows else "zero_yield")
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=len(rows), parsed_count=len(plan), staged_count=len(written),
                            duplicate_count=len(plan) - len(written), error_message=error, notes={"routes": routes})
        self.logger.info(f"rows={len(rows)} planned={len(plan)} written={len(written)} routes={routes}")
        return {"name": self.name, "status": status, "fetched": len(rows), "planned": len(plan),
                "staged": len(written), "duplicates": len(plan) - len(written), "routes": routes, "error": error}


def run() -> dict:
    return ProbatePilotReviewBot().run()


if __name__ == "__main__":
    print(run())
