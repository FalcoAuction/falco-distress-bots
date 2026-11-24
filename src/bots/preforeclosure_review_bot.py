"""
Pre-foreclosure lane review, run after the trustee sources and value bots.

Each run, over records already in staging (it collects nothing new):

  1. Shelby appointments of substitute trustee that have a street and parcel
     but no county check yet get one bounded lookup on the Shelby County
     Trustee's free realty bill search: who is billed for that exact parcel,
     and its assessment. That is the current-owner check.
  2. Every appointment is compared with the published Memphis Daily News
     notices. A notice that cites the appointment instrument, or forecloses
     the same deed of trust instrument, is linked and the days between the
     recording and the first advertisement are stored. A notice naming a
     different parcel is stored as a conflict, never as a link.
  3. Each appointment and each advertised sale (Memphis Daily News, Mackie
     Wolf, Brock & Scott) gets the shared pilot_review card. A postponement,
     a withdrawal from the trustee's list or a passed sale date rewrites the
     card on the same record; nothing is inserted.

Writes go through apply_staging_enrichment and touch only this bot's own
raw_payload keys. A card is rewritten only when its content changes, so a
second run over the same records writes nothing.

Access: the Trustee search is the same free endpoint memphis_county_value
uses, at the same 1.5 second spacing and with a per-run cap. No Register of
Deeds request is made here.

Distress type: N/A (reviewer only).
"""

from __future__ import annotations

import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dateutil.tz import gettz

from . import rubin_lublin_list
from ._base import BotBase, BotTimeout, _supabase, guard_phone_metadata
from ._owner_lookup import LegacyTLSAdapter
from ._pilot_review import merge_review
from ._preforeclosure_review import (COUNTY_KEY, EVIDENCE_KEY, LINK_KEY, REVIEWER, SALE_SOURCES, TRACK_KEY, _stamp,
                                     appointment_card, link_notice, notice_identity, sale_card, sale_state,
                                     same_card, track_of)
from ._preforeclosure_review import CORRECTION_KEY, name_corrections, shelby_parcel
from ._register_index_review import review_register_index
from ._shelby_trustee_value import HOST, lookup
from ._trustee_list_match import LIST_KEY, all_staged_books, check_notices, effective_date, page_book

TABLE = "homeowner_requests_staging"
APPOINTMENT_SOURCE = "shelby_substitute_trustee"
SALE_COLUMNS = (
    "id,bot_source,county,property_address,full_name,owner_name_records,property_value,property_value_source,"
    "trustee_sale_date,source_url,staging_status,pipeline_lead_key,"
    "notice_tracking:phone_metadata->notice_tracking,sale_status:phone_metadata->sale_status,"
    "trustee_review:phone_metadata->shelby_trustee_value_review,"
    "notice_id:raw_payload->structured->>tdn_no,loan_instrument:raw_payload->structured->>instrument_no,"
    "first_notice_date:raw_payload->structured->>date_of_first_public_notice,"
    "firm:raw_payload->structured->>substitute_trustee,publication_date:raw_payload->>publication_date,"
    "cited_appointment:raw_payload->substitute_trustee_appointment,notice_parcel:raw_payload->extracted->>parcel,"
    "calendar:raw_payload->memphis_trustee_calendar,value_estimate:raw_payload->value_estimate,"
    "comptroller:raw_payload->comptroller_parcel,borrowers:raw_payload->borrowers,"
    f"list_check:raw_payload->{LIST_KEY},"
    f"seed:raw_payload->{EVIDENCE_KEY},track:raw_payload->{TRACK_KEY},card:raw_payload->pilot_review"
)
APPOINTMENT_COLUMNS = ("id,pipeline_lead_key,property_address,source_url,property_value,property_value_source,"
                       "owner_name_records,staging_status,staged_at,raw_payload")


def county_check(bot, row: Dict[str, Any], today: date) -> Optional[Dict[str, Any]]:
    """One Trustee lookup for an appointment's street and parcel. Returns the
    evidence to store, or None when the source was unavailable (retry later)."""
    raw = row["raw_payload"]
    result = lookup(bot, {"property_address": row["property_address"], "pipeline_lead_key": row["pipeline_lead_key"],
                          "raw_payload": {"extracted": {"parcel": shelby_parcel(raw.get("parcel_id")) or raw.get("parcel_id")}}},
                    as_of=today)
    if result.get("status") == "source_unavailable":
        return None
    evidence = {"appointment_instrument": raw.get("instrument"), "checked_on": today.isoformat(),
                "status": result["status"], "search_url": result.get("search_url"),
                "basis": "Register deed of trust parcel and street matched to the current county realty bill."}
    if result.get("owner"):
        evidence.update(roll_owner=result["owner"], parcel_id=result.get("parcel_id"),
                        county_location=result.get("location"), county_source=result.get("source_url"),
                        tax_year=result.get("tax_year"), match_method=result.get("match_method"))
    for key in ("assessed_value", "assessment_ratio", "estimated_appraisal", "tax_class", "candidate_parcels"):
        if result.get(key) is not None:
            evidence[key] = result[key]
    return evidence


def merge_county(existing: Any, fresh: Dict[str, Any]) -> Dict[str, Any]:
    """Keep an earlier verified owner and parcel. Add only what it lacks, and
    record a disagreement instead of replacing it."""
    if not isinstance(existing, dict) or not existing.get("roll_owner"):
        return fresh
    same = (fresh.get("roll_owner") == existing.get("roll_owner")
            and shelby_parcel(fresh.get("parcel_id")) == shelby_parcel(existing.get("parcel_id")))
    if not same:
        return {**existing, "recheck": {k: fresh.get(k) for k in ("checked_on", "status", "roll_owner", "parcel_id")}}
    return {**fresh, **existing, "value_checked_on": fresh["checked_on"],
            **{k: fresh[k] for k in ("assessed_value", "assessment_ratio", "estimated_appraisal", "tax_class", "status")
               if k in fresh and existing.get(k) is None}}


class PreforeclosureReviewBot(BotBase):
    name = REVIEWER
    description = "Pre-foreclosure lane: owner check, appointment-to-notice link and review cards"
    throttle_seconds = 1.5
    expected_min_yield = 0
    frozen_after_runs = 10_000
    max_owner_checks = 12
    recheck_days = 7
    sale_lookback_days = 30
    appointment_lookback_days = 400

    def __init__(self):
        super().__init__()
        self.session.mount(HOST + "/", LegacyTLSAdapter(max_retries=0))

    def scrape(self) -> List[Any]:
        return []

    # ── Reads ───────────────────────────────────────────────────────────────

    def _page(self, build) -> List[Dict[str, Any]]:
        rows, last = [], None
        while True:
            query = build()
            if last:
                query = query.gt("id", last)
            page = query.order("id").limit(200).execute().data or []
            rows.extend(page)
            if len(page) < 200:
                return rows
            last = page[-1]["id"]

    def _appointments(self, client, today: date, only: Optional[set]) -> List[Dict[str, Any]]:
        cutoff = (today - timedelta(days=self.appointment_lookback_days)).isoformat()
        rows = self._page(lambda: client.table(TABLE).select(APPOINTMENT_COLUMNS).eq("bot_source", APPOINTMENT_SOURCE)
                          .in_("staging_status", ["pending", "promoted"]).gte("staged_at", cutoff))
        return [r for r in rows if isinstance(r.get("raw_payload"), dict) and (only is None or r["id"] in only)]

    def _notices(self, client, today: date) -> List[Dict[str, Any]]:
        cutoff = (today - timedelta(days=self.appointment_lookback_days)).isoformat()
        return self._page(lambda: client.table(TABLE).select(SALE_COLUMNS).eq("bot_source", "memphis_daily_news")
                          .ilike("county", "shelby").in_("staging_status", ["pending", "promoted"]).gte("staged_at", cutoff))

    def _sales(self, client, today: date, source: str) -> List[Dict[str, Any]]:
        cutoff = (today - timedelta(days=self.sale_lookback_days)).isoformat()
        return self._page(lambda: client.table(TABLE).select(SALE_COLUMNS).eq("bot_source", source)
                          .in_("staging_status", ["pending", "promoted"]).gte("trustee_sale_date", cutoff))

    # ── Memphis notices against their own trustee's sale list ───────────────

    def _list_checks(self, notices, sales, now: datetime, today: date) -> Dict[str, Dict[str, Any]]:
        """Evidence per Memphis notice id from the sale list of the trustee the
        notice names (_trustee_list_match). A list that cannot be read today
        confirms nothing and withdraws nothing."""
        books = all_staged_books(sales.values(), now)
        try:
            listings = rubin_lublin_list.load(self.session)
            if listings is not None:
                books["rubin_lublin_list"] = page_book("rubin_lublin_list", listings, today.isoformat(), rubin_lublin_list.URL)
        except BotTimeout:
            raise
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Rubin Lublin sale list not read (%s)", type(exc).__name__)
        return check_notices(notices, books, today.isoformat())

    def _redate(self, client, row: Dict[str, Any], check: Dict[str, Any], now: datetime) -> bool:
        """Move a Memphis notice record to the later date its trustee's list
        shows. Compare-and-set on the whole record identity, the stored date
        and phone_metadata, so a manual status or a concurrent write wins."""
        found = (client.table(TABLE).select("id,pipeline_lead_key,phone_metadata").eq("id", row["id"]).limit(1).execute().data) or []
        if not found:
            return False
        pm = found[0].get("phone_metadata")
        if (pm is not None and not isinstance(pm, dict)) or (pm or {}).get("sale_status"):
            return False
        tracking = (pm or {}).get("notice_tracking", {})
        if not isinstance(tracking, dict):
            return False
        query = client.table(TABLE).update({"trustee_sale_date": check["date"], "phone_metadata": {
            **(pm or {}), "notice_tracking": {**tracking, "last_seen_at": now.isoformat(), "last_seen_by": REVIEWER}}})
        for key, value in {"id": row["id"], "pipeline_lead_key": found[0]["pipeline_lead_key"],
                           "bot_source": "memphis_daily_news", "staging_status": row["staging_status"],
                           "property_address": row["property_address"], "trustee_sale_date": row["trustee_sale_date"],
                           "raw_payload->structured->>tdn_no": row["notice_id"]}.items():
            query = query.eq(key, value)
        return bool(guard_phone_metadata(query, pm).execute().data)

    def _ranked(self, client, ids: List[str]) -> set:
        """Which of these records are on the current ranked list. A failed
        read raises: a card must not change route because a query timed out."""
        latest = client.table("lead_opportunities").select("captured_on").order("captured_on", desc=True).limit(1).execute().data
        if not latest:
            raise RuntimeError("no ranked snapshot")
        found = set()
        for offset in range(0, len(ids), 100):
            rows = (client.table("lead_opportunities").select("staging_id").eq("captured_on", latest[0]["captured_on"])
                    .in_("staging_id", ids[offset:offset + 100]).execute().data) or []
            found.update(r["staging_id"] for r in rows)
        return found

    # ── The pass ────────────────────────────────────────────────────────────

    def review(self, client, *, dry_run: bool = False, only_ids: Optional[List[str]] = None,
               owner_checks: Optional[int] = None, now: Optional[datetime] = None,
               seeds: Optional[Dict[str, Dict[str, Any]]] = None, register_index: bool = False) -> Dict[str, Any]:
        """One pass. dry_run plans without writing (county lookups still run
        unless owner_checks=0). seeds attaches reviewed county evidence to
        named sale records that have none (the one-time baseline backfill).
        register_index adds the county Register index rows (see
        _register_index_review); the Shelby pass is the same either way."""
        now = now or datetime.now(timezone.utc)
        today = now.astimezone(gettz("America/Chicago")).date()
        only = set(only_ids) if only_ids is not None else None
        counts = dict(appointments=0, owner_checks=0, owners_verified=0, linked=0, link_conflicts=0, sales=0,
                      cards_written=0, unchanged=0, kept_existing=0, list_checked=0, list_confirmed=0,
                      list_postponed=0, redated=0)
        patches: Dict[str, Dict[str, Any]] = {}
        plan: List[Dict[str, Any]] = []

        appointments = self._appointments(client, today, only)
        notices = self._notices(client, today)
        sales = {r["id"]: r for r in notices if (r.get("trustee_sale_date") or "") >=
                 (today - timedelta(days=self.sale_lookback_days)).isoformat()}
        for source in SALE_SOURCES[1:]:
            sales.update({r["id"]: r for r in self._sales(client, today, source)})
        identities = [notice_identity(n) for n in notices]
        checks = self._list_checks(notices, sales, now, today)
        # A notice past its printed date comes back when the trustee's list shows a later one.
        sales.update({n["id"]: n for n in notices if n["id"] in checks and n["id"] not in sales})
        redates: List[tuple] = []

        ranked = self._ranked(client, [r["id"] for r in appointments] + [i for i in sales if only is None or i in only])
        budget = self.max_owner_checks if owner_checks is None else owner_checks
        started = time.monotonic()
        linked_to: Dict[str, Dict[str, Any]] = {}
        for row in sorted(appointments, key=lambda r: r["raw_payload"].get("recorded") or "", reverse=True):
            counts["appointments"] += 1
            raw = dict(row["raw_payload"])
            patch: Dict[str, Any] = {}
            county = raw.get(COUNTY_KEY) if isinstance(raw.get(COUNTY_KEY), dict) else None
            stale = (today - timedelta(days=self.recheck_days)).isoformat()
            if county is None:
                due = True
            elif county.get("estimated_appraisal") is not None:
                due = False
            else:
                last = county.get("value_checked_on") or (county.get("recheck") or {}).get("checked_on")
                # An earlier hand-verified owner has no status: check it once for the assessment.
                due = (last or (county.get("checked_on") if county.get("status") else None) or "") <= stale
            if due and row.get("property_address") and budget > 0 and time.monotonic() - started < 240:
                budget -= 1
                counts["owner_checks"] += 1
                try:
                    fresh = county_check(self, row, today)
                except BotTimeout:
                    raise
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("County check failed for %s (%s)", row["id"], type(exc).__name__)
                    fresh = None
                if fresh is not None:
                    merged = merge_county(county, fresh)
                    if merged != county:
                        raw[COUNTY_KEY] = patch[COUNTY_KEY] = merged
            county = raw.get(COUNTY_KEY) if isinstance(raw.get(COUNTY_KEY), dict) else {}
            if county.get("roll_owner"):
                counts["owners_verified"] += 1
            ref = raw.get("deed_of_trust") if isinstance(raw.get("deed_of_trust"), dict) else {}
            link = link_notice({"instrument": raw.get("instrument"), "recorded": raw.get("recorded"),
                                "loan_instrument": ref.get("instrument") or county.get("mortgage_instrument"),
                                "parcel": raw.get("parcel_id") or county.get("parcel_id"),
                                "address": row.get("property_address")}, identities)
            if link["status"] == "linked":
                counts["linked"] += 1
                linked_to[link["staging_id"]] = {**link, "instrument": raw.get("instrument")}
            elif link["status"] == "conflict":
                counts["link_conflicts"] += 1
            if link["status"] != "none" and raw.get(LINK_KEY) != link:
                raw[LINK_KEY] = patch[LINK_KEY] = link
            fixes = name_corrections(raw)   # index typo proved by the deed of trust and county bill; source spelling stays
            if fixes and raw.get(CORRECTION_KEY) != fixes:
                raw[CORRECTION_KEY] = patch[CORRECTION_KEY] = fixes
            card = appointment_card({**row, "raw": raw}, link, today,
                                    notice_carded=link.get("staging_id") in sales
                                    and (only is None or link.get("staging_id") in only),
                                    ranked=row["id"] in ranked)
            self._plan_card(row["id"], raw.get("pilot_review"), card, patch, counts)
            if patch:
                patches[row["id"]] = patch
                plan.append({"id": row["id"], "kind": "appointment", "instrument": raw.get("instrument"),
                             "keys": sorted(patch), "route": (patch.get("pilot_review") or raw.get("pilot_review") or {}).get("route"),
                             "link": link["status"]})

        if register_index:
            review_register_index(self, client, today, only, sales, linked_to, patches, plan, counts)

        last_read: Dict[str, Optional[datetime]] = {}
        for row in sales.values():
            tracking = row.get("notice_tracking") if isinstance(row.get("notice_tracking"), dict) else {}
            if tracking.get("last_seen_by") == row["bot_source"]:
                seen = _stamp(tracking.get("last_seen_at"))
                if seen and (last_read.get(row["bot_source"]) is None or seen > last_read[row["bot_source"]]):
                    last_read[row["bot_source"]] = seen
        for row in sales.values():
            if only is not None and row["id"] not in only:
                continue
            counts["sales"] += 1
            patch = {}
            if seeds and row["id"] in seeds and not row.get("seed"):
                row = {**row, "seed": seeds[row["id"]]}
                patch[EVIDENCE_KEY] = seeds[row["id"]]
            check = checks.get(row["id"])
            if check is not None:
                counts["list_checked"] += 1
                counts["list_confirmed"] += check["status"] == "confirmed"
                counts["list_postponed"] += check["status"] == "postponed"
                if check != row.get("list_check"):
                    patch[LIST_KEY] = check
                moved = effective_date(row, check, today.isoformat())
                if moved != row.get("trustee_sale_date"):
                    redates.append((row, check))
                row = {**row, "list_check": check, "trustee_sale_date": moved}
            row = {**row, "ranked": row["id"] in ranked}
            state = sale_state(row, last_read.get(row["bot_source"]), now, today)
            track = track_of(state, row.get("track"), today)
            if track != row.get("track"):
                patch[TRACK_KEY] = track
            card = sale_card(row, state, today, linked_to.get(row["id"]))
            self._plan_card(row["id"], row.get("card"), card, patch, counts)
            if patch:
                patches[row["id"]] = patch
                plan.append({"id": row["id"], "kind": "sale", "source": row["bot_source"], "keys": sorted(patch),
                             "state": state["status"], "sale_date": state.get("sale_date"),
                             "list": (check or {}).get("status"),
                             "route": (patch.get("pilot_review") or row.get("card") or {}).get("route")})

        if not dry_run:
            items = [{"id": key, "raw_patch": value} for key, value in patches.items()]
            for offset in range(0, len(items), 100):
                chunk = items[offset:offset + 100]
                written = client.rpc("apply_staging_enrichment", {"p_rows": chunk}).execute().data
                if written != len(chunk):
                    raise RuntimeError(f"review write incomplete: {written} of {len(chunk)}")
            for row, check in redates:
                counts["redated"] += self._redate(client, row, check, now)
        else:
            counts["redated"] = len(redates)
        return {**counts, "rows_changed": len(patches), "plan": plan, "patches": patches if dry_run else None,
                "source_last_read": {k: v.isoformat() if v else None for k, v in last_read.items()}}

    @staticmethod
    def _plan_card(row_id: str, existing: Any, card: Dict[str, Any], patch: Dict[str, Any], counts: Dict[str, int]) -> None:
        kept = merge_review(existing, card)
        if kept is not card:
            counts["kept_existing"] += 1     # a manual card, or a stronger card from another reviewer
        elif same_card(existing, card):
            counts["unchanged"] += 1
        else:
            patch["pilot_review"] = card
            counts["cards_written"] += 1

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase", "staged": 0, "duplicates": 0, "fetched": 0}
        error, result = None, {}
        try:
            result = self.review(client, register_index=True)
        except BotTimeout:
            raise
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
            self.logger.exception("Pre-foreclosure review failed")
        counts = {k: v for k, v in result.items() if isinstance(v, int)}
        status = "failed" if error else ("ok" if counts.get("rows_changed") else "all_dupes")
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=counts.get("appointments", 0) + counts.get("sales", 0),
                            parsed_count=counts.get("cards_written", 0), staged_count=counts.get("rows_changed", 0),
                            duplicate_count=counts.get("unchanged", 0), error_message=error, notes=counts or None)
        self.logger.info("review: %s", counts)
        return {"name": self.name, "status": status, "staged": counts.get("rows_changed", 0),
                "duplicates": counts.get("unchanged", 0),
                "fetched": counts.get("appointments", 0) + counts.get("sales", 0), "error": error, **counts}


def run() -> dict:
    return PreforeclosureReviewBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
