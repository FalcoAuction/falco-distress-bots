"""Tests for the final check before paid trace. No network.

    python -m unittest tests.test_pretrace_check -v
"""

from __future__ import annotations

import copy
import csv
import json
import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import _pretrace_check as pc  # noqa: E402
from src.bots._pilot_review import build_review, debt, source  # noqa: E402
from src.bots.pilot_worklist import COLUMNS, PRETRACE_COLUMNS, PRETRACE_CSV, build_worklist, write_outputs  # noqa: E402
from src.bots.pretrace_check_bot import collect, write_results  # noqa: E402

TODAY = date(2026, 11, 24)
SRC = source("Shelby County Trustee realty bill", "https://example.test/bill", "2026-11-23")
LIST = source("Trustee sale list", "https://example.test/list", "2026-11-23")


def row(rid="a", owner="STROUD GENEVA W", parcel="028042  00020", lane="pre_foreclosure", deadline="2026-12-18",
        checked="2026-11-23", county="shelby", bill_owner=None, verified=True, route="first_trace_review", by="lane_bot",
        decider=None, value=315600, slug=None, **cols):
    kind = "Trustee sale date" if deadline else None
    card = build_review(
        lane=lane, route=route, route_reason="Sourced and current.", reviewed_by=by, county=county,
        owner_names=[owner], today=date(2026, 11, 1),
        event={"what": "Trustee sale advertised", "status": "active", "checked_on": checked, "source": LIST},
        asset={"description": "Home", "parcel": parcel, "address": "38 N Alicia Dr", "tie": "Notice names this street", "source": SRC},
        decider={"name": decider or owner, "role": "owner of record", "basis": "county tax bill", "verified": verified, "source": SRC},
        economics={"deadline": deadline, "deadline_kind": kind, "value": value, "value_basis": "assessment / ratio",
                   "value_is_estimate": True, "value_source": SRC,
                   "known_debt": [debt("mortgage_original", 185050, "2020-05-08", SRC, "Not a payoff.")], "unknowns": ["payoff"]},
        availability={"evidence": "No transfer found", "checked_on": checked, "first_call_question": "Is the loan still behind?",
                      "verify_next": ["Recheck the list"] if route == "research_hold" else []},
    )
    card["reviewed_at"] = "2026-11-24T00:00:00+00:00"
    return {"id": rid, "county": county, "staging_status": "pending", "pipeline_lead_key": slug or f"slug-{rid}",
            "raw_payload": {"pilot_review": card},
            "phone_metadata": {"shelby_trustee_value_review": {"owner": bill_owner or owner, "parcel_id": parcel, "checked_on": "2026-11-23"}},
            **cols}


def check(r, ctx=None, today=TODAY, **kw):
    return pc.check_row(r, ctx or {}, today, **kw)


class DeadlineTests(unittest.TestCase):
    def test_clean_row_is_trace_now_and_states_the_listing_gap(self):
        x = check(row())
        self.assertEqual(x["queue"], pc.TRACE)
        self.assertEqual(x["days_left"], 24)
        self.assertEqual(x["listing_status"], "Listing not checked")
        self.assertIn("not checked", x["fact_to_verify"])
        self.assertEqual(x["resolve_by"], pc.SIGNER_TALK)
        self.assertEqual(x["date_confirmed_on"], "2026-11-23")
        self.assertIn("Trustee sale list", x["date_source"])

    def test_deadline_passed_is_removed(self):
        x = check(row(deadline="2026-11-20"))
        self.assertEqual((x["queue"], x["codes"][0]), (pc.REMOVE, "deadline_passed"))

    def test_deadline_inside_minimum_runway_is_not_trace_now(self):
        self.assertEqual(pc.MIN_RUNWAY_DAYS, 10)
        short = check(row(deadline="2026-12-03"))             # 9 days
        self.assertEqual((short["queue"], short["codes"][0]), (pc.REMOVE, "runway_too_short"))
        self.assertIn("10-day minimum", short["reason"])
        self.assertEqual(check(row(deadline="2026-12-04"))["queue"], pc.TRACE)   # exactly 10 days

    def test_stale_date_source_is_verify_first(self):
        x = check(row(checked="2026-11-18"))                  # 6 days old, limit 4
        self.assertEqual((x["queue"], x["codes"][0]), (pc.VERIFY, "date_source_stale"))
        self.assertEqual(x["resolve_by"], pc.RECORD_CHECK)
        self.assertIn("Trustee sale list", x["resolve_how"])

    def test_no_deadline_row_uses_the_event_check_age(self):
        self.assertEqual(check(row(lane="cre", deadline=None, checked="2026-11-18"))["queue"], pc.TRACE)
        self.assertEqual(check(row(lane="cre", deadline=None, checked="2026-11-10"))["codes"], ["date_source_stale"])


class OwnerTests(unittest.TestCase):
    def test_owner_changed_on_a_newer_county_record_is_removed(self):
        r = row(bill_owner="NEW BUYER LLC")
        r["phone_metadata"]["shelby_trustee_value_review"]["checked_on"] = "2026-11-24"
        x = check(r)
        self.assertEqual((x["queue"], x["codes"][0]), (pc.REMOVE, "owner_changed"))
        self.assertIn("NEW BUYER LLC", x["reason"])

    def test_owner_difference_on_an_undated_record_is_verify_not_removal(self):
        r = row(bill_owner="NEW BUYER LLC")
        del r["phone_metadata"]["shelby_trustee_value_review"]["checked_on"]
        self.assertEqual(check(r)["queue"], pc.VERIFY)

    def test_co_owner_on_the_county_line_but_not_the_card(self):
        x = check(row(bill_owner="STROUD THOMAS A & GENEVA W"))
        self.assertEqual((x["queue"], x["codes"][0]), (pc.VERIFY, "co_owner_missing"))
        self.assertIn("THOMAS", x["reason"])
        self.assertEqual(x["resolve_by"], pc.RECORD_CHECK)

    def test_co_owner_from_a_bill_history_print_entered_by_hand(self):
        facts = {"co_owner_prints": [{"name": "STROUD THOMAS A & GENEVA W", "source": "the Shelby Trustee bill history", "date": "2026-11-24"}]}
        x = check(row(), {"facts": facts})
        self.assertEqual(x["codes"], ["co_owner_missing"])
        self.assertIn('"STROUD THOMAS A & GENEVA W"', x["reason"])
        self.assertIn('"STROUD GENEVA W" only', x["reason"])

    def test_spelling_variant_and_care_of_line_are_not_co_owners(self):
        r = row(owner="PERCER BOBBIE E JR & DONNA M", bill_owner="PERCER BOBBIE E JR & DONNA M & ATTN MIKE SMITH")
        r["raw_payload"]["publication_evidence"] = {"parties": ["Bobbie E. & Donna Piercer"], "url": "https://example.test/suit"}
        self.assertEqual(check(r)["queue"], pc.TRACE)

    def test_county_record_for_another_parcel_is_not_owner_evidence(self):
        r = row()
        r["phone_metadata"]["shelby_trustee_value_review"]["parcel_id"] = "099999  00001"
        r["raw_payload"]["pilot_review"]["decider"]["source"] = None
        self.assertEqual(check(r)["codes"][0], "owner_record_missing")
        self.assertTrue(pc.parcels_agree("shelby", "028042", "028042  00020"))

    def test_stale_owner_record(self):
        r = row(lane="cre", deadline=None)
        r["phone_metadata"]["shelby_trustee_value_review"]["checked_on"] = "2026-10-01"
        r["raw_payload"]["pilot_review"]["decider"]["source"]["date"] = "2026-10-01"
        self.assertEqual(check(r)["codes"], ["owner_record_stale"])

    def test_company_with_no_verified_signer_is_never_a_person_lookup(self):
        x = check(row(owner="STANLEY VILLAGE 66 LLC", verified=False))
        self.assertEqual((x["queue"], x["codes"][0]), (pc.VERIFY, "company_no_signer"))
        self.assertIn("Do not run a person lookup", x["resolve_how"])
        self.assertIn("Secretary of State", x["resolve_how"])
        # "verified" on the company's own name is still not a person who can sign
        self.assertEqual(check(row(owner="STANLEY VILLAGE 66 LLC", verified=True))["codes"][0], "company_no_signer")
        ok = check(row(owner="WOLFE-SIDBERRY NANCY A REVOCABLE TRUST", decider="Nancy A. Wolfe-Sidberry", verified=True,
                       bill_owner="WOLFE-SIDBERRY NANCY A REVOCABLE TRUST"))
        self.assertEqual(ok["queue"], pc.TRACE)


class HoldTests(unittest.TestCase):
    def test_workflow_hold(self):
        x = check(row(), {"workflow": {"status": "attempting_contact", "next_action": "call", "last_contact_at": "2026-11-20T15:00:00+00:00"}})
        self.assertEqual((x["queue"], x["codes"][0]), (pc.VERIFY, "workflow_hold"))
        self.assertEqual(x["resolve_by"], pc.NOTHING)
        self.assertEqual(check(row(), {"workflow": {"status": "closed_lost"}})["queue"], pc.REMOVE)
        self.assertEqual(check(row(), {"workflow": {"status": "new", "next_action": "none", "attempt_count": 0}})["queue"], pc.TRACE)

    def test_do_not_contact(self):
        for ctx in ({"activities": [{"outcome": "do_not_call", "occurred_at": "2026-11-01T00:00:00+00:00"}]},
                    {"sms": {"status": "opted_out"}},
                    {"facts": {"do_not_contact": {"reason": "asked by letter", "date": "2026-11-02"}}}):
            x = check(row(), ctx)
            self.assertEqual((x["queue"], x["codes"][0]), (pc.REMOVE, "do_not_contact"), ctx)
        x = check(row(email="A@B.com".lower()), {"suppressed_emails": {"a@b.com": "unsubscribe"}})
        self.assertEqual(x["codes"][0], "do_not_contact")

    def test_already_contacted_manual_hold_and_phone_on_file(self):
        self.assertEqual(check(row(), {"activities": [{"outcome": "voicemail_left"}]})["codes"], ["already_contacted"])
        self.assertEqual(check(row(), {"activities": [{"outcome": "note_only"}]})["queue"], pc.TRACE)
        self.assertEqual(check(row(), {"sms": {"status": "paused"}})["codes"], ["manual_hold"])
        self.assertEqual(check(row(phone="+14235550100"))["codes"], ["already_traced"])
        self.assertEqual(check(row(staging_status="rejected"))["queue"], pc.REMOVE)

    def test_unreadable_hold_table_is_unknown_not_a_pass(self):
        x = check(row(), {"unread": ["dialer_lead_workflow"]})
        self.assertEqual((x["queue"], x["codes"]), (pc.VERIFY, ["holds_unread"]))

    def test_duplicate_owner_across_lanes_shares_a_hold(self):
        a = row("a")
        b = row("b", lane="tax", parcel="099001  00007", by="tax_bot")
        res = pc.check_all([a, b], {"b": {"workflow": {"status": "rpc_made"}}}, today=TODAY)
        self.assertEqual(res["b"]["codes"], ["workflow_hold"])
        self.assertEqual((res["a"]["queue"], res["a"]["codes"]), (pc.VERIFY, ["workflow_hold"]))
        self.assertIn("another record of this owner", res["a"]["reason"])
        self.assertEqual(res["a"]["also_on"], ["tax: first trace review (other property)"])

    def test_hold_on_a_non_worklist_record_of_the_owner_reaches_the_worklist_row(self):
        a = row("a")
        held = row("h", lane="probate", parcel="077001  00001", route="research_hold", by="probate_bot")
        cards = [{"id": r["id"], "county": r["county"], "pilot_review": r["raw_payload"]["pilot_review"]} for r in (a, held)]
        parts = pc._owner_parts(held["raw_payload"]["pilot_review"]["owner_group_key"])
        res = pc.check_all([a], {"h": {"owner_parts": parts, "activities": [{"outcome": "do_not_call"}]}}, cards, TODAY)
        self.assertEqual((res["a"]["queue"], res["a"]["codes"][0]), (pc.REMOVE, "do_not_contact"))

    def test_same_parcel_removed_on_another_card_is_verify_first(self):
        a = row("a")
        gone = row("g", owner="SOMEONE ELSE", lane="tax", route="removed", by="tax_bot")
        cards = [{"id": r["id"], "county": r["county"], "pilot_review": r["raw_payload"]["pilot_review"]} for r in (a, gone)]
        x = pc.check_all([a], {}, cards, TODAY)["a"]
        self.assertEqual((x["queue"], x["codes"]), (pc.VERIFY, ["sibling_card_removed"]))
        # removed only because it repeats this record: not evidence against the property
        cards[1]["pilot_review"] = {**cards[1]["pilot_review"], "route_reason": "Duplicate of the sale notice record for the same loan."}
        self.assertEqual(pc.check_all([a], {}, cards, TODAY)["a"]["queue"], pc.TRACE)

    def test_every_suit_party_missing_from_the_card_is_named_once(self):
        r = row(owner="PAYNE JAMES A & BERNICE", lane="tax", deadline="2027-04-13")
        r["raw_payload"]["publication_evidence"] = {"parties": ["AMANDA RINES", "ASHLEY CRUTCHER", "PAYNE JAMES A & BERNICE"]}
        x = check(r)
        self.assertEqual(x["codes"], ["co_owner_missing"])
        self.assertIn('"AMANDA RINES", "ASHLEY CRUTCHER"', x["reason"])
        self.assertEqual(x["fact_kind"], pc.FACT_KIND["co_owner_missing"])


class MoneyTests(unittest.TestCase):
    def test_original_loan_is_never_shown_as_a_payoff(self):
        r = row()
        r["raw_payload"]["lien_check"] = {"stack": {"as_of": "2026-11-21", "liens": [
            {"type": "DOT", "instrument": "20048291", "original_amount": 185050.0, "recorded": "2020-05-13",
             "status": "no_release_found_in_retrieved_records"},
            {"type": "DOT", "instrument": "22000001", "original_amount": 40000.0, "recorded": "2022-01-05",
             "status": "no_release_found_in_retrieved_records"}]}}
        r["raw_payload"]["trustee_detail"] = {"total_tax_balance": 2132.18, "checked_on": "2026-11-22"}
        r["phone_metadata"]["mortgage_estimate"] = {"estimated_current_balance": 175560.0}
        x = check(r)
        m = x["money"]
        self.assertEqual(m["payoff"], {"status": "unknown", "amount": None, "date": None, "source": None, "resolve_by": pc.PAYOFF_REQUEST})
        self.assertEqual([d["amount"] for d in m["recorded_debt"]], [185050.0, 40000.0])     # the card's loan is not listed twice
        self.assertTrue(all("not a payoff" in d["label"] for d in m["recorded_debt"]))
        self.assertEqual(m["recorded_releases"], [])
        self.assertEqual(m["tax_balance"][0]["amount"], 2132.18)
        text = pc.money_text(m)
        self.assertEqual(set(text), {"value", "recorded_debt", "recorded_releases", "tax_balance", "payoff"})
        self.assertEqual(text["payoff"], "unknown (no dated payoff document)")
        self.assertNotIn("185,050", text["payoff"])
        self.assertIn("none found", text["recorded_releases"])
        blob = json.dumps(x)
        for forbidden in ("175560", "225,050", "225050", "equity"):                          # no estimate, no sum, no equity
            self.assertNotIn(forbidden, blob)

    def test_payoff_needs_a_dated_document(self):
        undated = check(row(), {"facts": {"payoff_document": {"amount": 150000}}})
        self.assertEqual(undated["money"]["payoff"]["status"], "unknown")
        dated = check(row(), {"facts": {"payoff_document": {"amount": 150000, "date": "2026-11-20", "source": "lender letter"}}})
        self.assertEqual(pc.money_text(dated["money"])["payoff"], "$150,000 per payoff document dated 2026-11-20")


class StoredResultTests(unittest.TestCase):
    def stored(self, **kw):
        r = row(deadline="2027-02-03", **kw)
        r["raw_payload"][pc.KEY] = check(r, today=date(2026, 11, 24))
        return r

    def test_unchanged_row_is_not_re_evaluated(self):
        r = self.stored()
        x = check(r, today=date(2026, 11, 25))
        self.assertTrue(x["carried_forward"])
        self.assertEqual(x["checked_on"], "2026-11-24")
        self.assertEqual(pc.without_run_flags(x), r["raw_payload"][pc.KEY])

    def test_changed_row_is_re_evaluated(self):
        for change in ("deadline", "owner", "value", "hold", "parcel", "route"):
            r = self.stored()
            card, ctx = r["raw_payload"]["pilot_review"], {}
            if change == "deadline":
                card["economics"]["deadline"] = "2027-03-03"
            elif change == "owner":
                r["phone_metadata"]["shelby_trustee_value_review"]["owner"] = "STROUD THOMAS A & GENEVA W"
            elif change == "value":
                card["economics"]["value"] = 280000
            elif change == "hold":
                ctx = {"workflow": {"status": "rpc_made"}}
            elif change == "parcel":
                card["asset"]["parcel"] = "028042  00021"
                r["phone_metadata"]["shelby_trustee_value_review"]["parcel_id"] = "028042  00021"
            else:
                card["route"] = "research_hold"
            x = check(r, ctx, today=date(2026, 11, 25))
            self.assertNotIn("carried_forward", x, change)
            self.assertEqual(x["checked_on"], "2026-11-25", change)
            self.assertNotEqual(x["fingerprint"], r["raw_payload"][pc.KEY]["fingerprint"], change)

    def test_deadline_within_fourteen_days_is_re_evaluated_every_run(self):
        self.assertEqual(pc.URGENT_RECHECK_DAYS, 14)
        r = row(deadline="2026-12-08")
        r["raw_payload"][pc.KEY] = check(r, today=date(2026, 11, 24))
        x = check(r, today=date(2026, 11, 25))
        self.assertNotIn("carried_forward", x)
        self.assertEqual(x["checked_on"], "2026-11-25")
        # a far deadline is picked up again on the day it enters the window
        far = self.stored()
        self.assertTrue(check(far, today=date(2026, 11, 27))["carried_forward"])     # recheck_on is 2026-11-28 (date goes stale)
        self.assertEqual(far["raw_payload"][pc.KEY]["recheck_on"], "2026-11-28")
        self.assertEqual(check(far, today=date(2026, 11, 28))["codes"], ["date_source_stale"])

    def test_repeat_run_is_identical(self):
        r = row()
        first = check(r)
        self.assertEqual(first, check(copy.deepcopy(r)))
        r["raw_payload"][pc.KEY] = first
        self.assertEqual(pc.without_run_flags(check(r)), first)
        self.assertEqual(json.dumps(first, sort_keys=True), json.dumps(check(r, previous=None), sort_keys=True))


class _Table:
    def __init__(self, db, name):
        self.db, self.name, self.filters, self.patch = db, name, [], None

    def select(self, *_a, **_k): return self
    def limit(self, *_a): return self
    def update(self, patch): self.patch = patch; return self
    def eq(self, col, val): self.filters.append((col, val)); return self
    def in_(self, col, vals): self.filters.append((col, list(vals))); return self

    def execute(self):
        if self.name in self.db.get("fail", ()):
            raise RuntimeError("boom")
        rows = self.db.get(self.name, [])
        for col, val in self.filters:
            if "->" in col or col == "raw_payload":
                continue
            rows = [r for r in rows if (r.get(col) in val if isinstance(val, list) else r.get(col) == val)]
        if self.patch is not None:
            self.db.setdefault("writes", []).append((rows[0]["id"] if rows else None, self.patch))
            for r in rows:
                r.update(copy.deepcopy(self.patch))
        return type("R", (), {"data": copy.deepcopy(rows)})()


class _Client:
    def __init__(self, db): self.db = db
    def table(self, name): return _Table(self.db, name)


class BotTests(unittest.TestCase):
    def db(self):
        rows = [row("a"), row("b", owner="REED VIDA L", parcel="D0215M D00017", deadline="2027-01-05")]
        return {"homeowner_requests_staging": rows,
                "dialer_lead_workflow": [{"listing_slug": "slug-b", "status": "attempting_contact", "next_action": "call"}]}

    def cards(self, db):
        return [{"id": r["id"], "county": r["county"], "pipeline_lead_key": r["pipeline_lead_key"],
                 "pilot_review": r["raw_payload"]["pilot_review"]} for r in db["homeowner_requests_staging"]]

    def test_collect_reads_holds_by_crm_key(self):
        db = self.db()
        got = collect(_Client(db), self.cards(db), today=TODAY)
        self.assertEqual({k: v["queue"] for k, v in got["results"].items()}, {"a": pc.TRACE, "b": pc.VERIFY})
        self.assertEqual(got["unread"], [])
        self.assertNotIn("writes", db)

    def test_failed_table_read_is_never_a_pass_or_a_removal(self):
        db = {**self.db(), "fail": {"dialer_activities"}}
        got = collect(_Client(db), self.cards(db), today=TODAY)
        self.assertEqual(got["unread"], ["dialer_activities"])
        self.assertEqual({v["queue"] for v in got["results"].values()}, {pc.VERIFY})

    def test_write_stores_only_its_own_key_and_repeat_writes_nothing(self):
        db = self.db()
        client = _Client(db)
        before = copy.deepcopy(db["homeowner_requests_staging"])
        got = collect(client, self.cards(db), today=TODAY)
        out = write_results(client, got["rows"], got["results"])
        self.assertEqual(sorted(out["written"]), ["a", "b"])
        for old, new in zip(before, db["homeowner_requests_staging"]):
            self.assertEqual(new["raw_payload"]["pilot_review"], old["raw_payload"]["pilot_review"])     # card and route untouched
            self.assertEqual(set(new["raw_payload"]) - set(old["raw_payload"]), {pc.KEY})
            self.assertEqual({k: v for k, v in new.items() if k != "raw_payload"}, {k: v for k, v in old.items() if k != "raw_payload"})
        self.assertTrue(all(set(patch) == {"raw_payload"} for _id, patch in db["writes"]))
        db["writes"].clear()
        again = collect(client, self.cards(db), today=TODAY)
        out = write_results(client, again["rows"], again["results"])
        self.assertEqual((out["written"], sorted(out["unchanged"]), db["writes"]), ([], ["a", "b"], []))

    def test_write_refuses_a_card_that_moved(self):
        db = self.db()
        client = _Client(db)
        got = collect(client, self.cards(db), today=TODAY)
        db["homeowner_requests_staging"][0]["raw_payload"]["pilot_review"]["reviewed_at"] = "2026-11-24T09:00:00+00:00"
        db["homeowner_requests_staging"][1]["raw_payload"]["pilot_review"]["route"] = "research_hold"
        out = write_results(client, got["rows"], got["results"])
        self.assertEqual((out["written"], sorted(out["conflict"])), ([], ["a", "b"]))
        self.assertNotIn("writes", db)


class WorklistOutputTests(unittest.TestCase):
    def build(self):
        rows = [row("soon", deadline="2026-12-01", owner="WOLFE NANCY A", parcel="17000000600"),
                row("clean", owner="REED VIDA L", parcel="D0215M D00017", deadline="2027-01-05"),
                row("clean2", owner="REED VIDA L", parcel="D0215M D00099", deadline="2027-02-05", lane="tax", by="tax_bot"),
                row("co", bill_owner="STROUD THOMAS A & GENEVA W"),
                row("nodate", owner="ENNIS BRYAN J", parcel="127M-A-007", lane="cre", deadline=None, county="hamilton")]
        cards = [{"id": r["id"], "county": r["county"], "pilot_review": r["raw_payload"]["pilot_review"]} for r in rows]
        return cards, build_worklist(cards, None, TODAY, pretrace=pc.check_all(rows, {}, cards, TODAY))

    def test_three_queues_with_deadlines_first(self):
        cards, result = self.build()
        pre = result["pretrace"]
        self.assertEqual(pre["rows_by_queue"], {pc.TRACE: 3, pc.VERIFY: 1, pc.REMOVE: 1})
        self.assertEqual(pre["owner_groups_by_best_queue"], {pc.TRACE: 2, pc.VERIFY: 1, pc.REMOVE: 1})
        self.assertEqual([r["asset"] for r in pre["deadline_first"]], ["38 N Alicia Dr"])
        self.assertEqual(pre["deadline_first"][0]["queue"], "Remove from pursuit")
        lookups = [r["paid_lookup"] for r in pre["queues"][pc.TRACE]]
        self.assertEqual(lookups.count("One lookup for this owner group"), 2)                 # Reed is bought once
        self.assertEqual(lookups.count("Covered by the lookup already listed for this owner group"), 1)
        for r in [x for q in pre["queues"].values() for x in q]:
            for col in ("owner_group", "lane", "asset", "reason", "crm_link", "source", "payoff", "listing_status"):
                self.assertTrue(r[col], col)
            self.assertNotIn("—", json.dumps(r, ensure_ascii=False))
        self.assertEqual(pre["verify_first_facts"][0][1], 1)
        # the worklist itself is unchanged by the check
        plain = build_worklist(cards, None, TODAY)
        self.assertEqual(plain["rows"], result["rows"])
        self.assertNotIn("pretrace", plain)

    def test_outputs_keep_the_existing_columns(self):
        _cards, result = self.build()
        with tempfile.TemporaryDirectory() as tmp:
            paths = write_outputs(result, Path(tmp))
            self.assertIn(PRETRACE_CSV, [p.name for p in paths])
            with (Path(tmp) / "pilot-worklist.csv").open(encoding="utf-8") as f:
                self.assertEqual(next(csv.reader(f)), COLUMNS)
            with (Path(tmp) / PRETRACE_CSV).open(encoding="utf-8") as f:
                table = list(csv.DictReader(f))
            self.assertEqual(list(table[0]), PRETRACE_COLUMNS)
            self.assertEqual([t["queue"] for t in table], ["Worth tracing now"] * 3 + ["Verify one fact first", "Remove from pursuit"])
            saved = json.loads((Path(tmp) / "pilot-worklist.json").read_text(encoding="utf-8"))
            self.assertEqual(set(saved["rows"][0]), set(COLUMNS))
            try:
                from openpyxl import load_workbook
            except ImportError:
                return
            wb = load_workbook(Path(tmp) / "pilot-worklist.xlsx")
            self.assertEqual(wb.sheetnames, ["First trace review", "Summary", "Deadlines first", "Worth tracing now",
                                             "Verify one fact first", "Remove from pursuit"])
            self.assertEqual([c.value for c in wb["First trace review"][1]], [c.replace("_", " ").title() for c in COLUMNS])


if __name__ == "__main__":
    unittest.main()
