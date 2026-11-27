"""The scheduled enrichers read light columns first and skip rejected rows.

Each test runs the bot's real run() (or its candidate reader) against an
in-memory client that holds rejected, pending, verified and promoted rows,
with and without the bot's own "done" marker, and checks three things:

  - the rows written are exactly the rows still in play that need the work
  - the query no longer asks for whole raw_payload / phone_metadata objects
    except for the rows it is about to work
  - a rejected row that returns to pending is worked on the next run

    python -m unittest tests.test_slim_readers -v
"""

from __future__ import annotations

import inspect
import os
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _fake_supabase import FakeClient, project  # noqa: E402
from src.bots import (decision_engine_bot, free_county_completion_bot, free_lien_review_bot,  # noqa: E402
                      mortgage_amortizer_bot, mortgage_estimator_bot, owner_classifier_bot,
                      phone_resolver_bot, skip_trace_enricher_bot, stacked_distress_aggregator_bot,
                      stale_regrade_bot, xref_avm_enricher_bot)

STAGING = "homeowner_requests_staging"
LIVE = "homeowner_requests"
STATUSES = ("rejected", "pending", "verified", "promoted")
NOT_REJECTED = ("neq", "staging_status", "rejected")
BODY = "NOTICE OF SALE " + "x" * 400          # the bulk a light scan must not download
JUNK = {"other_bot": {"keep": "me"}}          # another bot's phone_metadata key


def rid(n):
    return f"{n:08d}-0000-0000-0000-000000000000"


def staged(n, status, raw=None, meta=None, **columns):
    row = {"id": rid(n), "staging_status": status, "bot_source": "tn_public_notice", "county": "davidson",
           "distress_type": "PRE_FORECLOSURE", "property_address": f"{n} Main St, Nashville, TN 37201",
           "owner_name_records": "JOHN SMITH", "full_name": None, "property_value": None,
           "property_value_source": None, "property_value_as_of": None, "year_built": None,
           "mortgage_balance": None, "phone": None, "priority_score": None, "admin_notes": None,
           "raw_payload": {"body": BODY, **(raw or {})}, "phone_metadata": {**JUNK, **(meta or {})}}
    row.update(columns)
    return row


def grid(work, done, **columns):
    """One row per status that needs the work (ids 1-4) and one per status
    that already carries the done marker (ids 11-14)."""
    rows = []
    for i, status in enumerate(STATUSES, start=1):
        rows.append(staged(i, status, raw=work.get("raw"), meta=work.get("meta"), **{**columns, **work.get("cols", {})}))
        rows.append(staged(10 + i, status, raw=done.get("raw"), meta=done.get("meta"), **{**columns, **done.get("cols", {})}))
    return rows


IN_PLAY_WORK = [rid(2), rid(3), rid(4)]        # pending, verified, promoted without the marker


def run_with(module, client, **attrs):
    with mock.patch.object(module, "_supabase", lambda: client):
        return module.run(**attrs)


class Base(unittest.TestCase):
    def assert_scan_is_light(self, client, allowed_raw=(), allowed_meta=(), table=STAGING):
        self.assertEqual(client.heavy_ids("raw_payload", table), sorted(allowed_raw), "whole raw_payload downloaded")
        self.assertEqual(client.heavy_ids("phone_metadata", table), sorted(allowed_meta),
                         "whole phone_metadata downloaded")

    def assert_other_keys_kept(self, client, ids):
        for i in ids:
            self.assertEqual(client.row(i)["phone_metadata"]["other_bot"], {"keep": "me"})
            self.assertEqual(client.row(i)["raw_payload"]["body"], BODY)

    def revive(self, client, row_id=rid(1)):
        """The rejected row returns to pending; forget what was recorded."""
        client.row(row_id)["staging_status"] = "pending"
        client.reads.clear(), client.updates.clear(), client.rpcs.clear()


class FakeClientTests(unittest.TestCase):
    def test_json_paths_and_aliases(self):
        row = {"id": "a", "raw_payload": {"k": {"f": 3, "g": "t"}, "n": None}, "phone_metadata": None}
        got = project(row, "id, x:raw_payload->k, f:raw_payload->k->>f, raw_payload->k->g, m:phone_metadata->z")
        self.assertEqual(got, {"id": "a", "x": {"f": 3, "g": "t"}, "f": "3", "g": "t", "m": None})

    def test_neq_excludes_only_the_named_value(self):
        client = FakeClient([{"id": "a", "staging_status": "rejected"}, {"id": "b", "staging_status": "pending"}])
        rows = client.table(STAGING).select("id").neq("staging_status", "rejected").execute().data
        self.assertEqual(rows, [{"id": "b"}])


class SelectedKeysTests(unittest.TestCase):
    """A light scan is right only while it selects every key the code reads.
    These fail when someone adds a raw.get("new_key") to a function a light
    scan feeds, until the key is added to that scan's list."""

    @staticmethod
    def read(names, *functions):
        source = "".join(inspect.getsource(f) for f in functions)
        return set(re.findall(r'\b(?:%s)\.get\("(\w+)"\)' % "|".join(names), source))

    def test_parcel_rule(self):
        from src.bots import _value_guard as guard
        from src.bots.hamilton_assessor_bot import tax_roll_parcel
        functions = (guard._doc_parcels, guard.value_evidence, tax_roll_parcel)
        selected = set(xref_avm_enricher_bot.RAW_KEYS) | {rec for rec, _ in xref_avm_enricher_bot.RAW_FIELDS}
        self.assertEqual(self.read(["raw"], *functions) - selected, set())
        self.assertEqual(self.read(["meta"], *functions) - set(xref_avm_enricher_bot.META_KEYS), set())
        self.assertIn("code_case_parcel", selected)

    def test_county_completion(self):
        from src.bots import _free_data as free
        functions = (free.county_patch, free.hmda_principal_repair, free.notice_phone_repair)
        selected = set(free_county_completion_bot.RAW_KEYS) | {"permit"}
        self.assertEqual(self.read(["raw"], *functions) - selected, set())
        self.assertEqual(self.read(["pm"], *functions) - set(free_county_completion_bot.META_KEYS), set())

    def test_mortgage_estimate_signals(self):
        got = self.read(["raw"], mortgage_estimator_bot.MortgageEstimatorBot._extract_signals)
        self.assertEqual(got, set(mortgage_estimator_bot.SIGNAL_KEYS))

    def test_owner_mailing(self):
        got = self.read(["raw"], skip_trace_enricher_bot.SkipTraceEnricherBot._compute)
        self.assertEqual(got, set(skip_trace_enricher_bot.MAILING_KEYS))


class FreeLienReviewTests(Base):
    def rows(self):
        return grid(work={"raw": {"lien_check": {"status": "queued"}}},
                    done={"raw": {"lien_check": {"status": "needs_free_records_review"}}})

    def test_only_queued_rows_in_play_are_read_and_written(self):
        client = FakeClient(self.rows())
        run_with(free_lien_review_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        for i in IN_PLAY_WORK:
            self.assertEqual(client.row(i)["raw_payload"]["lien_check"]["status"], "needs_free_records_review")
        self.assertEqual(client.row(rid(1))["raw_payload"]["lien_check"], {"status": "queued"})
        scan = client.staging_reads()[0]
        self.assertEqual(scan["select"], "id,lien_status:raw_payload->lien_check->>status")
        self.assertIn(NOT_REJECTED, scan["filters"])
        self.assertNotIn(rid(1), scan["ids"])
        self.assert_scan_is_light(client, IN_PLAY_WORK, IN_PLAY_WORK)
        self.assert_other_keys_kept(client, IN_PLAY_WORK)

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(free_lien_review_bot, client)
        self.revive(client)
        run_with(free_lien_review_bot, client)
        self.assertEqual(client.written_ids(), [rid(1)])


class FreeCountyCompletionTests(Base):
    PARCEL = {"method": "address", "appraisal": 188000, "owner": "DOE JANE", "parcelid": "057 055K E 00100"}

    def rows(self):
        return grid(work={"raw": {"comptroller_parcel": self.PARCEL}},
                    done={"raw": {"comptroller_parcel": self.PARCEL},
                          "cols": {"property_value": 188000, "property_value_source": "comptroller"}})

    def test_only_rows_with_a_change_are_read_in_full(self):
        live = {**staged(50, "pending", raw={"comptroller_parcel": self.PARCEL}), "source": "bot"}
        del live["staging_status"], live["bot_source"]
        live["raw_payload"] = {"comptroller_parcel": self.PARCEL}
        live["phone_metadata"] = {}
        client = FakeClient(self.rows(), [live])
        run_with(free_county_completion_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        for i in IN_PLAY_WORK:
            self.assertEqual((client.row(i)["property_value"], client.row(i)["property_value_source"]),
                             (188000, "comptroller"))
        self.assertIsNone(client.row(rid(1))["property_value"])
        scan = client.staging_reads()[0]
        self.assertEqual(scan["select"], free_county_completion_bot.SCAN_COLUMNS)
        self.assertIn(NOT_REJECTED, scan["filters"])
        self.assert_scan_is_light(client, IN_PLAY_WORK, IN_PLAY_WORK)
        # The live table has no staging_status and is read as before.
        self.assertEqual(client.written_ids(LIVE), [rid(50)])
        self.assertEqual(client.row(rid(50), LIVE)["property_value"], 188000)

    def test_light_row_reaches_the_same_decision_as_the_full_row(self):
        raw = {"metro_parcel": {"total_appraisal": 275000, "year_built": 1980},
               "permit": {"Const_Cost": 150000, "Description": "x"}, "public_contact_candidates": [],
               "source": "county_gis", "body": BODY}
        meta = {"phone_resolver": {"source": "notice_body", "phone": "6155550134"},
                "mortgage_signal": {"source": "hmda_match", "amount": 200000}}
        row = staged(1, "pending", raw=raw, meta=meta, phone="(615) 555-0134", mortgage_balance=200000,
                     bot_source="stalled_build")
        light = free_county_completion_bot._light_row(project(row, free_county_completion_bot.SCAN_COLUMNS))
        self.assertEqual(light["raw_payload"]["permit"], {"Const_Cost": 150000})
        self.assertEqual(sorted(free_county_completion_bot.changes(light)),
                         sorted(free_county_completion_bot.changes(row)))

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(free_county_completion_bot, client)
        self.revive(client)
        run_with(free_county_completion_bot, client)
        self.assertEqual(client.written_ids(), [rid(1)])


class MortgageAmortizerTests(Base):
    ROD = {"rod_lookup": {"original_principal": 175000, "document_date": "2014-05-02"}}

    def rows(self):
        # This bot has no done marker: it recomputes every defensible lead.
        return grid(work={"meta": self.ROD}, done={"meta": {"mortgage_signal": {"source": "estimate"}}})

    def test_only_defensible_leads_in_play_are_read_and_written(self):
        client = FakeClient(self.rows())
        run_with(mortgage_amortizer_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        for i in IN_PLAY_WORK:
            meta = client.row(i)["phone_metadata"]
            self.assertEqual(meta["mortgage_balance_amortized"]["source"], "ustitlesearch_rod")
            self.assertGreater(client.row(i)["mortgage_balance"], 0)
        self.assertNotIn("mortgage_balance_amortized", client.row(rid(1))["phone_metadata"])
        scan = client.staging_reads()[0]
        self.assertNotIn("raw_payload", scan["select"])
        self.assertIn(NOT_REJECTED, scan["filters"])
        self.assert_scan_is_light(client, IN_PLAY_WORK, IN_PLAY_WORK)
        self.assert_other_keys_kept(client, IN_PLAY_WORK)

    def test_light_gate_matches_the_gate_on_the_full_object(self):
        cases = [{}, {"rod_lookup": {}}, {"rod_lookup": {"a": 1}},
                 {"mortgage_signal": {"source": "hmda_match"}},
                 {"mortgage_signal": {"source": "hmda_match", "year_anchored": True}},
                 {"mortgage_signal": {"source": "nashville_ledger_extracted"}}, {"mortgage_signal": "text"}]
        for meta in cases:
            light = project({"id": "a", "phone_metadata": meta},
                            "id, rod_lookup:phone_metadata->rod_lookup, mortgage_signal:phone_metadata->mortgage_signal")
            self.assertEqual(mortgage_amortizer_bot._defensible_source(light),
                             mortgage_amortizer_bot._defensible_source(meta), meta)

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(mortgage_amortizer_bot, client)
        self.revive(client)
        run_with(mortgage_amortizer_bot, client)
        self.assertIn(rid(1), client.written_ids())


class MortgageEstimatorTests(Base):
    SALE = {"shelby_arcgis": {"last_sale_price": 250000, "last_sale_date": "2018-06-15", "appraised": 320000}}

    def rows(self):
        return grid(work={"raw": self.SALE},
                    done={"raw": self.SALE, "meta": {"mortgage_estimate": {"confidence": 1.0}}})

    def test_rows_in_play_without_an_estimate_are_written(self):
        client = FakeClient(self.rows())
        run_with(mortgage_estimator_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        for i in IN_PLAY_WORK:
            self.assertIn("estimated_current_balance", client.row(i)["phone_metadata"]["mortgage_estimate"])
        self.assertNotIn("mortgage_estimate", client.row(rid(1))["phone_metadata"])
        self.assertEqual(client.row(rid(12))["phone_metadata"]["mortgage_estimate"], {"confidence": 1.0})
        scan = client.staging_reads()[0]
        self.assertIn("rp_shelby_arcgis:raw_payload->shelby_arcgis", scan["select"])
        self.assertIn("pm_estimate:phone_metadata->mortgage_estimate", scan["select"])
        # Whole phone_metadata only for the rows written; never the whole raw_payload.
        self.assert_scan_is_light(client, [], IN_PLAY_WORK)
        self.assert_other_keys_kept(client, IN_PLAY_WORK)

    def test_same_window_as_before_and_no_rows_past_the_cap(self):
        # 1,200 rows, cap 1,000: the window is the first 1,000 ids of every
        # status, as it was. A rejected row inside it is skipped, not
        # replaced by row 1,001, and the second page is never requested.
        rows = [staged(n, "rejected" if n == 2 else "pending", raw=self.SALE) for n in range(1, 1201)]
        client = FakeClient(rows)
        with mock.patch.object(mortgage_estimator_bot.MortgageEstimatorBot, "max_leads_per_run", 1000):
            run_with(mortgage_estimator_bot, client)
        self.assertEqual(client.written_ids(), [rid(n) for n in range(1, 1001) if n != 2])
        scans = [r for r in client.staging_reads() if "pm_estimate" in r["select"]]
        self.assertEqual(len(scans), 1)

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(mortgage_estimator_bot, client)
        self.revive(client)
        run_with(mortgage_estimator_bot, client)
        self.assertEqual(client.written_ids(), [rid(1)])


class SkipTraceEnricherTests(Base):
    MAIL = {"shelby_arcgis": {"owner_mailing": "9 Beach Rd, Miami, FL 33101", "owner_state": "FL",
                              "owner_zip": "33101"}}

    def rows(self):
        rows = grid(work={"raw": self.MAIL}, done={"raw": self.MAIL})
        bot = skip_trace_enricher_bot.SkipTraceEnricherBot()
        for row in rows:
            if row["id"] >= rid(11):
                row["phone_metadata"]["skip_trace"] = bot._compute(row)
        return rows

    def test_rows_in_play_without_the_tag_are_written(self):
        client = FakeClient(self.rows())
        run_with(skip_trace_enricher_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        for i in IN_PLAY_WORK:
            self.assertTrue(client.row(i)["phone_metadata"]["skip_trace"]["is_out_of_state_owner"])
        self.assertNotIn("skip_trace", client.row(rid(1))["phone_metadata"])
        scan = client.staging_reads()[0]
        self.assertIn("pm_skip_trace:phone_metadata->skip_trace", scan["select"])
        self.assertIn(("not.is", "property_address", "null"), scan["filters"])
        self.assert_scan_is_light(client, [], IN_PLAY_WORK)
        self.assert_other_keys_kept(client, IN_PLAY_WORK)

    def test_light_row_computes_the_same_tag(self):
        bot = skip_trace_enricher_bot.SkipTraceEnricherBot()
        for raw in (self.MAIL, {"mailing_address": "77 Pine Rd, Atlanta, GA 30301"},
                    {"tpad": {"mailing_address": "1 Main St, Nashville, TN 37201"}}, {"padctn": {}}, {}):
            row = staged(1, "pending", raw=raw)
            client = FakeClient([row])
            light = bot._candidates(client, STAGING)[0]
            self.assertEqual(bot._compute(light), bot._compute(row), raw)

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(skip_trace_enricher_bot, client)
        self.revive(client)
        run_with(skip_trace_enricher_bot, client)
        self.assertEqual(client.written_ids(), [rid(1)])


class OwnerClassifierTests(Base):
    def rows(self):
        klass, _ = owner_classifier_bot.classify_owner("JOHN SMITH")
        return grid(work={}, done={"meta": {"owner_class": klass}})

    def test_rows_in_play_without_the_class_are_written(self):
        client = FakeClient(self.rows())
        run_with(owner_classifier_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        klass, _ = owner_classifier_bot.classify_owner("JOHN SMITH")
        for i in IN_PLAY_WORK:
            self.assertEqual(client.row(i)["phone_metadata"]["owner_class"], klass)
        self.assertNotIn("owner_class", client.row(rid(1))["phone_metadata"])
        scan = client.staging_reads()[0]
        self.assertIn("pm_owner_class:phone_metadata->owner_class", scan["select"])
        self.assert_scan_is_light(client, [], IN_PLAY_WORK)
        self.assert_other_keys_kept(client, IN_PLAY_WORK)

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(owner_classifier_bot, client)
        self.revive(client)
        run_with(owner_classifier_bot, client)
        self.assertEqual(client.written_ids(), [rid(1)])


class StackedDistressTests(Base):
    def rows(self):
        single = {"signal_count": 1, "signal_types": ["PRE_FORECLOSURE"], "is_stacked": False}
        rows = grid(work={}, done={"meta": {"distress_stack": single}})
        # A rejected tax row at the same address as pending row 2 and as
        # already-tagged promoted row 14.
        rows.append(staged(20, "rejected", distress_type="TAX_LIEN", property_address=rows[2]["property_address"]))
        rows.append(staged(21, "rejected", distress_type="TAX_LIEN",
                           property_address=client_address(rows, rid(14))))
        return rows

    def test_rejected_rows_still_count_as_signals_but_are_not_tagged(self):
        client = FakeClient(self.rows())
        run_with(stacked_distress_aggregator_bot, client)
        # In play without the tag (2, 3, 4) plus 14, whose stack changed.
        self.assertEqual(client.written_ids(), IN_PLAY_WORK + [rid(14)])
        stacked = {"signal_count": 2, "signal_types": ["PRE_FORECLOSURE", "TAX_LIEN"], "is_stacked": True}
        self.assertEqual(client.row(rid(2))["phone_metadata"]["distress_stack"], stacked)
        self.assertEqual(client.row(rid(14))["phone_metadata"]["distress_stack"], stacked)
        self.assertFalse(client.row(rid(3))["phone_metadata"]["distress_stack"]["is_stacked"])
        for i in (rid(1), rid(20), rid(21)):
            self.assertNotIn("distress_stack", client.row(i)["phone_metadata"])
        scan = client.staging_reads()[0]
        self.assertIn("pm_distress_stack:phone_metadata->distress_stack", scan["select"])
        self.assertIn(rid(20), scan["ids"])          # rejected rows are still scanned, light
        self.assert_scan_is_light(client, [], IN_PLAY_WORK + [rid(14)])
        self.assert_other_keys_kept(client, IN_PLAY_WORK + [rid(14)])

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(stacked_distress_aggregator_bot, client)
        self.revive(client)
        run_with(stacked_distress_aggregator_bot, client)
        self.assertEqual(client.written_ids(), [rid(1)])


def client_address(rows, row_id):
    return next(r["property_address"] for r in rows if r["id"] == row_id)


class StaleRegradeTests(Base):
    def rows(self):
        now = datetime.now(timezone.utc).isoformat()
        hold = {"decision_engine": {"action": "HOLD_FOR_DATA", "decided_at": now}}
        fresh = {"decision_engine": {"action": "PURSUE", "decided_at": now}}
        return grid(work={"meta": hold, "cols": {"property_value": 200000}}, done={"meta": fresh}, priority_score=40)

    def test_fresh_grades_are_not_read_in_full_and_rejected_rows_are_left(self):
        client = FakeClient(self.rows())
        run_with(stale_regrade_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        for i in IN_PLAY_WORK:
            self.assertIsNone(client.row(i)["priority_score"])
            self.assertEqual(client.row(i)["phone_metadata"]["decision_engine"]["previous_action"], "HOLD_FOR_DATA")
        for i in (rid(1), rid(12)):
            self.assertEqual(client.row(i)["priority_score"], 40)
        scan = client.staging_reads()[0]
        self.assertIn("de_action:phone_metadata->decision_engine->>action", scan["select"])
        self.assertIn(("not.is", "priority_score", "null"), scan["filters"])
        self.assert_scan_is_light(client, IN_PLAY_WORK, IN_PLAY_WORK)
        self.assert_other_keys_kept(client, IN_PLAY_WORK)

    def test_light_test_never_rules_out_a_row_the_real_test_accepts(self):
        cutoff = datetime(2026, 11, 19, tzinfo=timezone.utc)
        bot = stale_regrade_bot.StaleRegradeBot
        stamps = (None, "", "garbage", "2026-11-25T10:00:00+00:00", "2026-11-25T10:00:00Z", "2026-11-25T10:00:00",
                  "2026-10-01T10:00:00Z", 20261125)
        for action in (None, "PURSUE", "HOLD_FOR_DATA", "REJECT_NO_EQUITY", "REJECT_FORECLOSED"):
            for stamp in stamps:
                for value, phone, trusted in ((None, None, False), (250000, None, False), (250000, "6155550134", True)):
                    for meta in ({"decision_engine": {"action": action, "decided_at": stamp}}, None, "text",
                                 {"decision_engine": "text"}):
                        row = staged(1, "pending", priority_score=40, property_value=value, phone=phone,
                                     owner_name_records=None)
                        row["phone_metadata"] = meta
                        if trusted and isinstance(meta, dict) and isinstance(meta.get("decision_engine"), dict):
                            meta["phone_resolver"] = {"confidence": 0.9, "source": "skip_trace"}
                        light = bot()._candidates(FakeClient([row]), STAGING)[0]
                        if bot._should_regrade(row, cutoff):
                            self.assertTrue(bot._may_be_stale(light, cutoff), (action, stamp, value, meta))

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(stale_regrade_bot, client)
        self.revive(client)
        run_with(stale_regrade_bot, client)
        self.assertEqual(client.written_ids(), [rid(1)])


class DecisionEngineCandidateTests(Base):
    def candidates(self, client, cap):
        bot = decision_engine_bot.DecisionEngineBot()
        with mock.patch.object(decision_engine_bot.DecisionEngineBot, "max_leads_per_run", cap):
            return [r["id"] for r in bot._candidates(client)]

    def rows(self):
        # ids 1-4 ungraded, 11-14 already graded (priority_score is the marker).
        return grid(work={}, done={"cols": {"priority_score": 55}})

    def test_rejected_and_graded_rows_are_not_read(self):
        client = FakeClient(self.rows(), [{"id": rid(60), "source": "bot", "priority_score": None, "raw_payload": {}}])
        self.assertEqual(self.candidates(client, 500), IN_PLAY_WORK + [rid(60)])
        scan = client.staging_reads()[0]
        self.assertIn(NOT_REJECTED, scan["filters"])
        self.assertIn(("is", "priority_score", "null"), scan["filters"])
        self.assertEqual(client.heavy_ids("raw_payload"), IN_PLAY_WORK)

    def test_reading_stops_once_the_run_has_its_leads(self):
        rows = [staged(n, "pending", raw={"state": "FL"} if n == 1 else None) for n in range(1, 8)]
        client = FakeClient(rows, [{"id": rid(60), "source": "bot", "priority_score": None}], max_rows=1000)
        with mock.patch.object(decision_engine_bot, "_supabase", lambda: client):
            got = self.candidates(client, 3)
        # The Florida row does not count toward the cap (run() drops it).
        graded = [i for i in got if i != rid(1)][:3]
        self.assertEqual(graded, [rid(2), rid(3), rid(4)])
        self.assertEqual([r["table"] for r in client.reads], [STAGING])   # live is not read

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        self.assertNotIn(rid(1), self.candidates(client, 500))
        self.revive(client)
        self.assertIn(rid(1), self.candidates(client, 500))


class PhoneResolverTests(Base):
    def rows(self):
        work = {"body": "Substitute Trustee sale. For information call (615) 555-0134."}
        return grid(work={"raw": work}, done={})

    def test_payload_is_read_only_for_rows_in_play(self):
        rows = self.rows()
        client = FakeClient(rows)
        run_with(phone_resolver_bot, client)
        written = client.written_ids()
        self.assertNotIn(rid(1), written)
        self.assertNotIn("public_contact_candidates", client.row(rid(1))["raw_payload"])
        self.assertTrue(set(written) <= set(IN_PLAY_WORK))
        scan = client.staging_reads()[0]
        self.assertEqual(scan["select"], "id, staging_status")
        in_play = sorted(r["id"] for r in rows if r["staging_status"] != "rejected")
        self.assert_scan_is_light(client, in_play, in_play)

    def test_same_rows_as_the_full_read(self):
        """Whatever the resolver finds in a notice, it finds on exactly the
        rows in play that it found it on when it read every row in full."""
        rows = self.rows()
        bot = phone_resolver_bot.PhoneResolverBot()
        expected = sorted(r["id"] for r in rows if r["staging_status"] != "rejected" and bot._gather_candidates(r))
        client = FakeClient(rows)
        run_with(phone_resolver_bot, client)
        self.assertEqual(client.written_ids(), expected)

    def test_window_is_the_first_rows_by_id(self):
        rows = [staged(n, "rejected" if n == 2 else "pending") for n in range(1, 7)]
        client = FakeClient(rows, max_rows=1000)
        bot = phone_resolver_bot.PhoneResolverBot()
        with mock.patch.object(phone_resolver_bot.PhoneResolverBot, "max_leads_per_run", 3):
            got = [r["id"] for r in bot._candidates(client, STAGING)]
        self.assertEqual(got, [rid(1), rid(3)])

    def test_rejected_row_back_to_pending_is_picked_up(self):
        client = FakeClient(self.rows())
        run_with(phone_resolver_bot, client)
        self.revive(client)
        bot = phone_resolver_bot.PhoneResolverBot()
        self.assertIn(rid(1), [r["id"] for r in bot._candidates(client, STAGING)])


class XrefAvmEnricherTests(Base):
    PARCEL = "057 055K E 00100"

    def rows(self):
        donor = staged(1, "rejected", raw={"metro_parcel": {"parcel": self.PARCEL, "total_appraisal": 275000}},
                       meta={"metro_parcel_checked": "2026-11-05T00:00:00+00:00"}, property_value=275000,
                       property_value_source="metro_nashville_parcels")
        rows = [donor]
        for n, status in ((2, "pending"), (3, "verified"), (4, "promoted")):
            rows.append(staged(n, status, raw={"parcel": self.PARCEL}))
        rows.append(staged(5, "pending", raw={"parcel": "099 000 00900"}))               # another parcel
        rows.append(staged(12, "pending", raw={"parcel": self.PARCEL}, property_value=1))  # already valued
        return rows

    def test_same_parcel_rows_are_filled_and_a_rejected_donor_still_counts(self):
        client = FakeClient(self.rows())
        run_with(xref_avm_enricher_bot, client)
        self.assertEqual(client.written_ids(), IN_PLAY_WORK)
        for i in IN_PLAY_WORK:
            row = client.row(i)
            self.assertEqual((row["property_value"], row["property_value_source"]), (275000, "metro_nashville_parcels"))
            self.assertEqual(row["phone_metadata"]["property_value_xref"]["donor_id"], rid(1))
        self.assertIsNone(client.row(rid(5))["property_value"])
        self.assertEqual(client.row(rid(12))["property_value"], 1)
        scan = client.staging_reads()[0]
        self.assertEqual(scan["select"], xref_avm_enricher_bot.SCAN_COLUMNS)
        self.assertNotIn(NOT_REJECTED, scan["filters"])     # every status is read on purpose
        self.assertIn(rid(1), scan["ids"])
        self.assert_scan_is_light(client, [], IN_PLAY_WORK)
        self.assert_other_keys_kept(client, IN_PLAY_WORK)

    def test_light_row_holds_everything_the_parcel_rule_reads(self):
        from src.bots._value_guard import row_parcels, value_evidence, value_status
        from src.bots.hamilton_assessor_bot import tax_roll_parcel
        raw = {"parcel": "D0242N E00083", "alt_parcel": "D0242N0E000830", "parcel_id": "123456789",
               "county": "hamilton", "cumulative_owed": 0.0,
               "hamilton_roll_valuation": {"parcel": "123456789", "assessor_file_date": "2026-10-01"},
               "extracted": {"parcel": "057 055K E 00100", "lender": "Bank"},
               "nashville_codes": {"Property_APN": "05705500100", "Request_Nbr": "9"},
               "davidson_demolition_permit": {"Parcel": "05705500200", "Contact": "x"},
               "comptroller_parcel": {"parcelid": "057 055K E 00100 000 2026", "appraisal": 188000,
                                      "version": "v2026-11-10", "county": "davidson"},
               "hamilton_assessor": {"parcel": "123456789", "appraised": 150000, "file_date": "2026-10-20"},
               "metro_parcel": {"parcel": "05705500100", "total_appraisal": 275000},
               "padctn": {"parcel": "05705500100", "appraised": 275000, "checked_at": "2026-11-01"},
               "rutherford_arcgis": {"parcel_id": "R1234567", "appraised": 1, "checked_at": "2026-11-01"},
               "shelby_arcgis": {"parid": "D0242N0E000830", "appraised": 90000, "checked_at": "2026-11-01"},
               "williamson_inigo": {"parcel_id": "W1234567", "appraised": 2, "checked_at": "2026-11-01"},
               "trustee_detail": {"estimated_appraisal": 90000, "checked_on": "2026-11-02"},
               "preforeclosure_evidence": {"parcel": "057 055K E 00100", "value_basis": "b", "value": 250000,
                                           "observed_on": "2026-11-03"}}
        meta = {"metro_parcel_checked": "2026-11-05T00:00:00+00:00",
                "shelby_trustee_value_review": {"status": "verified_assessment", "parcel_id": "D0242N E00083",
                                                "estimated_appraisal": 91000, "checked_on": "2026-11-04"},
                "property_value_xref": {"match": "exact_parcel", "parcel": "057 055K E 00100", "county": "davidson",
                                        "observed_on": "2026-11-01", "basis": "b"}}
        for county in ("davidson", "shelby", "hamilton", None):
            row = staged(1, "pending", raw=raw, meta=meta, county=county, property_value=275000,
                         property_value_source="xref_avm_enricher")
            light = xref_avm_enricher_bot.XrefAvmEnricherBot()._fetch_all(FakeClient([row]), STAGING)[0]
            self.assertNotIn("body", light["raw_payload"])
            self.assertNotIn("other_bot", light["phone_metadata"])
            self.assertEqual(light["raw_payload"]["extracted"], {"parcel": "057 055K E 00100"})
            self.assertEqual(row_parcels(light), row_parcels(row))
            self.assertEqual(value_evidence(light), value_evidence(row))
            self.assertEqual(value_status(light), value_status(row))
            self.assertEqual(tax_roll_parcel(light["raw_payload"]), tax_roll_parcel(row["raw_payload"]))

    def test_rejected_recipient_back_to_pending_is_filled(self):
        rows = self.rows()
        rows.append(staged(30, "rejected", raw={"metro_parcel": {"parcel": self.PARCEL, "total_appraisal": 275000}},
                           meta={"metro_parcel_checked": "2026-11-05T00:00:00+00:00"}, property_value=275000,
                           property_value_source="metro_nashville_parcels"))
        client = FakeClient(rows)
        client.row(rid(2))["staging_status"] = "rejected"
        run_with(xref_avm_enricher_bot, client)
        self.revive(client, rid(2))
        client.row(rid(2))["property_value"] = None
        client.row(rid(2))["property_value_source"] = None
        run_with(xref_avm_enricher_bot, client)
        self.assertEqual(client.written_ids(), [rid(2)])


if __name__ == "__main__":
    unittest.main()
