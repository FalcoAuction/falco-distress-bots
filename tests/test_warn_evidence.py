import unittest
from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.bots._warn_letter import review
from src.bots._warn_refresh import HISTORY_LIMIT, attach_parcel, merge_observation, refresh
from src.bots.warn_notices_bot import LeadPayload, WarnNoticesBot


REC = dict(notice="#202600001", company="Example Co", county="Davidson", posted="2026-10-01",
           effective_raw="11/1/2026", letter_url="https://www.tn.gov/example.pdf")
SITE = "20 Main St, Nashville, TN 37214"
TODAY = date(2026, 11, 22)


def facts(event="a permanent closure effective November 1, 2026", tail=None, **changes):
    memo = ("SUBJECT: Employee Dislocation Notification Example Co WARN #202600001 "
            "Example Co has filed an official WARN notice, notifying the agency of " + event + ". "
            + (tail if tail is not None else "The affected location is " + SITE + ". The total is 10."))
    return review({**REC, **changes}, memo, "abc", TODAY.isoformat())


def lead():
    return LeadPayload(bot_source="warn_notices", pipeline_lead_key="key", county="davidson",
                       raw_payload={"warn": {**REC, "worksite": SITE, "asset_disposition": "unverified"},
                                    "warn_review": facts()})


class WarnEvidenceTests(unittest.TestCase):
    def test_employment_events_and_windows(self):
        self.assertEqual(facts("a partial layoff effective November 1, 2026")["event_type"], "partial_layoff")
        self.assertEqual(facts("a temporary layoff effective November 1, 2026")["event_type"], "temporary_layoff")
        self.assertEqual(facts("a permanent layoff effective November 1, 2026",
                               "This transition is not the result of a facility closure.")["event_type"], "permanent_layoff")
        r = facts("a mass layoff and permanent closure effective November 1, 2026 through January 1, 2027")
        self.assertEqual((r["event_type"], r["timing"], r["timing_basis"]),
                         ("permanent_closure", "in_window", "employment_notice"))
        self.assertEqual(r["employment_window"]["letter_dates"], ["2026-11-01", "2027-01-01"])
        self.assertNotIn("sale_date", r)
        self.assertNotIn("operations_stop_date", r)
        self.assertEqual(facts(effective_raw="11/2/2026")["timing"], "date_conflict")
        self.assertEqual(facts("a permanent layoff effective November 1, 2026 through December 2026")["timing"], "unknown")

    def test_identity_multisite_and_authority(self):
        for changes in (dict(notice="#202600002"), dict(company="Different Co")):
            result = facts(**changes)
            self.assertEqual((result["verification"], result["worksite"]), ("identity_review", None))
        r = facts(tail="The affected locations are, 20 Main St, Nashville, TN 37214, 30 Main St, Nashville, TN 37214. "
                       "The company intends to hold an equipment sale.")
        self.assertEqual(len(r["worksites"]), 2)
        self.assertIsNone(r["worksite"])
        self.assertTrue(r["disposition_excerpts_for_review"])
        self.assertEqual((r["asset_disposition"], r["authority_evidence"]), ("unverified", []))
        r = facts(tail="The receiver was authorized to sell the equipment by court order.")
        self.assertEqual(r["authority_evidence"][0]["verification"], "requires_underlying_document")
        self.assertEqual(r["asset_disposition"], "unverified")

    @patch("src.bots._warn_refresh.worksite_parcel")
    def test_cache_expiration_failure_and_recovery(self, lookup):
        existing = {"county": "davidson", "parcel_id": "apn", "checked_on": "2026-10-23", "printed_worksite": SITE}
        old = {"warn": {**REC, "worksite": SITE, "worksite_parcel": existing}}
        lookup.return_value = None
        failed = lead()
        attach_parcel(failed, old, None, TODAY)
        saved = merge_observation(old, failed.raw_payload)
        self.assertEqual(saved["warn"]["worksite_parcel"], existing)
        self.assertEqual(saved["warn"]["worksite_parcel_status"], "previous_observation")
        self.assertEqual(existing["checked_on"], "2026-10-23")
        lookup.return_value = {"county": "davidson", "parcel_id": "apn"}
        confirmed = lead()
        attach_parcel(confirmed, saved, None, TODAY)
        saved = merge_observation(saved, confirmed.raw_payload)
        self.assertNotIn("worksite_parcel_status", saved["warn"])
        self.assertEqual(saved["warn"]["worksite_parcel"]["checked_on"], TODAY.isoformat())
        lookup.reset_mock()
        revisit = lead()
        attach_parcel(revisit, saved, None, TODAY + timedelta(days=29))
        lookup.assert_not_called()
        self.assertEqual(revisit.raw_payload["warn"]["worksite_parcel"], saved["warn"]["worksite_parcel"])

    def test_failed_letter_preserves_parcel_as_previous_only(self):
        old = lead().raw_payload
        old["warn"]["worksite_parcel"] = {"printed_worksite": SITE, "parcel_id": "apn", "checked_on": "2026-10-01"}
        missing = lead().raw_payload
        missing["warn_review"].update(verification="letter_unavailable", event_type="review", worksite=None)
        missing["warn"]["worksite"] = None
        result = merge_observation(old, missing)
        self.assertIsNone(result["warn"]["worksite"])
        self.assertEqual(result["warn"]["worksite_parcel_status"], "previous_observation")
        self.assertEqual(result["warn"]["worksite_parcel"]["checked_on"], "2026-10-01")

    @patch("src.bots._warn_refresh.worksite_parcel")
    def test_only_one_verified_davidson_closure_site(self, lookup):
        for county, event, status, site in (("knox", "permanent_closure", "verified", SITE),
                                           ("davidson", "permanent_layoff", "verified", SITE),
                                           ("davidson", "review", "identity_review", SITE),
                                           ("davidson", "permanent_closure", "verified", None)):
            x = lead()
            x.county = county
            x.raw_payload["warn_review"].update(event_type=event, verification=status, worksite=site)
            attach_parcel(x, {}, None, TODAY)
        lookup.assert_not_called()

    def test_history_bounded_and_terminal_provenance_survives_replays(self):
        original = lead().raw_payload
        old = {"warn": {**original["warn"], "asset_disposition": "sold", "disposition_source": "https://example.org/sale"},
               "other_enrichment": {"retain": True}}
        for i in range(50):
            current = deepcopy(original)
            current["warn_review"].update(verification="letter_unavailable" if i % 2 else "verified",
                                          checked_on=(TODAY + timedelta(days=i)).isoformat())
            old = merge_observation(old, current)
        self.assertLessEqual(len(old["warn_history"]), 3)
        for i in range(30):
            current["warn_review"]["letter_sha256"] = str(i)
            old = merge_observation(old, current)
        self.assertEqual(len(old["warn_history"]), HISTORY_LIMIT)
        self.assertEqual(old["warn_review"]["asset_disposition"], "sold")
        self.assertEqual(old["warn_review"]["disposition_source"], "https://example.org/sale")
        self.assertEqual(old["other_enrichment"], {"retain": True})
        self.assertEqual(merge_observation(old, current), old)

    def test_failed_collection_cannot_age_out_rows(self):
        bot = object.__new__(WarnNoticesBot)
        bot.fetch = Mock(return_value=SimpleNamespace(status_code=200, text="<html/>"))
        with self.assertRaises(RuntimeError):
            bot.scrape()
        self.assertEqual(bot._write_staging([]), (0, 0))

    def test_duplicate_staged_keys_are_preserved(self):
        db = Mock()
        table = db.table.return_value
        row = dict(id="id", pipeline_lead_key="key", county="davidson", raw_payload=lead().raw_payload)
        table.select.return_value.eq.return_value.gt.return_value.order.return_value.limit.return_value.execute.return_value.data = [
            row, dict(row, id="other")]
        self.assertEqual(refresh(db, [lead()], Mock(), TODAY, 365, "run"), (0, 1))
        table.update.assert_not_called()
        table.upsert.assert_not_called()

    @patch("src.bots._warn_refresh.worksite_parcel")
    def test_normal_writer_enriches_new_then_reuses_cache_on_revisit(self, lookup):
        db, staging, live = Mock(), Mock(), Mock()
        db.table.side_effect = {"homeowner_requests_staging": staging, "homeowner_requests": live}.__getitem__
        scan = staging.select.return_value.eq.return_value.gt.return_value.order.return_value.limit.return_value
        scan.execute.return_value.data = []
        live.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value.data = []
        staging.upsert.return_value.execute.return_value.data = [{"id": "new"}]
        lookup.return_value = {"county": "davidson", "parcel_id": "apn"}
        self.assertEqual(refresh(db, [lead()], Mock(), TODAY, 365, "run"), (1, 0))
        inserted = staging.upsert.call_args.args[0]
        self.assertEqual(inserted["raw_payload"]["warn"]["worksite_parcel"]["parcel_id"], "apn")
        self.assertIsNone(inserted.get("property_value"))
        self.assertIsNone(inserted.get("owner_name_records"))
        scan.execute.return_value.data = [inserted]
        lookup.reset_mock()
        self.assertEqual(refresh(db, [lead()], Mock(), TODAY, 365, "run"), (0, 1))
        lookup.assert_not_called()
        self.assertEqual(staging.upsert.call_count, 1)
        staging.update.assert_not_called()
        live.update.assert_not_called()
