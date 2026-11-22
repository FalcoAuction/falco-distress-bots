import json
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.bots import trustee_status_reaper_bot as reaper
from src.bots import _base


NOW = datetime(2026, 11, 22, 12, tzinfo=timezone.utc)
OLD = "2026-11-10T00:00:00Z"
LEGACY = {"updated_by": reaper.REAPER, "status": "cancelled"}


def row(**fields):
    return {"id": "abc", "distress_type": "SOT", "trustee_sale_date": "2026-11-30",
            "phone_metadata": {"notice_tracking": {"last_seen_at": OLD}, "other": [1]}, **fields}


class NoticeReviewTests(unittest.TestCase):
    def test_stale_is_review_only_and_repeat_is_noop(self):
        lead = row()
        result = reaper.review_metadata(lead, NOW)
        self.assertEqual(result[reaper.REVIEW_KEY]["reason"], "stale_notice")
        self.assertNotIn("sale_status", result)
        self.assertNotIn(reaper.REVIEW_KEY, lead["phone_metadata"])
        self.assertIsNone(reaper.review_metadata(row(phone_metadata=result), NOW))

    def test_exact_age_with_offset_and_invalid_timestamps(self):
        for seen in ("2026-11-12T06:00:00-06:00", "bad", "2020-01-01", None, 123,
                     "2030-11-22T00:00:00Z"):
            with self.subTest(seen=seen):
                self.assertIsNone(reaper.review_metadata(row(phone_metadata={
                    "notice_tracking": {"last_seen_at": seen}}), NOW))

    def test_legacy_flag_retires_on_stale_fresh_and_out_of_scope(self):
        for changes, seen, expect_review in [
            ({}, OLD, True), ({}, NOW.isoformat(), False),
            ({"trustee_sale_date": None}, OLD, False),
            ({"trustee_sale_date": "2026-11-01"}, OLD, False),
            ({"distress_type": "PROBATE"}, OLD, False)
        ]:
            with self.subTest(changes=changes, seen=seen):
                pm = {"sale_status": LEGACY, "other": [1], "notice_tracking": {"last_seen_at": seen}}
                result = reaper.review_metadata(row(phone_metadata=pm, **changes), NOW)
                self.assertNotIn("sale_status", result)
                self.assertEqual(result["other"], [1])
                self.assertEqual(reaper.REVIEW_KEY in result, expect_review)

    def test_resighting_and_manual_transition_clear_only_our_review(self):
        pm = reaper.review_metadata(row(), NOW)
        for status in (None, {"status": "cancelled"}, {"status": "postponed", "updated_by": "operator"},
                       {"status": "cancelled", "updated_by": "other_bot"},
                       {"status": "ran", "updated_by": reaper.REAPER}, {"status": "new_manual_value"}):
            with self.subTest(status=status):
                changed = dict(pm)
                if status:
                    changed["sale_status"] = status
                else:
                    changed["notice_tracking"] = {"last_seen_at": NOW.isoformat()}
                result = reaper.review_metadata(row(phone_metadata=changed), NOW)
                self.assertEqual(result, {k: v for k, v in changed.items() if k != reaper.REVIEW_KEY})
        foreign = {reaper.REVIEW_KEY: {"updated_by": "operator"}, "notice_tracking": pm["notice_tracking"]}
        self.assertIsNone(reaper.review_metadata(row(phone_metadata=foreign), NOW))

    def test_compare_and_swap_miss_is_counted_and_never_retried_unguarded(self):
        client = MagicMock()
        table = client.table.return_value
        lead = row(distress_type=None, trustee_sale_date=None, phone_metadata={"sale_status": LEGACY})
        selection = table.select.return_value.eq.return_value.or_.return_value
        selection.order.return_value.limit.return_value.execute.return_value.data = [lead]
        q = table.update.return_value
        q.eq.return_value = q.is_.return_value = q.filter.return_value = q
        q.execute.return_value.data = []  # concurrent manual edit or changed date
        bot = reaper.TrusteeStatusReaperBot()
        with patch.object(reaper, "_supabase", return_value=client), patch.object(bot, "_report_health"):
            result = bot.run()
        self.assertEqual((result["cas_conflicts"], result["updated"]), (1, 0))
        self.assertEqual(q.execute.call_count, 1)
        q.eq.assert_any_call("id", "abc")
        q.eq.assert_any_call("source", "bot")
        q.is_.assert_any_call("distress_type", "null")
        q.is_.assert_any_call("trustee_sale_date", "null")
        q.filter.assert_called_once_with("phone_metadata", "eq", json.dumps(lead["phone_metadata"], separators=(",", ":")))
        self.assertIn("sale_status->>updated_by", table.select.return_value.eq.return_value.or_.call_args.args[0])

    def test_duplicate_notice_touch_guards_original_on_staging_and_live(self):
        client = MagicMock()
        snapshots = [{"notice_tracking": {"last_seen_at": OLD}, "sale_status": {"status": "ran"}}, None]
        tables = {}
        for name, original in zip(("homeowner_requests_staging", "homeowner_requests"), snapshots):
            table = tables[name] = MagicMock()
            lookup = table.select.return_value
            lookup.eq.return_value = lookup.in_.return_value = lookup
            lookup.execute.return_value.data = [{"id": "123", "pipeline_lead_key": "key", "phone_metadata": original}]
            q = table.update.return_value
            q.eq.return_value = q.filter.return_value = q.is_.return_value = q
            q.execute.return_value = SimpleNamespace(data=[])
        client.table.side_effect = tables.__getitem__
        bot = reaper.TrusteeStatusReaperBot()
        with patch.object(_base, "_supabase", return_value=client):
            self.assertEqual(bot._write_staging([_base.LeadPayload("test", "key")]), (0, 1))
        staged, live = tables.values()
        original = snapshots[0]
        self.assertEqual(original["notice_tracking"]["last_seen_at"], OLD)
        staged.update.return_value.filter.assert_called_once_with(
            "phone_metadata", "eq", json.dumps(original, separators=(",", ":")))
        self.assertEqual(staged.update.call_args.args[0]["phone_metadata"]["sale_status"], original["sale_status"])
        live.update.return_value.is_.assert_called_once_with("phone_metadata", "null")
