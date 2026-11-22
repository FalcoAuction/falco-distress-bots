import unittest
from unittest.mock import MagicMock, patch

from src.bots import _base
from src.bots.brock_scott_trustee_bot import BrockScottTrusteeBot


class BrockScottRepeatTests(unittest.TestCase):
    def setUp(self):
        self.bot = BrockScottTrusteeBot()
        self.fields = {"case #": "26-114-FC01", "address": "125 Main St, Nashville, Tennessee 37201",
                       "county": "Davidson", "state": "TN", "sale date": "12/15/2026"}

    def repeat(self, *, fields=None, status=None, live_only=None, **stored):
        lead = self.bot._lead_from_fields({**self.fields, **(fields or {})})
        tables = []
        client = MagicMock()
        for name in ("staging", "live"):
            table = MagicMock()
            tables.append(table)
            data = {"id": name, "pipeline_lead_key": lead.pipeline_lead_key,
                    "staging_status": "promoted", "county": "Davidson",
                    "property_address": "125 Main St, Nashville, TN 37201",
                    "trustee_sale_date": "2026-11-24",
                    "phone_metadata": {"sale_status": status} if status else {}, **stored}
            if name == "live" and live_only:
                data.update(live_only)
            lookup = table.select.return_value
            lookup.eq.return_value = lookup.in_.return_value = lookup
            lookup.execute.return_value.data = [data]
            update = table.update.return_value
            update.eq.return_value = update.filter.return_value = update.is_.return_value = update
            update.execute.return_value.data = [data]
        client.table.side_effect = lambda name: tables[0] if name.endswith("staging") else tables[1]
        with patch.object(_base, "_supabase", return_value=client):
            self.assertEqual(self.bot._write_staging([lead]), (0, 1))
        return tables

    def test_rescheduled_date_on_both_tables_with_exact_guards(self):
        staging, live = self.repeat()
        staging.select.return_value.in_.assert_any_call("staging_status", ["pending", "promoted"])
        for table in (staging, live):
            change = table.update.call_args.args[0]
            self.assertEqual(change["trustee_sale_date"], "2026-12-15")
            self.assertEqual(change["phone_metadata"]["notice_tracking"]["case_number"], "26-114-FC01")
            q = table.update.return_value
            for key, value in (("county", "Davidson"), ("property_address", "125 Main St, Nashville, TN 37201"),
                               ("trustee_sale_date", "2026-11-24"),
                               ("pipeline_lead_key", self.bot.make_lead_key("brock_scott", "26-114-FC01"))):
                q.eq.assert_any_call(key, value)
            q.filter.assert_called_once_with("phone_metadata", "eq", "{}")
        staging.update.return_value.eq.assert_any_call("staging_status", "promoted")
        live.update.return_value.eq.assert_any_call("source", "bot")

    def test_missing_new_date_or_case_never_changes_date(self):
        for fields in ({"sale date": ""}, {"case #": ""}):
            for table in self.repeat(fields=fields):
                table.update.assert_not_called()

    def test_missing_old_date_guarded_as_null(self):
        for table in self.repeat(trustee_sale_date=None):
            self.assertEqual(table.update.call_args.args[0]["trustee_sale_date"], "2026-12-15")
            table.update.return_value.is_.assert_any_call("trustee_sale_date", "null")

    def test_existing_case_with_different_property_or_county_never_touched(self):
        for mismatch in ({"property_address": "125 Main St Apt B"}, {"county": "Shelby"}, {"county": None}):
            for table in self.repeat(**mismatch):
                table.update.assert_not_called()

    def test_manual_status_protected_and_exact_old_heuristic_allows_date_update(self):
        for value in ("cancelled", "ran", "postponed", "reinstated", "operator_review"):
            manual = {"status": value, "updated_by": "operator"}
            for table in self.repeat(status=manual):
                table.update.assert_not_called()
        for table in self.repeat(status={"status": "cancelled", "updated_by": "trustee_status_reaper"}):
            self.assertEqual(table.update.call_args.args[0]["trustee_sale_date"], "2026-12-15")

    def test_live_only_manual_or_identity_conflict_also_blocks_staging_date(self):
        for change in ({"phone_metadata": {"sale_status": {"status": "postponed"}}},
                       {"property_address": "125 Main St Unit B"},
                       {"sale_date_last_seen_at": "2099-01-01T00:00:00Z"}):
            for table in self.repeat(live_only=change):
                table.update.assert_not_called()
                table.insert.assert_not_called()

    def test_older_observation_cannot_replace_newer_notice(self):
        for table in self.repeat(phone_metadata={"notice_tracking": {"last_seen_at": "2099-01-01T00:00:00Z"}}):
            table.update.assert_not_called()

    def test_conflicting_current_listings_with_same_case_withheld(self):
        with patch.object(self.bot, "fetch", return_value=MagicMock(status_code=200)), patch(
            "src.bots.brock_scott_trustee_bot.parse_table",
            side_effect=[[self.fields, {**self.fields, "address": "999 Other St"},
                          {**self.fields, "case #": "other-case"}], []],
        ):
            leads = self.bot.scrape()
        self.assertEqual([lead.raw_payload["fields"]["case #"] for lead in leads], ["other-case"])
