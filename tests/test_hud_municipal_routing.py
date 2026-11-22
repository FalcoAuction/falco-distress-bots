import unittest
from unittest.mock import MagicMock, patch

from src.bots._free_data import inventory_or_complaint_hold
from src.bots.auto_promoter_bot import AutoPromoterBot
from src.bots.decision_engine_bot import DecisionEngineBot


class InventoryAndComplaintGates(unittest.TestCase):
    def test_decision_canonical_and_legacy(self):
        complaint = {"city": "Mt. Juliet", "platform": "seeclickfix", "issue": {"id": 123}}
        rows = [
            {"bot_source": "hud_reo"}, {"bot_source": "mtn_cities_codes"},
            {"source": "bot", "raw_payload": {"hud_arcgis": {}}},
            {"bot_source": "bot", "raw_payload": complaint},
            {"source": "bot", "admin_notes": "other · bot_source=hud_reo"},
            {"source": "bot", "admin_notes": "bot_source=mtn_cities_codes"},
        ]
        for row in rows:
            with self.subTest(row=row):
                decision = DecisionEngineBot()._fast_rules(row)
                self.assertEqual((decision["action"], decision["priority_score"], decision["suggested_outreach"]),
                                 ("HOLD_FOR_DATA", 0, "none"))
        for row in [
            {"bot_source": "independent_case", "raw_payload": complaint},
            {"source": "website", "admin_notes": "bot_source=hud_reo"},
            {"source": "bot", "raw_payload": {"issue": {"id": 123}}},
            {"source": "bot", "admin_notes": "bot_source=hud_reo_archive"},
        ]:
            self.assertIsNone(inventory_or_complaint_hold(row))

    def test_direct_promoter_holds_pending_and_verified(self):
        client = MagicMock()
        live, staging = MagicMock(), MagicMock()
        client.table.side_effect = {"homeowner_requests": live, "homeowner_requests_staging": staging}.__getitem__
        live.select.return_value.range.return_value.execute.return_value.data = []
        staging.select.return_value.in_.return_value.range.return_value.execute.return_value.data = [
            {"bot_source": source, "staging_status": status, "county": "Washington County",
             "phone": "6155551212", "property_address": "101 Main St", "owner_name_records": "Jane Smith"}
            for source in ("hud_reo", "mtn_cities_codes") for status in ("pending", "verified")]
        with patch("src.bots.auto_promoter_bot._supabase", return_value=client), \
                patch("src.bots.auto_promoter_bot.paid_data_enabled", return_value=False), \
                patch.object(AutoPromoterBot, "_report_health"):
            result = AutoPromoterBot().run()
        self.assertEqual(result["held_inventory_or_complaint"], 4)
        self.assertEqual(result["attempted"], 0)
        live.insert.assert_not_called()
