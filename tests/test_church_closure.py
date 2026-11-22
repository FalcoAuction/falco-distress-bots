"""Tests for the church closure resolution parser. No network.

    python -m unittest tests.test_church_closure -v
"""

from __future__ import annotations

import sys
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots.church_closure_bot import ChurchClosureBot, closures  # noqa: E402

DOC = (
    "Resolution to Close Port Royal United Methodist Church Tennessee-Western Kentucky Conference WHEREAS, the "
    "worshipping community known as Port Royal United Methodist Church, located at, 2947 Highway 76 Adams, Tennessee "
    "37010, Montgomery County, Tennessee, in the Cumberland River District of the Tennessee- Western Kentucky "
    "Conference has faithfully served; WHEREAS, a final service of worship was held on June 28, 2026, and "
    "Resolution to Close Lebanon United Methodist Church WHEREAS, the worshipping community known as Lebanon United "
    "Methodist Church, located at 631 Tige Hopper Rd, Jackson 38305 in Madison County, Tennessee, in the Tennessee "
    "River District of the Conference "
    "Resolution to Close Christ United Methodist Church WHEREAS, the worshipping community known as Christ United "
    "Methodist Church, located at 1322 West Broadway, Mayfield, Kentucky 42066, Graves County in the Purchase "
    "District of the Conference"
)


class ChurchClosureTests(unittest.TestCase):
    @patch("src.bots.church_closure_bot.CLOSURE_PDFS", [])
    @patch("src.bots.church_closure_bot._city_index", return_value={})
    def test_saved_resolutions_identify_church_without_claiming_a_seller(self, _index):
        bot = ChurchClosureBot()
        leads = bot.scrape()
        self.assertTrue(leads)
        for lead in leads:
            church = lead.raw_payload["church_closure"]
            self.assertEqual(lead.pipeline_lead_key, bot.make_lead_key(
                bot.name, f"{church['church']}|{church['address']}"))
            self.assertTrue(lead.source_url)
            self.assertIsNone(lead.owner_name_records)
            self.assertIsNone(lead.full_name)
            self.assertNotIn("seller", lead.raw_payload)

    def test_parses_each_format(self):
        cs = closures(DOC)
        self.assertEqual([c["county"] for c in cs], ["montgomery", "madison", "graves"])
        self.assertEqual([c["state"] for c in cs], ["TN", "TN", "KY"])
        self.assertEqual(cs[0]["address"], "2947 Highway 76 Adams, 37010")
        self.assertEqual(cs[0]["final_service"], "June 28, 2026")
        self.assertEqual(cs[1]["address"], "631 Tige Hopper Rd, Jackson 38305")


if __name__ == "__main__":
    unittest.main()
