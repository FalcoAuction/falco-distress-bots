"""Tests for staging new bankruptcy petitions from the court RSS feeds.

    python -m unittest tests.test_bk_petitions -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots.bankruptcy_events_bot import BankruptcyEventsBot, petition_lead  # noqa: E402


class PetitionTests(unittest.TestCase):
    def test_voluntary_petition_becomes_bankruptcy_lead(self):
        item = {"case_number": "3:26-bk-05619", "caption": "JANE Q DOE and JOHN R DOE", "entry_type": "Voluntary Petition (Chapter 7)",
                "chapter": "7", "trustee": "LEMEH, EVA M", "docket_url": "https://ecf.tnmb.uscourts.gov/x", "published": "2026-11-20T02:28:40+00:00"}
        lead = petition_lead(BankruptcyEventsBot(), "tnmb", item)
        self.assertEqual(lead.distress_type, "BANKRUPTCY")
        self.assertEqual(lead.owner_name_records, "JANE Q DOE; JOHN R DOE")
        self.assertEqual(lead.raw_payload["chapter"], "7")
        self.assertEqual(lead.raw_payload["trustee"], "LEMEH, EVA M")
        self.assertEqual(lead.raw_payload["docket_number"], "3:26-bk-05619")
        self.assertEqual(lead.raw_payload["court_id"], "tnmb")

    def test_other_entries_are_not_petitions(self):
        item = {"case_number": "3:26-bk-05619", "caption": "JANE DOE", "entry_type": "Meeting of Creditors 7 (No Asset)", "chapter": "7"}
        self.assertIsNone(petition_lead(BankruptcyEventsBot(), "tnmb", item))


if __name__ == "__main__":
    unittest.main()
