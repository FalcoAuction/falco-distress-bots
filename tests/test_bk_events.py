"""Tests for the TN bankruptcy event classifier and lead builder. No network.

    python -m unittest tests.test_bk_events -v

Entry names are copied from the tnmb / tneb / tnwb RSS feeds of 2026-11-16.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._court_rss import (BK_ABANDONED, BK_DISMISSAL_MOTION, BK_DISMISSED,  # noqa: E402
                                 BK_STAY_RELIEF, bk_event, parse_feed)
from src.bots.bankruptcy_events_bot import BankruptcyEventsBot, build_lead  # noqa: E402


def ev(entry: str, chapter: str = "13"):
    return bk_event({"entry_type": entry, "chapter": chapter})


class ClassifyTests(unittest.TestCase):
    def test_final_dismissals(self):
        for entry in ("Dismiss for Failure to Confirm Plan - BK Order",
                      "Dismissal - BK Order",
                      "Dismiss Case for Failure to Make Plan Payments - BK Order",
                      "Dismiss Case Voluntarily - Section 1307(b) - BK Order (SA)",
                      "Dismissing Bankruptcy Case",
                      "Dismissing Case",
                      "Discharging Trustee and Closing Case ( DISMISSED 13)",
                      "Standing Ch 13 Trustee's Report and Recomm with Order Dismissing"):
            with self.subTest(entry=entry):
                self.assertEqual(ev(entry), {"type": BK_DISMISSED, "status": "order", "entry": entry})

    def test_dismissal_requests_are_never_called_dismissed(self):
        cases = {
            "Trustee's Motion To Dismiss": "motion",
            "Dismiss Case for Failure to Fund Plan (9013) - BK Motion": "motion",
            "Report of Nonappearance at MOC & Motion to Dismiss Debtor(s) - BK Motion": "motion",
            "Trustee's Motion to Dismiss Case (batch)": "motion",
            "Dismiss Case -Trustee": "motion",
            "Dismiss Case": "motion",
            # An order on the motion, outcome not in the feed.
            "Order On Trustee Motion To Dismiss": "order_on_motion",
        }
        for entry, status in cases.items():
            with self.subTest(entry=entry):
                self.assertEqual(ev(entry), {"type": BK_DISMISSAL_MOTION, "status": status, "entry": entry})

    def test_only_chapter_13_dismissals(self):
        self.assertIsNone(ev("Dismissing Case", chapter="7"))
        self.assertIsNone(ev("Dismissal - BK Order", chapter="11"))

    def test_undo_and_noise_entries_are_dropped(self):
        for entry in ("Order On Motion To Set Aside Dismissal",
                      "Plaintiff's Notice of Voluntary Dismissal of Adversary Proceeding",
                      "Objection to Motion to Dismiss",
                      "Response to Trustee's Motion to Dismiss",
                      "Withdrawal of Motion to Abandon",
                      "Order Denying Motion for Relief from Stay"):
            with self.subTest(entry=entry):
                self.assertIsNone(ev(entry))

    def test_stay_relief_orders_only(self):
        self.assertEqual(ev("Relief from Stay - BK Order", "7")["type"], BK_STAY_RELIEF)
        self.assertEqual(ev("Relief from Stay - BK Order", "7")["status"], "order")
        self.assertEqual(ev("Order on Motion for Relief From Stay")["status"], "order_on_motion")
        # A request the court may still refuse is not staged.
        self.assertIsNone(ev("Relief From Stay"))
        self.assertIsNone(ev("Relief from Stay and Adequate Protection"))
        # Co-debtor stays protect a co-signer, not the house.
        self.assertIsNone(ev("Relief from Co-Debtor Stay - BK Order"))
        self.assertIsNone(ev("Order On Motion For Relief From Co-Debtor Stay"))
        # Keeping the stay alive is the opposite signal.
        self.assertIsNone(ev("Extend Automatic Stay"))

    def test_abandonment_any_chapter(self):
        self.assertEqual(ev("Order On Motion To Abandon", "7"),
                         {"type": BK_ABANDONED, "status": "order_on_motion", "entry": "Order On Motion To Abandon"})
        self.assertEqual(ev("Notice of Intent to Abandon Property", "7")["status"], "notice")
        self.assertEqual(ev("Relief from Stay and Abandonment", "13")["type"], BK_ABANDONED)
        self.assertEqual(ev("Order Authorizing Abandonment of Real Property", "7")["status"], "order")

    def test_unrelated_entries(self):
        for entry in ("BNC Certificate of Notice", "Confirming Chapter 13 Plan - BK Order (SA)",
                      "Chapter 13 Trustee's Final Accounting And Report", None, ""):
            with self.subTest(entry=entry):
                self.assertIsNone(bk_event({"entry_type": entry, "chapter": "13"}))


FEED = """<?xml version="1.0"?><rss><channel>
<item><title>3:26-bk-04411 Jane Q Doe and John R Doe</title>
<link>https://ecf.tnmb.uscourts.gov/cgi-bin/DktRpt.pl?301001</link>
<description>Type: bk Office: 3 Chapter: 13 Trustee: Doe, Trustee [Dismissal - BK Order] (12)</description>
<pubDate>Mon, 16 Nov 2026 15:00:00 GMT</pubDate></item>
<item><title>3:26-bk-04412 Sam Smith</title>
<link>https://ecf.tnmb.uscourts.gov/cgi-bin/DktRpt.pl?301002</link>
<description>Type: bk Office: 3 Chapter: 13 Trustee: Doe, Trustee [Confirming Chapter 13 Plan - BK Order (SA)]</description>
<pubDate>Mon, 16 Nov 2026 15:01:00 GMT</pubDate></item>
</channel></rss>"""


class LeadTests(unittest.TestCase):
    def test_build_lead(self):
        bot = BankruptcyEventsBot()
        items = parse_feed(FEED)
        lead = build_lead(bot, "tnmb", items[0])
        self.assertEqual(lead.distress_type, BK_DISMISSED)
        self.assertEqual(lead.full_name, "Jane Q Doe")
        self.assertEqual(lead.owner_name_records, "Jane Q Doe; John R Doe")
        self.assertEqual(lead.raw_payload["court_id"], "tnmb")
        self.assertEqual(lead.raw_payload["state"], "TN")
        self.assertEqual(lead.raw_payload["bk_event"]["status"], "order")
        self.assertIn("Chapter 13 case dismissed", lead.admin_notes)
        self.assertIsNone(build_lead(bot, "tnmb", items[1]))

    def test_one_lead_per_case_and_event(self):
        bot = BankruptcyEventsBot()
        item = parse_feed(FEED)[0]
        a = build_lead(bot, "tnmb", item)
        b = build_lead(bot, "tnmb", {**item, "entry_type": "Trustee's Motion To Dismiss"})
        self.assertNotEqual(a.pipeline_lead_key, b.pipeline_lead_key)
        self.assertEqual(a.pipeline_lead_key, build_lead(bot, "tnmb", item).pipeline_lead_key)


if __name__ == "__main__":
    unittest.main()
