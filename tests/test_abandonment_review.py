import unittest
from src.bots.bankruptcy_events_bot import BankruptcyEventsBot, build_lead

class AbandonmentReviewTests(unittest.TestCase):
    def test_headings_keep_outcome_unverified(self):
        item = {"caption": "Example Debtor", "case_number": "26-123", "chapter": "7"}
        for entry, stage in [("Order On Motion To Abandon", "order_outcome_unverified"),
                             ("Relief from Stay and Abandonment", "request_filed")]:
            lead = build_lead(BankruptcyEventsBot(), "tneb", item | {"entry_type": entry})
            self.assertEqual(lead.raw_payload["abandonment_review"]["stage"], stage)
            self.assertNotIn("Trustee abandoning property", lead.admin_notes)
            self.assertEqual(lead.distress_type, "BK_ABANDONED")
