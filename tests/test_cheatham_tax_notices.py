import unittest
from datetime import date

from src.bots.court_notices_bot import cheatham_tax_tracts
from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class CheathamTaxTests(unittest.TestCase):
    def test_sale_scope_date_and_unknown_owner(self):
        today = date(2026, 11, 21)
        lines = ["Chancery Court for Cheatham County", "Civil Action No. 19076", "NOTICE OF DELINQUENT TAX SALE",
                 "highest and best bidder at 10:00 a.m. on Wednesday, December 16, 2026,",
                 "A. Estate Property- Parcel 027-008.00 (05-027-008.00-000), having an address of Spring Creek Road, TN 37035- Current Owners - Unknown, though possibly vested in heirs - $4569.80"]
        row = {"notice_id": "CL1", "lines": lines}
        lead, = cheatham_tax_tracts(row, today, today)
        self.assertEqual(lead.raw_payload["published_judgment"], 4569.8)
        self.assertIsNone(lead.property_address)
        self.assertIsNone(lead.owner_name_records)
        self.assertEqual(lead.trustee_sale_date, "2026-12-16")
        self.assertEqual(cheatham_tax_tracts(row, today, date(2026, 12, 17)), [])
        lines[2] = "NOTICE OF DELINQUENT TAX LAWSUIT"
        self.assertEqual(cheatham_tax_tracts(row, today, today), [])

    def test_short_excerpt_is_not_a_tract(self):
        def build(excerpt):
            return TnPublicNoticeBot()._build_lead({"county": "cheatham", "notice_id": "1", "excerpt": excerpt},
                                                   "TAX_LIEN", "Delinquent Taxes")
        self.assertIsNone(build("Civil Action No. 19076 Cheatham County vs. multiple taxpayers"))
        self.assertIsNone(build("TN-44027307 Cheatham County Planning Commission Meeting Agenda"))
        self.assertIsNotNone(build("Civil Action No. 19076 Parcel 027-008.00"))
