import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class ReviewedTaxNotices(unittest.TestCase):
    def build(self, county, text, distress="TAX_LIEN"):
        return TnPublicNoticeBot()._build_lead(
            {"notice_id": "test", "county": county, "excerpt": text}, distress, "Tax Sales")

    def test_rate_hearing_requires_the_reviewed_county_text_and_lane(self):
        notice = ("NOTICE OF INTEST TO EXCEED CERTIFIED TAX RATE The City of Red Boiling Springs "
                  "Council will conduct a public hearing on August 13th, 2026 at 6:45 pm, on the "
                  "City of Red Boiling Springs intent to exceed the certified property")
        self.assertIsNone(self.build("macon", notice))
        self.assertIsNotNone(self.build("clay", notice))
        self.assertIsNotNone(self.build("macon", notice, "PROBATE"))
        self.assertIsNotNone(self.build("macon", notice + " Delinquent tax exhibit: Map 012 Parcel 030.00"))

    def test_tax_sale_with_parcel_survives(self):
        notice = "Cumberland County delinquent tax parcels: 077L-F-002.00"
        self.assertEqual(self.build("cumberland", notice).distress_type, "TAX_LIEN")

    def test_misfiled_trustee_sale_preserves_foreclosure(self):
        notice = (
            'SUBSTITUTE TRUSTEE’S NOTICE OF SALE WHEREAS, REBA H. REA, a widow, executed a Promissory '
            'Note in favor of National Bank of Tennessee dated September 9, 2003 in the original amount '
            'of $25,000.00 (hereinafter referred to as the “Note”), which Note is more fully described '
            'in and secured by a Deed ...'
        )
        self.assertEqual(self.build("cocke", notice).distress_type, "PRE_FORECLOSURE")
