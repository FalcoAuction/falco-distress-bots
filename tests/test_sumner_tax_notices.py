import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class SumnerTaxNotices(unittest.TestCase):
    def build(self, excerpt, county="sumner"):
        return TnPublicNoticeBot()._build_lead(
            {"notice_id": "1", "county": county, "excerpt": excerpt}, "TAX_LIEN", "Delinquent Taxes")

    def test_planning_agenda_and_post_sale_hearing(self):
        for heading in ("CITY OF WHITE HOUSE Meeting of the Municipal Planning Commission",
                        "PUBLIC NOTICE DISTRIBUTION OF EXCESS PROCEEDS FROM THE 2024 SUMNER COUNTY DELINQUENT TAX SALE"):
            self.assertIsNone(self.build(heading))
            self.assertIsNotNone(self.build(heading, county="robertson"))

    def test_parcel_and_foreclosure(self):
        self.assertEqual(self.build("DELINQUENT TAX SALE PARCEL NO: 011-009.00").raw_payload["parcel"], "011-009.00")
        self.assertEqual(self.build("NOTICE OF SUBSTITUTE TRUSTEE'S SALE").distress_type, "PRE_FORECLOSURE")
