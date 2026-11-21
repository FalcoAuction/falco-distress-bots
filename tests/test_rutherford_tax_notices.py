import unittest
from datetime import date

from src.bots.main_street_media_bot import MainStreetMediaBot
from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class RutherfordTaxTests(unittest.TestCase):
    def test_hoa_sale_is_not_a_rutherford_tax_notice(self):
        row = {"county": "rutherford", "notice_id": "1",
               "excerpt": "NOTICE OF HOMEOWNERS ASSOCIATION LIEN SALE WHEREAS, Example"}
        bot = TnPublicNoticeBot()
        self.assertIsNone(bot._build_lead(row, "TAX_LIEN", "Delinquent Taxes"))
        row["excerpt"] = "Delinquent property tax sale of parcel 092-046.15 at 4013 Example Lane"
        self.assertIsNotNone(bot._build_lead(row, "TAX_LIEN", "Delinquent Taxes"))

    def test_repeat_service_notice_is_one_case_with_unverified_ownership(self):
        lines = ["DELINQUENT PROPERTY TAX NOTICE AND ORDER FOR PUBLICATION",
                 "INDIVIDUAL CASE NO. 25MB-42 TO: Example Defendant TAKE NOTICE",
                 "property taxes for the year 2023 and 2024, inclusive, are unpaid",
                 "map/group/control map/parcel # as 092/04615, Account Number R0108783, "
                 "which property is also known by the address of 4013 Example Lane, Murfreesboro, Tennessee"]
        bot = MainStreetMediaBot()
        a, = bot._leads("issue1", "rutherford", date(2026, 8, 11), 1, lines)
        b, = bot._leads("issue2", "rutherford", date(2026, 8, 18), 2, lines)
        self.assertEqual(a.pipeline_lead_key, b.pipeline_lead_key)
        self.assertEqual(a.raw_payload["tax_years"], ["2023", "2024"])
        self.assertEqual(a.raw_payload["named_defendant"], "Example Defendant")
        self.assertIsNone(a.owner_name_records)
        self.assertIsNone(a.trustee_sale_date)
