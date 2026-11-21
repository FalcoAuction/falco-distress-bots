import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class HendersonTaxNotices(unittest.TestCase):
    def build(self, text, county="henderson"):
        return TnPublicNoticeBot()._build_lead(
            {"county": county, "notice_id": "1", "excerpt": text}, "TAX_LIEN", "Tax Sales")

    def test_reviewed_civic_and_quiet_title_notices(self):
        for text in (
            "PUBLIC NOTICE Henderson County is undergoing a Current Value Update",
            "Notice of Intent to exceed certified tax rate\nThe Town of Scotts Hill",
            "The City of Scotts Hill, Tennessee, hereby provides certain financial information for the Fiscal Year 2027",
            "TO: \tANY UNKNOWN HEIRS OF MARY L. HUNT, claiming an interest in Gowan Road/Gurley Creek Road",
        ):
            with self.subTest(text=text):
                self.assertIsNone(self.build(text))
                self.assertIsNotNone(self.build(text, "knox"))
                self.assertIsNotNone(self.build(text + " Notice of tax sale, parcel 037 002.01"))

    def test_preserve_tax_exhibits_and_route_trustee_sale(self):
        self.assertEqual(self.build("PUBLIC NOTICE 2024 Delinquent Property Taxpayers").distress_type, "TAX_LIEN")
        sale = self.build("NOTICE OF SUBSTITUTE TRUSTEE?S SALE STATE OF TENNESSEE, HENDERSON COUNTY")
        self.assertEqual(sale.distress_type, "PRE_FORECLOSURE")
        self.assertEqual(sale.raw_payload["filed_as"], "TAX_LIEN")
