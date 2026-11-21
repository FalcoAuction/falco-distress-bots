import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class MauryTaxNotices(unittest.TestCase):
    def build(self, text, county="maury"):
        return TnPublicNoticeBot()._build_lead(
            {"county": county, "notice_id": "1", "excerpt": text}, "TAX_LIEN", "Delinquent Taxes")

    def test_zoning_heading_is_local_to_maury(self):
        text = "LEGAL NOTICE OF ZONING MAP AMENDMENTS MAURY COUNTY LEGISLATIVE BODY"
        self.assertIsNone(self.build(text))
        self.assertIsNotNone(self.build(text, county="dickson"))

    def test_hearing_about_a_specific_tax_sale_is_preserved(self):
        text = ("LEGAL NOTICE OF PUBLIC HEARING MAURY COUNTY LEGISLATIVE BODY "
                "Notice is hereby given that the Maury County Legislative Body will hold a Public Hearing")
        self.assertIsNone(self.build(text))
        self.assertIsNotNone(self.build(text + " on the tax sale of Parcel No: 001-003.00"))
