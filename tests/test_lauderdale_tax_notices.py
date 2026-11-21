import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class LauderdaleTaxNotices(unittest.TestCase):
    def build(self, text, county="lauderdale", distress="TAX_LIEN"):
        return TnPublicNoticeBot()._build_lead(
            {"notice_id": "1", "county": county, "excerpt": text}, distress, "Tax Sales")

    def test_civic_notices(self):
        for text in (
            "Request for bids for surplus property\nSealed Bids are invited and will be received by the City of Ripley",
            "PUBLIC NOTICE NOTICE OF INTENT TO DESTROY IMPOUNDED PROPERTY Lauderdale County Sheriff’s Office",
            "NOTICE Pursuant to Section 67-5-58, Tennessee Code Annotated, the property assessment records "
            "of Lauderdale County will be available for public inspection.",
            "NOTICE OF INTENT TO EXCEED CERTIFIED TAX RATE The City of Ripley",
        ):
            with self.subTest(text=text):
                self.assertIsNone(self.build(text))
                self.assertIsNotNone(self.build(text, county="knox"))
                self.assertIsNotNone(self.build(text, distress="PROBATE"))
                self.assertIsNotNone(self.build(text + " Delinquent property tax sale for Parcel No. 010 002.00"))

    def test_keeps_orders_of_publication(self):
        for plaintiff, case in (("Lauderdale County", 16696), ("Town of Gates", 16696),
                                ("City of Ripley", 16697), ("Town of Halls", 16698)):
            with self.subTest(plaintiff=plaintiff):
                self.assertIsNotNone(self.build(
                    f"IN THE CHANCERY COURT OF LAUDERDALE COUNTY, TENNESSEE {plaintiff} EX REL STATE OF "
                    f"TENNESSEE v. DELINQUENT TAXPAYERS CONSOLIDATED DELINQUENT TAXPAYERS FOR YEAR 2024 "
                    f"No. {case} ORDER OF PUBLICATION"))
