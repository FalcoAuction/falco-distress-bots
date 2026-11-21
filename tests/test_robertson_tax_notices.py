import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class RobertsonTaxNotices(unittest.TestCase):
    def lead(self, text, county="robertson"):
        return TnPublicNoticeBot()._build_lead({"notice_id": "1", "county": county, "excerpt": text},
                                              "TAX_LIEN", "Delinquent Taxes")

    def test_budget_heading(self):
        heading = "ROBERTSON COUNTY, TENNESSEE NOTICE OF PUBLIC HEARING FOR THE BUDGET FOR THE YEAR ENDING JUNE 30, 2027 ACTUAL ACTUAL"
        self.assertIsNone(self.lead(heading))

    def test_caption_only(self):
        text = ("IN THE CHANCERY COURT FOR ROBERTSON COUNTY, TENNESSEE, AT SPRINGFIELD "
                "CONSOLIDATED CIVIL ACTION NUMBERS: CH23-CV-121 (2021), CH24-CV-146 (2022); "
                "ROBERTSON COUNTY, TENNESSEE vs. DE- LINQUENT TAXPAYERS FOR ROBERTSON COUNTY and CITY OF S ...")
        self.assertIsNone(self.lead(text))
        self.assertIsNotNone(self.lead(text, "dickson"))
        self.assertIsNotNone(self.lead(text.replace("S ...", "SPRINGFIELD. Parcel: 091B-D-050.00")))
