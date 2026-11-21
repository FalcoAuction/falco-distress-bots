import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class CountyTaxHeadings(unittest.TestCase):
    def build(self, county, text, distress="TAX_LIEN"):
        return TnPublicNoticeBot()._build_lead(
            {"county": county, "notice_id": "7", "excerpt": text}, distress, "Tax Sales")

    def test_noise_stays_scoped_to_county_and_lane(self):
        cases = {
            "davidson": "Notice of homeowners association lien sale WHEREAS the owner acquired property",
            "montgomery": "12612309 BY ORDER OF THE CITY OF CLARKSVILLE BOARD OF ZONING APPEALS A PUBLIC HEARING",
            "dickson": "Official ballot - State of Tennessee, Dickson County",
            "williamson": "RESOLUTION 2026-47 A RESOLUTION TO ANNEX FIVE PROPERTIES",
        }
        for county, text in cases.items():
            with self.subTest(county=county):
                self.assertIsNone(self.build(county, text))
                self.assertIsNotNone(self.build("knox", text))
                self.assertEqual(self.build(county, text, "PROBATE").distress_type, "PROBATE")
                self.assertIsNotNone(self.build(county, text + " Delinquent real property taxes"))

    def test_genuine_notices_survive(self):
        for county, text in {
            "davidson": "PROPERTY TAX SALE PURSUANT TO TENNESSEE CODE ANNOTATED SECTION 67-5-2501",
            "montgomery": "Adkins, Charles L. Etux Sarah 032I B 057.00 2833 Sharpie Drive 2023 County",
            "dickson": "IN THE CHANCERY COURT FOR DICKSON COUNTY 2014-CV-126 DELINQUENT TAXPAYERS",
            "williamson": "Notice of delinquent property tax sale Parcel 012 031.00",
        }.items():
            with self.subTest(county=county):
                self.assertEqual(self.build(county, text).distress_type, "TAX_LIEN")

    def test_mortgage_notices_route_to_foreclosure(self):
        for county, text in {
            "davidson": "Further postponement announcement of substitute trustee’s notice of sale",
            "montgomery": "Notice of Sale by Foreclosure Christina M. Bartee, Substitute Trustee",
            "williamson": "SUBSTITUTE TRUSTEE'S NOTICE OF FORECLOSURE SALE POSTPONED",
        }.items():
            with self.subTest(county=county):
                self.assertEqual(self.build(county, text).distress_type, "PRE_FORECLOSURE")

