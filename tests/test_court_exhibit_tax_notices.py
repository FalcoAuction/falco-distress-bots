import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class CourtExhibitTaxNotices(unittest.TestCase):
    def build(self, county, excerpt, distress="TAX_LIEN"):
        return TnPublicNoticeBot()._build_lead(
            {"notice_id": "1", "county": county, "excerpt": excerpt}, distress, "Tax Sales")

    def test_rejects_only_reviewed_non_tax_headings(self):
        notices = [
            ("hardin", "Page 7A www.courieranywhere.com THURSDAY OCTOBER 29, 2026 SENATE JOINT RESOLUTION NO. 25 A RESOLUTION"),
            ("houston", "CHANCERY COURT AUCTION FRIDAY Jerry Wimberly, et al Vs Docket 2018-CV-687"),
            ("trousdale", "DAVENPORT CHANCERY COURT SALE IN HARTSVILLE Acting pursuant to the decree of the Probate Court"),
            ("stewart", "PUBLIC NOTICE Resolution 517-26 Notice of a resolution to annex certain territory"),
        ]
        for county, notice in notices:
            with self.subTest(county=county):
                self.assertIsNone(self.build(county, notice))
                self.assertIsNotNone(self.build(county, notice + " Delinquent property tax sale, parcel 001 004.00"))
                self.assertIsNotNone(self.build("lincoln", notice))
                self.assertIsNotNone(self.build(county, notice, "PROBATE"))

    def test_misfiled_montgomery_sale(self):
        notice = ("PUBLIC NOTICE - LAND SALE FOR DELINQUENT TAXES SALE OF REAL PROPERTY "
                  "BY THE CHANCERY COURT OF MONTGOMERY COUNTY, TENNESSEE")
        self.assertIsNone(self.build("hardin", notice))
        self.assertIsNotNone(self.build("hardin", notice.replace("MONTGOMERY", "HARDIN")))
