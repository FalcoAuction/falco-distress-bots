import unittest

from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class CarrollTaxNotices(unittest.TestCase):
    def build(self, excerpt, county="carroll", distress="TAX_LIEN", category="Tax Sales"):
        return TnPublicNoticeBot()._build_lead(
            {"notice_id": "1", "county": county, "excerpt": excerpt}, distress, category)

    def test_reviewed_non_tax_notices(self):
        for excerpt in (
            "Trezevant Board of Mayor and Aldermen hearing for a zoning map amendment",
            "The property assessment records of Carroll County will be available for public inspection",
            "NOTICE TO CREDITORS As Required by TCA 30-2-306 PROBATE CASE NO: 2026PE91",
            "A lease-to-own storage building has been left unattended on private property",
            "It is the duty of the Grand Jurors to investigate any public offense",
            "The whereabouts of the father are unknown and cannot be ascertained",
            "Whereas, the bid on the real property in this Estate has been raised",
            "Order filed in the above styled Estate, recorded in Probate Minute Book: 044",
            "CLERK AND MASTER AUCTION\nKenneth Tood\tAttorney for Estate",
        ):
            with self.subTest(excerpt=excerpt):
                self.assertIsNone(self.build(excerpt))

    def test_keeps_tax_claim_and_other_lanes(self):
        notice = "CLERK AND MASTER AUCTION, Attorney for Estate"
        self.assertEqual(self.build(notice + "; delinquent real property taxes, parcel 025-031.00").distress_type,
                         "TAX_LIEN")
        self.assertIsNotNone(self.build(notice, county="knox"))
        self.assertIsNotNone(self.build("NOTICE TO CREDITORS PROBATE CASE NO: 2026PE91",
                                        distress="PROBATE", category="Probate Notices"))
        self.assertIsNotNone(self.build("CLERK AND MASTER AUCTION; parcel 025-031.00"))

    def test_ocr_trustee_sale(self):
        notice = "TS#: 2026-21063-TN Whereas,GaryA McKennie,An Unmarried Man by Deed ofTrust"
        lead = self.build(notice)
        self.assertEqual(lead.distress_type, "PRE_FORECLOSURE")
        self.assertEqual(lead.raw_payload["filed_as"], "TAX_LIEN")
