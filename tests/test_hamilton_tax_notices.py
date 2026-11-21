import unittest
from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class HamiltonTaxNoticeTests(unittest.TestCase):
    def lead(self, text, county="hamilton"):
        return TnPublicNoticeBot()._build_lead({"notice_id": "1", "county": county, "excerpt": text},
                                              "TAX_LIEN", "Delinquent Taxes")

    def test_docket_is_not_a_property(self):
        self.assertIsNone(self.lead("Tax cause county docket #11256 Hamilton County and State of Tennessee"))
        self.assertIsNone(self.lead("Board meeting at 123 Main Street"))
        lead = self.lead("Delinquent tax sale. Docket 11256 Hamilton County. Parcel: 168O A 017")
        self.assertEqual(lead.raw_payload["parcel"], "168O A 017")
        self.assertIsNone(lead.property_address)

    def test_trustee_heading_uses_named_venue(self):
        lead = self.lead("NOTICE OF SUBSTITUTE TRUSTEE’S SALE STATE OF TENNESSEE, MARION COUNTY")
        self.assertEqual((lead.distress_type, lead.county), ("PRE_FORECLOSURE", "marion"))
