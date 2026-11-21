import unittest
from unittest.mock import Mock

from src.bots.comptroller_parcels_bot import wilson_tax_hit
from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class WilsonTaxTests(unittest.TestCase):
    def test_county_excerpt_requires_tax_and_property(self):
        def lead(text):
            return TnPublicNoticeBot()._build_lead(
                {"notice_id": "1", "county": "wilson", "excerpt": text}, "TAX_LIEN", "Delinquent Taxes")
        self.assertIsNone(lead("Towing auction at 315 South Maple Street"))
        self.assertIsNone(lead("Delinquent tax sale in courtroom 2"))
        self.assertEqual(lead("Delinquent property taxes MAP: 128 A GROUP: B PARCEL: 21.00").raw_payload["parcel"],
                         "128A-B-21.00")

    def test_ambiguous_or_special_interest_is_skipped(self):
        con = Mock()
        rows = con.execute.return_value.fetchall
        rows.return_value = [{"parcelid": "095 128A B 02100 001 2027"}]
        self.assertIsNone(wilson_tax_hit(con, "128A-B-21.00"))
        rows.return_value *= 2
        self.assertIsNone(wilson_tax_hit(con, "128A-B-21.00"))
