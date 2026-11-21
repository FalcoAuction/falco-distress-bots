import unittest
from unittest.mock import patch

from src.bots.comptroller_parcels_bot import dickson_tax_hit, dickson_tax_patch


class DicksonTaxTests(unittest.TestCase):
    def test_split_sheet_requires_unique_owner_confirmation(self):
        parcel = {"owner": "MATHIS CANDICE C"}
        other = {"owner": "REAVES JODY A"}
        idx = {"104-EA-6.00": other, "104-DA-6.00": parcel}
        printed = "104E/104D-A-6.00"
        self.assertIsNone(dickson_tax_hit(idx, printed))
        with patch("src.bots.comptroller_parcels_bot._record", side_effect=dict), patch(
                "src.bots.comptroller_parcels_bot.situs_address", return_value=None):
            self.assertEqual(dickson_tax_hit(idx, printed, "Candice Mathis")["owner"], parcel["owner"])
            idx["104-EA-6.00"] = parcel
            self.assertIsNone(dickson_tax_hit(idx, printed, "Candice Mathis"))

    def test_preserve_existing_fields_and_no_invented_street_number(self):
        row = {"raw_payload": {"parcel": "1-1.02"}, "owner_name_records": "verified", "property_value": 100}
        hit = {"owner": "other", "appraisal": 150, "situs_address": None}
        self.assertEqual(set(dickson_tax_patch(row, hit)), {"raw_payload"})
