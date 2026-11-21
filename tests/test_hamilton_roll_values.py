import unittest
from src.bots.hamilton_assessor_bot import tax_roll_parcel, may_update_roll_value

class HamiltonRollTests(unittest.TestCase):
    def test_roll_identification(self):
        raw = {"county": "hamilton", "cumulative_owed": 500, "parcel": "PER--42"}
        self.assertEqual(tax_roll_parcel(raw), "PER--42")
        self.assertIsNone(tax_roll_parcel({"county": "hamilton", "parcel": "123"}))
        self.assertEqual(tax_roll_parcel({"hamilton_roll_valuation": {"parcel": "119D-E-015C048"}}), "119D-E-015C048")

    def test_protect_other_sources(self):
        self.assertTrue(may_update_roll_value(None))
        self.assertTrue(may_update_roll_value("xref_avm_enricher"))
        self.assertFalse(may_update_roll_value("manual_appraisal"))
