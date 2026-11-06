"""python -m unittest tests.test_davidson_parcels  (synthetic parcels)"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import davidson_parcels_bot as d  # noqa: E402


class DavidsonParcelsTests(unittest.TestCase):
    def test_street_key(self):
        self.assertEqual(d.street_key("813 Chadwell Dr, Madison, TN 37115"), "813 CHADWELL DR")
        self.assertEqual(d.street_key("124 A  Woodmont Blvd, Nashville"), "124 A WOODMONT BLVD")
        self.assertIsNone(d.street_key("Notice To Creditors Est, Nashville"))
        self.assertIsNone(d.street_key(None))
        self.assertEqual(d._sql_list(["1 O'BRIEN ST"]), "'1 O''BRIEN ST'")

    def test_pick_and_facts(self):
        f = lambda owner, appr: {"attributes": {"Owner": owner, "TotlAppr": appr, "STANPAR": "P", "OwnAddr1": "5621 EXAMPLE  BLVD",
                                                "OwnCity": "HERMITAGE", "OwnState": "TN", "OwnZip": "37076", "OwnDate": 1531717200000}}
        self.assertIsNone(d.pick([]))
        self.assertIsNone(d.pick([f("A, B", 1), f("C, D", 2)]), "different owners at one address")
        hit = d.pick([f("A, B", 100), f("A, B", 300)])
        self.assertEqual(hit["TotlAppr"], 300)
        facts = d.parcel_facts(hit)
        self.assertEqual((facts["owner_mailing"], facts["owned_since"]), ("5621 EXAMPLE BLVD, HERMITAGE TN 37076", "2018-07-16"))


if __name__ == "__main__":
    unittest.main()
