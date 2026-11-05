"""python -m unittest tests.test_asset_profile"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._asset_profile import asset_profile  # noqa: E402
from src.bots import _owner_lookup as ol  # noqa: E402


class AssetProfileTests(unittest.TestCase):
    def test_tiers(self):
        self.assertIsNone(asset_profile({}))
        self.assertIsNone(asset_profile({"owner_match": {"status": "ambiguous", "parcels": [{"parcel": "1"}]}}))
        home = asset_profile({"padctn_owner_match": {"parcel": "1", "appraised": 300000, "land_use": "R11 - SINGLE FAMILY", "acres": 0.3}})
        self.assertEqual((home["tier"], home["parcel_count"], home["total_value"]), ("single_residence", 1, 300000))
        farm = asset_profile({"owner_match": {"status": "matched", "parcels": [
            {"parcel": "1", "acres": 3.5, "value": 100000}, {"parcel": "2", "acres": 2.0, "value": "$50,000"}]}})
        self.assertEqual((farm["tier"], farm["total_acres"], farm["total_value"], farm["is_multi_parcel"]), ("land", 5.5, 150000, True))
        two = asset_profile({"owner_match": {"status": "matched", "parcels": [{"parcel": "1", "acres": 0.2}, {"parcel": "2", "acres": 0.3}]}})
        self.assertEqual(two["tier"], "multi_parcel")
        vacant = asset_profile({"padctn_owner_match": {"parcel": "1", "land_use": "VACANT RESIDENTIAL LAND"}})
        self.assertEqual(vacant["tier"], "land")
        shop = asset_profile({"padctn_owner_match": {"parcel": "1", "land_use": "COMMERCIAL - RETAIL"}})
        self.assertEqual(shop["tier"], "commercial")


class DavidsonMatchTests(unittest.TestCase):
    def test_same_person_two_parcels_is_one_estate(self):
        c = lambda owner, parcel, value: ol.Candidate(owner=owner, address=f"{parcel} Example", parcel=parcel, value=value)
        r = ol.match_candidates("Norma Jean Example", [c("EXAMPLE, NORMA J.", "1", 100000), c("EXAMPLE, NORMA J.", "2", 250000),
                                                       c("EXAMPLE, NORMA D.", "3", 900000)], "Example, Norma")
        self.assertEqual((r["status"], len(r["parcels"]), r["primary"]["parcel"]), ("matched", 2, "2"))
        self.assertEqual(ol.match_candidates("Norma Jean Example", [c("EXAMPLE, NORMA D.", "3", 1)], "q")["status"], "not_found")
        self.assertEqual(ol.match_candidates("Cher", [], "q")["status"], "unparseable_name")


if __name__ == "__main__":
    unittest.main()
