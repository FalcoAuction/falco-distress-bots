"""Tests for the Comptroller parcel index: address keys, owner-name parsing,
matching, and how the lane classifier reads the matched record. No network,
no county files: a tiny SQLite index is built in a temp dir.

    python -m unittest tests.test_comptroller -v
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._comptroller import (SCHEMA, ParcelIndex, county_key, lead_first_last,  # noqa: E402
                                   street_key_from_comptroller, street_key_from_lead)
from src.bots._property_lane import property_lane  # noqa: E402


def parcel(county, address, owner, cls="00 RESIDENTIAL", city="PORTLAND", appraisal=200000, impval=150000,
           bldgs=1, owner2="", state="TN", acres=0.5):
    from src.bots._comptroller import name_tokens
    return (county, f"P-{address}-{owner}", street_key_from_comptroller(address), city, owner, owner2,
            (name_tokens(owner) or [None])[0], (name_tokens(owner2) or [None])[0], "1 MAIN ST", "NASHVILLE", state, "37201", cls, "", appraisal,
            impval, acres, bldgs, "2020-01-01", 100000, "", "", address)


class KeyTests(unittest.TestCase):
    def test_number_after_street_matches_number_first(self):
        self.assertEqual(street_key_from_comptroller("MAGNOLIA SPRINGS RD 1078"),
                         street_key_from_lead("1078 Magnolia Springs Road, Portland, TN 37148"))

    def test_highway_token_order(self):
        self.assertEqual(street_key_from_comptroller("31W HWY 6110"), street_key_from_lead("6110 Highway 31W, Portland, TN"))

    def test_units_and_directions(self):
        self.assertEqual(street_key_from_lead("12 North Main Street Apt 4, Gallatin, TN"),
                         street_key_from_comptroller("N MAIN ST 12"))

    def test_no_number(self):
        self.assertIsNone(street_key_from_comptroller("JOE MILLER DR"))
        self.assertIsNone(street_key_from_lead("Main St, Gallatin, TN"))

    def test_county_key(self):
        self.assertEqual(county_key("Sumner County"), "SUMNER")
        self.assertEqual(county_key("van buren"), "VANBUREN")
        self.assertEqual(county_key("DeKalb"), "DEKALB")

    def test_first_last(self):
        self.assertEqual(lead_first_last("John A Smith"), ("JOHN", "SMITH"))
        self.assertEqual(lead_first_last("Smith, John A"), ("JOHN", "SMITH"))
        self.assertEqual(lead_first_last("Estate of Mary Jones, Deceased"), ("MARY", "JONES"))
        self.assertIsNone(lead_first_last("John and Mary Smith"))
        from src.bots._comptroller import lead_middle_initial as lmi
        self.assertEqual((lmi("Linda A. Gibson"), lmi("Michael David Wiggins"), lmi("Melissa Marino")), ("A", "D", None))


class IndexTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = os.path.join(self.tmp.name, "i.sqlite")
        con = sqlite3.connect(db)
        con.executescript(SCHEMA)
        rows = [
            parcel("SUMNER", "MAGNOLIA SPRINGS RD 1078", "HIGH TIDE INC", cls="08 COMMERCIAL"),
            parcel("SUMNER", "MAIN ST 100", "DOE JANE", city="GALLATIN"),
            parcel("SUMNER", "MAIN ST 100", "ROE RICHARD", city="HENDERSONVILLE"),
            parcel("SUMNER", "OAK DR 5", "SMITH JOHN A"),
            parcel("SUMNER", "OAK DR 7", "SMITH JOHN A", appraisal=50000, impval=0, bldgs=0),
            parcel("SUMNER", "ELM DR 9", "SMITH JOHNATHAN"),
            parcel("SUMNER", "PINE DR 1", "BROWN MARY"),
            parcel("SUMNER", "PINE DR 2", "BROWN MARY E & ROBERT"),
            parcel("WILSON", "CEDAR LN 44", "LAKEVIEW HOLDINGS LLC", cls="11 AGRICULTURAL", state="FL", acres=22),
        ]
        con.executemany(f"INSERT INTO parcels VALUES ({','.join('?' * 23)})", rows)
        con.commit()
        con.close()
        self.idx = ParcelIndex(db)

    def tearDown(self):
        self.idx.con.close()
        self.tmp.cleanup()

    def test_address_match(self):
        hit = self.idx.match_address("1078 Magnolia Springs Rd, Portland, TN 37148", "Sumner County")
        self.assertEqual((hit["owner"], hit["method"], hit["county"]), ("HIGH TIDE INC", "address", "SUMNER"))

    def test_ambiguous_street_uses_city(self):
        self.assertEqual(self.idx.match_address("100 Main St, Gallatin, TN", "sumner")["owner"], "DOE JANE")
        self.assertIsNone(self.idx.match_address("100 Main St, Portland, TN", "sumner"))

    def test_uncovered_county_returns_none(self):
        self.assertIsNone(self.idx.match_address("1078 Magnolia Springs Rd, Nashville, TN", "Davidson"))

    def test_county_from_city_when_missing(self):
        self.assertEqual(self.idx.match_address("100 Main St, Gallatin, TN", None)["owner"], "DOE JANE")

    def test_owner_match_unique_owner_multi_parcel(self):
        hit = self.idx.match_owner("John A Smith", "sumner")
        self.assertEqual((hit["method"], hit["parcel_count"], hit["owner"]), ("owner_name", 2, "SMITH JOHN A"))
        self.assertEqual(hit["appraisal"], 200000)          # most valuable parcel first

    def test_owner_names_person(self):
        from src.bots._comptroller import owner_names_person as onp
        self.assertTrue(onp("CURRY J B & VIRGIE MAE", "CURRY", "VIRGIE", "M"))
        self.assertTrue(onp("AEBERSOLD MONICA ETVIR MARK C", "AEBERSOLD", "MARK", "C"))
        self.assertTrue(onp("SCHWABE KIRK & SCHWABE JANET S ETUX", "SCHWABE", "JANET", None))
        self.assertTrue(onp("BAKER MARY E", "BAKER", "MARY", "E"))
        self.assertFalse(onp("GRAVES AMANDA CAROL", "GRAVES", "CAROL", "J"))
        self.assertFalse(onp("BRATCHER KATHRYN PARKER", "BRATCHER", "KATHRYN", "E"))
        self.assertFalse(onp("JONES MARY", "SMITH", "MARY", None))
        self.assertFalse(onp("GIBSON LINDA K", "GIBSON", "LINDA", "A"))
        self.assertFalse(onp("WIGGINS MICHAEL P", "WIGGINS", "MICHAEL", "D"))

    def test_address_without_comma_before_city(self):
        hit = self.idx.match_address("1078 Magnolia Springs Rd Portland, TN, 37148", "sumner")
        self.assertEqual(hit["owner"], "HIGH TIDE INC")

    def test_assessor_style_lead_address(self):
        self.assertEqual(self.idx.match_address("MAGNOLIA SPRINGS RD 1078", "sumner")["owner"], "HIGH TIDE INC")

    def test_owner_match_refuses_two_owner_strings(self):
        self.assertIsNone(self.idx.match_owner("Mary Brown", "sumner"))

    def test_owner_match_requires_first_name_token(self):
        self.assertIsNone(self.idx.match_owner("Jon Smith", "sumner"))

    def test_lane_reads_comptroller_record(self):
        hit = self.idx.match_address("44 Cedar Lane, Lebanon, TN", "wilson")
        lane = property_lane({"id": "1", "distress_type": "TAX_LIEN", "owner_name_records": hit["owner"],
                              "raw_payload": {"comptroller_parcel": hit}, "phone_metadata": {}})
        self.assertEqual(lane["lane"], "land")
        self.assertTrue(lane["contact"]["out_of_state"])
        self.assertEqual(lane["grade"], "B")

    def test_vacant_residential_parcel_is_land(self):
        hit = self.idx.match_address("7 Oak Drive, Portland, TN", "sumner")
        lane = property_lane({"id": "1", "distress_type": "PROBATE", "raw_payload": {"comptroller_parcel": hit},
                              "phone_metadata": {}})
        self.assertEqual((lane["lane"], lane["subtype"]), ("land", "vacant_residential"))

    def test_slucm_land_use_codes(self):
        mk = lambda lu: property_lane({"id": "1", "raw_payload": {"comptroller_parcel": {"land_use": lu}},
                                       "phone_metadata": {}})["lane"]
        self.assertEqual(mk("11 - HOUSEHOLD UNITS"), "residential")
        self.assertEqual(mk("53 - RETAIL TRADE - GENERAL"), "commercial")
        self.assertEqual(mk("52 - RETAIL TRADE - BLDG AND FARM"), "commercial")
        self.assertEqual(mk("91 - UNDEVELOPED AND UNUSED LAND AREA"), "land")
        self.assertEqual(mk("81:91 - MULTIPLE LAND USES"), "land")
        self.assertEqual(mk("11:81 - MULTIPLE LAND USES"), "residential")
        self.assertEqual(mk("11:74 - MULTIPLE LAND USES"), "residential")
        self.assertEqual(mk("11:62 - MULTIPLE LAND USES"), "commercial")
        self.assertEqual(mk("14 - MOBILE HOME PARKS OR COURTS"), "commercial")
        self.assertEqual(mk("67 - GOVERNMENTAL AGENCIES"), "unknown")

    def test_religious_class_church_owner(self):
        rec = {"class": "05 RELIGIOUS", "appraisal": 400000, "improvement_value": 300000, "buildings": 1,
               "owner_mailing": "1 A ST, GALLATIN TN 37066", "owner_state": "TN"}
        lane = property_lane({"id": "1", "distress_type": "TAX_LIEN", "owner_name_records": "FIRST BAPTIST CHURCH",
                              "raw_payload": {"comptroller_parcel": rec}, "phone_metadata": {}})
        self.assertEqual((lane["lane"], lane["subtype"]), ("commercial", "church"))


if __name__ == "__main__":
    unittest.main()
