"""Tests for the FAA aircraft overlay matcher. No network.

    python -m unittest tests.test_aircraft_match -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._aircraft_match import (  # noqa: E402
    AircraftIndex, district_of, entity_key, load_aircraft, load_ref, parse_addr, person_keys,
    street_key, units_of, value_band,
)

MASTER_HEADER = ("N-NUMBER,SERIAL NUMBER,MFR MDL CODE,ENG MFR MDL,YEAR MFR,TYPE REGISTRANT,NAME,STREET,STREET2,"
                 "CITY,STATE,ZIP CODE,REGION,COUNTY,COUNTRY,LAST ACTION DATE,CERT ISSUE DATE,CERTIFICATION,"
                 "TYPE AIRCRAFT,TYPE ENGINE,STATUS CODE,MODE S CODE,FRACT OWNER,AIR WORTH DATE,OTHER NAMES(1),"
                 "OTHER NAMES(2),OTHER NAMES(3),OTHER NAMES(4),OTHER NAMES(5),EXPIRATION DATE,UNIQUE ID,KIT MFR,"
                 " KIT MODEL,MODE S CODE HEX,")
REF = ["CODE,MFR,MODEL,TYPE-ACFT,TYPE-ENG,AC-CAT,BUILD-CERT-IND,NO-ENG,NO-SEATS,AC-WEIGHT,SPEED,TC-DATA-SHEET,TC-DATA-HOLDER,",
       "JET01,CIRRUS DESIGN CORP ,SF50 ,4,5 ,1,0,01,007,CLASS 1,0000,,,",
       "TWN01,BEECH ,58 ,5,1 ,1,0,02,006,CLASS 1,0000,,,",
       "SNG01,CESSNA ,150M ,4,1 ,1,0,01,002,CLASS 1,0000,,,"]


def master_row(n, mdl, year, name, street, city, zipc, county, type_acft="4", type_eng="5", cert="1N",
               street2="", others=()):
    o = list(others) + [""] * (5 - len(others))
    cols = [n, "SER" + n, mdl, "", year, "1", name, street, street2, city, "TN", zipc, "2", county, "US",
            "20260101", "20200101", cert, type_acft, type_eng, "V", "", "", "", *o, "20300101", "", "", "", "", ""]
    return ",".join(cols)


def build(rows):
    ref = load_ref(REF)
    return AircraftIndex(load_aircraft([MASTER_HEADER] + rows, ref))


JET = master_row("722AP", "JET01", "2018", "REARDEN CAPITAL GROUP INC", "96 JAMESTOWN PARK", "NASHVILLE",
                 "37205", "037")
BARON = master_row("4590S", "TWN01", "1975", "CONNER CHAD E", "595 STILLWATER DR", "PARIS", "38242", "079",
                   type_acft="5", type_eng="1")


class HelperTests(unittest.TestCase):
    def test_street_key_normalizes_suffix_and_direction(self):
        self.assertEqual(street_key("207 S Idlewild Street", "38104"), street_key("207 IDLEWILD ST", "38104"))

    def test_street_key_ignores_unit_text(self):
        self.assertEqual(street_key("600 12TH AVE S UNIT 1000", "37203"), "600|12TH|37203")

    def test_units(self):
        self.assertEqual(units_of("7022 SHALLOWFORD RD STE 1 # 146"), frozenset({"1", "146"}))
        self.assertEqual(units_of("96 JAMESTOWN PARK"), frozenset())

    def test_parse_addr_keeps_unit_part(self):
        st, city, z = parse_addr("600 12TH AVE S, Unit 509, NASHVILLE, 37203")
        self.assertEqual((units_of(st), city, z), (frozenset({"509"}), "NASHVILLE", "37203"))

    def test_person_keys_both_orders(self):
        keys = person_keys("Chad E Conner")
        self.assertIn(("CONNER", "CHAD", "E"), keys)
        self.assertEqual(person_keys("ACME HOLDINGS LLC"), [])

    def test_entity_key(self):
        self.assertEqual(entity_key("Basa, Inc."), entity_key("BASA INC"))
        self.assertIsNone(entity_key("John Smith"))

    def test_district(self):
        self.assertEqual(district_of("henry"), "tnwb")
        self.assertEqual(district_of("cumberland"), "tnmb")
        self.assertEqual(district_of("hamilton"), "tneb")

    def test_value_bands(self):
        ref = load_ref(REF)
        base = {"type_acft": "4", "type_eng": "1", "cert": "1N"}
        self.assertEqual(value_band(ref["JET01"], {**base, "type_eng": "5", "year": "2018"}), "A")
        self.assertEqual(value_band(ref["TWN01"], {**base, "type_acft": "5", "year": "1975"}), "B")
        self.assertIsNone(value_band(ref["SNG01"], {**base, "year": "1975"}))
        self.assertEqual(value_band(ref["SNG01"], {**base, "year": "2005"}), "B")
        self.assertIsNone(value_band(ref["SNG01"], {**base, "year": "2005", "cert": "4E"}))   # experimental
        self.assertIsNone(value_band(ref["SNG01"], {**base, "year": "2005", "type_acft": "1"}))  # glider


class MatchTests(unittest.TestCase):
    def test_address_match_on_house(self):
        idx = build([JET])
        tags = idx.match({"id": "1", "distress_type": "PRE_FORECLOSURE", "full_name": "Benjamin Holman",
                          "property_address": "96 Jamestown Park, Nashville, TN 37205"})
        self.assertEqual([(t["n_number"], t["match_type"], t["confidence"]) for t in tags],
                         [("N722AP", "address", "high")])

    def test_condo_different_unit_is_not_a_match(self):
        tbm = master_row("93BN", "JET01", "2010", "PETER LITCHFIELD M D LLC", "600 12TH AVE S UNIT 1000",
                         "NASHVILLE", "37203", "037")
        idx = build([tbm])
        self.assertEqual(idx.match({"id": "1", "distress_type": "BANKRUPTCY",
                                    "property_address": "600 12TH AVE S, Unit 509, NASHVILLE, 37203"}), [])
        self.assertEqual(idx.match({"id": "1", "distress_type": "PRE_FORECLOSURE",
                                    "property_address": "600 12Th Ave S, Nashville, TN 37203"}), [])

    def test_mail_drop_suite_is_not_a_match(self):
        warbird = master_row("75BB", "TWN01", "1952", "WARBIRDS 1 LLC", "7022 SHALLOWFORD RD STE 1 # 146",
                             "CHATTANOOGA", "37421", "065", type_acft="5", type_eng="1")
        idx = build([warbird])
        self.assertEqual(idx.match({"id": "1", "distress_type": "TAX_LIEN",
                                    "full_name": "MAIL BOX STORE OF CHATT THE",
                                    "mailing_address": "7022 SHALLOWFORD RD STE 1, CHATTANOOGA TN37421"}), [])

    def test_crowded_address_is_skipped(self):
        rows = [master_row(f"{i}AB", "JET01", "2015", f"JET {i} LLC", "832 GEORGIA AVE STE 1100",
                           "CHATTANOOGA", "37402", "065") for i in range(5)]
        idx = build(rows)
        self.assertEqual(idx.match({"id": "1", "distress_type": "TAX_LIEN",
                                    "property_address": "832 GEORGIA AVE STE 1100, Chattanooga, TN 37402"}), [])

    def test_bankruptcy_name_needs_district_and_middle(self):
        idx = build([BARON])
        hit = idx.match({"id": "1", "distress_type": "BANKRUPTCY", "full_name": "Chad E Conner",
                         "case_name": "Chad E Conner and Gail D Dunlap-Conner", "court_id": "tnwb"})
        self.assertEqual([(t["n_number"], t["match_type"]) for t in hit], [("N4590S", "name_middle_bk_district")])
        self.assertEqual(idx.match({"id": "1", "distress_type": "BANKRUPTCY", "full_name": "Chad E Conner",
                                    "court_id": "tneb"}), [])
        self.assertEqual(idx.match({"id": "1", "distress_type": "BANKRUPTCY", "full_name": "Chad Conner",
                                    "court_id": "tnwb"}), [])

    def test_creditor_parties_never_match(self):
        heli = master_row("901TN", "JET01", "1975", "TN DEPT OF PUBLIC SAFETY", "225 EZELL PIKE", "NASHVILLE",
                          "37217", "037", type_acft="6", type_eng="3")
        idx = build([heli])
        self.assertEqual(idx.match({"id": "1", "distress_type": "BANKRUPTCY", "full_name": "John Q Doe",
                                    "case_name": "TN Dept of Public Safety", "court_id": "tnmb"}), [])

    def test_probate_estate_name_with_geo(self):
        bonanza = master_row("36PY", "TWN01", "2000", "MOORE GARY E", "22 VICTORIA WAY", "CROSSVILLE",
                             "38558", "035", type_acft="5", type_eng="1")
        idx = build([bonanza])
        tags = idx.match({"id": "1", "distress_type": "PROBATE", "county": "cumberland",
                          "excerpt": "NOTICE TO CREDITORS ESTATE OF GARY E. MOORE, DECEASED ..."})
        self.assertEqual([(t["n_number"], t["match_type"]) for t in tags], [("N36PY", "name_middle_geo")])

    def test_entity_name_match(self):
        duke = master_row("341D", "TWN01", "1976", "BASA INC", "1 OTHER RD", "HARRISON", "37341", "065",
                          type_acft="5", type_eng="1")
        idx = build([duke])
        tags = idx.match({"id": "1", "distress_type": "TAX_LIEN", "full_name": "BASA INC"})
        self.assertEqual([t["match_type"] for t in tags], ["entity_name"])


if __name__ == "__main__":
    unittest.main()
