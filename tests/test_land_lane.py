"""Tests for the land lane rules. No network.

    python -m unittest tests.test_land_lane -v
"""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._land import person_name, signals, street_address  # noqa: E402

TODAY = date(2026, 11, 20)


def tract(**kw):
    row = {"owner": "SMITH JOHN A", "owner2": "", "calcac": 85.0, "appraisal": 420000, "class": "11 AGRICULTURAL",
           "landuse": "81 - AGRICULTURE AND RELATED ACTIVITIES", "saledate": "19880817", "state": "TN"}
    row.update(kw)
    return row


class LandSignalTests(unittest.TestCase):
    def test_estate_owner_is_heirs(self):
        self.assertEqual(signals(tract(owner="SMITH JOHN A ESTATE"), TODAY), ["heirs"])

    def test_etal_needs_a_long_hold(self):
        self.assertEqual(signals(tract(owner="INGRAM SAM H ETAL", saledate="19861231"), TODAY), ["family_etal"])
        self.assertEqual(signals(tract(owner="COLLIER KESHA S ETAL", saledate="20250820"), TODAY), [])

    def test_absentee_long_hold(self):
        self.assertEqual(signals(tract(state="GA", saledate="1995"), TODAY), ["absentee"])
        self.assertEqual(signals(tract(state="GA", saledate="2015"), TODAY), [])

    def test_plain_local_owner_has_no_signal(self):
        self.assertEqual(signals(tract(), TODAY), [])

    def test_size_value_class_and_entity_filters(self):
        heirs = dict(owner="SMITH JOHN ESTATE")
        self.assertEqual(signals(tract(calcac=12, **heirs), TODAY), [])
        self.assertEqual(signals(tract(appraisal=90000, **heirs), TODAY), [])
        self.assertEqual(signals(tract(appraisal=9_000_000, **heirs), TODAY), [])
        self.assertEqual(signals(tract(**{"class": "08 COMMERCIAL"}, **heirs), TODAY), [])
        self.assertEqual(signals(tract(owner="GREEN ACRES LLC ETAL", saledate="1980"), TODAY), [])
        self.assertEqual(signals(tract(owner="FIRST BAPTIST CHURCH ETAL", saledate="1980"), TODAY), [])

    def test_life_estate_and_real_estate_are_not_heirs(self):
        self.assertEqual(signals(tract(owner="MCDOLE EMMIE LIFE ESTATE"), TODAY), [])
        self.assertEqual(signals(tract(owner="FOX FAMILY REAL ESTATE LLC"), TODAY), [])
        self.assertEqual(signals(tract(owner="SMITH JOHN ESTATE OF"), TODAY), ["heirs"])

    def test_street_address_number_first(self):
        self.assertEqual(street_address("S COMMERCE RD 4185"), "4185 S COMMERCE RD")
        self.assertEqual(street_address("STEWARTS FERRY PK"), "STEWARTS FERRY PK")
        self.assertEqual(street_address("4185 S COMMERCE RD"), "4185 S COMMERCE RD")

    def test_names(self):
        self.assertEqual(person_name("LANE LOIS ETAL ERNESTINE LANE"), "Lois Lane")
        self.assertEqual(person_name("JOHNSON EDMOND D LE ET AL"), "Edmond D Johnson")
        self.assertEqual(person_name("BATEMAN DANNY RAY ETAL"), "Danny Ray Bateman")
        self.assertEqual(person_name("TAGG WILLIAM T JR"), "William T Tagg Jr")
        self.assertEqual(person_name("SMITH JOHN A ESTATE"), "Smith John A Estate")
        self.assertEqual(person_name("EAKIN JAMES D III ET AL"), "James D Eakin III")
        self.assertEqual(person_name("TANG, TIANLAI & DONG, CHRISTINE"), "Tianlai Tang")

    def test_county_gis_rows_use_the_same_rules(self):
        from src.bots._land_gis import davidson_row, rutherford_row
        r = rutherford_row({"Owner1": "WOODSON LINDY ARCHER ESTATE OF", "CALCACRES": 122.1, "TotalValue": 395200,
                            "SaleDate": 671864400000, "MailingState": "TN", "AccountType": "11 - Agricultural",
                            "ParcelID": "1", "CITY": "BELL BUCKLE"})
        self.assertEqual(signals(r, TODAY), ["heirs"])
        d = davidson_row({"Owner": "SMITH, JOHN ET AL", "Acres": 40, "TotlAppr": 600000, "OwnDate": 600000000000,
                          "OwnState": "TN", "LUDesc": "VACANT RURAL LAND", "PropAddr": "100 LONG RD"})
        self.assertEqual(signals(d, TODAY), ["family_etal"])
        store = davidson_row({"Owner": "SMITH, JOHN ET AL", "Acres": 40, "TotlAppr": 600000, "OwnDate": 600000000000,
                              "OwnState": "TN", "LUDesc": "ONE STORY GENERAL RETAIL STORE"})
        self.assertEqual(signals(store, TODAY), [])


if __name__ == "__main__":
    unittest.main()
