"""Tests for the bankruptcy debtor -> parcel matcher. No network; a tiny
in-memory index with made-up owners in the Comptroller's layout.

    python -m unittest tests.test_bk_match -v
"""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._bk_match import (_parcel_base, district_counties, match_debtors, names_person_loosely,  # noqa: E402
                                person, situs_address)
from src.bots._comptroller import SCHEMA, name_tokens, street_key_from_comptroller  # noqa: E402


def row(county, parcel, owner, owner2="", address="", city=None, mailaddr="", mailcity="", zip_="",
        cls="00 RESIDENTIAL", appraisal=200000.0):
    o, o2 = owner.upper(), owner2.upper()
    return (county, parcel, street_key_from_comptroller(address), city, o, o2, (name_tokens(o) or [None])[0],
            (name_tokens(o2) or [None])[0], mailaddr, mailcity, "TN", zip_, cls, "11 - HOUSEHOLD UNITS", appraisal,
            0.0, 0.0, 1, "", 0.0, "", "", address)


PARCELS = [
    # A couple, two tax years of the same parcel and a spelling drift.
    row("WILSON", "001 001 00100 000 2026", "DOE JOHN A", "AND WIFE JANE B DOE", "MAIN ST 12", "LEBANON",
        "12 MAIN ST", "LEBANON", "37087"),
    row("WILSON", "001 001 00100 000 2027", "DOE JOHN A", "AND WIFE JANE B DOE", "MAIN ST 12", "LEBANON",
        "12 MAIN ST", "LEBANON", "37087"),
    row("WILSON", "001 001 00200 000 2027", "DOE JOHN", "AND WIFE JANE DOE", "MAIN ST 0", None,
        "12 MAIN ST", "LEBANON", "37087", cls="00 RESIDENTIAL", appraisal=40000.0),
    # A different John Doe in another district county: a single-name search
    # for "John Doe" must not guess between them.
    row("MAURY", "002 002 00100 000 2027", "DOE JOHN", "", "OAK AVE 5", "COLUMBIA", "PO BOX 9", "COLUMBIA", "38401"),
    # A single filer with a middle initial, unique in the district.
    row("SUMNER", "003 003 00100 000 2027", "ROE RICHARD Q", "", "ELM DR 77", "GALLATIN", "77 ELM DR", "GALLATIN",
        "37066", appraisal=310000.0),
    # Unique name, no middle initial on record.
    row("SUMNER", "003 003 00200 000 2027", "ZYX PAT", "", "PINE RD 3", "GALLATIN", "PO BOX 1", "GALLATIN", "37066"),
    # Eastern district county, never searched for a Middle TN case.
    row("ANDERSON", "004 004 00100 000 2027", "ROE RICHARD Q", "", "BAY RD 1", "CLINTON", "1 BAY RD", "CLINTON", "37716"),
]


def index() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    con.executemany(f"INSERT INTO parcels VALUES ({','.join('?' * 23)})", PARCELS)
    con.execute("CREATE INDEX ix_owner ON parcels(county, owner_first)")
    con.execute("CREATE INDEX ix_owner2 ON parcels(county, owner2_first)")
    return con


MIDDLE = ["WILSON", "MAURY", "SUMNER"]


class HelperTests(unittest.TestCase):
    def test_district_counties(self):
        avail = ["WILSON", "MAURY", "SUMNER", "ANDERSON", "SHELBY", "TIPTON"]
        self.assertEqual(district_counties("tnmb", avail), ["MAURY", "SUMNER", "WILSON"])
        self.assertEqual(district_counties("tnwb", avail), ["SHELBY", "TIPTON"])
        self.assertEqual(district_counties("tneb", avail), ["ANDERSON"])
        self.assertEqual(district_counties("flsb", avail), [])

    def test_person_and_names(self):
        self.assertEqual(person("Jane B Doe"), ("JANE", "DOE", "B"))
        self.assertIsNone(person("Doe Holdings LLC and Another"))
        self.assertTrue(names_person_loosely("DOE JOHN AND WIFE JANE B DOE", "JANE", "DOE"))
        self.assertEqual(_parcel_base("001 001    00100 000 2027"), "001 001 00100 000")
        # "AND" inside a surname is not a spouse separator.
        from src.bots._bk_match import middle_conflicts
        self.assertFalse(middle_conflicts("ANDERSON MARY K", "ANDERSON", "MARY", "K"))
        self.assertTrue(middle_conflicts("ANDERSON MARY K", "ANDERSON", "MARY", "J"))


class MatchTests(unittest.TestCase):
    def setUp(self):
        self.con = index()

    def test_joint_filing_finds_the_couple(self):
        hit = match_debtors(self.con, ["John A Doe", "Jane B Doe"], MIDDLE)
        self.assertEqual(hit["confidence"], "joint")
        self.assertEqual(hit["county"], "WILSON")
        # Two parcels (tax-year duplicates collapsed); the house, not the lot.
        self.assertEqual(hit["parcel_count"], 2)
        self.assertEqual(hit["situs_address"], "12 MAIN ST, LEBANON, TN 37087")

    def test_single_common_name_is_ambiguous(self):
        self.assertIsNone(match_debtors(self.con, ["John Doe"], MIDDLE))

    def test_single_with_middle_initial(self):
        hit = match_debtors(self.con, ["Richard Q Roe"], MIDDLE)
        self.assertEqual(hit["confidence"], "middle_initial")
        self.assertEqual(hit["situs_address"], "77 ELM DR, GALLATIN, TN 37066")
        # The Anderson parcel is in another district and was never searched.
        self.assertEqual(hit["county"], "SUMNER")

    def test_conflicting_middle_initial_is_rejected(self):
        self.assertIsNone(match_debtors(self.con, ["Richard X Roe"], MIDDLE))

    def test_unique_without_middle_is_name_only(self):
        hit = match_debtors(self.con, ["Pat Zyx"], MIDDLE)
        self.assertEqual(hit["confidence"], "name_only")
        # Owner mails to a PO box, so the city comes from the parcel and there is no ZIP.
        self.assertEqual(situs_address(self.con.execute(
            "SELECT * FROM parcels WHERE owner='ZYX PAT'").fetchone()), "3 PINE RD, GALLATIN, TN")

    def test_no_counties_or_names(self):
        self.assertIsNone(match_debtors(self.con, ["John A Doe"], []))
        self.assertIsNone(match_debtors(self.con, [], MIDDLE))


if __name__ == "__main__":
    unittest.main()
