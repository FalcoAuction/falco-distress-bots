"""Tests for the Pinellas Clerk daily file adapter. No network. Rows copy the
clerk's layout (2026-11-16 files) with made-up names.

    python -m unittest tests.test_fl_pinellas -v
"""

from __future__ import annotations

import os
import sys
import unittest
from datetime import date
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._fl_clerk import civil_case, group_cases, probate_case, read_rows  # noqa: E402
from src.bots._fl_pinellas import (civil_rows, civil_type, file_day, probate_rows,  # noqa: E402
                                   probate_type, read_probate)

CIVIL_HEADER = ('"Case Category","Case Type","Case Number","Title","Case Create Date","Party Type","Party First Name",'
                '"Party Middle Name","Party Last Name/Company Name","Party Address","Bar Number","Attorney First Name",'
                '"Attorney Middle Name","Attorney Last Name"')


def civ(ctype, case, ptype, first, mid, last, addr, bar="NO ATTORNEY", af="", am="", al=""):
    return f'"CV","{ctype}","{case}","LENDER LLC Vs. DOE","2026-11-16","{ptype}","{first}","{mid}","{last}","{addr}","{bar}","{af}","{am}","{al}"'


HOMESTEAD = "REAL PROP/MTGE FRCL HOMESTEAD RES3 $250,000 OR MORE"
CIVIL = "\n".join([
    CIVIL_HEADER,
    civ(HOMESTEAD, "26-000001-CI", "PLAINTIFF", "", "", "SAMPLE MORTGAGE SERVICING INC", "1 MAIN ST, DALLAS TX 75201",
        "12345", "AMY", "B", "LAWYER"),
    civ(HOMESTEAD, "26-000001-CI", "DEFENDANT", "", "", "SAMPLE HOMEOWNERS ASSOCIATION INC", "9 POOL RD, CLEARWATER FL 33755"),
    civ(HOMESTEAD, "26-000001-CI", "DEFENDANT", "", "", "SAMPLE ROOFING INC", "8 NAIL ST, LARGO FL 33770"),
    civ(HOMESTEAD, "26-000001-CI", "DEFENDANT", "", "", "UNKNOWN TENANT 1", ""),
    civ(HOMESTEAD, "26-000001-CI", "DEFENDANT", "PATRICIA", "Q", "DOE", "100 BAY DR, CLEARWATER FL 33755"),
    civ("REAL PROP - MORTGAGE FORECLOSURE - COUNTY  UP TO $15,000", "26-000002-CC", "DEFENDANT", "SAM", "", "ROE",
        "5 GULF BLVD UNIT 3, MADEIRA BEACH FL 33708"),
    civ("AUTO NEGLIGENCE - CIRCUIT", "26-000003-CI", "DEFENDANT", "AL", "", "DRIVER", "1 ROAD, TAMPA FL 33601"),
])

PROBATE = (
    "Case Category,Case Type,Case Number,Title,Case Create Date,Decedent's First Name,Decedent's Middle Name,"
    "Decedent's Last Name,Decedent's Date of Birth,Decedent's Date of Death,Rep or Petitioner Type,"
    "Rep or Petitioner First Name,Rep or Petitioner's Middle Name,Rep or Petitioner's Last Name,"
    "Rep or Petitioner Attorney's First Name,Rep or Petitioner Attorney's Middle Name,"
    "Rep or Petitioner Attorney 's Last Name,Rep or Petitioner Attorney's Address,Uniform Case Number\n"
    # Unquoted comma in the title, as the clerk writes it.
    "PR,SUMMARY ADMINISTRATION OVER $1000,26-000010-ES,IN RE: THE ESTATE OF JOHN W DOE, SR,11/16/2026,JOHN,W,DOE SR,,"
    "03/05/2025,PETITIONER,MARY,,DOE,TED,R,COUNSEL ESQ,COUNSEL LAW FIRM 1 MAIN ST LARGO FL 33774,522026CP000010XXESPR\n"
    # And one in the attorney address.
    "PR,FORMAL ADMINISTRATION,26-000011-ES,IN RE: THE ESTATE OF ANN ROE,11/16/2026,ANN,,ROE,,08/25/2026,"
    "PERSONAL REPRESENTATIVE,BOB,,ROE,EVE,,ATTY,ATTY LAW PA, 2 OAK ST, CLEARWATER FL 33755,522026CP000011XXESPR\n"
    "PR,SUMMARY ADMINISTRATION UNDER $1000,26-000012-ES,IN RE: THE ESTATE OF SMALL ONE,11/16/2026,SMALL,,ONE,,,"
    "PETITIONER,KIN,,ONE,,,,,522026CP000012XXESPR\n"
)


class TypeTests(unittest.TestCase):
    def test_civil_types(self):
        self.assertEqual(civil_type(HOMESTEAD), "Mortgage Foreclosure - Homestead $250,000 OR MORE")
        self.assertEqual(civil_type("REAL PROP/MTGE FRCL NON-HOMESTEAD RES2 $50,001 - $249,999"),
                         "Mortgage Foreclosure - NonHomestead $50,001 - $249,999")
        self.assertEqual(civil_type("REAL PROP/MTGE FRCL COMMERCIAL3 $250,000 OR MORE"),
                         "Mortgage Foreclosure - Commercial $250,000 OR MORE")
        self.assertTrue(civil_type("REAL PROP - MORTGAGE FORECLOSURE - COUNTY  UP TO $15,000")
                        .startswith("CC Real Property/Mortgage Foreclosure"))
        self.assertEqual(civil_type("AUTO NEGLIGENCE - CIRCUIT"), "AUTO NEGLIGENCE - CIRCUIT")

    def test_probate_types(self):
        self.assertEqual(probate_type("SUMMARY ADMINISTRATION OVER $1000"), "Summary Administration Greater than $1000")
        self.assertEqual(probate_type("FORMAL ADMINISTRATION"), "Formal Administration")

    def test_file_day(self):
        self.assertEqual(file_day("CivilNewFilingsDaily_11-16-2026.csv"), date(2026, 11, 16))
        self.assertIsNone(file_day("README.docx"))


class CivilTests(unittest.TestCase):
    def setUp(self):
        self.cases = group_cases(civil_rows(read_rows(CIVIL)))

    def test_borrower_beats_lienholder_companies(self):
        c = civil_case("26-000001-CI", self.cases["26-000001-CI"])
        self.assertEqual(c["kind"], "mortgage")
        self.assertEqual(c["occupancy"], "homestead")
        self.assertEqual(c["owners"][0], "PATRICIA Q DOE")
        self.assertEqual(c["owner_address"], "100 BAY DR, CLEARWATER FL 33755")
        self.assertEqual(c["plaintiff"], "SAMPLE MORTGAGE SERVICING INC")
        self.assertEqual(c["plaintiff_attorney"], "AMY B LAWYER")
        self.assertEqual(c["filed"], "2026-11-16")

    def test_county_court_and_noise(self):
        self.assertEqual(civil_case("26-000002-CC", self.cases["26-000002-CC"])["kind"], "association_lien")
        self.assertIsNone(civil_case("26-000003-CI", self.cases["26-000003-CI"]))


class ProbateTests(unittest.TestCase):
    def test_comma_in_title_and_address(self):
        rows = read_probate(PROBATE)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["Title"], "IN RE: THE ESTATE OF JOHN W DOE, SR")
        self.assertEqual(rows[0]["Decedent's Last Name"], "DOE SR")
        self.assertEqual(rows[0]["Uniform Case Number"], "522026CP000010XXESPR")
        self.assertEqual(rows[1]["Rep or Petitioner Attorney's Address"], "ATTY LAW PA, 2 OAK ST, CLEARWATER FL 33755")

    def test_cases(self):
        cases = group_cases(probate_rows(read_probate(PROBATE)))
        a = probate_case("26-000010-ES", cases["26-000010-ES"])
        self.assertEqual(a["decedent"], "JOHN W DOE SR")
        self.assertEqual(a["date_of_death"], "2025-03-05")
        self.assertEqual(a["petitioner"], "MARY DOE")
        self.assertEqual(a["attorney"], "TED R COUNSEL ESQ")
        self.assertEqual(probate_case("26-000011-ES", cases["26-000011-ES"])["petitioner"], "BOB ROE")
        # Summary administration under $1000 is not worth a lead.
        self.assertIsNone(probate_case("26-000012-ES", cases["26-000012-ES"]))


if __name__ == "__main__":
    unittest.main()
