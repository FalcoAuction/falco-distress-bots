"""Tests for sorting court-order and public-sale notices. No network.
Excerpts follow real notice wording with made-up names.

    python -m unittest tests.test_court_order -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._court_order import kind, parties, tenants_address  # noqa: E402

LENDER = ("IN THE CHANCERY COURT OF MONTGOMERY COUNTY, TENNESSEE MIDFIRST BANK, Plaintiff, v. Case No. MCCHCVRE-25-46 "
          "HARLAN ROH EDWARDS; MARY EDWARDS; TENANTS OF 2203SPRINGLOT ROAD, CLARKSVILLE, TN (IF ANY); THE SECRETARY ...")
BOOKED = ("CLERK AND MASTER AUCTION SATURDAY, NOVEMBER 14TH AT 10:00 AM Kenneth Todd Taylor Real Estate & Auction Inc "
          "Dwayne D. Maddox III Clerk & Master")
PARTITION = ("IN THE CHANCERY COURT FOR SUMNER COUNTY, TENNESSEE JOHN A DOE, Petitioner, v. JANE B DOE and MARK C DOE, "
             "Respondents. Petition for partition and sale of real property at 100 Main St, Gallatin")
ORDER_OF_SALE = ("IN THE CHANCERY COURT FOR WILSON COUNTY Pursuant to an Order of Sale entered in the cause, the Clerk and "
                 "Master will sell the real property of the late Jane Roe")
DIVORCE = ("ORDER OF PUBLICATION In the Chancery Court of Greene County, Tennessee Sonia Tomas, Plaintiff vs. Antonio "
           "Diaz, Defendant. Complaint for Divorce")
STORAGE = "Notice of Self Storage Sale Please take notice Stor-Alot Self Storage intends to hold a public sale"
TOW = "NOTICE Affordable Towing is auctioning off 2014 Dodge Vin # 2C4RDGBG5ER410594"


class CourtOrderTests(unittest.TestCase):
    def test_kinds(self):
        self.assertEqual(kind(LENDER), "PRE_FORECLOSURE")
        self.assertEqual(kind(PARTITION), "PARTITION")
        self.assertEqual(kind(ORDER_OF_SALE), "COURT_SALE")
        self.assertEqual(kind(DIVORCE), "DIVORCE")
        self.assertIsNone(kind(BOOKED))
        self.assertIsNone(kind(STORAGE))
        self.assertIsNone(kind(TOW))

    def test_tenants_address(self):
        self.assertEqual(tenants_address(LENDER), "2203 Springlot Road, Clarksville, TN")
        self.assertIsNone(tenants_address(PARTITION))

    def test_parties_skip_companies_and_tenants(self):
        self.assertEqual(parties(LENDER), ["HARLAN ROH EDWARDS", "MARY EDWARDS"])
        self.assertEqual(parties(PARTITION), ["JOHN A DOE", "JANE B DOE", "MARK C DOE"])


if __name__ == "__main__":
    unittest.main()
