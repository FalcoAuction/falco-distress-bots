"""python -m unittest tests.test_landlord_portfolio  (synthetic parcels)"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import landlord_portfolio_bot as lp  # noqa: E402

P = lambda pid, lu, v, own=946684800000, state="TN": {"STANPAR": pid, "LUDesc": lu, "TotlAppr": v, "PropAddr": f"{pid} EXAMPLE ST",
                                                      "OwnAddr1": "1 OWNER RD", "OwnCity": "NASHVILLE", "OwnState": state, "OwnZip": "37201", "OwnDate": own}
CASES = [{"case": "26-1", "status": "OPEN"}, {"case": "26-2", "status": "OPEN"}]


class PortfolioTests(unittest.TestCase):
    def test_portfolio_row(self):
        row = lp.build_portfolio("EXAMPLE, PAT", [P("1", "SINGLE FAMILY", 200000), P("2", "DUPLEX", 300000),
                                                  P("2", "DUPLEX", 300000), P("3", "COMMERCIAL", 900000)], CASES, today=date(2026, 1, 1))
        self.assertEqual((row["parcel_count"], row["residential_count"], row["residential_value"], row["total_value"]), (3, 2, 500000, 1400000))
        self.assertEqual((row["expected_fee"], row["owned_since"], row["out_of_area"], row["is_company"]), (10000.0, "2000-01-01", False, False))
        self.assertEqual(row["tired_score"], round(2 * 3 + 2 + 4, 2))
        self.assertEqual([p["parcel"] for p in row["parcels"]], ["2", "1"])

    def test_filters(self):
        self.assertIsNone(lp.build_portfolio("EXAMPLE, PAT", [P("1", "SINGLE FAMILY", 1)], CASES))
        self.assertIsNone(lp.build_portfolio("METROPOLITAN DEVELOPMENT & HOUSING AUTHORITY", [P("1", "SINGLE FAMILY", 1), P("2", "SINGLE FAMILY", 1)], CASES))
        many = [P(str(i), "SINGLE FAMILY", 100000) for i in range(41)]
        self.assertIsNone(lp.build_portfolio("BIG RENTALS LLC", many, CASES))
        row = lp.build_portfolio("EXAMPLE RENTALS LLC", [P("1", "SINGLE FAMILY", 1), P("2", "SINGLE FAMILY", 1, state="CA")], CASES)
        self.assertTrue(row["is_company"])
        self.assertEqual(lp._sql_list(["O'NEIL, A"]), "'O''NEIL, A'")
        two = [P("1", "SINGLE FAMILY", 1), P("2", "SINGLE FAMILY", 1)]
        self.assertIsNone(lp.build_portfolio("WOODBINE COMMUNITY ORGANIZATION", two, CASES))
        self.assertTrue(lp.build_portfolio("HJL, L.P.", two, CASES)["is_company"])
        self.assertTrue(lp.build_portfolio("EXAMPLE, L.L.C.", two, CASES)["is_company"])
        self.assertFalse(lp.build_portfolio("HELP, PAT", two, CASES)["is_company"])


if __name__ == "__main__":
    unittest.main()
