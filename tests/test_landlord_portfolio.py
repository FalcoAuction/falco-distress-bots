"""python -m unittest tests.test_landlord_portfolio  (synthetic parcels)"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import landlord_portfolio_bot as lp  # noqa: E402


def P(pid, residential=True, value=200000, owned=date(2000, 1, 1), state="TN"):
    return {"parcel": pid, "owner": "X", "address": f"{pid} EXAMPLE ST", "city": "NASHVILLE", "land_use": "SINGLE FAMILY",
            "residential": residential, "value": value, "acres": 0.2, "mail_lines": ["1 OWNER RD"], "mail_city": "NASHVILLE",
            "mail_state": state, "mail_zip": "37201", "owned_since": owned}


TODAY = date(2026, 1, 1)


class PortfolioTests(unittest.TestCase):
    def test_signal_portfolio(self):
        row = lp.build_portfolio("davidson", "EXAMPLE, PAT", [P("1"), P("2", value=300000), P("2"), P("3", residential=False, value=900000)],
                                 {"code_housing": 2, "tax": 1}, ["26-1", "26-2"], today=TODAY)
        self.assertEqual((row["parcel_count"], row["residential_count"], row["residential_value"]), (3, 2, 500000))
        self.assertEqual((row["expected_fee"], row["owned_since"], row["passive"], row["signal_count"]), (9000.0, "2000-01-01", False, 3))
        self.assertEqual(row["tired_score"], 2 * 3 + 4 + 2 + 4)
        self.assertEqual([p["parcel"] for p in row["parcels"]], ["2", "1"])

    def test_passive_requires_size_distance_and_age(self):
        five = lambda **k: [P(str(i), **k) for i in range(5)]
        self.assertIsNone(lp.build_portfolio("rutherford", "EXAMPLE PAT", five(), {}, [], today=TODAY), "in-state, no signal")
        self.assertIsNone(lp.build_portfolio("rutherford", "EXAMPLE PAT", five(state="CA", owned=date(2020, 1, 1)), {}, [], today=TODAY))
        row = lp.build_portfolio("rutherford", "EXAMPLE PAT", five(state="CA"), {}, [], today=TODAY)
        self.assertTrue(row["passive"] and row["out_of_area"])
        self.assertIsNone(lp.build_portfolio("rutherford", "EXAMPLE PAT", [P("1"), P("2", state="CA")], {}, [], today=TODAY))

    def test_filters_and_company(self):
        two = [P("1"), P("2")]
        sig = {"foreclosure": 1}
        self.assertIsNone(lp.build_portfolio("davidson", "EXAMPLE, PAT", [P("1")], sig, []))
        for name in ("METROPOLITAN DEVELOPMENT & HOUSING AUTHORITY", "WOODBINE COMMUNITY ORGANIZATION",
                     "PROGRESS RESIDENTIAL BORROWER 6 LLC", "INVITATION HOMES 4 LP", "DREES PREMIER HOMES, INC",
                     "FEDERAL NATIONAL MORTGAGE ASSOCIATION", "BNA DEVELOPMENT, LLC", "BELMONT BUILDS LLC"):
            self.assertIsNone(lp.build_portfolio("davidson", name, two, sig, []), name)
        self.assertIsNone(lp.build_portfolio("davidson", "BIG RENTALS LLC", [P(str(i)) for i in range(41)], sig, []))
        self.assertTrue(lp.build_portfolio("davidson", "HJL, L.P.", two, sig, [])["is_company"])
        self.assertFalse(lp.build_portfolio("davidson", "HELP, PAT", two, sig, [])["is_company"])
        self.assertIsNotNone(lp.build_portfolio("davidson", "EXAMPLE 2026 TENNESSEE COMMUNITY PROPERTY TRUST", two, sig, []))

    def test_lead_signal_and_helpers(self):
        self.assertEqual(lp.lead_signal({"bot_source": "nashville_codes", "sub": "Codes Housing Request", "status": "OPEN"}), "code_housing")
        self.assertIsNone(lp.lead_signal({"bot_source": "nashville_codes", "sub": "Codes High Grass and Weeds", "status": "OPEN"}))
        self.assertEqual(lp.lead_signal({"bot_source": "x", "distress_type": "TRUSTEE_NOTICE"}), "foreclosure")
        self.assertIsNone(lp.lead_signal({"bot_source": "davidson_demolition", "distress_type": "DEMOLITION"}))
        self.assertEqual(lp._money("$193,000"), 193000.0)
        self.assertEqual(lp.street_key("107 BEECH ST, Montgomery County, TN"), "107 BEECH ST")
        m = lp._montgomery({"ParcelID": 2048714, "Owner1": "Example  Pat", "AssessmentClass": "RS", "AppraisedValue": "$193,000",
                            "SalesDate": 1224828000000, "MailState": "TN"})
        self.assertEqual((m["parcel"], m["owner"], m["residential"], m["value"], m["owned_since"]),
                         ("2048714", "EXAMPLE PAT", True, 193000.0, date(2008, 10, 24)))


if __name__ == "__main__":
    unittest.main()
