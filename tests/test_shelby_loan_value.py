import unittest

from src.bots._memphis_hpi import MEMPHIS_HPI
from src.bots.shelby_loan_value_bot import LATEST_Q, LTV_ASSUMED, dot_ref, estimate, quarter_of


class ShelbyLoanValueTests(unittest.TestCase):
    def test_quarter(self):
        self.assertEqual(quarter_of("2021-08-11"), "2021Q3")
        self.assertIsNone(quarter_of("not a date"))

    def test_estimate_grows_loan_by_memphis_index(self):
        est = estimate(200000.0, "2021-08-11")
        expected = round(200000 / LTV_ASSUMED * MEMPHIS_HPI[LATEST_Q] / MEMPHIS_HPI["2021Q3"], -3)
        self.assertEqual(est["value"], int(expected))
        self.assertEqual((est["hpi_from"], est["hpi_to"]), ("2021Q3", LATEST_Q))

    def test_loan_newer_than_index_gets_no_growth(self):
        est = estimate(190000.0, "2026-10-01")
        self.assertEqual(est["value"], 200000)

    def test_unindexable_loans(self):
        self.assertIsNone(estimate(100000.0, "1990-01-01"))
        self.assertIsNone(estimate(0, "2021-01-01"))
        self.assertIsNone(estimate(20650000.0, "2018-08-29"))  # blanket loan

    def test_dot_ref_from_notice(self):
        self.assertEqual(dot_ref({"structured": {"instrument_no": "21100977"}}), {"instrument": "21100977", "year": "2021"})
        self.assertIsNone(dot_ref({"structured": {"instrument_no": "LM6983"}}))
        self.assertIsNone(dot_ref({}))


if __name__ == "__main__":
    unittest.main()
