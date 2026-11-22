"""Tests for the open-lien rules and the equity range. No network.

    python -m unittest tests.test_lien_stack -v
"""

from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.enrichment.lien_stack import (Instrument, amortize, equity_range, lien_stack,  # noqa: E402
                                       ref_key)
from src.enrichment.shortlist import register_system, screen  # noqa: E402

TODAY = date(2026, 11, 20)


def deed(y, m=1, d=1, arm=True):
    return Instrument("DEED", date(y, m, d), arm_length=arm)


def dot(y, amount, book, page, m=1, d=1, **kw):
    return Instrument("DOT", date(y, m, d), amount=amount, lender=kw.pop("lender", "Bank"), book=book, page=page, **kw)


class RefTests(unittest.TestCase):
    def test_citations_in_any_form_match(self):
        loan = dot(2015, 200_000, "4012", "0077")
        self.assertEqual(ref_key("4012", "0077"), ["B4012P77"])
        for ref in ("4012/77", "Book 4012 Page 77", "BK 4012, PG 77", "B4012P77"):
            rel = Instrument("RELEASE", date(2020, 1, 1), refs=[ref])
            self.assertTrue(rel.cites(loan), ref)

    def test_instrument_number_with_dash_is_not_book_page(self):
        loan = Instrument("DOT", date(2015, 1, 1), amount=1, instrument="20150101-0001234")
        rel = Instrument("RELEASE", date(2020, 1, 1), refs=["20150101-0001234"])
        self.assertTrue(rel.cites(loan))
        other = Instrument("RELEASE", date(2020, 1, 1), refs=["20150101-0009999"])
        self.assertFalse(other.cites(loan))


class StatusTests(unittest.TestCase):
    def test_released_loan_owes_nothing(self):
        s = lien_stack([deed(2010), dot(2010, 150_000, "1", "1"),
                        Instrument("RELEASE", date(2018, 5, 1), refs=["1/1"])], TODAY)
        self.assertEqual(s["liens"][0]["status"], "released")
        self.assertEqual(s["liens"][0]["balance_high"], 0)

    def test_loans_before_purchase_are_ignored(self):
        s = lien_stack([dot(2005, 90_000, "9", "9"), deed(2012), dot(2012, 200_000, "2", "2")], TODAY)
        self.assertEqual([l["book"] for l in s["liens"]], ["2"])

    def test_quitclaim_does_not_reset_ownership(self):
        s = lien_stack([deed(2012), dot(2012, 200_000, "2", "2"), deed(2020, arm=False)], TODAY)
        self.assertEqual(len(s["liens"]), 1)

    def test_substitute_trustee_confirms_default_and_widens_range(self):
        s = lien_stack([deed(2019), dot(2019, 300_000, "5", "5"),
                        Instrument("SUB_TRUSTEE", date(2026, 9, 1), refs=["5/5"])], TODAY)
        l = s["liens"][0]
        self.assertEqual(l["status"], "confirmed_open")
        self.assertGreater(l["balance_high"], l["balance_low"])
        self.assertAlmostEqual(l["balance_low"], amortize(300_000, 3.94, 30, (TODAY - date(2019, 1, 1)).days / 365.25), delta=1)

    def test_refinance_marks_old_loan_likely_paid(self):
        s = lien_stack([deed(2012), dot(2012, 200_000, "2", "2"), dot(2021, 210_000, "3", "3")], TODAY)
        by = {l["book"]: l for l in s["liens"]}
        self.assertEqual(by["2"]["status"], "likely_paid")
        self.assertEqual(by["2"]["balance_low"], 0)
        self.assertEqual(by["3"]["status"], "open")

    def test_second_loan_at_purchase_is_not_a_refinance(self):
        s = lien_stack([deed(2020, 3, 1), dot(2020, 240_000, "2", "2", m=3, d=1),
                        dot(2020, 200_000, "3", "3", m=3, d=1)], TODAY)
        self.assertEqual(s["counts"]["open"], 2)

    def test_credit_line_ranges_from_zero_to_limit(self):
        s = lien_stack([deed(2015), Instrument("HELOC", date(2022, 1, 1), amount=50_000, book="7", page="7")], TODAY)
        l = s["liens"][0]
        self.assertEqual((l["balance_low"], l["balance_high"]), (0, 50_000))
        self.assertIn("credit_line_recorded_at_limit", l["flags"])

    def test_old_judgment_lapses_after_ten_years(self):
        s = lien_stack([deed(2005), Instrument("JUDGMENT", date(2014, 1, 1), amount=8_000),
                        Instrument("JUDGMENT", date(2023, 1, 1), amount=4_000)], TODAY)
        st = sorted(l["status"] for l in s["liens"])
        self.assertEqual(st, ["likely_paid", "open"])

    def test_mers_is_flagged(self):
        s = lien_stack([deed(2016), dot(2016, 100_000, "1", "1", mers=True)], TODAY)
        self.assertIn("mers_lender_may_differ", s["liens"][0]["flags"])


class EquityTests(unittest.TestCase):
    def test_range_counts_likely_paid_only_at_the_low_end(self):
        s = lien_stack([deed(2012), dot(2012, 200_000, "2", "2"), dot(2021, 210_000, "3", "3")], TODAY)
        eq = equity_range(400_000, s)
        self.assertLess(eq["low"], eq["high"])
        open_low = [l for l in s["liens"] if l["book"] == "3"][0]["balance_low"]
        self.assertEqual(eq["high"], round(400_000 - open_low))

    def test_no_value_means_unknown(self):
        self.assertIsNone(equity_range(None, lien_stack([], TODAY))["low"])


class ShortlistTests(unittest.TestCase):
    def opp(self, **kw):
        return {"county": "Davidson", "sale_date": None, "expected_fee": 6000, **kw}

    def test_middle_tn_lead_is_queued_with_its_register(self):
        ok, d = screen(self.opp(), {}, TODAY)
        self.assertTrue(ok)
        self.assertEqual(d["register_system"], "davidson_portal")

    def test_outside_tennessee_is_left_out(self):
        self.assertEqual(screen(self.opp(county="Fulton"), {}, TODAY), (False, {"reason": "outside_tennessee"}))
        self.assertTrue(screen(self.opp(county="Shelby"), {}, TODAY)[0])

    def test_sale_too_soon_is_left_out_but_past_dates_are_not(self):
        ok, d = screen(self.opp(sale_date="2026-12-01"), {}, TODAY)
        self.assertFalse(ok)
        self.assertEqual(d["reason"], "sale_too_soon")
        ok, d = screen(self.opp(sale_date="2026-12-30"), {}, TODAY)
        self.assertTrue(ok)
        self.assertTrue(d["tight"])
        self.assertTrue(screen(self.opp(sale_date="2026-10-01"), {}, TODAY)[0])

    def test_recent_check_and_open_queue_are_not_requeued(self):
        self.assertFalse(screen(self.opp(), {"lien_check": {"checked_at": "2026-11-10"}}, TODAY)[0])
        self.assertTrue(screen(self.opp(), {"lien_check": {"checked_at": "2026-09-01"}}, TODAY)[0])
        self.assertFalse(screen(self.opp(), {"lien_check": {"status": "queued"}}, TODAY)[0])

    def test_commercial_and_land_need_readiness(self):
        not_ready = {"property_lane": {"lane": "land", "readiness": {"ready": False, "missing": ["parcel_id"]}}}
        ok, d = screen(self.opp(), not_ready, TODAY)
        self.assertFalse(ok)
        self.assertEqual(d["missing"], ["parcel_id"])
        ready = {"property_lane": {"lane": "land", "readiness": {"ready": True, "missing": []}}}
        self.assertTrue(screen(self.opp(), ready, TODAY)[0])
        self.assertTrue(screen(self.opp(), {"property_lane": {"lane": "residential"}}, TODAY)[0])

    def test_outer_counties_default_to_titlesearcher(self):
        self.assertEqual(register_system("Robertson County"), "titlesearcher")
        self.assertEqual(register_system("sumner"), "ustitlesearch")

    def test_mackie_uses_current_date_and_honors_missing_file_review(self):
        row = self.opp(bot_source="mackie_wolf_trustee", sale_date="2026-12-01",
                       trustee_sale_date="2027-03-02")
        ok, detail = screen(row, {}, TODAY)
        self.assertTrue(ok)
        self.assertEqual(detail["days_to_sale"], (date(2027, 3, 2) - TODAY).days)
        row["phone_metadata"] = {"trustee_notice_review": {"reason": "stale_notice"}}
        self.assertFalse(screen(row, {}, TODAY)[0])
        row["phone_metadata"] = {"sale_status": {"status": "ran"}}
        self.assertFalse(screen(row, {}, TODAY)[0])
        row.update(phone_metadata={}, trustee_sale_date=None)
        self.assertFalse(screen(row, {}, TODAY)[0])


if __name__ == "__main__":
    unittest.main()
