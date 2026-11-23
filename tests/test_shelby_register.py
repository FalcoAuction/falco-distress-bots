"""Tests for the Shelby County Register of Deeds parser and the substitute
trustee lead builder. No network. Fixtures copy the register's markup
(2026-11-16) with made-up names and numbers.

    python -m unittest tests.test_shelby_register -v
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._shelby_register import (address_of, deed_of_trust_ref, parse_detail, parse_results,  # noqa: E402
                                       split_parties, trustees)
from src.bots.shelby_substitute_trustee_bot import ShelbySubstituteTrusteeBot, build_lead  # noqa: E402

F = '<font color="black" face="times" size="2">{}<br/></font>'


def cell(*names):
    return "<td>" + "".join(F.format(n) for n in names) + "</td>"


RESULTS = (
    "<html><body><table><tr><td>Menu</td></tr></table><table>"
    "<tr><th>Details</th><th>Inst #</th><th>Inst Code</th><th>Grantor/Debtor</th><th>Grantee/ Secured Party</th>"
    "<th>Date</th><th>Transfer Amount</th><th>Mortgage Amount</th><th>Street Number</th><th>Street Name</th>"
    "<th>City</th><th>State</th><th>Zip</th><th>Image</th></tr>"
    "<tr><td><a href='pdetail.php?year=2026&amp;instnum=26000001&amp;db=0&amp;book=**0'>INDEX.</a></td>"
    "<td>26000001</td><td>APPT</td>" + cell("ACME MORTGAGE COMPANY LLC ", "DOE JANE Q") + cell("SMITH LAW PLLC S TR ")
    + "<td>11/16/2026</td><td>0.00</td><td>0.00</td><td></td><td></td><td></td><td></td><td></td><td>NOT ON FILE</td></tr>"
    "<tr><td><a href='pdetail.php?year=2026&amp;instnum=26000002&amp;db=0&amp;book=**0'>INDEX.</a></td>"
    "<td>26000002</td><td>APPT</td>" + cell("ROE RICHARD", "ROE RITA", "U S BANK NATIONAL ASSOCIATION TR ")
    + cell("JONES WILLIAM P III S TR ")
    + "<td>11/13/2026</td><td>0.00</td><td>0.00</td><td></td><td></td><td></td><td></td><td></td><td>NOT ON FILE</td></tr>"
    "</table></body></html>"
)

SILVER = "<td bgcolor=silver><font size=2>{}</font></td>"
VAL = "<td><font size=2>{}</font></td>"


def detail(doc_type, mort, inst_date, grantors, grantees, xref=None, parcel=("", "", ""), street=("", "", "", "", "")):
    x = ""
    if xref:
        x = (f"<tr><FORM ACTION=pdetail.php METHOD=POST><td><INPUT TYPE=HIDDEN NAME=instnum VALUE={xref[0]}>"
             f"<INPUT TYPE=HIDDEN NAME=year VALUE={xref[1]}><INPUT TYPE=HIDDEN NAME=db VALUE=0></td></FORM>"
             f"<td>{xref[0]}</td><td></td><td></td><td>0</td></tr>")
    return (
        "<html><body><table border=3><tr><td>File Date: 11/16/2026</td><td>File Time: 03:15:30 PM</td></tr>"
        f"<tr><td colspan=2>Doc Type: {doc_type}</td><td></td><td>Mort $ {mort}</td></tr>"
        f"<tr><td>Book # </td><td>Page # 0</td><td>File # </td><td>Instrument Date : {inst_date}</td></tr></table>"
        "<table><tr><td>Grantor(s)</td><td>Grantee(s)</td><td>Cross References</td></tr><tr>"
        f"<td><TEXTAREA NAME=GRANTORS READONLY>{chr(10).join(grantors)}\n</textarea></td>"
        f"<td><TEXTAREA NAME=GRANTORS READONLY>{chr(10).join(grantees)}\n</textarea></td>"
        "<td><table><tr><td>Link</td><td>Inst #</td><td>Year</td><td>XRef Book</td><td>XRef Page</td></tr>"
        f"{x}</table></td></tr></table>"
        "<table><tr>" + "".join(SILVER.format(h) for h in ("Parcel ID", "Lot", "Subdivision")) + "</tr><tr>"
        + "".join(VAL.format(v) for v in parcel) + "</tr></table>"
        "<table><tr>" + "".join(SILVER.format(h) for h in ("Street #", "Street Name", "City", "State", "Zip"))
        + "</tr><tr>" + "".join(VAL.format(v) for v in street) + "</tr></table></body></html>"
    )


APPT = detail("APPOINTMENT OF SUBSTITUTE TR", "0.00", "11/13/2026",
              ["ACME MORTGAGE COMPANY LLC", "DOE JANE Q"], ["SMITH LAW PLLC S TR"], xref=("25001234", "2025"))
APPT_OLD_BOOK = detail("APPOINTMENT OF SUBSTITUTE TR", "0.00", "11/13/2026",
                       ["ROE RICHARD", "ROE RITA", "U S BANK NATIONAL ASSOCIATION TR"], ["JONES WILLIAM P III S TR"],
                       xref=("LM6983", "2001"))
DOT = detail("DEED OF TRUST", "200000.00", "01/17/2025", ["DOE JANE Q"],
             ["TRUSTEE TOM W TR", "SOUTHWEST FUNDING LP"],
             parcel=("08300800028", "61", "SAMPLE HILLS SEC B"),
             street=("1234", "EAST SAMPLE RD", "MEMPHIS", "TN", "38128"))


class ParseTests(unittest.TestCase):
    def test_results(self):
        rows = parse_results(RESULTS)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["instrument"], "26000001")
        self.assertEqual(rows[0]["code"], "APPT")
        self.assertEqual(rows[0]["grantors"], ["ACME MORTGAGE COMPANY LLC", "DOE JANE Q"])
        self.assertEqual(rows[0]["grantees"], ["SMITH LAW PLLC S TR"])
        self.assertEqual(rows[0]["recorded"], "2026-11-16")
        self.assertTrue(rows[0]["detail_url"].endswith("pdetail.php?year=2026&instnum=26000001&db=0&book=**0"))
        self.assertEqual(parse_results("<html>layout changed</html>"), [])

    def test_appt_detail_and_xref(self):
        d = parse_detail(APPT)
        self.assertEqual(d["doc_type"], "APPOINTMENT OF SUBSTITUTE TR")
        self.assertEqual(d["instrument_date"], "2026-11-13")
        self.assertIsNone(d["mortgage_amount"])
        self.assertEqual(d["grantors"], ["ACME MORTGAGE COMPANY LLC", "DOE JANE Q"])
        self.assertEqual(deed_of_trust_ref(d), {"instrument": "25001234", "year": "2025"})
        self.assertIsNone(address_of(d))
        # Pre-instrument-number book references have no detail page.
        self.assertIsNone(deed_of_trust_ref(parse_detail(APPT_OLD_BOOK)))

    def test_deed_of_trust(self):
        d = parse_detail(DOT)
        self.assertEqual(d["mortgage_amount"], 200000.0)
        self.assertEqual(d["instrument_date"], "2025-01-17")
        self.assertEqual(d["parcel_id"], "08300800028")
        self.assertEqual(address_of(d), "1234 EAST SAMPLE RD, MEMPHIS, TN 38128")

    def test_split_parties(self):
        by_dot = split_parties(["ACME MORTGAGE COMPANY LLC", "DOE JANE Q"], ["DOE JANE Q"])
        self.assertEqual(by_dot, {"borrowers": ["DOE JANE Q"], "lenders": ["ACME MORTGAGE COMPANY LLC"],
                                  "basis": "deed_of_trust"})
        by_words = split_parties(["ROE RICHARD", "ROE RITA", "U S BANK NATIONAL ASSOCIATION TR"], None)
        self.assertEqual(by_words["borrowers"], ["ROE RICHARD", "ROE RITA"])
        self.assertEqual(by_words["lenders"], ["U S BANK NATIONAL ASSOCIATION TR"])
        self.assertEqual(trustees(["JONES WILLIAM P III S TR", "SMITH LAW PLLC S TR"]),
                         ["JONES WILLIAM P III", "SMITH LAW PLLC"])


class LeadTests(unittest.TestCase):
    def test_full_lead(self):
        bot = ShelbySubstituteTrusteeBot()
        row = parse_results(RESULTS)[0]
        lead = build_lead(bot, row, parse_detail(APPT), parse_detail(DOT), today=date(2026, 11, 16))
        self.assertEqual(lead.distress_type, "SUBSTITUTION_OF_TRUSTEE")
        self.assertEqual(lead.county, "Shelby")
        self.assertEqual(lead.full_name, "DOE JANE Q")
        self.assertEqual(lead.property_address, "1234 EAST SAMPLE RD, MEMPHIS, TN 38128")
        loan = lead.raw_payload["loan"]
        self.assertEqual(loan["original_amount"], 200000.0)
        # Under two years into a 30-year loan: most of the principal is still owed.
        self.assertTrue(190000 <= lead.mortgage_balance <= 200000)
        self.assertEqual(lead.raw_payload["lenders"], ["ACME MORTGAGE COMPANY LLC"])
        self.assertEqual(lead.raw_payload["substitute_trustees"], ["SMITH LAW PLLC"])
        self.assertIn("by ACME MORTGAGE COMPANY LLC", lead.admin_notes)

    def test_lead_without_deed_of_trust(self):
        bot = ShelbySubstituteTrusteeBot()
        row = parse_results(RESULTS)[1]
        lead = build_lead(bot, row, parse_detail(APPT_OLD_BOOK), None)
        self.assertEqual(lead.raw_payload["appointment_cross_references"],
                         [{"instrument": "LM6983", "year": "2001"}])
        self.assertIsNone(lead.raw_payload["deed_of_trust"])
        self.assertIsNone(lead.raw_payload["parcel_id"])
        self.assertEqual(lead.raw_payload["party_basis"], "lender_words")
        self.assertIsNone(lead.property_address)
        self.assertIsNone(lead.mortgage_balance)
        self.assertEqual(lead.owner_name_records, "ROE RICHARD; ROE RITA")
        self.assertIn("No address yet", lead.admin_notes)
        self.assertEqual(lead.pipeline_lead_key, bot.make_lead_key(bot.name, "shelby|26000002"))

    def test_all_lender_grantors_still_staged(self):
        bot = ShelbySubstituteTrusteeBot()
        row = {"instrument": "26000003", "code": "APPT", "grantors": ["ACME BANK", "ACME MORTGAGE LLC"],
               "grantees": ["SMITH LAW PLLC S TR"], "recorded": "2026-11-16", "detail_url": None}
        lead = build_lead(bot, row, None, None)
        self.assertIsNone(lead.full_name)
        self.assertIsNone(lead.owner_name_records)
        self.assertEqual(lead.raw_payload["borrowers"], [])
        self.assertEqual(lead.raw_payload["party_basis"], "unidentified")
        self.assertIn("Borrower not identified", lead.admin_notes)


class ScheduledSelectionTests(unittest.TestCase):
    def test_budget_shares_newest_and_oldest_unstaged_filings(self):
        bot = ShelbySubstituteTrusteeBot()
        dates = ["2026-10-31", "2026-11-01", "2026-11-08", "2026-11-09",
                 "2026-11-10", "2026-11-11", "2026-11-20", "2026-11-21",
                 "2026-11-22", "2026-11-23", None]
        rows = [dict(parse_results(RESULTS)[0], instrument=str(i), recorded=day,
                     detail_url=f"https://example/{i}") for i, day in enumerate(dates)]
        known = {bot.make_lead_key(bot.name, "shelby|2")}
        with patch("src.bots.shelby_substitute_trustee_bot.date", wraps=date) as clock, \
                patch("src.bots.shelby_substitute_trustee_bot.parse_results", return_value=rows + [rows[1]]), \
                patch.object(bot, "_known", side_effect=lambda keys: known & keys), \
                patch.object(bot, "fetch", return_value=Mock(status_code=200)) as fetch, \
                patch.object(bot, "_detail", return_value={}) as detail, \
                patch.object(bot, "retry_deferred_appointment", return_value=2), patch.object(bot, "backfill") as backfill:
            clock.today.return_value = date(2026, 11, 22)
            for expected in (["8", "7", "1", "3", "4", "5"], ["6"], []):
                leads = bot.scrape()
                self.assertEqual([lead.raw_payload["instrument"] for lead in leads], expected)
                self.assertEqual([call.args[0] for call in detail.call_args_list],
                                 [f"https://example/{i}" for i in expected])
                self.assertEqual(fetch.call_args.kwargs["data"]["startdate"], "11/01/2026")
                backfill.assert_called_with(3)     # 14 requests less the new filings and the retry
                known.update(lead.pipeline_lead_key for lead in leads)
                detail.reset_mock()


class RequestBudgetTests(unittest.TestCase):
    """New filings spend first, deferred filings always keep two requests,
    and the first refused request ends the run's Register work."""

    def scrape(self, new_filings, detail, retry_used=2):
        bot = ShelbySubstituteTrusteeBot()
        rows = [dict(parse_results(RESULTS)[0], instrument=str(i), recorded="2026-11-20",
                     detail_url=f"https://example/{i}") for i in range(new_filings)]
        with patch("src.bots.shelby_substitute_trustee_bot.date", wraps=date) as clock,                 patch("src.bots.shelby_substitute_trustee_bot.parse_results", return_value=rows),                 patch.object(bot, "_known", return_value=set()),                 patch.object(bot, "fetch", return_value=Mock(status_code=200)),                 patch.object(bot, "_detail", side_effect=detail) as read,                 patch.object(bot, "retry_deferred_appointment", return_value=retry_used) as retry,                 patch.object(bot, "backfill") as backfill:
            clock.today.return_value = date(2026, 11, 22)
            leads = bot.scrape()
        return leads, read, retry, backfill

    def test_quiet_day_gives_the_spare_requests_to_deferred_filings(self):
        _, read, retry, backfill = self.scrape(1, lambda url: {}, retry_used=8)
        self.assertEqual(read.call_count, 1)
        retry.assert_called_once_with(8)            # four deferred filings, two requests each
        backfill.assert_called_once_with(3)

    def test_busy_day_still_reserves_one_deferred_filing(self):
        linked = [parse_detail(APPT), parse_detail(DOT)] * 6
        leads, read, retry, backfill = self.scrape(9, linked)
        self.assertEqual((len(leads), read.call_count), (6, 12))
        retry.assert_called_once_with(2)
        backfill.assert_not_called()

    def test_first_refused_request_stops_the_run(self):
        leads, read, retry, backfill = self.scrape(6, [None])
        self.assertEqual((leads, read.call_count), ([], 1))
        retry.assert_not_called()
        backfill.assert_not_called()


class DeferredDetailTests(unittest.TestCase):
    def setUp(self):
        self.bot = ShelbySubstituteTrusteeBot()
        self.raw = {"instrument": "26000001", "recorded": "2026-11-16", "borrowers": ["INDEX NAME"],
                    "recovery": {"source": "saved_register_APPT_index", "detail_issue": "HTTP 503"}}
        self.row = {"id": "id1", "raw_payload": self.raw, "property_address": None}
        self.client = Mock()
        self.query = self.client.table.return_value
        for method in ("update", "eq", "filter", "is_"):
            getattr(self.query, method).return_value = self.query
        self.enterContext(patch("src.bots.shelby_substitute_trustee_bot._supabase", return_value=self.client))
        self.rows = self.enterContext(patch.object(self.bot, "_deferred_rows", return_value=[self.row]))
        self.detail = self.enterContext(patch.object(self.bot, "_verified_detail"))

    def retry(self, docs, expected_requests):
        self.detail.side_effect = docs
        self.assertEqual(self.bot.retry_deferred_appointment(), expected_requests)
        self.assertEqual(self.detail.call_count, expected_requests)
        self.query.filter.assert_called_once_with("raw_payload", "eq", json.dumps(self.raw, separators=(",", ":")))
        return self.query.update.call_args.args[0]

    def test_linked_loan_fills_property_and_keeps_distinct_roles(self):
        patch = self.retry([parse_detail(APPT), parse_detail(DOT)], 2)
        raw = patch["raw_payload"]
        self.assertEqual(set(patch), {"raw_payload", "property_address"})
        self.assertEqual(raw["borrowers"], ["INDEX NAME"])
        self.assertEqual(raw["recovery"]["loan_grantors"], ["DOE JANE Q"])
        self.assertEqual(raw["parcel_id"], "08300800028")
        self.assertTrue(raw["dot_checked_at"])
        self.assertTrue(raw["recovery"]["detail_checked_at"])
        self.query.is_.assert_called_once_with("property_address", "null")

    def test_conflicting_evidence_preserves_fields(self):
        self.row["property_address"] = "previous address"
        self.raw["parcel_id"] = "previous parcel"
        patch = self.retry([parse_detail(APPT), parse_detail(DOT)], 2)
        self.assertEqual(set(patch), {"raw_payload"})
        self.assertEqual(patch["raw_payload"]["parcel_id"], "previous parcel")
        self.assertEqual(patch["raw_payload"]["recovery"]["register_parcel"], "08300800028")
        self.query.eq.assert_any_call("property_address", "previous address")

    def test_failed_link_remains_due_and_rotates(self):
        patch = self.retry([parse_detail(APPT), None], 2)
        recovery = patch["raw_payload"]["recovery"]
        self.assertTrue(recovery["last_detail_attempt"])
        self.assertNotIn("detail_checked_at", recovery)

    def test_unavailable_appointment_still_due(self):
        patch = self.retry([None], 1)
        self.assertNotIn("detail_checked_at", patch["raw_payload"]["recovery"])
        self.assertNotIn("deed_of_trust", patch["raw_payload"])

    def test_legacy_reference_finishes_without_inventing_parcel(self):
        patch = self.retry([parse_detail(APPT_OLD_BOOK)], 1)
        self.assertEqual(patch["raw_payload"]["recovery"]["detail_outcome"], "no_linked_loan")
        self.assertNotIn("parcel_id", patch["raw_payload"])

    def test_attempts_are_counted_and_the_original_loan_is_kept(self):
        patch = self.retry([parse_detail(APPT), parse_detail(DOT)], 2)
        raw = patch["raw_payload"]
        self.assertEqual((raw["recovery"]["detail_attempts"], raw["recovery"]["last_detail_result"]),
                         (1, "deed of trust page read"))
        self.assertEqual(raw["loan"]["original_amount"], parse_detail(DOT)["mortgage_amount"])
        self.assertTrue(raw["loan"]["loan_date"])

    def test_refused_page_is_recorded_and_ends_the_queue(self):
        self.raw["recovery"]["detail_attempts"] = 1
        second = {"id": "id2", "raw_payload": dict(self.raw, instrument="26000009"), "property_address": None}
        self.rows.return_value = [self.row, second]
        self.detail.side_effect = lambda *a: setattr(self.bot, "_register_down", True)
        self.assertEqual(self.bot.retry_deferred_appointment(8), 1)
        self.rows.assert_called_once_with(self.client, 4)
        recovery = self.query.update.call_args.args[0]["raw_payload"]["recovery"]
        self.assertEqual((recovery["detail_attempts"], recovery["last_detail_result"]), (2, "appointment page not served"))
        self.assertEqual(self.query.update.call_count, 1)        # the second filing was not requested

    def test_several_deferred_filings_within_the_budget(self):
        second = {"id": "id2", "raw_payload": dict(self.raw, instrument="26000001"), "property_address": None}
        third = {"id": "id3", "raw_payload": dict(self.raw), "property_address": None}
        self.rows.return_value = [self.row, second, third]
        self.detail.side_effect = [parse_detail(APPT), parse_detail(DOT)] * 3
        self.assertEqual(self.bot.retry_deferred_appointment(4), 4)
        self.assertEqual(self.query.update.call_count, 2)

    def test_empty_queue_uses_no_requests_or_writes(self):
        self.rows.return_value = []
        self.assertEqual(self.bot.retry_deferred_appointment(), 0)
        self.detail.assert_not_called()
        self.query.update.assert_not_called()

    def test_mismatched_instrument_page_is_rejected(self):
        response = Mock(status_code=200, text="<a>Detail Information For Instrument # 26000002</a>" + APPT)
        with patch.object(self.bot, "fetch", return_value=response) as fetch:
            read = ShelbySubstituteTrusteeBot._verified_detail
            self.assertIsNone(read(self.bot, "26000001", "2026"))
            self.assertEqual(read(self.bot, "26000002", "2026")["doc_type"], "APPOINTMENT OF SUBSTITUTE TR")
            self.assertFalse(fetch.call_args.kwargs["allow_redirects"])


if __name__ == "__main__":
    unittest.main()
