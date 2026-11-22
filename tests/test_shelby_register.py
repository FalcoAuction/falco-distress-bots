"""Tests for the Shelby County Register of Deeds parser and the substitute
trustee lead builder. No network. Fixtures copy the register's markup
(2026-11-16) with made-up names and numbers.

    python -m unittest tests.test_shelby_register -v
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


if __name__ == "__main__":
    unittest.main()
