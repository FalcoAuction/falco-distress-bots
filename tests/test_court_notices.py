"""Court-notice parser tests. Synthetic notices modeled on the five layouts
seen in The Nashville Ledger and Memphis Daily News; all names invented.

    python -m unittest tests.test_court_notices -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import _court_notices as cn  # noqa: E402

STATUTE = [
    "All persons, resident and non-resident, having claims, matured or unmatured, against the estate are required to file the same with the Clerk of the above named Court.",
    "(1)(A) Four (4) months from the date of the first publication (or posting, as the case may be) of this notice; or",
    "(2) Twelve (12) months from the decedent's date of death.",
]


def page(rows, grid_id="ContentPane_CourtGridView"):
    """Index-page HTML with a court grid of the given (notice_id, lines) rows."""
    body = "".join(
        f"<tr><td><a href=\"javascript:OpenChildFT2('{nid}','10%2f30%2f2026')\">View</a></td><td>&nbsp;</td>"
        f"<td>{''.join(f'<p>{line}</p>' for line in lines)}</td><td>&nbsp;</td></tr>"
        for nid, lines in rows
    )
    return (f'<div><table class="data-table" id="ForeclosureGridView"><tr><td>x</td></tr></table>'
            f'<table class="data-table" id="{grid_id}"><tr><th>Details</th></tr>{body}</table></div>')


SHELBY = ["NOTICE TO CREDITORS", "Docket: PR099001", "In Re the Matter of: Ada Q. Sample",
          "Notice is hereby given that on the 28th day of October, 2026, Letters Testamentary in respect of Ada Q. Sample who died Sept. 28, 2026, were issued to the undersigned by the Probate Court of Shelby County, Tennessee.",
          *STATUTE, "This 28th day of October, 2026.", "Bea Sample", "Cal Sample Jr. Executor",
          "Attorney for the Estate:", "Dana E. Lawyer", "Oct. 30, Nov. 6, 2026 Cd099001"]
DAVIDSON = ["NOTICE TO CREDITORS", "Docket No: 26P9001", "Estate of Eve Fictional Person, Deceased",
            "Notice is hereby given on the 21st day of October, Letters of Authority in respect to the estate of Eve Fictional Person, who died on 10/2/2026, were issued to the undersigned by the Circuit Court of Davidson County, Tennessee, Probate Division.",
            *STATUTE, "This 21st day of October, 2026.", "Fay Fictional", "100 Example Drive", "Mt. Juliet, TN 37122",
            "Personal Representative(s)", "Counsel Jr., Gus Allen", "200 Example Plaza", "Nashville, TN 37219",
            "Attorney for Personal Representative(s)", "JANE Q. CLERKPERSON", "Probate Court Clerk",
            "1 Public Square, Room 303", "Oct. 30, Nov. 6, 2026 Cl099002"]
CHEATHAM = ["STATE OF TENNESSEE", "PROBATE COURT OF CHEATHAM COUNTY", "AT ASHLAND CITY", "NOTICE TO CREDITORS",
            "Probate Docket No.: P-9003", "Estate of Hal Invented, Deceased",
            "Notice is hereby given that on the 28th day of October, 2026, Letters of Testamentary in respect of the estate of Hal Invented, deceased, who died on October 13, 2026, were issued to the undersigned by the Probate Court of Cheatham County, Tennessee.",
            *STATUTE, "This the 28th day of October, 2026.", "Ian Invented II and", "Joy Invented Empson",
            "Co-Executors of the Estate", "of Hal Invented", "Deceased", "Attorney: K. Example Counsel",
            "112 Example Street", "Ashland City, TN 37015", "Lee Person, Clerk & Master", "Oct. 30, Nov. 6, 2026 Cl099003"]
MONTGOMERY_FOR = ["IN THE PROBATE COURT FOR MONTGOMERY COUNTY, TENNESSEE AT CLARKSVILLE", "CASE No.: MC CH CV PB-26-999",
                  "IN RE:", "ESTATE OF MAX MADEUP, JR.", "NOTICE TO CREDITORS",
                  "Notice is hereby given that on the 19th day of October, 2026 letters testamentary in respect of the estate of MAX MADEUP, JR. were issued to the undersigned by the Probate court of Montgomery County, Tennessee. Claims within four (4) months.",
                  "This 21st day of October, 2026.", "Ned Q. Counsel, Esq.", "Attorney for Ora E. Madeup, Executrix",
                  "Estate of Max Madeup Jr.", "Deputy C&M: P. Person", "Oct. 30, Nov. 6, 2026 Cl099004"]
MONTGOMERY_LABELS = ["IN THE CHANCERY COURT OF MONTGOMERY COUNTY, TENNESSEE AT CLARKSVILLE", "PROBATE DIVISION",
                     "Case No. MCCHCVPB260009999", "NOTICE TO CREDITORS", "ESTATE OF Pat Placeholder, (Deceased)",
                     "Notice is hereby given that on the 14th day of October, 2026, Letters of Authority in respect of the Estate of Pat Placeholder, who died 9/16/2026, were issued to the undersigned.",
                     *STATUTE, "This the 16th day of October, 2026", "Personal Representative:", "Quinn Placeholder",
                     "Estate Attorney:", "Ray Example Counsel", "512 Example Street, Suite A", "Clarksville, TN 37040",
                     "Clerk and Master:", "Sue Person", "Oct. 30, Nov. 6, 2026 Cl099005"]
JUVENILE = ["IN THE JUVENILE COURT OF DAVIDSON COUNTY, TENNESSEE", "Docket No. 2026-DN-999",
            "STATE OF TENNESSEE DEPARTMENT OF CHILDREN'S SERVICES, Petitioner", "ORDER OF PUBLICATION",
            "a petition to terminate parental rights"]
PARTITION = ["IN THE CHANCERY COURT OF RUTHERFORD COUNTY, TENNESSEE", "Case No. 26CV-9999",
             "NOTICE OF SALE", "Pursuant to an order of sale entered in this partition action, the Clerk and Master will sell the real property"]


class RowParsingTests(unittest.TestCase):
    def test_parses_only_the_court_grid_and_both_grid_ids(self):
        html = page([("CL099001", SHELBY), ("CL099002", DAVIDSON)])
        rows = cn.parse_court_rows(html)
        self.assertEqual([r["notice_id"] for r in rows], ["CL099001", "CL099002"])
        self.assertEqual(rows[0]["lines"][0], "NOTICE TO CREDITORS")
        nested = page([("CD099001", SHELBY)], grid_id="ctl00_ctl00_ContentPane_ContentPane_CourtGridView")
        self.assertEqual(len(cn.parse_court_rows(nested)), 1)

    def test_no_grid_returns_empty_not_an_error(self):
        self.assertEqual(cn.parse_court_rows("<html><body>No notices</body></html>"), [])

    def test_entities_and_nbsp_are_normalized(self):
        rows = cn.parse_court_rows(page([("CL1", ["Estate&nbsp;of O&#8217;Brien &amp; Co, Deceased"])]))
        self.assertEqual(rows[0]["lines"][0], "Estate of O’Brien & Co, Deceased")


class ClassificationTests(unittest.TestCase):
    def test_kinds(self):
        for layout in (SHELBY, DAVIDSON, CHEATHAM, MONTGOMERY_FOR, MONTGOMERY_LABELS):
            self.assertEqual(cn.classify(layout), cn.KIND_PROBATE)
        self.assertEqual(cn.classify(JUVENILE), cn.KIND_OTHER)
        self.assertEqual(cn.classify(PARTITION), cn.KIND_COURT_SALE)


class ProbateExtractionTests(unittest.TestCase):
    def reps(self, layout):
        return [(r["name"], r["role"]) for r in cn.extract_probate(layout)["representatives"]]

    def test_shelby_layout(self):
        p = cn.extract_probate(SHELBY)
        self.assertEqual((p["county"], p["docket"], p["decedent"]), ("shelby", "PR099001", "Ada Q. Sample"))
        self.assertEqual(p["date_of_death"], "Sept. 28, 2026")
        self.assertEqual(p["letters_type"], "testamentary")
        self.assertEqual(self.reps(SHELBY), [("Bea Sample", "executor"), ("Cal Sample Jr.", "executor")])
        self.assertEqual(p["attorney"]["names"], ["Dana E. Lawyer"])

    def test_davidson_layout_attorney_after_name_and_clerk_dropped(self):
        p = cn.extract_probate(DAVIDSON)
        self.assertEqual((p["county"], p["docket"]), ("davidson", "26P9001"))
        self.assertEqual(self.reps(DAVIDSON), [("Fay Fictional", "personal_representative")])
        self.assertEqual(p["representatives"][0]["address"], ["100 Example Drive", "Mt. Juliet, TN 37122"])
        # "Last, First" is reordered; the clerk is never a signer.
        self.assertEqual(p["attorney"]["names"], ["Gus Allen Counsel Jr."])
        self.assertEqual(p["attorney"]["address"], ["200 Example Plaza", "Nashville, TN 37219"])
        self.assertNotIn("JANE Q. CLERKPERSON", [n for n, _ in self.reps(DAVIDSON)])

    def test_cheatham_layout_co_executors_split_on_trailing_and(self):
        p = cn.extract_probate(CHEATHAM)
        self.assertEqual((p["county"], p["docket"]), ("cheatham", "P-9003"))
        self.assertEqual(self.reps(CHEATHAM), [("Ian Invented II", "executor"), ("Joy Invented Empson", "executor")])
        self.assertEqual(p["attorney"]["names"], ["K. Example Counsel"])
        self.assertEqual(p["attorney"]["address"], ["112 Example Street", "Ashland City, TN 37015"])

    def test_attorney_for_named_client(self):
        p = cn.extract_probate(MONTGOMERY_FOR)
        self.assertEqual(p["docket"], "MCCHCVPB-26-999")  # spaces in the case number removed, not truncated
        self.assertEqual(self.reps(MONTGOMERY_FOR), [("Ora E. Madeup", "executor")])
        self.assertEqual(p["attorney"]["names"], ["Ned Q. Counsel"])  # ", Esq." stripped

    def test_colon_labels_before_names(self):
        p = cn.extract_probate(MONTGOMERY_LABELS)
        self.assertEqual(p["docket"], "MCCHCVPB260009999")
        self.assertEqual(self.reps(MONTGOMERY_LABELS), [("Quinn Placeholder", "personal_representative")])
        self.assertEqual(p["attorney"]["names"], ["Ray Example Counsel"])
        self.assertNotIn("Sue Person", p["attorney"]["names"])

    def test_missing_fields_are_empty_not_guessed(self):
        p = cn.extract_probate(["NOTICE TO CREDITORS", "Estate of Someone Unknown, Deceased"])
        self.assertIsNone(p["docket"])
        self.assertIsNone(p["date_of_death"])
        self.assertEqual(p["representatives"], [])
        self.assertEqual(p["attorney"]["names"], [])


class CourtSaleTests(unittest.TestCase):
    def test_court_sale_fields(self):
        s = cn.extract_court_sale(PARTITION)
        self.assertEqual(s["county"], "rutherford")
        self.assertEqual(s["docket"], "26CV-9999")


if __name__ == "__main__":
    unittest.main()


class LeadBuildingTests(unittest.TestCase):
    def setUp(self):
        from datetime import date
        from src.bots import court_notices_bot as bot
        self.bot = bot
        self.date = date(2026, 10, 30)

    def test_probate_lead_carries_decedent_county_and_structured_notice(self):
        lead = self.bot.build_lead("court_notices", "nashville_ledger", "https://www.tnledger.com", self.date,
                                   {"notice_id": "CL099003", "lines": CHEATHAM})
        self.assertEqual(lead.distress_type, "PROBATE")
        self.assertEqual((lead.full_name, lead.owner_name_records, lead.county), ("Hal Invented", "Hal Invented", "cheatham"))
        self.assertIsNone(lead.property_address)  # the enricher finds the property
        notice = lead.raw_payload["court_notice"]
        self.assertEqual(notice["attorney"]["names"], ["K. Example Counsel"])
        self.assertIn("ViewNotice.aspx?id=CL099003&date=10/30/2026", lead.source_url)
        self.assertNotIn("..", lead.admin_notes)

    def test_same_estate_in_another_week_or_paper_has_the_same_key(self):
        a = self.bot.build_lead("court_notices", "nashville_ledger", "https://www.tnledger.com", self.date,
                                {"notice_id": "CL1", "lines": SHELBY})
        b = self.bot.build_lead("court_notices", "memphis_daily_news", "https://www.memphisdailynews.com", self.date,
                                {"notice_id": "CD2", "lines": SHELBY})
        self.assertEqual(a.pipeline_lead_key, b.pipeline_lead_key)

    def test_court_sale_and_ignored_kinds(self):
        sale = self.bot.build_lead("court_notices", "nashville_ledger", "https://www.tnledger.com", self.date,
                                   {"notice_id": "CL5", "lines": PARTITION})
        self.assertEqual(sale.distress_type, "COURT_SALE")
        self.assertIsNone(self.bot.build_lead("court_notices", "x", "https://x", self.date, {"notice_id": "CL6", "lines": JUVENILE}))
        self.assertIsNone(self.bot.build_lead("court_notices", "x", "https://x", self.date,
                                              {"notice_id": "CL7", "lines": ["NOTICE TO CREDITORS", "estate claims"]}))

    def test_edition_dates(self):
        from datetime import date
        fridays = self.bot.edition_dates(date(2026, 11, 5), {4}, 21)
        self.assertEqual(fridays, [date(2026, 10, 30), date(2026, 10, 23), date(2026, 10, 16)])
        weekdays = self.bot.edition_dates(date(2026, 11, 2), {0, 1, 2, 3, 4}, 3)
        self.assertEqual(weekdays, [date(2026, 11, 2), date(2026, 10, 30)])
