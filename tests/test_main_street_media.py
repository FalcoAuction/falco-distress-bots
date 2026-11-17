"""python -m unittest tests.test_main_street_media  (synthetic notices)"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import main_street_media_bot as msm  # noqa: E402

PROBATE = """<div class="entry-content"><p>NOTICE TO CREDITORS<br>As Required by TCA 30-2-306<br>Estate of Norma Example<br>Deceased<br>
Case Number 22CH1-2026-PR-001<br>Notice is Hereby Given that on October 19 of 2026, letters testamentary in respect to the estate
Norma Example who died 09/02/2026, were issued to the undersigned by the Dickson County Chancery Court of Dickson County, Tennessee.
All persons are required to file the same on or before (2) Twelve (12) months from the decedent's date of death.<br>
Pat Example and Sam Example<br>Co Executors<br>Ann Counsel<br>1 Law Sq<br>Dickson TN 37055<br>Attorney<br>Lynn Clerk<br>Clerk &amp; Master<br>
October 28, 2026, November 4, 2026</p><hr>
<p>PUBLIC NOTICE<br>IN THE CHANCERY COURT FOR DICKSON COUNTY, TENNESSEE<br>Publication in Lieu of Personal Service - Non-Resident Tax Notice<br>
NAME MAP/PARCEL YEAR DEED BK/PAGE NAME MAP/PARCEL YEAR<br>Known Heirs 23-17.01 2024 108/355 Pat Owner 127-30.00 2024 729/669<br>
Other Owner 111-EB-14.00 2024 1551/2570 Family LLC 82-53.00 001 2024 City of<br>1024 Example Circle 2023<br>Box 23-17.01 2023</p><hr>
<p>INVITATION TO BID Mulch for 100 Example Park, bids due 11/20/2026</p></div>"""

HUB = ('<a href="https://mainstreetmediatn.com/articles/bargainbrowser/public-notices-dickson-post-november-04-2026/?pubid=x">a</a>'
       '<a href="https://mainstreetmediatn.com/articles/bargainbrowser/public-notices-pulaski-citizen-november-04-2026/">b</a>'
       '<a href="https://mainstreetmediatn.com/articles/bargainbrowser/public-notices-wilson-post-august-01-2026/">c</a>')


class MainStreetMediaTests(unittest.TestCase):
    def test_issue_links_focus_counties_and_lookback(self):
        links = msm.issue_links(HUB, date(2026, 11, 5))
        self.assertEqual([(c, d) for _, c, d in links], [("dickson", date(2026, 11, 4))])

    def test_probate_and_tax_notices(self):
        bot = msm.MainStreetMediaBot()
        leads = [l for i, lines in enumerate(msm.notices(PROBATE))
                 for l in bot._leads("https://example.invalid/post/", "dickson", date(2026, 11, 4), i, lines)]
        probate = [l for l in leads if l.distress_type == "PROBATE"]
        self.assertEqual(len(probate), 1)
        p = probate[0]
        self.assertEqual((p.bot_source, p.county, p.full_name), ("court_notices", "dickson", "Norma Example"))
        n = p.raw_payload["court_notice"]
        self.assertEqual([r["name"] for r in n["representatives"]], ["Pat Example", "Sam Example"])
        self.assertEqual(n["attorney"]["names"], ["Ann Counsel"])
        self.assertEqual(n["attorney"]["address"], ["1 Law Sq", "Dickson TN 37055"])
        tax = {l.raw_payload["parcel"]: l.raw_payload["tax_years"] for l in leads if l.distress_type == "TAX_LIEN"}
        self.assertEqual(tax, {"23-17.01": ["2024", "2023"], "127-30.00": ["2024"], "111-EB-14.00": ["2024"], "82-53.00 001": ["2024"]})
        self.assertTrue(all(l.bot_source == "main_street_media" for l in leads if l.distress_type == "TAX_LIEN"))
        self.assertEqual(len(leads), 5, "the bid invitation is not a lead")

    def test_robertson_names_only_layout_and_split_paragraphs(self):
        html = ('<div class="entry-content"><p>NOTICE TO CREDITORS<br>Case Number: 74CH1-2026-CV-001<br>Estate of Mark Example, Deceased<br>'
                'Letters Testamentary in respect of the estate of Mark Example, who died 9/17/2026, were issued by the Chancery Court for Robertson County, Tennessee.<br>'
                '(2) Twelve (12) months from the decedent date of death.<br>Keith Example, Executor<br>Rebecca Counsel, Attorney<br>'
                'Jon Clerk, Clerk &amp; Master</p><hr>'
                '<p>IN THE CHANCERY COURT FOR ROBERTSON COUNTY, TENNESSEE Case 74CH1-2022-CV-364 Final Order of Sale, Special Commissioner</p>'
                '<p>This judicial sale shall occur onsite at 2 Example St on October 22, 2026.</p><hr>'
                '<p>NOTICE OF SALE OF REAL ESTATE WHEREAS, by deed of trust dated 2019, default has occurred</p></div>')
        bot = msm.MainStreetMediaBot()
        leads = [l for i, lines in enumerate(msm.notices(html))
                 for l in bot._leads("https://example.invalid/p/", "robertson", date(2026, 11, 4), i, lines)]
        self.assertEqual([l.distress_type for l in leads], ["PROBATE", "COURT_SALE"], "two paragraphs are one sale; trustee sale skipped")
        n = leads[0].raw_payload["court_notice"]
        self.assertEqual((n["representatives"][0]["name"], n["attorney"]["names"]), ("Keith Example", ["Rebecca Counsel"]))
        self.assertEqual(leads[1].county, "robertson")

    def test_wilson_labeled_parcels(self):
        lines = ["CHANCERY SALE OF LAND FOR DELINQUENT TAXES",
                 "In obedience to decrees of the Chancery Court of Wilson County vs. DELINQUENT TAXPAYERS",
                 "TAX MAP & PARCEL NUMBER: MAP: 127 PARCEL: 34.00 Tennessee as being Map 128A",
                 "TAX MAP & PARCEL NUMBER: MAP: 128 A GROUP: B PARCEL: 21.00", "MAP: 81 G GROUP: A PARCEL: 4.00"]
        self.assertEqual([p["parcel"] for p in msm.tax_parcels(lines)], ["127-34.00", "128A-B-21.00", "81G-A-4.00"])

    def test_same_estate_key_as_ledger(self):
        from src.bots.court_notices_bot import lead_key
        self.assertEqual(lead_key("probate_notice_to_creditors", "dickson", "22CH1-2026-PR-001", "x"),
                         ("court_notice", "probate_notice_to_creditors|dickson|22CH1-2026-PR-001"))

    def test_trustee_sale_layouts(self):
        # "believed to be" layout, hyphenated column breaks, numeric date
        a = msm.trustee_sale([
            "TS#: 2026-00001-TN Notice Of Substitute Trustee's Sale Whereas, Pat Example, unmarried by Deed of Trust,",
            "certain real property located in Montgomery County, Tennessee. The street address of the above-de- scribed",
            "property is believed to be 12 Example Ct, Clarksville, TN 37042, but if such address is not part",
            "will proceed to sell the below-mentioned property on 11/18/2026, at 2:00 PM at the Courthouse.",
            "THIS LAW FIRM IS ATTEMPTING TO COLLECT A DEBT. ANY INFORMATION OBTAINED WILL BE USED FOR THAT PURPOSE. Example Law, PLLC, Substitute Trustee"])
        self.assertEqual((a["county"], a["address"], a["sale_date"], a["owner"], a["trustee"]),
                         ("montgomery", "12 Example Ct, Clarksville, TN 37042", "2026-11-18", "Pat Example, unmarried", "Example Law, PLLC"))
        # labeled layout, "on or about", printed in a neighboring county's paper
        b = msm.trustee_sale([
            "SUBSTITUTE TRUSTEE'S SALE Sale at public auction will be on December 3, 2026 on or about 12:00PM local time,",
            "The following real estate located in Rutherford County, Tennessee, will be sold.",
            "Current Owner(s) of Property: LEE SAMPLE Street Address: 47 Sample Rd, Murfreesboro, TN 37128 Parcel ID: 1-2"])
        self.assertEqual((b["county"], b["address"], b["sale_date"], b["owner"]),
                         ("rutherford", "47 Sample Rd, Murfreesboro, TN 37128", "2026-12-03", "LEE SAMPLE"))
        self.assertIsNone(msm.trustee_sale(["NOTICE OF SUBSTITUTE TRUSTEE'S SALE", "Land described by metes and bounds only."]))
        self.assertIsNone(msm.trustee_sale(["PUBLIC NOTICE", "The Beer Board will meet on November 12, 2026 at 5:30 p.m."]))
        self.assertEqual(msm.address_key("12 Example Ct., Clarksville, TN"), "12 EXAMPLE CT")


if __name__ == "__main__":
    unittest.main()
