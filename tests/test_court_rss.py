"""Tests for the court RSS parser, vessel-arrest detection, FL aircraft
matching, and the vessel-arrest lane. No network.

    python -m unittest tests.test_court_rss -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._court_rss import debtors, parse_feed, vessel_case  # noqa: E402
from src.bots._aircraft_match import AircraftIndex, district_of, load_aircraft, load_ref  # noqa: E402
from src.bots._personal_property import classify  # noqa: E402
from tests.test_aircraft_match import MASTER_HEADER, REF  # noqa: E402

BK_FEED = """<?xml version="1.0"?><rss><channel>
<item><title>26-25183 Tina Lucy Griffin</title>
<link>https://ecf.flsb.uscourts.gov/cgi-bin/DktRpt.pl?856496</link>
<description>Type: bk Office: 0 Chapter: 13 Trustee: Weiner, Robin R [Chapter 13 Plan] (&lt;a href='x'&gt;8&lt;/a&gt;)</description>
<pubDate>Sun, 15 Nov 2026 20:25:28 GMT</pubDate></item>
<item><title>26-23359- Blue Water Yacht Charters, LLC</title>
<link>https://ecf.flsb.uscourts.gov/cgi-bin/DktRpt.pl?854611</link>
<description>Type: bk Office: 1 Chapter: 7 Trustee: Doe, Jane [Voluntary Petition]</description>
<pubDate>Sun, 15 Nov 2026 20:23:58 GMT</pubDate></item>
</channel></rss>"""

DC_FEED = """<rss><channel>
<item><title>1:26-cv-27401 Seacoast National Bank v. M/Y SEA DREAM, her engines, tackle and apparel, in rem</title>
<link>https://ecf.flsd.uscourts.gov/cgi-bin/DktRpt.pl?726401</link>
<description>[Verified Complaint In Rem]</description><pubDate>Sun, 15 Nov 2026 18:00:00 GMT</pubDate></item>
<item><title>1:26-cv-27402 In re: Complaint of John Roe, as owner of a 2019 vessel, for Exoneration from or Limitation of Liability</title>
<link>https://ecf.flsd.uscourts.gov/cgi-bin/DktRpt.pl?726402</link>
<description>[Complaint]</description><pubDate>Sun, 15 Nov 2026 18:01:00 GMT</pubDate></item>
<item><title>1:26-cr-20417-1 USA v. Yasmani Rojas Caballero</title>
<link>https://ecf.flsd.uscourts.gov/cgi-bin/DktRpt.pl?725110</link>
<description>[Response]</description><pubDate>Sun, 15 Nov 2026 20:11:31 GMT</pubDate></item>
<item><title>2:26-cv-01999 Harbor Lights Marina LLC v. M/V Lady Luck, Official No. 1234567, in rem</title>
<link>https://ecf.flmd.uscourts.gov/cgi-bin/DktRpt.pl?999</link>
<description>[Order Directing Issuance of Warrant of Arrest]</description><pubDate>Sun, 15 Nov 2026 17:00:00 GMT</pubDate></item>
</channel></rss>"""


def fl_row(n, name, county_fips, year="2015", mdl="JET01", type_acft="4", type_eng="5"):
    o = [""] * 5
    cols = [n, "S" + n, mdl, "", year, "1", name, "1 OCEAN DR", "", "MIAMI", "FL", "33139", "2", county_fips, "US",
            "20260101", "20200101", "1N", type_acft, type_eng, "V", "", "", "", *o, "20300101", "", "", "", "", ""]
    return ",".join(cols)


def tn_row(n, name):
    return fl_row(n, name, "037").replace(",MIAMI,FL,33139,", ",NASHVILLE,TN,37205,")


def index(rows):
    return AircraftIndex(load_aircraft([MASTER_HEADER] + rows, load_ref(REF), ("TN", "FL")))


class FeedTests(unittest.TestCase):
    def test_bankruptcy_items(self):
        items = parse_feed(BK_FEED)
        self.assertEqual([i["case_number"] for i in items], ["26-25183", "26-23359"])
        self.assertEqual((items[0]["caption"], items[0]["chapter"], items[0]["trustee"], items[0]["entry_type"]),
                         ("Tina Lucy Griffin", "13", "Weiner, Robin R", "Chapter 13 Plan"))
        self.assertEqual(items[0]["published"], "2026-11-15T20:25:28+00:00")

    def test_debtors_split(self):
        self.assertEqual(debtors("Chad E Conner and Gail D Dunlap-Conner"), ["Chad E Conner", "Gail D Dunlap-Conner"])

    def test_vessel_cases(self):
        items = parse_feed(DC_FEED)
        found = [(i["case_number"], vessel_case(i)) for i in items]
        hits = [(c, v["vessel"], v["claim_kind"], v["yacht"]) for c, v in found if v]
        self.assertEqual(hits, [("1:26-cv-27401", "SEA DREAM", "lender", True),
                                ("2:26-cv-01999", "Lady Luck", "service_lien", False)])


class FloridaAircraftTests(unittest.TestCase):
    def test_fl_districts(self):
        self.assertEqual(district_of("palm beach", "FL"), "flsb")
        self.assertEqual(district_of("leon", "FL"), "flnb")
        self.assertEqual(district_of("orange", "FL"), "flmb")

    def test_fl_bankruptcy_matches_fl_registrant_in_district(self):
        idx = index([fl_row("100FL", "ROE JOHN Q", "086")])   # Miami-Dade
        tags = idx.match({"distress_type": "BANKRUPTCY", "full_name": "John Q Roe", "court_id": "flsb", "state": "FL"})
        self.assertEqual([(t["n_number"], t["registrant_county"]) for t in tags], [("N100FL", "miami-dade")])
        self.assertEqual(idx.match({"distress_type": "BANKRUPTCY", "full_name": "John Q Roe", "court_id": "flmb",
                                    "state": "FL"}), [])

    def test_states_never_cross(self):
        idx = index([fl_row("100FL", "ROE JOHN Q", "086"), tn_row("200TN", "ACME JETS LLC")])
        self.assertEqual(idx.match({"distress_type": "BANKRUPTCY", "full_name": "John Q Roe", "court_id": "tnmb"}), [])
        self.assertEqual(idx.match({"distress_type": "BANKRUPTCY", "full_name": "Acme Jets LLC", "court_id": "flsb",
                                    "state": "FL"}), [])
        self.assertEqual([t["n_number"] for t in idx.match({"distress_type": "TAX_LIEN", "full_name": "ACME JETS LLC"})],
                         ["N200TN"])

    def test_vessel_arrest_plaintiff_never_matched_to_aircraft(self):
        idx = index([fl_row("300FL", "SEACOAST NATIONAL BANK", "111")])
        self.assertEqual(idx.match({"distress_type": "VESSEL_ARREST", "full_name": "Seacoast National Bank",
                                    "state": "FL"}), [])


class VesselLaneTests(unittest.TestCase):
    def raw(self, caption):
        item = parse_feed(f"<item><title>1:26-cv-1 {caption}</title><link>u</link><description>[x]</description></item>")[0]
        return {"state": "FL", "court_id": "flsd", "case_number": "1:26-cv-1", "case_name": caption,
                "docket_url": "https://ecf/x", "vessel_case": vessel_case(item)}

    def test_yacht_lender_is_a(self):
        t = classify({"distress_type": "VESSEL_ARREST", "raw_payload": self.raw("Seacoast National Bank v. M/Y SEA DREAM, in rem")})
        self.assertEqual((t["lane"], t["grade"], t["categories"]), ("vessel_arrest", "A", ["boats"]))
        self.assertEqual(t["contacts"]["plaintiff"], "Seacoast National Bank")
        self.assertEqual(t["contacts"]["bankruptcy_docket"], "https://ecf/x")

    def test_commercial_vessel_is_b_with_caveat(self):
        t = classify({"distress_type": "VESSEL_ARREST", "raw_payload": self.raw("Gulf Fuel Co. v. M/V ATLAS STAR, in rem")})
        self.assertEqual(t["grade"], "B")
        self.assertTrue(any("commercial vessel" in c for c in t["caveats"]))

    def test_fl_marine_business_bankruptcy_is_liquidation(self):
        t = classify({"distress_type": "BANKRUPTCY", "full_name": "Blue Water Yacht Charters, LLC",
                      "raw_payload": {"chapter": "7", "state": "FL", "case_number": "26-23359"}})
        self.assertEqual((t["lane"], t["categories"]), ("business_liquidation", ["vehicles"]))


if __name__ == "__main__":
    unittest.main()


class Chapter7TrusteeTests(unittest.TestCase):
    def test_case_key_normalizes_judge_suffix_and_padding(self):
        from src.bots._court_rss import case_key
        self.assertEqual(case_key("3:26-bk-04209-CW"), "3:26-bk-04209")
        self.assertEqual(case_key("3:26-bk-4209"), "3:26-bk-04209")
        self.assertIsNone(case_key("26P1969"))

    def test_only_chapter_7_entries_with_a_trustee(self):
        from src.bots._court_rss import chapter7_trustees
        items = [
            {"case_number": "3:26-bk-04209-CW", "chapter": "7", "trustee": "Weiner, Robin R"},
            {"case_number": "3:26-bk-05000", "chapter": "13", "trustee": "Rothschild, Henry E"},
            {"case_number": "3:26-bk-05001", "chapter": "7", "trustee": None},
        ]
        self.assertEqual(chapter7_trustees("tnmb", items), {("tnmb", "3:26-bk-04209"): "Weiner, Robin R"})
