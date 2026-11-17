"""Tests for the boat lanes: TN marina-lien notice parsing and grading, the
vessel_lien lane, USTP trustee parsing, CourtListener counsel shaping, and
the parked boat-owner skip trace. No network.

    python -m unittest tests.test_boat_lanes -v
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

from src.bots._boat_notice import grade, hin_model_year, parse  # noqa: E402
from src.bots._paid import PAID_BOTS  # noqa: E402
from src.bots._personal_property import classify  # noqa: E402
from src.bots._ust_trustees import index, lookup, name_key  # noqa: E402
from src.bots._ust_trustees import parse as parse_ust  # noqa: E402
from src.bots.boat_owner_skiptrace_bot import area_of  # noqa: E402
from src.bots.court_contacts_bot import counsel_from  # noqa: E402

ATTENTION = ("NOTICE ATTENTION Mark Nelsen or current owner of a Holiday Mansion vessel with TN registration "
             "#:TN7447BA and hull ID#: HMH01039B787 that is stored at Cedar Creek marina located at 9120 "
             "Saundersville Road in Mount Juliet, TN 37122. You have 30 days to pay Cedar Creek Marina for your past")
CLAIMS = ("PUBLIC NOTICE Lake Life Boats, LLC, 14745 Smithville Hwy, Silver Point, TN 38582, claims a lien for "
          "unpaid storage charges against a 2000 Lakeview 16' x 80' Aluminum Hull Vessel , TN Registration No. "
          "TN4494DB, USCG Documentation No. 1289189, HIN LGV05536E000, owned by Thomas O. Holcomb. Amount due as")
AUCTION = ("NOTICE OF PUBLIC AUCTION MARINA LIEN SALE NOTICE IS HEREBY GIVEN that Lakeshore Marina Management, LLC "
           "(\"Lakeshore Marina\") will sell at public auction the vessel s described below to enforce its marina lien")
BASS = ("NOTICE ATTENTION Jason Lanfair or the current owner of a 1991 Stratos Boats vessel with TN registration #: "
        "TN0012CK and hull ID#: BNZ4V176A191 that is stored at Cedar Creek Marina located at 9120 Saundersville Road")
LIMITATION = ("US DISTRICT COURT, EASTERN DISTRICT OF TN, IN THE MATTER OF THE COMPLAINT OF MICHAEL COLE, AS OWNER OF "
              "A 2003 23-FOOT MASTERCRAFT RECREATIONAL VESSEL , HIN# MBCNVMG4E303 FOR EXONERATION FROM OR LIMITATION")
JON_BOAT = ("PUBLIC NOTICE Notice of an abandoned vessel recovered on the Pigeon River in Cocke County, Tennessee. The "
            "vessel is a 14-foot aluminum V-bottom/jon boat. The vessel has no identifying numbers or registration")

UST_HTML = """<h3>FLORIDA</h3><dl><dd>Note</dd></dl>
<dl><dd><h3><a name="sfl"></a>TRUSTEE(S) COVERING THE SOUTHERN DISTRICT OF FLORIDA</h3>
<dl><dd><b> Robin R. Weiner </b><br> P.O. Box 559007 <br><b>Fort Lauderdale</b>, FL 33355 <br>
E-mail: ecf@ch13weiner.com <br> Phone: (954) 382-2001 <br></dd></dl>
</dd></dl><h3>TENNESSEE</h3>
<dl><dd><h3>TRUSTEE(S) COVERING THE MIDDLE DISTRICT OF TENNESSEE</h3>
<dl><dd><b> Jeanne Ann Burton </b><br> 102 Woodmont Blvd <br><b>Nashville</b>, TN 37205 <br>
E-mail: jeanne@burtonlaw.com <br> Phone: (615) 678-7234 <br></dd></dl></dd></dl>"""


class NoticeParseTests(unittest.TestCase):
    def test_attention_format(self):
        v = parse(ATTENTION)
        self.assertEqual((v["kind"], v["owner"], v["description"], v["marina"], v["registration"], v["hin"]),
                         ("marina_lien_notice", "Mark Nelsen", "Holiday Mansion", "Cedar Creek marina",
                          "TN7447BA", "HMH01039B787"))
        self.assertEqual(v["year"], 1987)
        self.assertIn("Mount Juliet, TN 37122", v["marina_address"])

    def test_claims_lien_format(self):
        v = parse(CLAIMS)
        self.assertEqual((v["owner"], v["marina"], v["registration"], v["hin"], v["year"], v["length_ft"], v["width_ft"]),
                         ("Thomas O. Holcomb", "Lake Life Boats, LLC", "TN4494DB", "LGV05536E000", 2000, 80, 16))
        self.assertEqual(grade(v), "A")

    def test_marina_auction_is_the_marina(self):
        v = parse(AUCTION)
        self.assertEqual((v["kind"], v["marina"], grade(v)), ("marina_auction", "Lakeshore Marina Management", "B"))

    def test_picky_grades(self):
        self.assertEqual(grade(parse(ATTENTION)), "B")      # 1987 houseboat
        self.assertIsNone(grade(parse(BASS)))               # 1991 bass boat
        self.assertIsNone(parse(LIMITATION))                # owner's liability suit, not a sale
        self.assertIsNone(parse(JON_BOAT))                  # no HIN, no registration

    def test_hin_model_year(self):
        self.assertEqual(hin_model_year("HMH01039B787", date(2026, 11, 16)), 1987)
        self.assertEqual(hin_model_year("SERV1234K526", date(2026, 11, 16)), 2026)
        self.assertIsNone(hin_model_year("2215131263"))


class VesselLienLaneTests(unittest.TestCase):
    def test_lane_and_contacts(self):
        v = {**parse(CLAIMS), "grade": "A"}
        t = classify({"distress_type": "MARINA_LIEN", "raw_payload": {"state": "TN", "vessel": v}})
        self.assertEqual((t["lane"], t["grade"], t["categories"]), ("vessel_lien", "A", ["boats"]))
        self.assertEqual(t["contacts"]["owner"], "Thomas O. Holcomb")
        self.assertEqual(t["contacts"]["marina"]["name"], "Lake Life Boats, LLC")

    def test_ungraded_boat_is_not_tagged(self):
        v = {**parse(BASS), "grade": None}
        self.assertIsNone(classify({"distress_type": "MARINA_LIEN", "raw_payload": {"vessel": v}}))

    def test_b2b_contacts_flow_to_lane(self):
        raw = {"state": "FL", "case_number": "1:26-cv-1", "vessel_case": {"vessel": "SEA DREAM", "plaintiff": "Bank",
                                                                            "claim_kind": "lender", "yacht": True},
               "b2b_contacts": {"counsel": [{"name": "A Lawyer", "phone": "305-555-0100", "email": None}]}}
        t = classify({"distress_type": "VESSEL_ARREST", "raw_payload": raw})
        self.assertEqual(t["contacts"]["counsel"][0]["phone"], "305-555-0100")


class ContactsTests(unittest.TestCase):
    def test_ust_parse_and_lookup(self):
        e = parse_ust(UST_HTML)
        self.assertEqual([(x["name"], x["court"], x["phone"]) for x in e],
                         [("Robin R. Weiner", "flsb", "(954) 382-2001"), ("Jeanne Ann Burton", "tnmb", "(615) 678-7234")])
        idx = index(e)
        self.assertEqual(lookup(idx, "Weiner, Robin R", "flsb")["email"], "ecf@ch13weiner.com")
        self.assertEqual(name_key("Burton, Jeanne Ann"), ("BURTON", "JEANNE"))
        self.assertIsNone(lookup(idx, "Doe, Jane", "flsb"))

    def test_counsel_shape(self):
        rows = [{"name": "Jane Roe", "contact_raw": "Jane Roe\nRoe Maritime Law\n100 Main St", "phone": "(305) 555-0100",
                 "email": "jr@roe.law"}, {"name": "No Contact", "contact_raw": "", "phone": "", "email": ""}]
        self.assertEqual(counsel_from(rows), [{"name": "Jane Roe", "firm": "Roe Maritime Law",
                                               "phone": "(305) 555-0100", "email": "jr@roe.law"}])


class TnBankruptcyFeedTests(unittest.TestCase):
    def test_only_new_petitions(self):
        from src.bots.court_rss_bot import NEW_PETITION
        keep = ["Voluntary Petition (Chapter 7)", "Chapter 13 Voluntary Petition (fee) - case upload",
                "Voluntary Petition (Ch 13 - Atty Case Opening Only)"]
        drop = ["Notice of Postpetition Mortgage Fees, Expenses, and Charges", "Voluntary Petition (Amended)",
                "Amended/Corrected Voluntary Petition", "BNC Certificate of Notice"]
        self.assertTrue(all(NEW_PETITION.search(t) for t in keep))
        self.assertFalse(any(NEW_PETITION.search(t) for t in drop))


class SkiptraceTests(unittest.TestCase):
    def test_area_only_lookup(self):
        self.assertEqual(area_of("9120 Saundersville Road in Mount Juliet, TN 37122")["addressLine2"],
                         "Mount Juliet, TN 37122")
        self.assertEqual(area_of("14745 Smithville Hwy, Silver Point, TN 38582")["addressLine2"],
                         "Silver Point, TN 38582")
        self.assertEqual(area_of("4027 Lavergne Couchville Pike in Antioch TN, 37013")["addressLine2"],
                         "Antioch, TN 37013")
        self.assertEqual(area_of(""), {})

    def test_parked_with_paid_data(self):
        self.assertIn("boat_owner_skiptrace", PAID_BOTS)


if __name__ == "__main__":
    unittest.main()
