"""Owner-name matching and county adapters. Synthetic names only.

    python -m unittest tests.test_owner_lookup -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import _owner_lookup as ol  # noqa: E402


def cands(*owners):
    return [ol.Candidate(owner=o, address=f"{i} Example St", parcel=f"P{i}", value=100000 + i) for i, o in enumerate(owners)]


class ParsePersonTests(unittest.TestCase):
    def test_variants(self):
        p = ol.parse_person("Raymond Gerstell Swadley Jr.")
        self.assertEqual((p.first, p.middle_initial, p.last, p.suffix), ("RAYMOND", "G", "SWADLEY", "JR"))
        p = ol.parse_person("Swadley, Raymond G.")
        self.assertEqual((p.first, p.middle_initial, p.last), ("RAYMOND", "G", "SWADLEY"))
        p = ol.parse_person("Ann O’Brien")
        self.assertEqual(p.last, "O'BRIEN")
        self.assertIsNone(ol.parse_person("Cher"))
        self.assertIsNone(ol.parse_person(""))


class SegmentTests(unittest.TestCase):
    def test_co_owner_segments(self):
        self.assertEqual(ol.owner_segments("SMITH HEATHER MARIE & SMITH JOHN MARTIN JR"),
                         [["SMITH", "HEATHER", "MARIE"], ["SMITH", "JOHN", "MARTIN", "JR"]])
        self.assertEqual(ol.owner_segments("WOMBLE JONATHAN J ETUX MISTY M"), [["WOMBLE", "JONATHAN", "J"], ["MISTY", "M"]])


class ChooseTests(unittest.TestCase):
    def match(self, decedent, *owners):
        return ol.choose(ol.parse_person(decedent), cands(*owners))

    def test_single_owner_with_middle_initial(self):
        r = self.match("Mary A. Example", "EXAMPLE MARY A", "EXAMPLE MARK")
        self.assertEqual((r.status, r.confidence, [c.owner for c in r.parcels]), ("matched", "middle_initial", ["EXAMPLE MARY A"]))

    def test_suffix_must_agree(self):
        r = self.match("William T. Example", "EXAMPLE WILLIAM T JR & JENNIFER S", "EXAMPLE WILLIAM T")
        self.assertEqual([c.owner for c in r.parcels], ["EXAMPLE WILLIAM T"])
        r = self.match("Terry Allan Example Jr.", "EXAMPLE TERRY A", "EXAMPLE TERRY A JR")
        self.assertEqual([c.owner for c in r.parcels], ["EXAMPLE TERRY A JR"])

    def test_conflicting_middle_initial_never_matches(self):
        self.assertEqual(self.match("John Q. Example", "EXAMPLE JOHN R").status, "not_found")

    def test_same_person_several_parcels_is_one_estate(self):
        r = self.match("Norma Jean Example", "EXAMPLE NORMA J", "EXAMPLE NORMA J")
        self.assertEqual((r.status, len(r.parcels)), ("matched", 2))
        self.assertEqual(r.primary.parcel, "P1")  # highest value is primary

    def test_middle_initial_narrows_a_common_name(self):
        r = self.match("Michael D. Example", "EXAMPLE MICHAEL", "EXAMPLE MICHAEL D", "EXAMPLE MICHAEL", "EXAMPLE MICHAEL D")
        self.assertEqual((r.status, r.confidence, len(r.parcels)), ("matched", "middle_initial", 2))

    def test_common_name_without_initial_is_ambiguous(self):
        r = self.match("Charles P. Example", *["EXAMPLE CHARLES"] * 6)
        self.assertEqual(r.status, "ambiguous")
        r = self.match("Charles P. Example", "EXAMPLE CHARLES", "EXAMPLE CHARLES")
        self.assertEqual((r.status, r.confidence), ("matched", "name_only"))

    def test_two_different_initials_is_ambiguous_when_notice_has_none(self):
        self.assertEqual(self.match("John Example", "EXAMPLE JOHN A", "EXAMPLE JOHN B").status, "ambiguous")

    def test_entities_and_co_owner_position(self):
        self.assertEqual(self.match("John Example", "EXAMPLE JOHN C JR REV TRUST").status, "not_found")
        self.assertEqual(self.match("John Example", "EXAMPLE JOHN LLC").status, "not_found")
        r = self.match("Heather Example", "OTHER PAT & EXAMPLE HEATHER MARIE")
        self.assertEqual(r.status, "matched")
        # Last name must lead; "JOHN EXAMPLE" order is a different name.
        self.assertEqual(self.match("Example John", "EXAMPLE JOHN").status, "not_found")


class OwnerAgreesTests(unittest.TestCase):
    def test_single_hit_checks(self):
        self.assertFalse(ol.owner_agrees("Norma Jean Example", "EXAMPLE, NORMA D."))
        self.assertTrue(ol.owner_agrees("Norma Jean Example", "EXAMPLE, NORMA J."))
        self.assertTrue(ol.owner_agrees("Norma Jean Example", "EXAMPLE, NORMA"))
        self.assertTrue(ol.owner_agrees("Nestor Joseph Example Jr.", "EXAMPLE, NESTOR JOSEPH JR."))
        self.assertFalse(ol.owner_agrees("Nestor Joseph Example Jr.", "EXAMPLE, NESTOR JOSEPH"))
        self.assertTrue(ol.owner_agrees("Gregory Garland Example", "EXAMPLE, GREGORY G. & AMY M."))
        self.assertFalse(ol.owner_agrees("Gregory Example", "EXAMPLE GREGORY FAMILY TRUST"))
        self.assertFalse(ol.owner_agrees("Gregory Example", None))

    def test_spouse_coowner_maiden_and_senior(self):
        agrees = ol.owner_agrees
        self.assertTrue(agrees("Paulette D Example", "EXAMPLE THOMAS E ET UX PAULETTE D"))
        self.assertTrue(agrees("Cathy L. Example", "EXAMPLE JOHN N ETUX CATHY L"))
        self.assertTrue(agrees("Wanda K. Example", "EXAMPLE ERNEST CLAYTON & WANDA"))
        self.assertTrue(agrees("Brenda Example", "EXAMPLE DAVID C ETUX BRENDA M C/O PAUL EXAMPLE"))
        self.assertFalse(agrees("Paul Example", "EXAMPLE DAVID C ETUX BRENDA M C/O PAUL EXAMPLE"))
        self.assertTrue(agrees("Jeffrey Ray Example", "EXAMPLE CARL W ETAL JEFFREY R EXAMPLE"))
        self.assertTrue(agrees("Duane Keith Example", "EXAMPLE BRADLEY K ETAL DUANE EXAMPLE"))
        self.assertFalse(agrees("Mary Example", "EXAMPLE JOHN ETAL MARY OTHER"))
        self.assertFalse(agrees("George S. Example", "EXAMPLE PEARL R & GEORGE S OTHER"))
        self.assertFalse(agrees("Patricia Jannette Example", "EXAMPLE CHARLES D ETUX PATRICIA ANN"))
        self.assertTrue(agrees("Pamela Hobbs Example", "EXAMPLE, PAMELA MARIE HOBBS"))
        self.assertFalse(agrees("Pamela Jones Example", "EXAMPLE, PAMELA MARIE HOBBS"))
        self.assertTrue(agrees("Ronald Dale Example Sr.", "EXAMPLE, RONALD DALE ETUX"))
        self.assertFalse(agrees("Joseph Brown Example", "EXAMPLE, JOSEPH B., JR"))
        self.assertFalse(agrees("Charles James Example, Jr.", "EXAMPLE CHARLES J"))
        self.assertTrue(agrees("Mary Virginia Example", "EXAMPLE  MARY V "))
        self.assertTrue(agrees("Kristine Example", "EXAMPLE KENNETH D AND WIFE KRISTINE M"))

class ShelbyParsingTests(unittest.TestCase):
    RESULTS = ("<table><tr><th>Parcel ID</th><th>Owner Name</th><th>Property Location</th><th>Link</th></tr>"
               "<tr><td>011111  00001</td><td>EXAMPLE NORMA J</td><td>1 EXAMPLE AVE</td><td><a>view</a></td></tr>"
               "<tr><td>D0241A E00002</td><td>EXAMPLE NORMA J</td><td>2 EXAMPLE RD</td><td><a>view</a></td></tr></table>")
    DETAIL = ("<table><tr><td>Parcel ID:</td><td>011111  00001</td></tr><tr><td>Property Address:</td><td>1  EXAMPLE AVE</td></tr>"
              "<tr><td>Owner Name :</td><td>EXAMPLE MARY A</td></tr><tr><td>Acres :</td><td>0.246</td></tr>"
              "<tr><td>Class:</td><td>RESIDENTIAL</td></tr><tr><td>Total Appraisal:</td><td>$200,100</td></tr></table>")

    def test_results_table(self):
        rows = ol.parse_shelby_results(self.RESULTS)
        self.assertEqual([(r.parcel, r.owner, r.address) for r in rows],
                         [("011111  00001", "EXAMPLE NORMA J", "1 EXAMPLE AVE"), ("D0241A E00002", "EXAMPLE NORMA J", "2 EXAMPLE RD")])

    def test_single_hit_detail_page(self):
        [row] = ol.parse_shelby_results(self.DETAIL)
        self.assertEqual((row.owner, row.address, row.value, row.acres, row.property_class),
                         ("EXAMPLE MARY A", "1 EXAMPLE AVE", 200100.0, 0.246, "RESIDENTIAL"))
        self.assertTrue(row.extra["detail_loaded"])


class LookupTests(unittest.TestCase):
    def test_lookup_statuses_and_value_parsing(self):
        class Res:
            status_code = 200

            def __init__(self, payload):
                self.payload = payload

            def json(self):
                return self.payload

        def fetch(url, **kw):
            assert "EXAMPLE NORMA" in kw["params"]["where"]
            return Res({"features": [{"attributes": {"ParcelID": "1", "Owner1": "EXAMPLE NORMA J", "Owner2": "",
                                                     "PropertyAddress": "1 EXAMPLE DR", "AppraisedValue": "$200,900",
                                                     "AssessmentClass": "RS", "CalcAcreage": 1.1}}]})
        r = ol.lookup(fetch, "Montgomery County", "Norma J. Example")
        self.assertEqual((r["status"], r["primary"]["address"], r["primary"]["value"]), ("matched", "1 EXAMPLE DR", 200900.0))
        self.assertEqual(ol.lookup(fetch, "knox", "Norma Example")["status"], "unsupported_county")
        # Shelby is opt-in: its Assessor blocks this bot's honest user agent.
        import os
        os.environ.pop("FALCO_SHELBY_OWNER_LOOKUP", None)
        self.assertEqual(ol.lookup(fetch, "shelby", "Norma Example")["status"], "unsupported_county")
        self.assertNotIn("shelby", ol.supported_counties())
        os.environ["FALCO_SHELBY_OWNER_LOOKUP"] = "1"
        self.assertIn("shelby", ol.supported_counties())
        os.environ.pop("FALCO_SHELBY_OWNER_LOOKUP")
        self.assertEqual(ol.lookup(fetch, "montgomery", "Cher")["status"], "unparseable_name")
        self.assertEqual(ol.lookup(lambda url, **kw: None, "montgomery", "Norma Example")["status"], "source_error")

    def test_apostrophes_are_escaped_for_arcgis(self):
        seen = {}

        def fetch(url, **kw):
            seen["where"] = kw["params"]["where"]
            return None
        ol.lookup(fetch, "rutherford", "Ann O'Brien")
        self.assertIn("O''BRIEN ANN", seen["where"])


if __name__ == "__main__":
    unittest.main()
