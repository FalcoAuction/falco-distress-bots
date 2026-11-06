"""Tests for the residential / commercial / land lane classifier and the
contact-path grading. No network.

    python -m unittest tests.test_property_lane -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._property_lane import property_lane, owner_type  # noqa: E402
from src.bots.property_lane_bot import _dedupe  # noqa: E402

TRUSTED_PHONE_META = {"phone_resolver": {"confidence": 0.9, "source": "skip_trace"}}


def row(**kw):
    base = {"id": "1", "distress_type": "TAX_LIEN", "property_address": "1 Main St, Nashville, TN",
            "owner_name_records": "JOHN SMITH", "raw_payload": {}, "phone_metadata": {}}
    base.update(kw)
    return base


class LaneTests(unittest.TestCase):
    def test_davidson_office_is_commercial(self):
        r = property_lane(row(raw_payload={"padctn": {"land_use": "C32 - OFFICE BLDG", "appraised": 400000,
                                                      "mailing_address": "12 Oak Ave, Nashville, TN 37203"}}))
        self.assertEqual(r["lane"], "commercial")
        self.assertEqual(r["grade"], "B")          # value + strong distress, mail only

    def test_metro_parcel_layer(self):
        raw = {"metro_parcel": {"land_use": "STRIP SHOPPING CENTER", "total_appraisal": 1200000, "acres": 1.4,
                                "owner_mailing": "220 GREAT CIRCLE RD STE 126, NASHVILLE TN 37228"}}
        r = property_lane(row(raw_payload=raw))
        self.assertEqual((r["lane"], r["value"], r["contact"]["mailing_state"]), ("commercial", 1200000, "TN"))
        self.assertEqual(property_lane(row(raw_payload={"metro_parcel": {"land_use": "QUADPLEX"}}))["lane"],
                         "residential")

    def test_duplex_stays_residential(self):
        r = property_lane(row(raw_payload={"padctn": {"land_use": "C12 - DUPLEX"}}))
        self.assertEqual(r["lane"], "residential")
        self.assertIsNone(r["grade"])

    def test_vacant_residential_land_is_land(self):
        r = property_lane(row(raw_payload={"padctn": {"land_use": "R10 - VACANT RESIDENTIAL LAND"}}))
        self.assertEqual(r["lane"], "land")

    def test_vacant_commercial_land_subtype(self):
        r = property_lane(row(raw_payload={"padctn": {"land_use": "C20 - VACANT COMMERCIAL LAND"}}))
        self.assertEqual((r["lane"], r["subtype"]), ("land", "commercial_land"))

    def test_exempt_church_owner_is_commercial_church(self):
        r = property_lane(row(owner_name_records="FIRST BAPTIST CHURCH",
                              raw_payload={"padctn": {"land_use": "E91 - CHURCH"}}))
        self.assertEqual((r["lane"], r["subtype"]), ("commercial", "church"))

    def test_exempt_state_parcel_not_promoted_to_land_by_acreage(self):
        r = property_lane(row(raw_payload={"padctn": {"land_use": "E04 - STATE OTHER THAN OFC", "land_acres": 40}}))
        self.assertEqual(r["lane"], "unknown")

    def test_hamilton_codes(self):
        mk = lambda code: property_lane(row(bot_source="hamilton_tax_delinquent",
                                            raw_payload={"county": "hamilton", "land_use": code}))["lane"]
        self.assertEqual(mk("111"), "residential")
        self.assertEqual(mk("550"), "commercial")
        self.assertEqual(mk("910"), "land")
        self.assertEqual(mk("830"), "land")

    def test_hamilton_code_ignored_for_other_counties(self):
        r = property_lane(row(bot_source="nashville_codes", raw_payload={"land_use": "550"}))
        self.assertEqual(r["lane"], "unknown")

    def test_hamilton_assessor_descriptions(self):
        mk = lambda d: property_lane(row(raw_payload={"hamilton_assessor": {"land_use": d}}))
        self.assertEqual(mk("One Family Household Unit")["lane"], "residential")
        self.assertEqual(mk("UNDEVELOPED LAND AND WATER AREAS")["lane"], "land")
        self.assertEqual(mk("Business Services")["lane"], "commercial")
        self.assertEqual(mk("Transient Lodging")["lane"], "commercial")
        self.assertEqual(mk("Mobile Home Park")["lane"], "commercial")
        self.assertEqual(mk("Mobile Homes (Single Trailer)")["lane"], "residential")
        self.assertEqual(mk("Governmental Functions and Services")["lane"], "unknown")
        self.assertEqual(mk("Parks")["lane"], "unknown")
        self.assertEqual(property_lane(row(raw_payload={"metro_parcel": {"land_use": "PARKING LOT"}}))["lane"],
                         "commercial")

    def test_hamilton_assessor_mailing_and_value(self):
        raw = {"hamilton_assessor": {"land_use": "Retail Trade - General", "appraised": 300000,
                                     "owner_mailing": "PO BOX 12, ATLANTA GA 30301", "owner_state": "GA"}}
        r = property_lane(row(raw_payload=raw))
        self.assertEqual((r["lane"], r["value"], r["contact"]["mailing_state"]), ("commercial", 300000, "GA"))

    def test_shelby_class_letter(self):
        r = property_lane(row(raw_payload={"shelby_arcgis": {"class": "C"}}))
        self.assertEqual(r["lane"], "commercial")

    def test_house_on_big_acreage_is_land(self):
        r = property_lane(row(raw_payload={"padctn": {"land_use": "R11 - SINGLE FAMILY", "land_acres": "25"}}))
        self.assertEqual((r["lane"], r["subtype"]), ("land", "acreage_with_home"))

    def test_unknown_without_land_use(self):
        self.assertEqual(property_lane(row())["lane"], "unknown")


class GradeTests(unittest.TestCase):
    def office(self, **kw):
        raw = {"padctn": {"land_use": "C25 - ONE STORY GENERAL RETAIL STORE", "appraised": 300000,
                          "mailing_address": "9 Elm St, Nashville, TN 37206"}}
        return property_lane(row(raw_payload=raw, **kw))

    def test_trusted_phone_and_strong_distress_is_A(self):
        r = self.office(phone="615-555-0101", phone_metadata=TRUSTED_PHONE_META)
        self.assertEqual(r["contact"]["tier"], "phone")
        self.assertEqual(r["grade"], "A")

    def test_untrusted_phone_does_not_count(self):
        r = self.office(phone="615-555-0101",
                        phone_metadata={"phone_resolver": {"confidence": 0.9, "source": "notice_body"}})
        self.assertEqual(r["contact"]["tier"], "mail")
        self.assertTrue(r["contact"]["unverified_phone"])

    def test_code_violation_never_A(self):
        r = self.office(distress_type="CODE_VIOLATION", phone="615-555-0101", phone_metadata=TRUSTED_PHONE_META)
        self.assertEqual(r["grade"], "B")

    def test_demolition_permit_excluded(self):
        r = self.office(distress_type="DEMOLITION")
        self.assertEqual(r["exclude_reason"], "permit_not_distress")

    def test_no_contact_excluded(self):
        r = property_lane(row(raw_payload={"padctn": {"land_use": "C32 - OFFICE BLDG", "appraised": 400000}}))
        self.assertEqual(r["exclude_reason"], "no_contact_path")

    def test_government_owner_excluded(self):
        r = self.office(owner_name_records="METROPOLITAN GOVERNMENT OF NASHVILLE")
        self.assertEqual(r["exclude_reason"], "government_owner")

    def test_institutional_size_excluded(self):
        raw = {"padctn": {"land_use": "C38 - APARTMENT", "appraised": 45000000, "mailing_address": "1 A St, Nashville, TN 37203"}}
        self.assertEqual(property_lane(row(raw_payload=raw))["exclude_reason"], "institutional_size")

    def test_hamilton_personal_property_excluded(self):
        raw = {"county": "hamilton", "parcel": "PER--10052662",
               "hamilton_assessor": {"land_use": "Apartment:  10 units or more", "appraised": 6940000,
                                     "owner_mailing": "5583 BOBBY HICKS HWY, GRAY TN 37615", "owner_state": "TN"}}
        r = property_lane(row(bot_source="hamilton_tax_delinquent", owner_name_records="HATTIE BELL CATERING",
                              raw_payload=raw))
        self.assertEqual((r["grade"], r["exclude_reason"]), (None, "personal_property_tax"))
        from src.bots.hamilton_tax_delinquent_bot import HamiltonTaxDelinquentBot as H
        self.assertTrue(H.is_personal_property("PER--10052662"))
        self.assertFalse(H.is_personal_property("147G-B-012"))

    def test_owner_mismatch_excluded(self):
        raw = {"hamilton_assessor": {"land_use": "Apartment:  10 units or more", "appraised": 4150000,
                                     "owner": "1717 WHITE OAK TN LLC",
                                     "owner_mailing": "321 N CLARK ST, CHICAGO IL 60654", "owner_state": "IL"}}
        r = property_lane(row(owner_name_records="OPTIC UNITY", raw_payload=raw))
        self.assertEqual(r["exclude_reason"], "owner_mismatch")
        generic = property_lane(row(owner_name_records="GW & SONS PROFESSIONAL TRUCK & TRAILER CENTERS", raw_payload={
            "hamilton_assessor": {"land_use": "Repair Services", "appraised": 1500000,
                                  "owner": "FRONTIER TRUCK WASH INC & ATTN DAVID MYERS",
                                  "owner_mailing": "1 A ST, CHATTANOOGA TN 37402"}}))
        self.assertEqual(generic["exclude_reason"], "owner_mismatch")
        ok = property_lane(row(owner_name_records="Byrd Tyrone & Beth", raw_payload={
            "hamilton_assessor": {"land_use": "Retail Trade - General", "appraised": 300000,
                                  "owner": "BYRD TYRONE & BETH DENISE", "owner_mailing": "127 FAIR ST, CHATTANOOGA TN 37415"}}))
        self.assertIsNone(ok["exclude_reason"])

    def test_past_trustee_sale_excluded(self):
        r = self.office(distress_type="PRE_FORECLOSURE", trustee_sale_date="2020-01-01")
        self.assertEqual(r["exclude_reason"], "sale_passed")


class LandFlipTests(unittest.TestCase):
    def test_out_of_state_hamilton_mailing(self):
        raw = {"county": "hamilton", "land_use": "910", "mailing_address": "P O BOX 5416, TYLER TX75712"}
        r = property_lane(row(bot_source="hamilton_tax_delinquent", property_value=50000, raw_payload=raw))
        self.assertTrue(r["contact"]["out_of_state"])
        self.assertEqual(r["contact"]["mailing_state"], "TX")
        self.assertIn("out_of_state_owner", r["land_flip"]["signals"])
        self.assertEqual(r["land_flip"]["score"], 6)   # 3 out of state + 2 tax + 1 price band

    def test_in_state_mailing(self):
        raw = {"county": "hamilton", "land_use": "910", "mailing_address": "2141 CRESCENT CLUB DR, HIXSON TN37343"}
        r = property_lane(row(bot_source="hamilton_tax_delinquent", raw_payload=raw))
        self.assertEqual(r["contact"]["mailing_state"], "TN")
        self.assertFalse(r["contact"]["out_of_state"])


class HelperTests(unittest.TestCase):
    def test_owner_type(self):
        self.assertEqual(owner_type("ACME HOLDINGS LLC"), "entity")
        self.assertEqual(owner_type("JANE DOE"), "person")
        self.assertEqual(owner_type("HAMILTON COUNTY"), "government")
        self.assertEqual(owner_type("GRACE FELLOWSHIP"), "religious")

    def test_dedupe_keeps_best_grade(self):
        a = {"row": {"id": "1", "property_address": "1 Main St, Nashville, TN"}, "lane": {"grade": "B"}}
        b = {"row": {"id": "2", "property_address": "1 MAIN ST NASHVILLE TN"}, "lane": {"grade": "A"}}
        out = _dedupe([a, b])
        self.assertEqual([it["row"]["id"] for it in out], ["2"])


if __name__ == "__main__":
    unittest.main()
