import unittest

from src.bots import _hamilton_herald_assessor as h


class HeraldAssessorTests(unittest.TestCase):
    def setUp(self):
        self.parcel = "081K-B-012"
        self.street = "801 PICKETT GULF RD"
        self.row = dict(bot_source=h.SOURCE, county="hamilton", pipeline_lead_key=h.BotBase.make_lead_key(h.SOURCE, "FL-53968"),
                        property_address=self.street + ", Hixson, TN", phone_metadata={"manual": "keep"})
        self.raw = dict(publication=h.SOURCE, notice_id="53968", property_address=self.row["property_address"],
                        body="Hamilton County. LESS AND EXCEPT PARCEL 081K A 019. Parcel ID: 081K-B-012 ",
                        parcel="081K A 019")
        self.row["raw_payload"] = self.raw
        self.hit = dict(property_address=self.street, appraised=108800, owner="ROLL OWNER")
        self.index = {self.parcel:self.hit}
        self.stamp = "Sat, 21 Nov 2026 06:02:03 GMT"

    def review(self, ambiguous=(), duplicates=()):
        return h.parcel_review(self.row,self.index,{self.street:self.parcel},ambiguous,duplicates,self.stamp,"county")

    def test_excluded_parcel_and_repeat_refresh(self):
        e = self.review(ambiguous={self.street})
        self.assertEqual(e["parcel"], self.parcel)
        patch = h.review_patch(self.row,e)
        self.assertEqual(patch["property_value_as_of"],"2026-11-21")
        self.assertEqual(patch["property_value"],108800)
        self.assertEqual(patch["phone_metadata"]["manual"],"keep")
        self.row.update(patch)
        self.assertEqual(h.review_patch(self.row,e),{})
        self.raw["body"] += " Parcel ID: 081K-B-013"
        held = self.review()
        self.assertEqual(held["status"],"multiple_parcels")
        self.assertIsNone(h.review_patch(self.row,held)["property_value"])
        self.row.update(property_value_source="other",owner_name_records="MANUAL")
        self.assertNotIn("property_value",h.review_patch(self.row,held))
        self.assertNotIn("owner_name_records",h.review_patch(self.row,held))

    def test_conflicts_multiple_and_nonstandard_controls(self):
        self.raw.pop("parcel")
        for text,status in [
            ("Tax ID: 082N A 015, 016 and 017", "multiple_parcels"),
            ("Parcel Number: 1200 E 017", "unresolved_parcel"),
            ("Tax ID: UNKNOWN", "unparsed_parcel"),
            ("Parcel ID: 081K B 012 L000", "multiple_parcels"),
        ]:
            with self.subTest(text=text):
                self.raw["body"] = "Hamilton County. "+text
                self.assertEqual(self.review()["status"],status)
        self.assertEqual(h.control("137O_H_001.01"),"137O-H-001.01")
        self.assertEqual(h.control("021 010.07"),"021--010.07")

    def test_address_only_requires_printed_subject_and_unique_full_situs(self):
        self.raw.update(parcel=None,body="Hamilton County. ALSO KNOWN AS: 801 Pickett Gulf Road, Hixson, TN")
        self.assertEqual(self.review()["status"],"unique_county_address")
        self.assertEqual(self.review({self.street})["status"],"ambiguous_address")
        self.assertEqual(self.review(duplicates={self.parcel})["status"],"unresolved_parcel")
        self.raw["body"] = self.raw["body"].replace("ALSO KNOWN AS:","Trustee office:")
        self.assertEqual(self.review()["status"],"unconfirmed_notice_address")
        self.row["pipeline_lead_key"]="wrong"
        self.assertIsNone(self.review())

    def test_matching_existing_fields_never_claimed(self):
        self.row.update(property_value=108800,property_value_source="other",owner_name_records="ROLL OWNER")
        patch = h.review_patch(self.row,self.review())
        self.assertEqual(set(patch),{"phone_metadata"})
        self.row.update(patch)
        self.assertEqual(set(h.review_patch(self.row,{"status":"ambiguous_address"})),{"phone_metadata"})

    def test_truncated_publisher_street_needs_printed_complete_address(self):
        self.row["property_address"] = self.raw["property_address"] = "801 PICKETT GULF, Hixson, TN"
        self.assertEqual(self.review()["status"],"address_conflict")
        self.raw["body"] += " Property address: 801 Pickett Gulf Rd, Hixson, TN."
        self.assertEqual(self.review()["parcel"],self.parcel)
        self.hit["situs_unit"] = "2"
        self.assertEqual(self.review()["status"],"address_conflict")
