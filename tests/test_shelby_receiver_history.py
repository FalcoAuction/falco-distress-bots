from unittest import TestCase
from unittest.mock import Mock

from src.bots.shelby_receiver_history import book_results, collect_history, matched_parcels


class ReceiverHistoryTests(TestCase):
    key = "0720220E000140"
    target = {"address": "3055 SAINT CROIX PL", "parcel": "072022 E00014", "warrant": "21400073"}

    def entry(self, key=None, location="3055 ST CROIX"):
        key = key or self.key
        return f"Parent Parcel {key}, Parcel No. {key}, Location: {location}, Lot No. UNIT 74"

    def matches(self, *pages):
        return list(book_results(pages, {self.key: [self.target]}))

    def test_page_continuation_keeps_original_parcel_and_provenance(self):
        match, = self.matches("", self.entry(), "The property sold to: JEAN EMILE METELLUS whose address is: withheld")
        self.assertEqual(match["page"], 2)
        self.assertEqual(match["printed_unit"], "74")
        self.assertEqual(match["parcel"], "072022 E00014")
        self.assertEqual(match["historical_buyer"], "JEAN EMILE METELLUS")
        self.assertEqual(match["outcome"], "historical_purchase")

    def test_same_street_or_parent_does_not_match_a_different_unit(self):
        for entry in (self.entry("0720220E000480"), self.entry(self.key + "A"),
                      self.entry(location="3055 ST CROIX UNIT 91"),
                      self.entry().replace("Parcel No. " + self.key, "Parcel No. 0720220E000480")):
            self.assertEqual(self.matches(entry), [])

    def test_next_parcel_buyer_is_never_attributed_to_previous_parcel(self):
        tail = " The property sold to: OTHER PARTY whose address is: withheld"
        for next_entry in (self.entry("0720220E000480"), "Parent Parcel unreadable"):
            match, = self.matches(self.entry(), next_entry + tail)
            self.assertEqual(match["outcome"], "historical_result_unresolved")
            self.assertIsNone(match["historical_buyer"])

    def test_unsold_county_and_unknown(self):
        for buyer, outcome in (("NO BID BY COUNTY, PROPERTY NOT SOLD", "historical_unsold"),
                               ("SHELBY COUNTY GOVERNMENT", "historical_county_purchase"),
                               ("WITHDRAWN", "historical_result_unresolved")):
            match, = self.matches(self.entry() + f" The property sold to: {buyer} whose address is: withheld")
            self.assertEqual(match["outcome"], outcome)

    def test_only_unique_bill_verified_overlaps_are_searched(self):
        listing = {"parcel": self.target["parcel"], "alt_parcel": self.key,
                   "property_review": {"parcel": self.target["parcel"], "status": "matched_county_bill"}}
        case = dict(self.target, current_trustee_listings=[listing])
        receipt = {"cases": [case], "sha256": "digest"}
        self.assertEqual(matched_parcels(receipt), {self.key: [self.target]})
        receipt["cases"] = [dict(case, current_trustee_listings=[listing, listing]),
                            dict(case, current_trustee_listings=[dict(listing, property_review={})])]
        session = Mock()
        report = collect_history(receipt, session)
        session.get.assert_not_called()
        self.assertFalse(report["receiver_appointment_verified"])
        self.assertFalse(report["sale_authority_verified"])
