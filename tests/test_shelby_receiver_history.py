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

class RecurringArchiveTests(TestCase):
    def test_daily_archive_rotates_two_and_reuses_completed_books(self):
        from datetime import date
        from unittest.mock import patch
        from src.bots.shelby_receiver_history import collect_history
        session = Mock()
        index = Mock(content=b"".join(f'<a href="/DocumentCenter/View/{i}">TX-2024 Tax Sale 22{i:02}</a>'.encode()
                                      for i in range(5)))
        from unittest.mock import MagicMock
        pdf = MagicMock()
        pdf.__enter__.return_value.iter_content.return_value = [b"pdf"]
        session.get.side_effect = lambda url, **kw: index if "Information" in url else pdf
        day = date(2026, 11, 22)
        with patch("src.bots.shelby_receiver_history.matched_parcels", return_value={"A": []}), patch(
                "src.bots.shelby_receiver_history.read_pages", return_value=["sample"]):
            cache = {}
            for expected in (2, 2, 1, 0):
                before = session.get.call_count
                cache = collect_history({"sha256":"snapshot"}, session, previous=cache, max_books=2, day=day)
                self.assertEqual(session.get.call_count - before, expected + 1)
                self.assertEqual(cache["books_attempted"], expected)
            cache["targets"] = {"other": []}
            changed = collect_history({"sha256":"snapshot"}, session, previous=cache, max_books=2, day=day)
            self.assertEqual(changed["books_attempted"], 2)
            self.assertEqual(sum(b["status"]=="pending" for b in changed["books"]), 3)

    def test_projection_binds_historical_buyer_and_unit_to_own_tax_card(self):
        from datetime import date
        from src.bots._shelby_receiver_crm import reviews
        prop = {"status":"matched_county_bill","tax_roll_owner":"ROLL OWNER","source_url":"bill"}
        listing = {"parcel":"072022 E00014","alt_parcel":"0720220E000140","sale_id":"TS2302",
                   "property_review":prop,"advertised_date":"2027-04-13"}
        case = {"current_trustee_listings":[listing],"warrant":"21400073","address":"3055 ST CROIX",
                "page":1,"hearing_listed":"2026-11-24"}
        hit = {"historical_buyer":"PRIOR BUYER","outcome":"historical_purchase","page":96,"printed_unit":"74",
               "parcel":listing["parcel"],"alt_parcel":listing["alt_parcel"]}
        book = {"status":"searched_extractable_text","url":"book","sale":"1904",
                "matches":[dict(hit, parcel="072022 E00048"), hit]}
        receipt = {"cases":[case],"sale_book_history":{"books":[book]},"source_url":"roster","source_date":"2026-11-13"}
        card, = reviews(receipt, date(2026,11,22)).values()
        self.assertEqual(card["property_review"],prop)
        self.assertEqual(len(card["sale_history"]),1)
        self.assertEqual(card["sale_history"][0]["printed_unit"],"74")
        self.assertFalse(card["sale_authority_verified"])
        listing["property_review"] = {"status":"source_unavailable"}
        held, = reviews(receipt,date(2026,11,22)).values()
        self.assertEqual(held["status"],"property_review_unverified")
        self.assertNotIn("sale_history",held)

    def test_projection_write_merges_only_receiver_field(self):
        from datetime import date
        from unittest.mock import MagicMock
        from src.bots._shelby_receiver_crm import FIELD, publish
        client = MagicMock()
        table = client.table.return_value
        raw = {"parcel":"072022 E00014", "alt_parcel":"0720220E000140", "sale_id":"TS2302", "other":True}
        old = {"id":"a", "raw_payload":raw, "property_address":"3055 ST CROIX, Shelby County, TN"}
        active = table.select.return_value.eq.return_value.eq.return_value.gt.return_value.order.return_value.limit.return_value
        active.execute.return_value.data = []
        table.select.return_value.eq.return_value.in_.return_value.execute.return_value.data = [old]
        cas = client.rpc.return_value
        cas.execute.return_value.data = 1
        review = {**{k:raw[k] for k in ("parcel","alt_parcel","sale_id")},"cases":[{"address":"3055 ST CROIX"}]}
        result = publish(client,{"a":review},date(2026,11,22))
        self.assertEqual(result["updated_staging_ids"],["a"])
        client.rpc.assert_called_once_with("apply_staging_enrichment",{"p_rows":[{"id":"a","raw_patch":{FIELD:review}}]})
        table.update.assert_not_called()
        cas.execute.return_value.data = 0
        with self.assertRaisesRegex(RuntimeError,"not written"):
            publish(client,{"a":review},date(2026,11,22))
        with self.assertRaisesRegex(ValueError,"identity"):
            publish(client,{"a":dict(review,parcel="072022 E00048")},date(2026,11,22))
