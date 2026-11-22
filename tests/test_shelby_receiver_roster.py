from unittest import TestCase
from unittest.mock import MagicMock, patch

from src.bots.shelby_receiver_roster_bot import attach_trustee_list, read_roster

HEADER = [["As of November 13,", "", "", "", ""],
          ["ADDRESS", "CIVIL WARRANT", "DEFENDANT", "NEXT COURT", "Tax Sale or other"]]


def roster(rows, text):
    doc = MagicMock()
    doc.metadata = {"CreationDate": "D:20261113200000"}
    pg = MagicMock()
    pg.extract_table.return_value = HEADER + rows
    pg.extract_text.return_value = text
    doc.pages = [pg]
    with patch("src.bots.shelby_receiver_roster_bot.pdfplumber.open") as file:
        file.return_value.__enter__.return_value = doc
        return read_roster(b"test")["cases"]


class ShelbyReceiverTests(TestCase):
    def test_shared_warrant_uses_each_address_and_hearing_separately(self):
        rows = [[address, "24400001", "overlapping PDF cell", "12/3/2026 -- 9:00 AM", "Tax Sale 23.02 -- 4/13/2027"]
                for address in ["1 MAIN ST", "2 MAIN ST"]]
        text = ("1 MAIN ST 24400001 1 Main St, Shelby County, Parcel ID 012345 00001 12/3/2026 "
                "2 MAIN ST 24400001 2 Main St, Shelby County, Parcel ID 012345 00002 12/3/2026")
        cases = roster(rows, text)
        self.assertEqual([r["parcel"] for r in cases], ["012345 00001", "012345 00002"])
        self.assertEqual(cases[0]["hearing_listed"], "2026-12-03")
        self.assertEqual(cases[0]["county_note"], "Tax Sale 23.02 -- 4/13/2027")

    def test_missing_parcel_never_borrows_from_next_case(self):
        rows = [["1 MAIN ST", "24400001", "", "12/3/2026 -- 9:00 AM", ""]]
        text = "1 MAIN ST 24400001 2 MAIN ST 24400002 Parcel ID 012345 00002 12/3/2026"
        self.assertIsNone(roster(rows, text)[0]["parcel"])

    def test_duplicate_row_or_changed_layout_fails_visibly(self):
        row = ["1 MAIN ST", "24400001", "", "", ""]
        with self.assertRaises(ValueError):
            roster([row, row], "")
        with self.assertRaises(ValueError):
            roster([row[:3]], "")

    def test_tax_listing_bound_to_parcel_independently_of_county_note(self):
        lead = MagicMock(raw_payload={"parcel": "123456 A00012", "sale_id": "TS2302"},
                         trustee_sale_date="2027-04-13", source_url="county")
        receipt = {"cases": [{"parcel": "123456A00012"}, {"parcel": "123456A00013", "county_note": "Tax Sale 23.02"}]}
        attach_trustee_list(receipt, [lead])
        self.assertEqual(receipt["cases"][0]["current_trustee_listings"][0]["sale_id"], "TS2302")
        self.assertEqual(receipt["cases"][1]["current_trustee_listings"], [])
