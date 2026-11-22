"""Tests for reading trustee's sale notices from their excerpt. No network.
Excerpts follow the real notice wording with made-up names.

    python -m unittest tests.test_trustee_notice -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._trustee_notice import borrowers, classify, deed_ref, is_entity, sale_date  # noqa: E402

SUB = ('TS#: 2026-28473-TN Notice Of Substitute Trustee\'s Sale Whereas, Ryan James Holder, unmarried man by '
       'Deed of Trust (the "Deed of Trust"), dated 9/21/2023 and of record in Deed Book 6240, Page(s) 23-35, '
       'and/or as Instrument Number 1462312, in Register\'s Office of Sumner County, Ten ...')
SUB2 = ('TS#: 2026-28484-TN Notice Of Substitute Trustee\'s Sale Whereas, James E Blevins, An Unmarried Man by '
        'Deed of Trust (the "Deed of Trust"), dated 10/30/2014 and of record in Deed Book 1591, Page(s) 854-868')
LLC = ('NOTICE OF SUCCESSOR TRUSTEE\'S SALE WHEREAS, on May 4, 2022, WARHORSE ANTIOCH EL I, LLC, a Tennessee '
       'limited liability company ("Grantor"), executed a Deed of Trust, Security Agreement, and Fixture Filing')
COUPLE = ('SUBSTITUTE TRUSTEE\'S SALE Default has occurred in the performance of the covenants, terms, and conditions '
          'of a Deed of Trust dated March 3, 2019, executed by JOHN A SMITH AND MARY B SMITH, conveying certain real '
          'property therein described')
TAX = ('TAXPAYERS The 2026 Real and Personal Property taxes are now due and payable. These taxes may be paid at the '
       'Trustee\'s Office in the Dickson County Administration Building')


class TrusteeNoticeTests(unittest.TestCase):
    def test_auction_venue_is_not_the_distressed_property(self):
        from src.bots._trustee_notice import property_address_text
        venue = "At the county courthouse, 140 ADAMS AVENUE, Memphis."
        self.assertEqual(property_address_text(venue), "")
        self.assertEqual(property_address_text(venue + " Property address: 52 Pine Street"), "52 PINE STREET")

    def test_only_supported_unambiguous_auction_dates(self):
        notice = "Deed of Trust dated May 2, 2021. Will sell at public outcry on Monday, December 7, 2026."
        self.assertEqual(sale_date(notice), "2026-12-07")
        self.assertIsNone(sale_date(notice + " Foreclosure sale date: January 4, 2027."))
        self.assertIsNone(sale_date("Foreclosure sale date: February 30, 2027"))
        self.assertIsNone(sale_date("Deed of Trust dated May 2, 2021. Appointment of Substitute Trustee."))
        self.assertEqual(sale_date("Sale at public auction will be on 12/22/2026 on or about 10:00 AM"), "2026-12-22")
        self.assertEqual(sale_date("Sale at public auction will be held Dec. 11, 2026"), "2026-12-11")
        self.assertIsNone(sale_date(notice + " The sale has been postponed."))
        self.assertEqual(sale_date(notice + " The sale may be postponed."), "2026-12-07")

    def test_grantors_in_recorded_deed_clauses(self):
        self.assertEqual(borrowers('Deed of Trust from John Roe and Jane Roe ("Grantor"), to Chris Doe, Trustee'),
                         ["John Roe", "Jane Roe"])
        self.assertEqual(borrowers("recorded in Knox County, Tennessee, Jane Roe conveyed the described property to John Doe"),
                         ["Jane Roe"])

    def test_classify(self):
        self.assertEqual(classify(SUB), "foreclosure")
        self.assertEqual(classify(LLC), "foreclosure")
        self.assertEqual(classify(TAX), "general_tax")
        self.assertIsNone(classify("NOTICE TO CREDITORS Estate of Jane Doe"))

    def test_whereas_borrower(self):
        self.assertEqual(borrowers(SUB), ["Ryan James Holder"])
        self.assertEqual(borrowers(SUB2), ["James E Blevins"])

    def test_entity_grantor_kept_whole(self):
        b = borrowers(LLC)
        self.assertEqual(b, ["WARHORSE ANTIOCH EL I, LLC"])
        self.assertTrue(is_entity(b[0]))

    def test_executed_by_couple_split(self):
        self.assertEqual(borrowers(COUPLE), ["JOHN A SMITH", "MARY B SMITH"])

    def test_deed_reference(self):
        self.assertEqual(deed_ref(SUB), {"book": "6240", "page": "23", "instrument": "1462312"})
        self.assertEqual(deed_ref(TAX), {"book": None, "page": None, "instrument": None})

    def test_the_grantor_form(self):
        x = ("Notice of Successor Trustee's Sale WHEREAS, on March 17, 2025, Marc Daniel Giguere (the \"Grantor\"), "
             "executed a Deed of Trust in the original principal amount of $304,871.00")
        self.assertEqual(borrowers(x), ["Marc Daniel Giguere"])

    def test_executed_date_by_form(self):
        x = ("SUBSTITUTE TRUSTEE'S SALE Default having been made in the payment of the debts and obligations secured "
             "to be paid by a certain Deed of Trust executed August 15, 2023, by Leslie Gaynair, Jr. and Misty Gaynair, "
             "Husband and Wife, to Title Co")
        self.assertEqual(borrowers(x), ["Leslie Gaynair, Jr", "Misty Gaynair"])
        y = ("SUBSTITUTE TRUSTEE SALE Default having occurred in the payment of the debts and obligations by a certain "
             "Deed of Trust executed December 16, 2024 by Bobby N. Groves, Jr., his heirs and assigns, to AMT Title")
        self.assertEqual(borrowers(y), ["Bobby N. Groves, Jr"])

    def test_sale_date_first_format(self):
        x = ("SUBSTITUTE TRUSTEE'S NOTICE OF SALE Sale at public auction will be on October 28, 2026, at or about "
             "11:00 AM, local time, outside the East door of the Montgomery County Courthouse")
        self.assertEqual(sale_date(x), "2026-10-28")
        self.assertEqual(classify(x), "foreclosure")

    def test_legal_wording_is_not_a_name(self):
        x = ("NOTICE OF TRUSTEE'S SALE WHEREAS, default has occurred in the performance of the covenants, terms, and "
             "conditions of a Deed of Trust dated December 31, 2024, and the Deed of Trust of even date")
        self.assertEqual(borrowers(x), [])

    def test_tax_notice_has_no_borrower(self):
        self.assertEqual(borrowers(TAX), [])


class LisPendensFormTests(unittest.TestCase):
    def test_hyphen_broken_conveying_and_comma_by(self):
        x = ("NOTICE OF SUBSTITUTE TRUSTEE'S SALE WHEREAS, default has occurred in the performance of the covenants, "
             "terms and conditions of a Deed of Trust dated April 7, 2022, executed by JASON SWAW and KELLY SWAW con- "
             "veying certain real property therein described to WILBURN J. EVANS, as Trustee")
        self.assertEqual(borrowers(x), ["JASON SWAW", "KELLY SWAW"])
        y = ('NOTICE OF FORECLOSURE SALE WHEREAS, CYNTHIA PARTON, by Deed of Trust (the "Deed of Trust") dated May 3, '
             '2024 of record in Book 6516, Page(s) 195-203')
        self.assertEqual(borrowers(y), ["CYNTHIA PARTON"])


class ReparseTests(unittest.TestCase):
    def test_garbage_name_replaced(self):
        from src.bots.notice_reparse_bot import reparse
        bad = "Jason Swaw And Kelly Swaw Con- Veying Certain Real Property Therein Described To Wilburn J. Evans"
        x = ("NOTICE OF SUBSTITUTE TRUSTEE'S SALE WHEREAS, default has occurred in the performance of the covenants, "
             "terms and conditions of a Deed of Trust dated April 7, 2022, executed by JASON SWAW and KELLY SWAW con- "
             "veying certain real property")
        u = reparse({"distress_type": "PRE_FORECLOSURE", "full_name": bad, "owner_name_records": bad,
                     "raw_payload": {"excerpt": x}})
        self.assertEqual(u["full_name"], "JASON SWAW")
        self.assertEqual(u["owner_name_records"], "JASON SWAW; KELLY SWAW")

    def test_tax_filed_trustee_sale_becomes_foreclosure(self):
        from src.bots.notice_reparse_bot import reparse
        u = reparse({"distress_type": "TAX_LIEN", "full_name": None, "owner_name_records": None,
                     "trustee_sale_date": None, "raw_payload": {"excerpt": SUB}})
        self.assertEqual(u["distress_type"], "PRE_FORECLOSURE")
        self.assertEqual(u["full_name"], "Ryan James Holder")
        self.assertEqual(u["raw_payload"]["filed_as"], "TAX_LIEN")
        self.assertEqual(u["raw_payload"]["deed_of_trust"]["book"], "6240")

    def test_general_tax_rejected_and_done_rows_skipped(self):
        from src.bots.notice_reparse_bot import reparse
        u = reparse({"distress_type": "TAX_LIEN", "raw_payload": {"excerpt": TAX}})
        self.assertEqual(u["rejection_reason"], "general_tax_notice")
        self.assertIsNone(reparse({"raw_payload": {"excerpt": TAX, "notice_reparse": {"v": 1}}}))

    def test_existing_name_is_kept(self):
        from src.bots.notice_reparse_bot import reparse
        u = reparse({"distress_type": "PRE_FORECLOSURE", "full_name": "Kept Name", "owner_name_records": "Kept Name",
                     "raw_payload": {"excerpt": SUB}})
        self.assertNotIn("full_name", u)


if __name__ == "__main__":
    unittest.main()
