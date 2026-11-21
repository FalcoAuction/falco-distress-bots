import unittest

from src.bots._county_tax_notices import non_tax_reason, parcel_rows


class CountyTaxHeadings(unittest.TestCase):
    def test_court_and_budget_headings_are_limited_to_originating_county(self):
        notice = "FY24 Spent FY25 Adopted FY26 Proposed FY27 Difference Percent General Government"
        self.assertTrue(non_tax_reason("sullivan", notice))
        self.assertIsNone(non_tax_reason("henry", notice))
        self.assertIsNone(non_tax_reason("sullivan", notice + " Delinquent real property tax sale"))

    def test_actual_tax_auction_in_sullivan_survives(self):
        self.assertIsNone(non_tax_reason("sullivan", "NOTICE OF SULLIVAN COUNTY DELINQUENT TAX SALE"))
        self.assertTrue(non_tax_reason("sullivan", "Notice of proposed sale: property acquired by Sullivan County"))

    def test_madison_excludes_incomplete_table_row(self):
        row = {"notice_id": "1", "excerpt": "Tax yr. Bill # Dist Map# GP CtrlMap Parcel SI Property Address "
               "2023 601 4 24 24 39.14 0 307 WAYNICK RD ALEXANDER 723-1834 $750.64 2023 753 5 78G"}
        lead, = parcel_rows("madison", row)
        self.assertEqual(lead.raw_payload["parcel"], "057 024 03914 000")
        self.assertEqual(lead.raw_payload["published_amount"], "750.64")
        self.assertIsNone(lead.trustee_sale_date)

    def test_henry_uses_control_map_and_special_interest(self):
        row = {"notice_id": "2", "pub_date": "June 1, 2026",
               "excerpt": "ALSO, TAKE NOTICE THAT THE FOLLOWING ADDITIONAL PARCELS ARE CURRENTLY DELINQUENT "
                          "Example 01-095 P -K -095 O -009.00- -001"}
        lead, = parcel_rows("henry", row)
        self.assertEqual(lead.raw_payload["parcel"], "040 095O K 00900 001")
        self.assertIsNone(parcel_rows("madison", row))
