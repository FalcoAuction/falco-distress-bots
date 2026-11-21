import unittest

from src.bots.hardeman_tax_notice_bot import exhibit_parcels
from src.bots.tn_public_notice_bot import TnPublicNoticeBot


class HardemanTaxTests(unittest.TestCase):
    def test_only_real_tables_and_exact_special_interest(self):
        lines = ["HARDEMAN COUNTY and all MUNICIPALITIES", "CIVIL ACTION NO. 20358",
                 "certified herein for the year 2024", "HARDEMAN COUNTY, TENNESSEE – REAL PROPERTY TAXES",
                 "02-045I/B/045P/", "70.00", "04-167/7.00-001", "HARDEMAN COUNTY PERSONAL PROPERTY TAXES",
                 "04-167/9.00", "CITY OF BOLIVAR, TENNESSEE – REAL PROPERTY TAXES", "04-167/7.00-001",
                 "CITY OF WHITEVILLE - REAL & PERSONAL PROPERTY TAXES", "04-167/8.00"]
        self.assertEqual(set(exhibit_parcels(lines)), {"035 045P B 07000 000", "035 167 00700 001"})
        self.assertFalse(exhibit_parcels(lines + ["CIVIL ACTION NO. 23456"]))

    def test_clipped_zoning_address_never_becomes_tax_parcel(self):
        text = "The Hardeman County Board of Zoning Appeals will meet to consider 123 Example Rd"
        row = {"county": "hardeman", "excerpt": text, "notice_id": "1"}
        bot = TnPublicNoticeBot()
        self.assertIsNone(bot._build_lead(row, "TAX_LIEN", "Delinquent Taxes"))
        row["excerpt"] = "Notice of tax sale, Parcel No: 167-007.00, 123 Example Rd"
        self.assertIsNotNone(bot._build_lead(row, "TAX_LIEN", "Delinquent Taxes"))
