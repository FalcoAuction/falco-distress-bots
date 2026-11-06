"""Tests for the Davidson tax-sale list parsing and how a listing stacks onto
a lead's lane grade. No network.

    python -m unittest tests.test_davidson_tax_stack -v
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

from src.bots._davidson_tax_sale import final_revisions, parse_list, sale_revisions  # noqa: E402
from src.bots._property_lane import property_lane  # noqa: E402

SAMPLE = """PROPERTY TAX SALE
... ON MONDAY, August 17, 2026, 12:00 NOON. IN THE EVENT ...
Map/Parcel Book & Page or
1.53 $9,805.40
BURGESS, A P ETUX 12809000400 00003704 0000609 204 HARPETH VALLEY RD
1.76 $1,294.80
COOK LAND DEVELOPMENT LLC 07505004400 20221024 0115072 0 HILLS LN
0 $2,645.74
MCCLAIN, EUGENIA 043100B10200CO 20220623 0071676 555 N DUPONT AVE E-102
"""


def media(name, d):
    return {"source_url": f"https://x/wp-content/uploads/{name}", "date": d, "mime_type": "application/pdf"}


class ParseTests(unittest.TestCase):
    def test_parcels_amounts_and_date(self):
        sale, parcels = parse_list(SAMPLE)
        self.assertEqual(sale, "2026-08-17")
        self.assertEqual(parcels, {"12809000400": 9805.40, "07505004400": 1294.80, "043100B10200CO": 2645.74})

    def test_revisions_grouped_across_spellings(self):
        m = [media("January16th_Tax-Sale2023_1.pdf", "2026-01-06"), media("January16th_Tax-Sale2023_6.pdf", "2026-01-15"),
             media("January_Tax-Sale2023_14.pdf", "2026-01-21"), media("June17th_Tax-Sale2023_3-1.pdf", "2026-05-28"),
             media("June17th_Tax-Sale2023_16.pdf", "2026-06-17"), media("OCTOBER15THTax_Sale2012_4.pdf", "2014-10-14"),
             media("Delinquent-Tax-Sale-Excess-Proceeds-List.pdf", "2026-07-29")]
        sales = sale_revisions(m, date(2026, 11, 5))
        self.assertEqual(sorted(sales), ["januarytaxsale2023", "junetaxsale2023"])
        self.assertEqual(len(sales["januarytaxsale2023"]), 3)
        self.assertEqual([u.rsplit("/", 1)[-1] for u in final_revisions(m, date(2026, 11, 5))],
                         ["January_Tax-Sale2023_14.pdf", "June17th_Tax-Sale2023_16.pdf"])


class StackTests(unittest.TestCase):
    def lead(self, status):
        raw = {"metro_parcel": {"land_use": "STRIP SHOPPING CENTER", "total_appraisal": 900000,
                                "owner_mailing": "1 A ST, NASHVILLE TN 37203"},
               "davidson_tax_sale": {"status": status, "sale_date": "2026-08-17", "amount_due": 9805.4}}
        return property_lane({"id": "1", "distress_type": "CODE_VIOLATION", "raw_payload": raw, "phone_metadata": {}})

    def test_unpaid_listing_lifts_code_violation_to_B(self):
        r = self.lead("unpaid_at_sale")
        self.assertEqual((r["stacked_distress"], r["grade"]), (["davidson_tax_sale_unpaid"], "B"))

    def test_paid_listing_does_not_count(self):
        r = self.lead("paid_before_sale")
        self.assertEqual((r["stacked_distress"], r["grade"]), ([], "C"))


if __name__ == "__main__":
    unittest.main()
