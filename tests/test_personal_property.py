"""Tests for the picky personal-property lane rules. No network.

    python -m unittest tests.test_personal_property -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._personal_property import category_of, classify, implied_equipment_value  # noqa: E402

JET = {"n_number": "N722AP", "aircraft": "2018 CIRRUS DESIGN CORP SF50", "value_band": "A",
       "match_type": "address", "confidence": "high"}
BARON = {"n_number": "N4590S", "aircraft": "1975 BEECH 58", "value_band": "B",
         "match_type": "name_middle_bk_district", "confidence": "medium"}
NOTICE = {"county": "davidson", "docket": "26P1785", "decedent": "Jane Doe",
          "attorney": {"names": ["Robert Akin"], "address": ["611 Woodland Street", "Nashville, TN 37206"]},
          "representatives": [{"name": "Michele Smith", "role": "personal_representative",
                               "address": ["512 Alta Loma Rd.", "Goodlettsville, TN 37072"]}]}


def lead(dt, raw=None, **kw):
    return {"distress_type": dt, "raw_payload": raw or {}, **kw}


class CategoryTests(unittest.TestCase):
    def test_categories(self):
        self.assertEqual(category_of("Volunteer Firearms LLC"), "guns")
        self.assertEqual(category_of("Music City Coin & Jewelry"), "collections")
        self.assertEqual(category_of("Elliott & Son Excavating LLC"), "heavy_equipment")
        self.assertEqual(category_of("Engineered Alloys LLC"), "business_equipment")
        self.assertEqual(category_of("James Spearman Aircraft, LLC"), "vehicles")

    def test_false_friends_rejected(self):
        self.assertIsNone(category_of("Oak Arms Apartments LLC"))
        self.assertIsNone(category_of("Southern Air Heating LLC"))
        self.assertIsNone(category_of("799 Auto Repair, LLC"))
        self.assertIsNone(category_of("Bright Consulting Group LLC"))

    def test_implied_value(self):
        # $18,566 over 4 years -> principal 13,647 -> 3,412/yr -> /3.5% /30% ~ $325K
        self.assertEqual(implied_equipment_value(18566, 4), 325000.0)
        self.assertIsNone(implied_equipment_value(None, 3))


class EstateTests(unittest.TestCase):
    def test_ordinary_estate_not_tagged(self):
        self.assertIsNone(classify(lead("PROBATE", {"court_notice": NOTICE}, property_value=310000)))

    def test_affluent_home_is_b_with_contacts(self):
        t = classify(lead("PROBATE", {"court_notice": NOTICE}, property_value=1_400_000))
        self.assertEqual((t["lane"], t["grade"]), ("estate", "B"))
        self.assertEqual(t["contacts"]["attorney"]["names"], ["Robert Akin"])
        self.assertEqual(t["contacts"]["representatives"][0]["name"], "Michele Smith")
        self.assertTrue(t["caveats"])

    def test_two_million_home_is_a(self):
        self.assertEqual(classify(lead("PROBATE", property_value=2_049_200))["grade"], "A")

    def test_decedent_jet_is_a(self):
        t = classify(lead("PROBATE", {"aircraft": [JET]}, property_value=0))
        self.assertEqual((t["grade"], t["categories"], t["est_value_min"]), ("A", ["aircraft"], 300000))


class LiquidationTests(unittest.TestCase):
    def test_individual_bankruptcy_never_liquidation(self):
        self.assertIsNone(classify(lead("BANKRUPTCY", {"chapter": "7"}, full_name="John Q Trucking")))

    def test_chapter_13_entity_skipped(self):
        self.assertIsNone(classify(lead("BANKRUPTCY", {"chapter": "13"}, full_name="Elliott Excavating LLC")))

    def test_ch7_hard_asset_entity_is_b_with_size_caveat(self):
        t = classify(lead("BANKRUPTCY", {"chapter": "7", "courtlistener_docket_url": "https://cl/1"},
                          full_name="J.R. COLEMAN TRUCKING CO. LLC"))
        self.assertEqual((t["lane"], t["grade"], t["categories"]), ("business_liquidation", "B", ["heavy_equipment"]))
        self.assertIn("Official Form 201", t["caveats"][0])
        self.assertEqual(t["contacts"]["bankruptcy_docket"], "https://cl/1")

    def test_any_chapter_7_business_goes_to_the_trustee(self):
        t = classify(lead("BANKRUPTCY", {"chapter": "7", "trustee": "LEMEH, EVA M"}, full_name="MyEyeris, Inc."))
        self.assertEqual(t["categories"], ["business_assets"])
        self.assertEqual(t["controller"], "chapter 7 trustee")
        self.assertEqual(t["primary_contact"], "Trustee Eva M Lemeh")
        self.assertTrue(t["routable"])
        self.assertEqual(t["route_to"], "McLemore Auction Company")

    def test_chapter_11_business_not_tagged(self):
        self.assertIsNone(classify(lead("BANKRUPTCY", {"chapter": "11"}, full_name="MyEyeris, Inc.")))

    def test_florida_and_court_sales_not_routable(self):
        fl = classify(lead("BANKRUPTCY", {"chapter": "7", "state": "FL"}, full_name="ACME TRUCKING LLC"))
        self.assertFalse(fl["routable"])
        self.assertIn("Florida", fl["not_routable_reason"])

    def test_big_warn_without_hard_assets_not_tagged(self):
        warn = {"company": "Kroger Fulfillment Network LLC", "workers": 132}
        self.assertIsNone(classify(lead("BUSINESS_CLOSURE", {"warn": warn})))

    def test_big_hard_asset_warn_is_a(self):
        warn = {"company": "Smoky Mountain Logistics, LLC", "workers": 100}
        t = classify(lead("BUSINESS_CLOSURE", {"warn": warn}))
        self.assertEqual((t["lane"], t["grade"]), ("business_liquidation", "A"))


class SeizureTests(unittest.TestCase):
    def per(self, owed, years, name):
        return lead("TAX_LIEN", {"parcel": "PER--1", "cumulative_owed": owed, "years_delinquent": years,
                                 "owner_name_1": name})

    def test_small_account_not_tagged(self):
        self.assertIsNone(classify(self.per(1152, 1, "ACME LLC")))

    def test_one_year_late_not_tagged_even_if_large(self):
        self.assertIsNone(classify(self.per(9000, 1, "MAYSE CONSTRUCTION COMPANY")))

    def test_recent_hard_asset_is_a(self):
        t = classify(self.per(18566, 4, "MAYSE CONSTRUCTION COMPANY"))
        self.assertEqual((t["lane"], t["grade"], t["est_value_min"]), ("seizure", "A", 325000.0))

    def test_dormant_needs_bigger_value_and_is_b(self):
        self.assertIsNone(classify(self.per(38131, 7, "AVM ENTERPRISES INC")))   # ~$318K, dormant
        t = classify(self.per(469828, 13, "SHELTON CONSTRUCTION CO INC"))
        self.assertEqual(t["grade"], "B")
        self.assertTrue(any("may be gone" in c for c in t["caveats"]))

    def test_real_property_tax_lien_ignored(self):
        self.assertIsNone(classify(lead("TAX_LIEN", {"parcel": "075014", "cumulative_owed": 90000,
                                                     "years_delinquent": 3})))


class OwnerAssetTests(unittest.TestCase):
    def test_foreclosure_owner_with_jet(self):
        t = classify(lead("PRE_FORECLOSURE", {"aircraft": [JET]}))
        self.assertEqual((t["lane"], t["grade"]), ("owner_asset", "A"))

    def test_bankrupt_individual_with_baron_is_owner_asset_b(self):
        t = classify(lead("BANKRUPTCY", {"chapter": "13", "aircraft": [BARON]}, full_name="Chad E Conner"))
        self.assertEqual((t["lane"], t["grade"]), ("owner_asset", "B"))

    def test_no_signal_no_tag(self):
        self.assertIsNone(classify(lead("PRE_FORECLOSURE", property_value=2_500_000)))


if __name__ == "__main__":
    unittest.main()
