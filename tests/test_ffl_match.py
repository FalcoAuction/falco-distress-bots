"""Tests for the ATF firearms-licensee matcher and the estate contents signals. No network.

    python -m unittest tests.test_ffl_match -v
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._ffl_match import FflIndex, list_files, load_rows, rows_from_text  # noqa: E402
from src.bots._personal_property import classify, outbuilding_signals  # noqa: E402

HEADER = ("LIC_REGN,LIC_DIST,LIC_CNTY,LIC_TYPE,LIC_XPRDTE,LIC_SEQN,LICENSE_NAME,BUSINESS_NAME,PREMISE_STREET,"
          "PREMISE_CITY,PREMISE_STATE,PREMISE_ZIP_CODE,MAIL_STREET,MAIL_CITY,MAIL_STATE,MAIL_ZIP_CODE,VOICE_PHONE")
ROWS = [
    '1,62,125,01,8G,07193,"SINCLAIR, BRENT D","SINCLAIR ARMS","2649 OLD HWY 70 RD",ASHLAND CITY,TN,37015,,,,,6155550100',
    '1,62,037,07,9A,01111,"VOLUNTEER FIREARMS LLC","VOLUNTEER FIREARMS","10 MAIN ST",NASHVILLE,TN,37203,,,,,6155550101',
    '1,62,165,01,9B,02222,"SMITH, JOHN A",,"5 OAK DR",GALLATIN,TN,37066,,,,,6155550102',
]


def index(rows=ROWS, source="1026-ffl-list-tennessee.csv"):
    return FflIndex(rows_from_text("\n".join([HEADER] + rows), source))


class FflMatchTests(unittest.TestCase):
    def test_decedent_name_and_home_address(self):
        tags = index().match({"distress_type": "PROBATE", "decedent": "Brent D. Sinclair",
                              "property_address": "2649 Old Hwy 70 Rd, Ashland City, TN 37015"})
        self.assertEqual([(t["license_type"], t["match_type"], t["confidence"]) for t in tags],
                         [("01", "name_and_address", "high")])
        self.assertEqual(tags[0]["listing"], "1026-ffl-list-tennessee.csv")

    def test_name_middle_zip_without_address(self):
        tags = index().match({"distress_type": "PROBATE", "decedent": "John A Smith",
                              "property_address": "99 Elm St, Gallatin, TN 37066"})
        self.assertEqual([t["match_type"] for t in tags], ["name_middle_zip"])

    def test_common_name_without_middle_or_address_not_matched(self):
        self.assertEqual(index().match({"distress_type": "PROBATE", "decedent": "John Smith",
                                        "property_address": "99 Elm St, Gallatin, TN 37066"}), [])

    def test_wrong_middle_initial_not_matched(self):
        self.assertEqual(index().match({"distress_type": "PROBATE", "decedent": "John B Smith",
                                        "property_address": "5 Oak Dr, Gallatin, TN 37066"}), [])

    def test_business_name(self):
        tags = index().match({"distress_type": "BANKRUPTCY", "full_name": "Volunteer Firearms, LLC"})
        self.assertEqual([(t["license_label"], t["match_type"]) for t in tags],
                         [("firearms manufacturer", "business_name")])

    def test_same_license_across_months_counted_once(self):
        rows = rows_from_text("\n".join([HEADER] + ROWS), "0926.csv") + rows_from_text("\n".join([HEADER] + ROWS), "1026.csv")
        self.assertEqual(FflIndex(rows).count, 3)

    def test_folder_loading(self):
        with tempfile.TemporaryDirectory() as d:
            for name in ("0926-ffl-list-tennessee.csv", "1026-ffl-list-tennessee.csv"):
                Path(d, name).write_text("\n".join([HEADER] + ROWS), encoding="utf-8")
            Path(d, "notes.txt").write_text("ignore me", encoding="utf-8")
            files = list_files(d)
            self.assertEqual(len(files), 2)
            self.assertEqual(len(load_rows(files)), 6)
        self.assertEqual(list_files(os.path.join("no", "such", "dir")), [])


LIC = {"license_type": "01", "license_label": "dealer/gunsmith", "license_name": "SINCLAIR, BRENT D",
       "premise": "2649 OLD HWY 70 RD, ASHLAND CITY TN 37015", "listing": "1026.csv",
       "match_type": "name_and_address", "confidence": "high"}


class EstateContentsTests(unittest.TestCase):
    def test_ffl_decedent_qualifies_modest_home_as_a(self):
        t = classify({"distress_type": "PROBATE", "property_value": 285_000, "raw_payload": {"ffl": [LIC]}})
        self.assertEqual((t["lane"], t["grade"], t["categories"]), ("estate", "A", ["guns"]))

    def test_big_detached_garage_qualifies_as_b(self):
        raw = {"outbuildings": [{"type": "DETACHED GARAGE", "description": "FRAME", "area": 1200}]}
        t = classify({"distress_type": "PROBATE", "property_value": 340_000, "raw_payload": raw})
        self.assertEqual((t["grade"], t["categories"]), ("B", ["vehicles"]))
        self.assertTrue(any("ask the executor" in c for c in t["caveats"]))

    def test_small_shed_and_attached_garage_ignored(self):
        raw = {"outbuildings": [{"type": "UTILITY SHED", "area": 120}, {"type": "GARAGE", "description": "ATTACHED", "area": 480}]}
        self.assertEqual(outbuilding_signals(raw), [])
        self.assertIsNone(classify({"distress_type": "PROBATE", "property_value": 340_000, "raw_payload": raw}))

    def test_barn_and_horse_barn(self):
        raw = {"outbuildings": [{"type": "POLE BARN", "area": 2400}, {"type": "HORSE STABLE", "area": 800}]}
        self.assertEqual([c for c, _ in outbuilding_signals(raw)], ["farm_equipment", "farm_equipment"])

    def test_obituary_plus_shop_is_a(self):
        raw = {"obituary": {"categories": ["vehicles"], "phrases": ["restored classic cars"], "url": "https://fh/obit/1"},
               "outbuildings": [{"type": "WORKSHOP", "area": 900}]}
        t = classify({"distress_type": "PROBATE", "property_value": 410_000, "raw_payload": raw})
        self.assertEqual(t["grade"], "A")
        self.assertEqual(t["categories"], ["business_equipment", "vehicles"])
        self.assertIn("https://fh/obit/1", t["signals"][-1])

    def test_ffl_business_in_bankruptcy_is_guns_a(self):
        t = classify({"distress_type": "BANKRUPTCY", "full_name": "Volunteer Firearms LLC",
                      "raw_payload": {"chapter": "7", "ffl": [dict(LIC, match_type="business_name")]}})
        self.assertEqual((t["lane"], t["grade"], t["categories"]), ("business_liquidation", "A", ["guns"]))


if __name__ == "__main__":
    unittest.main()
