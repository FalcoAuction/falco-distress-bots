"""Tests for the church closure resolution parser. No network.

    python -m unittest tests.test_church_closure -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots.church_closure_bot import closures  # noqa: E402

DOC = (
    "Resolution to Close Port Royal United Methodist Church Tennessee-Western Kentucky Conference WHEREAS, the "
    "worshipping community known as Port Royal United Methodist Church, located at, 2947 Highway 76 Adams, Tennessee "
    "37010, Montgomery County, Tennessee, in the Cumberland River District of the Tennessee- Western Kentucky "
    "Conference has faithfully served; WHEREAS, a final service of worship was held on June 28, 2026, and "
    "Resolution to Close Lebanon United Methodist Church WHEREAS, the worshipping community known as Lebanon United "
    "Methodist Church, located at 631 Tige Hopper Rd, Jackson 38305 in Madison County, Tennessee, in the Tennessee "
    "River District of the Conference "
    "Resolution to Close Christ United Methodist Church WHEREAS, the worshipping community known as Christ United "
    "Methodist Church, located at 1322 West Broadway, Mayfield, Kentucky 42066, Graves County in the Purchase "
    "District of the Conference"
)


class ChurchClosureTests(unittest.TestCase):
    def test_parses_each_format(self):
        cs = closures(DOC)
        self.assertEqual([c["county"] for c in cs], ["montgomery", "madison", "graves"])
        self.assertEqual([c["state"] for c in cs], ["TN", "TN", "KY"])
        self.assertEqual(cs[0]["address"], "2947 Highway 76 Adams, 37010")
        self.assertEqual(cs[0]["final_service"], "June 28, 2026")
        self.assertEqual(cs[1]["address"], "631 Tige Hopper Rd, Jackson 38305")


if __name__ == "__main__":
    unittest.main()
