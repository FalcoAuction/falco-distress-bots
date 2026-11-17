"""python -m unittest tests.test_warn_notices  (synthetic rows)"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import warn_notices_bot as w  # noqa: E402

PAGE = """<table><tr><th>Date of Posting</th><th>Company</th><th>County</th><th>Affected Workers</th>
<th>Closure/Layoff Date</th><th>Notice/Type</th></tr>
<tr><td>11/3/2026</td><td>Example Medical</td><td>Wilson</td><td>52</td><td>12-31-2026</td><td>#202600013</td></tr>
<tr><td>10/20/2026</td><td>Example Foods</td><td>Davidson / Knox</td><td>1,076</td><td>Company did not disclose</td><td>#202600010</td></tr>
<tr><td>10/1/2026</td><td>Far Away Co</td><td>Shelby</td><td>80</td><td>1-1-2027</td><td>#202600009</td></tr></table>
<table><tr><th>Unrelated</th></tr><tr><td>x</td></tr></table>"""


class WarnTests(unittest.TestCase):
    def test_parse(self):
        rows = w.parse_warn(PAGE)
        self.assertEqual(len(rows), 3)
        self.assertEqual((rows[0]["posted"], rows[0]["workers"], rows[0]["effective"]), ("2026-11-03", 52, "2026-12-31"))
        self.assertEqual((rows[1]["workers"], rows[1]["effective"]), (1076, None))
        self.assertEqual(w._counties("Davidson / Knox"), ["davidson", "knox"])
        self.assertEqual(w._counties("Rutherford County, Wilson"), ["rutherford", "wilson"])


if __name__ == "__main__":
    unittest.main()
