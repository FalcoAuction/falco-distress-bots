"""python -m unittest tests.test_davidson_tax_sale"""
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import davidson_tax_sale_bot as d  # noqa: E402


class RevisionTests(unittest.TestCase):
    def test_schedule_distinguishes_ad_date_and_cancelled_sale(self):
        html = ("<h2><del>October 30, 2026 November 18, 2026</del> CANCELLED</h2>"
                "<h2>January 1, 2027 January 20, 2027</h2>")
        self.assertEqual(d.scheduled_sales(html), {"2026-11-18": "cancelled", "2027-01-20": "scheduled"})

    def test_newest_revision_per_sale_inside_lookback(self):
        m = lambda day, name, mime="application/pdf": {"date": day, "mime_type": mime, "source_url": f"https://x/{name}"}
        media = [m("2026-07-24", "August_7th_Tax-Sale2023_1.pdf"), m("2026-08-14", "August_17th_Tax-Sale2023_5.pdf"), m("2026-08-12", "August_17th_Tax-Sale2023_4.pdf"),
                 m("2026-07-10", "July13th_Tax-Sale2023_7-1.pdf"), m("2026-07-10", "July13th_Tax-Sale2023_6.pdf"),
                 m("2026-07-24", "August_17th_Tax-Sale2023_1.doc", "application/msword"),
                 m("2026-07-29", "Delinquent-Tax-Sale-Excess-Proceeds-List.pdf"), m("2026-01-02", "January_Tax-Sale2023_3.pdf")]
        self.assertEqual(sorted(d.newest_revisions(media, date(2026, 11, 5))),
                         ["https://x/August_17th_Tax-Sale2023_5.pdf", "https://x/July13th_Tax-Sale2023_7-1.pdf"])
        self.assertEqual(d._money("$3,344.28"), 3344.28)


if __name__ == "__main__":
    unittest.main()
