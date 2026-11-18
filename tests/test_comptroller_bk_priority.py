import unittest
from unittest import mock

from src.bots import comptroller_parcels_bot as cpb


def _row(id_, dt, name):
    return {"id": id_, "distress_type": dt, "county": None, "property_address": None,
            "owner_name_records": name, "full_name": None, "property_value": None,
            "done": None, "court": "tnmb", "state": "TN"}


class FakeIdx:
    counties = {"davidson"}
    con = None


class BkPriorityTests(unittest.TestCase):
    def run_match(self, rows, cap, hits):
        searched = []

        def fake_match(con, names, counties):
            searched.append(names[0])
            return hits.get(names[0])

        bot = cpb.ComptrollerParcelsBot(write=False)
        with mock.patch.object(cpb, "_rows", return_value=iter(rows)), \
                mock.patch.object(cpb, "_load_tried", return_value=set()), \
                mock.patch.object(cpb, "district_counties", return_value=["davidson"]), \
                mock.patch.object(cpb, "match_debtors", side_effect=fake_match), \
                mock.patch.object(cpb, "MAX_DEBTOR_SEARCHES", cap):
            out = bot.match_all(None, FakeIdx())
        return searched, out

    def test_events_searched_before_filings(self):
        # Filings come first in id order; the events still get the budget.
        rows = [_row("a", "BANKRUPTCY", "FILER ONE"), _row("b", "BANKRUPTCY", "FILER TWO"),
                _row("c", "BK_DISMISSED", "EVENT ONE"), _row("d", "BK_STAY_RELIEF", "EVENT TWO")]
        searched, out = self.run_match(rows, cap=2, hits={})
        self.assertEqual(searched, ["EVENT ONE", "EVENT TWO"])
        self.assertEqual(out["stats"]["bk_deferred"], 2)
        self.assertEqual(out["stats"]["bk_events_deferred"], 0)

    def test_leftover_budget_goes_to_filings(self):
        rows = [_row("a", "BANKRUPTCY", "FILER ONE"), _row("c", "BK_ABANDONED", "EVENT ONE")]
        searched, _ = self.run_match(rows, cap=5, hits={})
        self.assertEqual(searched, ["EVENT ONE", "FILER ONE"])

    def test_trusted_event_match_sets_address(self):
        hit = {"method": "debtor_name", "confidence": "middle_initial", "county": "DAVIDSON",
               "situs_address": "1 MAIN ST", "appraisal": 250000, "class": "00"}
        rows = [_row("c", "BK_DISMISSED", "EVENT ONE")]
        _, out = self.run_match(rows, cap=5, hits={"EVENT ONE": hit})
        self.assertEqual(out["stats"]["bk_event_matched"], 1)
        u = out["updates"][0]
        self.assertEqual((u["address"], u["county"], u["value"]), ("1 MAIN ST", "Davidson", 250000))


if __name__ == "__main__":
    unittest.main()
