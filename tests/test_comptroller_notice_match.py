import unittest
from unittest import mock

from src.bots import comptroller_parcels_bot as cpb


def _row(id_, names, county="scott", dt="PRE_FORECLOSURE", address=None):
    return {"id": id_, "distress_type": dt, "county": county, "property_address": address,
            "owner_name_records": "; ".join(names), "full_name": names[0], "property_value": None,
            "done": None, "court": None, "state": None, "borrowers": names}


class FakeIdx:
    counties = {"scott", "davidson"}
    con = None

    def resolve_county(self, county, address):
        c = (county or "").lower().replace(" county", "")
        return c if c in self.counties else None

    def match_address(self, address, county):
        return None


HIT = {"method": "debtor_name", "confidence": "joint", "county": "SCOTT",
       "situs_address": "12 OAK RD", "appraisal": 180000, "class": "00"}


class NoticeMatchTests(unittest.TestCase):
    def run_match(self, rows, hits):
        searched = []

        def fake_match(con, names, counties):
            searched.append((tuple(names), tuple(counties)))
            return hits.get(names[0])

        bot = cpb.ComptrollerParcelsBot(write=False)
        with mock.patch.object(cpb, "_rows", return_value=iter(rows)), \
                mock.patch.object(cpb, "_load_tried", return_value=set()), \
                mock.patch.object(cpb, "match_debtors", side_effect=fake_match):
            out = bot.match_all(None, FakeIdx())
        return searched, out

    def test_joint_match_sets_address_and_value(self):
        searched, out = self.run_match([_row("a", ["James Nathan Marcum", "Ashley Marcum"])],
                                       {"James Nathan Marcum": HIT})
        self.assertEqual(searched, [(("James Nathan Marcum", "Ashley Marcum"), ("scott",))])
        u = out["updates"][0]
        self.assertEqual((u["address"], u["county"], u["value"]), ("12 OAK RD", "Scott", 180000))

    def test_name_only_match_kept_for_review(self):
        hit = {**HIT, "confidence": "name_only"}
        _, out = self.run_match([_row("a", ["Brian Newton"])], {"Brian Newton": hit})
        u = out["updates"][0]
        self.assertEqual((u["address"], u["value"]), (None, None))

    def test_republished_notice_searched_once(self):
        rows = [_row(i, ["Brian Newton"]) for i in ("a", "b", "c")]
        searched, out = self.run_match(rows, {"Brian Newton": HIT})
        self.assertEqual(len(searched), 1)
        self.assertEqual(len(out["updates"]), 3)

    def test_outside_coverage_or_addressed_not_name_searched(self):
        rows = [_row("a", ["Ann Lee"], county="knox"), _row("b", ["Ann Lee"], address="1 MAIN ST")]
        searched, _ = self.run_match(rows, {})
        self.assertEqual(searched, [])


if __name__ == "__main__":
    unittest.main()
