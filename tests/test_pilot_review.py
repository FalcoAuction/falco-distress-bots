"""Tests for the shared pilot review card. No network.

    python -m unittest tests.test_pilot_review -v
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

from src.bots._pilot_review import (  # noqa: E402
    build_review, debt, dedupe_worklist, merge_review, owner_group_key, property_key, source,
)

TODAY = date(2026, 11, 22)
SRC = source("Foundation Legal Group sale calendar", "https://example.test/calendar", "2026-11-22")


def card(route="first_trace_review", lane="pre_foreclosure", by="lane_bot", **over):
    args = dict(
        lane=lane, route=route, route_reason="Sale is advertised and the owner is on the county bill.",
        reviewed_by=by, county="Shelby", owner_names=["REED VIDA L"], today=TODAY,
        event={"what": "Trustee sale advertised", "status": "postponed", "checked_on": "2026-11-22", "source": SRC},
        asset={"description": "Single-family home", "parcel": "075042 00012", "address": "1 Woodland Brook Ln",
               "tie": "Notice cites the deed of trust recorded against this parcel", "source": SRC},
        decider={"name": "REED VIDA L", "role": "owner of record", "basis": "2026 county tax bill",
                 "verified": True, "source": SRC},
        economics={"deadline": "2027-01-05", "deadline_kind": "trustee sale", "value": 210000,
                   "value_basis": "2026 county assessment / class ratio", "value_is_estimate": True,
                   "value_source": SRC,
                   "known_debt": [debt("mortgage_original", 150000, "2018-05-01", SRC)],
                   "unknowns": ["payoff", "seller intent"]},
        availability={"evidence": "No listing or transfer found", "checked_on": "2026-11-22",
                      "first_call_question": "Has the lender given you a reinstatement figure?",
                      "verify_next": ["Recheck trustee calendar before buying contact data"]},
    )
    args.update(over)
    return build_review(**args)


class KeyTests(unittest.TestCase):
    def test_roll_order_and_prose_order_agree(self):
        self.assertEqual(owner_group_key("REED VIDA L", "Shelby"), owner_group_key("Vida Reed", "Shelby County"))

    def test_group_is_order_independent(self):
        self.assertEqual(owner_group_key(["SMITH JOHN A", "SMITH MARY"], "Knox"),
                         owner_group_key("SMITH MARY & SMITH JOHN A", "knox"))

    def test_entities_and_people_and_counties_stay_apart(self):
        self.assertNotEqual(owner_group_key("REED VIDA L", "Shelby"), owner_group_key("REED VIDA L", "Knox"))
        self.assertNotEqual(owner_group_key("REED HOLDINGS LLC", "Shelby"), owner_group_key("REED VIDA L", "Shelby"))
        self.assertEqual(owner_group_key("Acme Properties, LLC", "Shelby"), owner_group_key("ACME PROPERTIES LLC", "Shelby"))

    def test_unknown_owner_has_no_key(self):
        self.assertIsNone(owner_group_key("", "Shelby"))
        self.assertIsNone(owner_group_key(None, "Shelby"))

    def test_parcel_beats_address_and_ignores_spacing(self):
        self.assertEqual(property_key("Shelby", "075042  00012"), property_key("shelby county", "075042 00012", "x"))
        self.assertEqual(property_key("Shelby", None, "12 Main Street, Memphis, TN 38103"),
                         property_key("Shelby", None, "12 MAIN ST MEMPHIS"))
        self.assertIsNone(property_key("Shelby", None, "Main Street"))

    def test_comptroller_roll_year_is_not_part_of_the_key(self):
        self.assertEqual(property_key("Robertson", "074 058    12100 000 2027"),
                         property_key("Robertson", "074 058    12100 000"))
        self.assertEqual(property_key("Robertson", "074 058    12100 000 2026"),
                         property_key("robertson county", "074 058    12100 000 2027"))
        self.assertNotEqual(property_key("Shelby", "D0215M D02027"), property_key("Shelby", "D0215M D0"))


class BuildTests(unittest.TestCase):
    def test_complete_card(self):
        c = card()
        self.assertEqual((c["route"], c["economics"]["runway_days"]), ("first_trace_review", 44))
        self.assertEqual(c["owner_group_key"], "tn|shelby|P:REED V")
        self.assertEqual(c["property_key"], "tn|shelby|p:07504200012")

    def test_first_trace_needs_every_answer(self):
        base = card()
        for part, field in (("event", "source"), ("asset", "tie"), ("decider", "basis"),
                            ("economics", "value_source"), ("availability", "first_call_question")):
            broken = dict(base[part]); broken[field] = None
            with self.assertRaises(ValueError, msg=f"{part}.{field}"):
                card(**{part: broken})

    def test_first_trace_refuses_resolved_past_or_conflicted(self):
        base = card()
        with self.assertRaises(ValueError):
            card(event={**base["event"], "status": "resolved"})
        with self.assertRaises(ValueError):
            card(economics={**base["economics"], "deadline": "2026-11-01"})
        with self.assertRaises(ValueError):
            card(availability={**base["availability"], "conflicts": ["Listed with a broker"]})

    def test_unknown_deadline_is_allowed(self):
        base = card()
        c = card(economics={**base["economics"], "deadline": None})
        self.assertIsNone(c["economics"]["runway_days"])

    def test_research_hold_must_name_next_check(self):
        base = card()
        with self.assertRaises(ValueError):
            card(route="research_hold", availability={**base["availability"], "verify_next": []})
        thin = card(route="research_hold", decider={"role": "unknown"})
        self.assertEqual(thin["route"], "research_hold")

    def test_bad_enums(self):
        with self.assertRaises(ValueError):
            card(lane="boats")
        with self.assertRaises(ValueError):
            card(route="call_now")
        with self.assertRaises(ValueError):
            debt("payoff", 1, None, SRC)


class MergeTests(unittest.TestCase):
    def test_manual_card_stays(self):
        manual = {**card(route="research_hold"), "manual": True}
        self.assertIs(merge_review(manual, card()), manual)

    def test_same_reviewer_refreshes(self):
        old, new = card(), card(route="removed")
        self.assertIs(merge_review(old, new), new)

    def test_other_reviewer_only_wins_when_stronger(self):
        hold, rec, gone = card(route="research_hold", by="a"), card(by="b"), card(route="removed", by="c")
        self.assertIs(merge_review(hold, rec), rec)
        self.assertIs(merge_review(rec, hold), rec)
        self.assertIs(merge_review(rec, gone), gone)
        self.assertIs(merge_review(gone, rec), gone)


class WorklistTests(unittest.TestCase):
    def test_same_owner_and_property_across_lanes_counts_once(self):
        rows = dedupe_worklist([card(), card(lane="tax", by="tax_bot"), card(route="research_hold")])
        self.assertEqual(len(rows), 1)
        self.assertEqual((len(rows[0]["assets"]), rows[0]["lanes"]), (1, ["pre_foreclosure", "tax"]))

    def test_second_property_same_owner_is_kept(self):
        base = card()
        other = card(asset={**base["asset"], "parcel": "075042 00099"})
        rows = dedupe_worklist([base, other])
        self.assertEqual((len(rows), len(rows[0]["assets"])), (1, 2))

    def test_unknown_owners_never_merge(self):
        a = {**card(), "owner_group_key": None}
        b = {**card(), "owner_group_key": None}
        self.assertEqual(len(dedupe_worklist([a, b])), 2)


if __name__ == "__main__":
    unittest.main()
