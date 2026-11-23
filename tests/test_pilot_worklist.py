"""Tests for the statewide pilot worklist. No network.

    python -m unittest tests.test_pilot_worklist -v
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._pilot_review import build_review, debt, source  # noqa: E402
from src.bots.pilot_worklist import COLUMNS, build_worklist, write_outputs  # noqa: E402

TODAY = date(2026, 11, 22)
SRC = source("County record", "https://example.test/r", "2026-11-22")


def row(rid, owner="REED VIDA L", parcel="D0215M D00017", lane="pre_foreclosure", route="first_trace_review",
        deadline="2027-01-05", county="Shelby", by="lane_bot"):
    card = build_review(
        lane=lane, route=route, route_reason="Sourced and current.", reviewed_by=by, county=county,
        owner_names=[owner], today=TODAY,
        event={"what": "Trustee sale advertised", "status": "active", "checked_on": "2026-11-22", "source": SRC},
        asset={"description": "Home", "parcel": parcel, "address": "9655 Woodland Brook Ln",
               "tie": "Notice cites this parcel", "source": SRC},
        decider={"name": owner, "role": "owner of record", "basis": "county tax bill", "verified": True, "source": SRC},
        economics={"deadline": deadline, "deadline_kind": "trustee sale", "value": 524600,
                   "value_basis": "assessment / ratio", "value_is_estimate": True, "value_source": SRC,
                   "known_debt": [debt("mortgage_original", 300000, "2019-01-01", SRC)], "unknowns": ["payoff"]},
        availability={"evidence": "No transfer found", "checked_on": "2026-11-22",
                      "first_call_question": "Has the lender quoted a reinstatement?",
                      "verify_next": ["Recheck the trustee calendar"]},
    )
    return {"id": rid, "county": county, "pilot_review": card}


BASELINE = [
    {"owner_name": "REED VIDA L", "owner_names_as_sourced": ["REED VIDA L"], "county": "Shelby",
     "qualification": "candidate_for_first_trace_review", "opportunity_type": "pre-foreclosure trustee sale",
     "assets": [{"staging_id": "a", "parcel_id": "D0215M D00017", "asset_address": "9655 Woodland Brook Ln, Cordova, TN"}]},
    {"owner_name": "GONE SAM", "owner_names_as_sourced": ["GONE SAM"], "county": "Knox",
     "qualification": "candidate_for_first_trace_review", "opportunity_type": "delinquent tax sale",
     "assets": [{"staging_id": "z", "parcel_id": "999999", "asset_address": "1 Gone St"}]},
    {"owner_name": "PENDING PAT", "owner_names_as_sourced": ["PENDING PAT"], "county": "Cheatham",
     "qualification": "published_tract_pretrace_review", "assets": []},
]


class WorklistTests(unittest.TestCase):
    def test_cross_lane_duplicate_counts_once(self):
        r = build_worklist([row("a"), row("b", lane="tax", by="tax_bot")], BASELINE, TODAY)
        self.assertEqual((r["owner_groups"], r["properties"], r["first_trace_cards"]), (1, 1, 2))
        self.assertEqual(r["rows"][0]["lanes"], "Pre-foreclosure + Delinquent tax")
        self.assertIn("falco.llc/crm/leads/b", r["rows"][0]["other_records"])

    def test_only_first_trace_cards_are_listed_but_all_are_counted(self):
        r = build_worklist([row("a"), row("h", owner="HOLD HAL", parcel="111111", route="research_hold")], None, TODAY)
        self.assertEqual(r["owner_groups"], 1)
        self.assertEqual(r["cards_by_lane_and_route"]["pre_foreclosure"], {"first_trace_review": 1, "research_hold": 1})

    def test_baseline_comparison(self):
        r = build_worklist([row("a"), row("n", owner="NEW NED", parcel="222222")], BASELINE, TODAY)
        self.assertEqual((r["new_owner_groups_vs_baseline"], r["baseline_groups_still_qualified"]), (1, 1))
        self.assertEqual([d["owner"] for d in r["baseline_groups_not_on_worklist"]], ["GONE SAM"])

    def test_baseline_matches_by_staging_id_when_names_differ(self):
        r = build_worklist([row("a", owner="REED VIDA LYNN ESTATE", parcel="DIFFERENT1")], BASELINE, TODAY)
        self.assertEqual(r["rows"][0]["vs_baseline"], "already on the Nov 22 list")

    def test_soonest_deadline_first_and_unknown_last(self):
        r = build_worklist([row("u", owner="UNK UMA", parcel="333333", deadline=None),
                            row("l", owner="LATE LEE", parcel="444444", deadline="2027-04-13"),
                            row("s", owner="SOON SUE", parcel="555555", deadline="2026-12-10")], None, TODAY)
        self.assertEqual([x["owner_group"] for x in r["rows"]], ["SOON SUE", "LATE LEE", "UNK UMA"])
        self.assertEqual([x["days_left"] for x in r["rows"]], [18, 142, None])

    def test_row_text_keeps_debt_and_estimate_labels(self):
        x = build_worklist([row("a")], None, TODAY)["rows"][0]
        self.assertEqual(x["known_debt"], "original loan (not a payoff): $300,000 as of 2019-01-01")
        self.assertTrue(x["value_basis"].startswith("Estimate: "))
        self.assertEqual(x["missing_evidence"], "payoff; Recheck the trustee calendar")

    def test_stale_stored_keys_are_recomputed(self):
        a, b = row("a", owner="PERCER BOBBIE E JR & DONNA M"), row("b", owner="PERCER BOBBIE E JR & DONNA M", lane="tax", by="t")
        a["pilot_review"]["owner_group_key"] = "tn|shelby|P:DONNA M+P:PERCER B"   # written under the old rule
        a["pilot_review"]["property_key"] = "stale"
        r = build_worklist([a, b], None, TODAY)
        self.assertEqual((r["owner_groups"], r["properties"]), (1, 1))

    def test_malformed_cards_are_ignored(self):
        r = build_worklist([{"id": "x", "county": "Knox", "pilot_review": {"version": "old"}},
                            {"id": "y", "county": "Knox", "pilot_review": None}], None, TODAY)
        self.assertEqual((r["owner_groups"], r["rows"]), (0, []))

    def test_outputs_written(self):
        r = build_worklist([row("a")], BASELINE, TODAY)
        with tempfile.TemporaryDirectory() as d:
            paths = write_outputs(r, Path(d))
            self.assertTrue(all(p.exists() and p.stat().st_size > 0 for p in paths))
            header = (Path(d) / "pilot-worklist.csv").read_text(encoding="utf-8").splitlines()[0]
            self.assertEqual(header.split(","), COLUMNS)


if __name__ == "__main__":
    unittest.main()
