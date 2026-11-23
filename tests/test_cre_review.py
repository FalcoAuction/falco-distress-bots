"""Replay tests for the commercial review cards. No network.

    python -m unittest tests.test_cre_review -v
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

from src.bots._cre_review import (  # noqa: E402
    LISTING_CHECK, REVIEWER, SOS_PATH, notice_trustee, owner_kind, parse_sale_list, review_row, roll_index,
    roll_status,
)
from src.bots._pilot_review import build_review, debt, dedupe_worklist, owner_group_key, property_key, source  # noqa: E402
from src.bots._property_lane import parcel_id  # noqa: E402
from src.bots.cre_review_bot import in_scope, plan_rows  # noqa: E402

TODAY = date(2026, 11, 22)


def lane(**kw):
    tag = {"lane": "commercial", "grade": "B", "exclude_reason": None, "basis": "metro:MEDICAL OFFICE"}
    tag.update(kw)
    return tag


def dated_notice(**over):
    """A trustee sale on a medical office held in a revocable trust."""
    row = {
        "id": "row-dated", "bot_source": "nashville_ledger", "distress_type": "PRE_FORECLOSURE", "county": "davidson",
        "property_address": "10 Example Ln, Nashville, TN 37027", "owner_name_records": "Jane Q Sample Revocable Trust",
        "full_name": "Jane Q Sample Revocable Trust", "property_value": 2045000,
        "property_value_source": "metro_nashville_parcels", "trustee_sale_date": "2026-12-01",
        "source_url": "https://example.test/notice/1",
        "phone_metadata": {"notice_tracking": {"last_seen_at": "2026-11-22T01:19:11+00:00"}},
        "raw_payload": {
            "property_lane": lane(),
            "publication_date": "2026-11-06",
            "body": ("NOTICE OF SUBSTITUTE TRUSTEES SALE\n\nOwner of Property: Jane Q. Sample Revocable Trust, "
                     "Jane Q. Sample, Trustee\n\nProperty ID: 170 00 0 006.00\n"),
            "structured": {"borrower": "Jane Q Sample Revocable Trust", "substitute_trustee": "Trustee Firm LLC",
                           "original_trustee": "John Doe"},
            "metro_parcel": {"parcel": "17000000600", "owner": "SAMPLE, JANE Q REVOCABLE TRUST, THE",
                             "owned_since": "2021-09-21", "total_appraisal": 2045000,
                             "land_use": "MEDICAL OFFICE (ONE OR TWO STORIES)"},
        },
    }
    row.update(over)
    return row


def hamilton_row(**raw_over):
    raw = {"county": "hamilton", "parcel": "168O-A-013", "owner_name_1": "MILLER CHARLES", "owner_name_2": None,
           "cumulative_owed": 3594.38, "property_lane": lane(grade="A", basis="hamilton_assessor:Repair Services")}
    raw.update(raw_over)
    return {"id": "row-ham", "bot_source": "hamilton_tax_delinquent", "distress_type": "TAX_LIEN", "county": "hamilton",
            "property_address": "4700 Dodds Ave, Chattanooga, TN", "owner_name_records": "MILLER CHARLES",
            "full_name": "MILLER CHARLES", "property_value": 207600, "property_value_source": "hamilton_assessor",
            "source_url": "https://www.hamiltontn.gov/_downloadsTrusteeDelinquent/CTRUDELQCSV.zip", "raw_payload": raw}


def bills(*years, owner="MILLER CHARLES", turned=True):
    return [{"year": y, "owed": 1000.0, "memo": (f"03{y + 2}" if turned and y <= 2024 else f"09{y}"), "owner": owner,
             "mortgage_company": ""} for y in years]


def evidence(**over):
    ev = {"roll": roll_status(bills(2023, 2024, 2025)), "roll_date": "2026-11-01", "assessor_date": "2026-11-21",
          "assessor": {"parcel": "168O-A-013", "owner": "MILLER CHARLES", "property_address": "4700 DODDS AVE",
                       "appraised": 207600.0, "land_use": "Repair Services", "last_sale_date": "2021-06-10"},
          "duplicate_parcel": False, "sale_list": {}, "sale_list_year": 2026,
          "sale_list_url": "https://example.test/list.pdf"}
    ev.update(over)
    return ev


class DatedCommercialCase(unittest.TestCase):
    def test_complete_dated_case_clears_first_trace(self):
        card, facts = review_row(dated_notice(), today=TODAY)
        self.assertEqual(card["lane"], "cre")
        self.assertEqual(card["route"], "first_trace_review")
        self.assertEqual(card["decider"]["name"], "Jane Q. Sample")
        self.assertEqual(card["decider"]["role"], "trustee of the owning trust")
        self.assertEqual(card["economics"]["deadline"], "2026-12-01")
        self.assertEqual(card["economics"]["runway_days"], 9)
        self.assertEqual(card["economics"]["value"], 2045000)
        self.assertEqual(card["asset"]["parcel"], "17000000600")
        self.assertIn(LISTING_CHECK, card["availability"]["verify_next"])
        self.assertNotIn("unlisted", card["availability"]["evidence"].lower())
        self.assertEqual(facts["owner_type"], "fiduciary")
        self.assertEqual(facts["listing_status"], "not verified")
        self.assertEqual(card["owner_group_key"], owner_group_key("Jane Q Sample Revocable Trust", "davidson"))

    def test_other_parties_in_the_notice_are_listed_as_unknown_liens(self):
        row = dated_notice()
        row["raw_payload"]["body"] += ("In addition, the following parties may claim an interest in the above-referenced "
                                       "property:\n\nExample Owners Association, Junior Lienholder, in Instrument No. 1\n")
        card, _ = review_row(row, today=TODAY)
        self.assertTrue(any("Example Owners Association" in u for u in card["economics"]["unknowns"]))

    def test_metro_parcel_number_counts_as_parcel_id(self):
        self.assertEqual(parcel_id(dated_notice()["raw_payload"]), "17000000600")

    def test_llc_owner_holds_for_signer_and_never_guesses(self):
        row = dated_notice(owner_name_records="Sample Village LLC", full_name="Sample Village LLC")
        row["raw_payload"]["structured"]["borrower"] = "Sample Village LLC"
        row["raw_payload"]["metro_parcel"]["owner"] = "SAMPLE VILLAGE LLC"
        row["raw_payload"]["metro_parcel"]["owner_mailing"] = "1 MANAGER RD, NASHVILLE TN"
        card, facts = review_row(row, today=TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("signer is unresolved", card["route_reason"])
        self.assertIn(SOS_PATH, card["availability"]["verify_next"])
        self.assertIsNone(facts["signer"])

    def test_trustee_name_only_from_the_owner_line(self):
        self.assertIsNone(notice_trustee("conveyed to John Doe, Trustee, the real property"))
        self.assertEqual(notice_trustee("Owner of Property: The A B Trust, Ann B. Cole, Trustee"), "Ann B. Cole")

    def test_passed_sale_holds_and_later_transfer_is_removed(self):
        card, _ = review_row(dated_notice(trustee_sale_date="2026-11-18"), today=TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("has passed", card["route_reason"])
        row = dated_notice()
        row["raw_payload"]["metro_parcel"]["owned_since"] = "2026-11-12"
        card, _ = review_row(row, today=TODAY)
        self.assertEqual(card["route"], "removed")
        self.assertIn("transfer", card["route_reason"])

    def test_borrower_not_the_parcel_owner_holds(self):
        row = dated_notice()
        row["raw_payload"]["metro_parcel"]["owner"] = "UNRELATED HOLDINGS LLC"
        card, _ = review_row(row, today=TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(card["availability"]["conflicts"])


class TenantFalsePositives(unittest.TestCase):
    def test_tenant_closure_is_not_a_landlord_sale(self):
        row = {"id": "row-warn", "bot_source": "warn_notices", "distress_type": "BUSINESS_CLOSURE", "county": "davidson",
               "property_address": "500 Plant Rd, Nashville, TN", "full_name": "Acme Widgets Inc",
               "source_url": "https://example.test/warn",
               "raw_payload": {"warn": {"employees": 120}, "property_lane": lane(grade="A"),
                               "metro_parcel": {"parcel": "09100001200", "owner": "PLANT ROAD LANDLORD LLC",
                                                "total_appraisal": 5000000}}}
        card, facts = review_row(row, today=TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("not a landlord sale", card["route_reason"])
        self.assertIsNone(card["decider"]["name"])
        self.assertIsNone(card["owner_group_key"])
        self.assertIsNone(card["economics"]["value"])
        self.assertEqual(facts["equipment_owner"], "Acme Widgets Inc")
        self.assertIn("Equipment owner (separate from the landlord): Acme Widgets Inc", card["asset"]["description"])
        self.assertIn("PLANT ROAD LANDLORD LLC", card["asset"]["description"])

    def test_tenant_personal_property_tax_is_not_owner_distress(self):
        row = hamilton_row(parcel="PER--10052662", owner_name_1="CORNER DELI LLC",
                           property_lane=lane(grade=None, exclude_reason="personal_property_tax"))
        row.update(owner_name_records="CORNER DELI LLC", full_name="CORNER DELI LLC")
        # Even with county evidence for the landlord's parcel in hand, the tenant account is not carded as real estate.
        card, facts = review_row(row, evidence(), today=TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("Tenant personal property tax", card["route_reason"])
        self.assertIsNone(card["asset"]["parcel"])
        self.assertIsNone(card["decider"]["name"])
        self.assertEqual(facts["equipment_owner"], "CORNER DELI LLC")
        self.assertEqual(card["economics"]["known_debt"], [])

    def test_name_that_is_not_the_parcel_owner(self):
        row = dated_notice(distress_type="TAX_LIEN", trustee_sale_date=None)
        row["raw_payload"]["property_lane"] = lane(grade=None, exclude_reason="owner_mismatch")
        card, _ = review_row(row, today=TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("tenant, contractor or prior owner", card["route_reason"])


class HamiltonTaxCases(unittest.TestCase):
    def test_owner_in_tax_suit_clears_first_trace(self):
        card, facts = review_row(hamilton_row(), evidence(), today=TODAY)
        self.assertEqual(card["route"], "first_trace_review")
        self.assertEqual(card["decider"]["name"], "MILLER CHARLES")
        self.assertTrue(card["decider"]["verified"])
        self.assertEqual(card["economics"]["known_debt"][0]["kind"], "delinquent_tax")
        self.assertEqual(card["economics"]["known_debt"][0]["amount"], 3000.0)
        self.assertIsNone(card["economics"]["deadline"])
        self.assertEqual(card["economics"]["deadline_kind"], "none scheduled")
        what = card["event"]["what"]
        self.assertIn("Delinquent on the Trustee file dated 2026-11-01 for bill years 2023 to 2025, $3,000", what)
        self.assertIn("years 2023 and 2024 are past the statutory turnover date; the tax suit docket for this parcel "
                      "has not been read", what)
        self.assertIn("Tax suit docket and any scheduled sale not verified; next Hamilton sale list expected May 2027",
                      card["economics"]["unknowns"])
        text = " ".join([what, card["route_reason"], card["availability"]["first_call_question"]]).lower()
        for claim in ("turned over to", "with the clerk and master", "next tax sale", "before the"):
            self.assertNotIn(claim, text)
        self.assertEqual(facts["roll"]["turned_over_years"], [2023, 2024])
        self.assertIn(LISTING_CHECK, card["availability"]["verify_next"])

    def test_same_parcel_counts_once_across_tax_and_commercial_lanes(self):
        cre, _ = review_row(hamilton_row(), evidence(), today=TODAY)
        self.assertEqual(cre["property_key"], property_key("hamilton", "168O-A-013"))
        self.assertEqual(cre["owner_group_key"], owner_group_key(["MILLER CHARLES"], "Hamilton County"))
        src = source("Trustee file", "https://example.test", TODAY)
        tax = build_review(
            lane="tax", route="first_trace_review", route_reason="Tax suit", reviewed_by="tax_bot", county="Hamilton",
            owner_names=["MILLER CHARLES"], today=TODAY,
            event={"what": "Tax suit", "status": "active", "checked_on": TODAY, "source": src},
            asset={"description": "Parcel", "parcel": "168O A 013", "tie": "Same parcel", "source": src},
            decider={"name": "MILLER CHARLES", "role": "owner", "basis": "roll", "source": src},
            economics={"value": 207600, "value_basis": "County appraisal", "value_source": src,
                       "known_debt": [debt("delinquent_tax", 3000, TODAY, src)]},
            availability={"evidence": "No transfer", "checked_on": TODAY, "first_call_question": "Planning to pay?"})
        groups = dedupe_worklist([tax, cre])
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]["assets"]), 1)
        self.assertEqual(sorted(groups[0]["lanes"]), ["cre", "tax"])

    def test_care_of_line_is_not_an_owner(self):
        card, _ = review_row(hamilton_row(owner_name_2="ATTN MIKE MILLER"), evidence(), today=TODAY)
        self.assertEqual(card["owner_group_key"], owner_group_key(["MILLER CHARLES"], "hamilton"))

    def test_holds_and_removals(self):
        cases = [
            (evidence(roll=roll_status([])), "removed", "no longer on the Trustee"),
            (evidence(roll=roll_status(bills(2025))), "research_hold", "no year is past the statutory turnover date"),
            (evidence(roll=roll_status(bills(2023))), "research_hold", "single delinquent year"),
            (evidence(assessor=None), "research_hold", "not matched to exactly one parcel"),
            (evidence(duplicate_parcel=True), "research_hold", "not matched to exactly one parcel"),
            (evidence(assessor=dict(evidence()["assessor"], owner="NEW BUYER LLC")), "research_hold", "differs from the taxpayer"),
            (evidence(assessor=dict(evidence()["assessor"], last_sale_date="2025-02-01")), "research_hold", "transfer on 2025-02-01"),
            (evidence(sale_list={"168O-A-013": {"docket": "11256", "item": "1", "flag": "LISTED", "minimum_bid": 9.0}}),
             "research_hold", "tax sale list and is not marked paid"),
            (evidence(assessor=dict(evidence()["assessor"], appraised=90000.0)), "research_hold", "under the $150,000 floor"),
            (evidence(assessor=dict(evidence()["assessor"], appraised=None)), "research_hold", "No county appraisal"),
        ]
        for ev, route, phrase in cases:
            card, _ = review_row(hamilton_row(), ev, today=TODAY)
            self.assertEqual(card["route"], route, phrase)
            self.assertIn(phrase, card["route_reason"])

    def test_owner_types_route_away_from_a_trace(self):
        for owner, route, phrase in (
                ("KNC HOLDINGS LLC", "research_hold", "signer is unresolved"),
                ("18TH PAULINA LIMITED PARTNERSHIP", "research_hold", "signer is unresolved"),
                ("BARLEY AUGUSTA MAE TR", "research_hold", "trustee is not named"),
                ("ROBINSON RAYMOND ET AL", "research_hold", "unnamed co-owners"),
                ("SMITH JOHN ESTATE", "research_hold", "estate or heirs"),
                ("GRACE BAPTIST CHURCH", "research_hold", "congregation"),
                ("FIRST HORIZON BANK", "professional_relationship", "lender"),
                ("CITY OF CHATTANOOGA", "removed", "public body")):
            ev = evidence(roll=roll_status(bills(2023, 2024, 2025, owner=owner)),
                          assessor=dict(evidence()["assessor"], owner=owner))
            row = hamilton_row(owner_name_1=owner)
            row.update(owner_name_records=owner, full_name=owner)
            card, _ = review_row(row, ev, today=TODAY)
            self.assertEqual(card["route"], route, owner)
            self.assertIn(phrase, card["route_reason"])
        self.assertEqual(owner_kind("DELTA REAL ESTATE GP"), "entity")

    def test_business_names_without_a_legal_suffix_are_not_people(self):
        for name in ("HIXSON AUTO CENTER", "LAKSHWATI CORPORATION", "OM MD PLAZA", "18TH PAULINA LIMITED PARTNERSHIP"):
            self.assertEqual(owner_kind(name), "entity", name)
        for name in ("MILLER CHARLES", "MC ALLISTER ANGELA L", "BYRD TYRONE & BETH DENISE", "LUKEN HENRY G III",
                     "WILSON DEMETIRUS & GHOLSTON WILLIE P", "FRYAR BOBBY H & & SHERELENE T", "GINN JERRY HERBERT JR"):
            self.assertEqual(owner_kind(name), "person", name)

    def test_county_files_unavailable_leaves_the_row_alone(self):
        self.assertIsNone(review_row(hamilton_row(), None, today=TODAY))

    def test_trustee_file_and_sale_list_parsing(self):
        rows = [{"Map": "168O", "Group": "A", "Parcel": "013", "Bill Year": "2023", "Memo Date": "032025",
                 "Owner Name 1": "MILLER CHARLES", "Owner Name 2": "", "Current County Owed": "000058796",
                 "Current Mun Owed": "000000000", "Current Stw Owed": "000000000", "E and R Pickup Owed": "000000",
                 "Mortgage Company Name": ""},
                {"Map": "168O", "Group": "A", "Parcel": "013", "Bill Year": "2025", "Memo Date": "092025",
                 "Owner Name 1": "MILLER CHARLES", "Owner Name 2": "", "Current County Owed": "000088274",
                 "Current Mun Owed": "******", "Current Stw Owed": "", "E and R Pickup Owed": "000-100",
                 "Mortgage Company Name": ""},
                {"Map": "PER", "Group": "", "Parcel": "10052662", "Bill Year": "2025", "Current County Owed": "000010000"},
                {"Map": "204 N SWEETBRIAR AVE", "Group": None, "Parcel": None, "Bill Year": "x"}]
        status = roll_status(roll_index(rows, {"168O-A-013"}).get("168O-A-013"))
        self.assertEqual(status["years"], [2023, 2025])
        self.assertEqual(status["turned_over_years"], [2023])
        self.assertEqual(status["balance"], 1469.7)
        listed = parse_sale_list("REVISED 6/4/2026\nPAID 11255 1738 1415 PARK AVE 145L K 002.04 $14,757.42\n"
                                 "11256 161630 PLOW LN 033 014.14 $1,530.49\n"
                                 "PAID 11255 1763 900 MOUNTAIN CREEK RD 117O A 011 C158 $6,508.33\n")
        self.assertEqual(listed["145L-K-002.04"]["flag"], "PAID")
        self.assertEqual(listed["033--014.14"]["flag"], "LISTED")
        self.assertIn("117O-A-011C158", listed)


class RepeatRun(unittest.TestCase):
    def test_second_run_writes_nothing_and_manual_cards_stay(self):
        rows = [hamilton_row(), dated_notice()]
        look = lambda row: evidence() if row["bot_source"] == "hamilton_tax_delinquent" else None
        first = plan_rows(rows, look, TODAY)
        self.assertEqual([bool(p["patch"]) for p in first], [True, True])
        for row, p in zip(rows, first):
            row["raw_payload"].update(p["patch"])
        second = plan_rows(rows, look, TODAY)
        self.assertEqual([p["patch"] for p in second], [{}, {}])
        # A person's decision on the card is never replaced by the bot.
        rows[0]["raw_payload"]["pilot_review"] = dict(rows[0]["raw_payload"]["pilot_review"], manual=True, route="removed",
                                                      reviewed_by="patrick")
        third = plan_rows(rows, look, TODAY)
        self.assertNotIn("pilot_review", third[0]["patch"])
        self.assertTrue(third[0]["kept_existing_card"])

    def test_stronger_card_from_another_lane_is_kept(self):
        row = hamilton_row()
        row["raw_payload"]["pilot_review"] = {"route": "removed", "reviewed_by": "tax_bot", "lane": "tax"}
        plan = plan_rows([row], lambda r: evidence(), TODAY)
        self.assertNotIn("pilot_review", plan[0]["patch"])
        self.assertIn("cre_review", plan[0]["patch"])
        self.assertEqual(plan[0]["card"]["reviewed_by"], REVIEWER)

    def test_scope(self):
        self.assertTrue(in_scope({"grade": "B", "staging_status": "pending"}))
        self.assertTrue(in_scope({"trustee_sale_date": "2026-12-01", "staging_status": "pending"}))
        self.assertTrue(in_scope({"exclude": "personal_property_tax", "staging_status": "pending"}))
        self.assertFalse(in_scope({"exclude": "personal_property_tax", "staging_status": "rejected"}))
        self.assertFalse(in_scope({"exclude": "weak_signal_only", "staging_status": "pending"}))


if __name__ == "__main__":
    unittest.main()
