"""Pre-foreclosure lane: appointment-to-notice link, sale list status, cards
and the repeat-run behaviour of the scheduled reviewer. No network."""

from __future__ import annotations

import copy
import os
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test")

from src.bots import preforeclosure_review_bot as bot_module  # noqa: E402
from src.bots._preforeclosure_review import (COUNTY_KEY, EVIDENCE_KEY, LINK_KEY, TRACK_KEY, appointment_card,  # noqa: E402
                                             link_notice, names_agree, notice_identity, parcels_differ, sale_card,
                                             sale_state, shelby_parcel)
from src.bots.preforeclosure_review_bot import PreforeclosureReviewBot, merge_county  # noqa: E402

TODAY = date(2026, 11, 22)
NOW = datetime(2026, 11, 23, 1, 0, tzinfo=timezone.utc)
READ = "2026-11-22T19:48:00+00:00"


def appt(**over):
    base = {"instrument": "26097972", "recorded": "2026-11-09", "loan_instrument": "22115033",
            "parcel": "096514 F00010", "address": "1856 Pheasant Acre Ln E, Memphis, TN 38016"}
    return {**base, **over}


def notice(**over):
    base = {"staging_id": "n1", "notice_id": "FD117695", "loan_instrument": "22115033", "cited_appointment": None,
            "parcel": "096514 F00010", "address": "1856 Pheasant Acre Ln E, Memphis, TN 38016",
            "first_advertised": "2026-11-18", "sale_date": "2026-12-17", "borrower": "Jacques Rawls", "url": "https://x/n1"}
    return {**base, **over}


class LinkTests(unittest.TestCase):
    def test_confirmed_by_cited_appointment_instrument(self):
        link = link_notice(appt(loan_instrument=None, parcel=None, address=None),
                           [notice(cited_appointment="26097972")])
        self.assertEqual(link["status"], "linked")
        self.assertEqual((link["first_advertised"], link["appointment_recorded"], link["days_gained"]),
                         ("2026-11-18", "2026-11-09", 9))
        self.assertIn("cites this appointment instrument", link["basis"])

    def test_confirmed_by_same_deed_of_trust_and_parcel(self):
        link = link_notice(appt(), [notice()])
        self.assertEqual(link["status"], "linked")
        self.assertIn("the parcels agree", link["basis"])
        self.assertEqual(link["sale_date"], "2026-12-17")

    def test_same_borrower_different_parcel_is_not_a_link(self):
        other = notice(staging_id="n2", notice_id="FD999999", loan_instrument="19000001", parcel="081051 D00061",
                       address="6199 Solway Dr, Memphis, TN 38119")
        self.assertEqual(link_notice(appt(), [other]), {"status": "none"})

    def test_instrument_tie_with_a_different_parcel_is_a_conflict_not_a_link(self):
        link = link_notice(appt(), [notice(parcel="081051 D00061", address="6199 Solway Dr, Memphis, TN 38119")])
        self.assertEqual(link["status"], "conflict")
        self.assertIn("081051 D00061", link["reason"])
        self.assertNotIn("staging_id", link)

    def test_republished_notice_is_one_link_with_the_earliest_advertisement(self):
        again = notice(staging_id="n9", notice_id="FD117800", first_advertised="2026-11-25", sale_date="2027-01-05")
        link = link_notice(appt(), [again, notice(), notice()])
        self.assertEqual(link["status"], "linked")
        self.assertEqual(link["notice_ids"], ["FD117695", "FD117800"])
        self.assertEqual((link["first_advertised"], link["days_gained"]), ("2026-11-18", 9))
        self.assertEqual((link["staging_id"], link["sale_date"]), ("n9", "2027-01-05"))

    def test_two_properties_under_one_instrument_hold(self):
        link = link_notice(appt(parcel=None, address=None),
                           [notice(), notice(staging_id="n3", notice_id="FD1", parcel="081051 D00061")])
        self.assertEqual(link["status"], "conflict")

    def test_appointment_recorded_after_the_advertisement_gains_nothing(self):
        link = link_notice(appt(recorded="2026-11-19"), [notice(first_advertised="2026-11-06")])
        self.assertEqual(link["days_gained"], -13)

    def test_register_and_trustee_parcel_spellings(self):
        self.assertEqual(shelby_parcel("076077 00000210"), shelby_parcel("076077  00021"))
        self.assertEqual(shelby_parcel("A0142D0B000690"), shelby_parcel("A0142D B00069"))
        self.assertIsNone(shelby_parcel("A0142D"))
        self.assertFalse(parcels_differ("A0142D0B000690", "A0142D"))     # ward and block only: consistent
        self.assertTrue(parcels_differ("A0142D0B000690", "A0142D C00069"))
        self.assertTrue(parcels_differ("096514 F00010", "081051"))

    def test_notice_identity_reads_the_staging_aliases(self):
        row = {"id": "n1", "notice_id": "FD1", "loan_instrument": "22115033", "property_address": "1 A St, Memphis, TN 38016",
               "first_notice_date": "11/17/2026", "publication_date": "2026-11-20", "trustee_sale_date": "2027-01-07",
               "cited_appointment": {"instrument_number": "26000001"},
               "trustee_review": {"status": "verified_assessment", "parcel_id": "D0241G K00050"}}
        identity = notice_identity(row)
        self.assertEqual((identity["first_advertised"], identity["cited_appointment"], identity["parcel"]),
                         ("2026-11-17", "26000001", "D0241G K00050"))

    def test_names(self):
        self.assertTrue(names_agree("PRICE SHUNTEA S", "Shuntea Price"))
        self.assertTrue(names_agree("CHILDRESS COTY L & JACQUES RAWLS", ["RAWLS JACQUES A", "CHILDRESS COTY J"]))
        self.assertFalse(names_agree("CALDWELL, KENNETH H. III", "MCGEE, DOUGLAS & MICHELLE"))
        self.assertFalse(names_agree("SMITH JOHN", "SMITH MARY"))


def sale_row(**over):
    base = {"id": "s1", "bot_source": "brock_scott_trustee", "county": "Sumner",
            "property_address": "122 Read Tavern Rd Hendersonville, TN 37075", "full_name": None,
            "owner_name_records": "GERDES KEITH & GERDES CHRISTIENE ETUX", "property_value": 507800,
            "property_value_source": "comptroller", "trustee_sale_date": "2026-12-18",
            "source_url": "https://www.brockandscott.com/foreclosure-sales/?_sft_foreclosure_state=tn",
            "staging_status": "pending", "ranked": True,
            "notice_tracking": {"last_seen_at": READ, "last_seen_by": "brock_scott_trustee", "case_number": "26-1-FC01"},
            "seed": {"owner": "GERDES KEITH & GERDES CHRISTIENE ETUX", "parcel": "083 137J D 01500 000 2027",
                     "value": 507800, "value_basis": "Tennessee Comptroller county assessment roll appraisal",
                     "url": None, "observed_on": "2026-11-22", "label": "Sumner County parcel and assessment record"}}
    return {**base, **over}


def state_of(row):
    return sale_state(row, datetime(2026, 11, 22, 19, 48), NOW, TODAY)


class SaleStateTests(unittest.TestCase):
    def test_listed(self):
        state = state_of(sale_row())
        self.assertEqual((state["status"], state["dates"]), ("listed", ["2026-12-18"]))
        card = sale_card(sale_row(), state, TODAY)
        self.assertEqual((card["route"], card["event"]["status"]), ("first_trace_review", "active"))
        self.assertEqual(card["economics"]["deadline"], "2026-12-18")
        self.assertEqual(card["property_key"], "tn|sumner|p:083137JD01500000")
        self.assertEqual(card["owner_group_key"], "tn|sumner|P:GERDES C+P:GERDES K")   # both joint roll owners
        self.assertEqual(card["owner_names"], ["GERDES KEITH & GERDES CHRISTIENE ETUX"])   # as sourced
        self.assertIn("does not name the borrower", card["decider"]["role"])

    def test_postponement_updates_the_same_record(self):
        row = sale_row(trustee_sale_date="2027-01-15", track={"dates": ["2026-12-18"], "status": "listed", "since": "2026-11-20"})
        state = state_of(row)
        self.assertEqual((state["status"], state["prior_date"], state["dates"]),
                         ("postponed", "2026-12-18", ["2026-12-18", "2027-01-15"]))
        card = sale_card(row, state, TODAY)
        self.assertEqual((card["route"], card["event"]["status"], card["economics"]["deadline"]),
                         ("first_trace_review", "postponed", "2027-01-15"))
        self.assertIn("postponed from 2026-12-18 to 2027-01-15", card["event"]["what"])

    def test_foundation_calendar_postponement(self):
        row = sale_row(bot_source="memphis_daily_news", county="shelby", firm="Foundation Legal Group, LLP",
                       notice_id="FD117483", trustee_sale_date="2027-01-05", full_name="Vida Reed",
                       owner_name_records="Vida Reed", seed=None,
                       notice_tracking={"last_seen_at": "2026-11-22T22:33:56+00:00", "last_seen_by": "memphis_trustee_calendar"},
                       trustee_review={"status": "verified_assessment", "owner": "REED VIDA L", "parcel_id": "D0215M D00017",
                                       "estimated_appraisal": 524600, "assessed_value": 131150, "assessment_ratio": 0.25,
                                       "tax_year": 2026, "checked_on": "2026-11-22",
                                       "source_url": "https://apps2.shelbycountytrustee.com/Parcel?parcel=D0215M+D00017"},
                       calendar={"status": "postponed", "checked_on": "2026-11-22",
                                 "source_url": "https://sales.thefoundationlegalgroup.com/",
                                 "listing": {"date": "2027-01-05", "prior_date": "2026-12-01"}},
                       value_estimate={"original_loan": 280000, "loan_date": "2019-05-01", "instrument": "19044444"})
        state = state_of(row)
        self.assertEqual((state["status"], state["prior_date"]), ("postponed", "2026-12-01"))
        card = sale_card(row, state, TODAY)
        self.assertEqual((card["route"], card["economics"]["deadline"], card["economics"]["runway_days"]),
                         ("first_trace_review", "2027-01-05", 44))
        self.assertIn("Taxpayer on the county realty bill and borrower named by the trustee", card["decider"]["role"])
        self.assertTrue(card["economics"]["value_is_estimate"])
        debt = card["economics"]["known_debt"][0]
        self.assertEqual((debt["kind"], debt["amount"]), ("mortgage_original", 280000.0))
        self.assertIn("Not a payoff", debt["note"])
        self.assertEqual(card["owner_group_key"], "tn|shelby|P:REED V")

    def test_cancellation_is_a_hold_not_a_guess(self):
        row = sale_row(notice_tracking={"last_seen_at": "2026-11-19T19:48:00+00:00", "last_seen_by": "brock_scott_trustee"})
        state = state_of(row)
        self.assertEqual(state["status"], "withdrawn")
        card = sale_card(row, state, TODAY)
        self.assertEqual((card["route"], card["event"]["status"]), ("research_hold", "unknown"))
        self.assertIn("No longer on Brock & Scott's sale list", card["route_reason"])

    def test_a_stale_source_never_reads_as_a_cancellation(self):
        state = sale_state(sale_row(), datetime(2026, 11, 17, 19, 48), NOW, TODAY)
        self.assertEqual(state["status"], "unverified")
        self.assertEqual(sale_card(sale_row(), state, TODAY)["route"], "research_hold")

    def test_completed_sale_date_removes_the_record(self):
        row = sale_row(trustee_sale_date="2026-11-19")
        state = state_of(row)
        self.assertEqual(state["status"], "passed")
        card = sale_card(row, state, TODAY)
        self.assertEqual((card["route"], card["event"]["status"]), ("removed", "resolved"))
        self.assertIn("has passed", card["route_reason"])

    def test_manual_status_wins(self):
        card = sale_card(sale_row(sale_status={"status": "sold", "updated_by": "patrick"}),
                         state_of(sale_row(sale_status={"status": "sold", "updated_by": "patrick"})), TODAY)
        self.assertEqual(card["route"], "removed")

    def test_newspaper_date_alone_is_held(self):
        row = sale_row(bot_source="memphis_daily_news", county="shelby", notice_id="FD1", seed=None,
                       notice_tracking={"last_seen_at": READ, "last_seen_by": "memphis_daily_news"},
                       trustee_review={"status": "verified_assessment", "owner": "GERDES KEITH", "parcel_id": "081051 D00061",
                                       "estimated_appraisal": 200000, "assessed_value": 50000, "assessment_ratio": .25,
                                       "tax_year": 2026, "checked_on": "2026-11-22", "source_url": "https://x"})
        state = state_of(row)
        self.assertEqual(state["status"], "publisher_only")
        self.assertEqual(sale_card(row, state, TODAY)["route"], "research_hold")

    def test_gates(self):
        comptroller = {"owner": "GERDES KEITH", "parcelid": "083 137J D 01500 000 2027", "appraisal": 507800,
                       "method": "address", "county": "SUMNER", "version": "comptroller-parcels-2026-11-05"}
        bulk = sale_card(sale_row(seed=None, comptroller=comptroller), state_of(sale_row()), TODAY)
        self.assertEqual(bulk["route"], "research_hold")
        self.assertIn("bulk street match", bulk["route_reason"])
        self.assertFalse(bulk["decider"]["verified"])
        self.assertEqual(sale_card(sale_row(ranked=False), state_of(sale_row()), TODAY)["route"], "research_hold")
        self.assertEqual(sale_card(sale_row(seed=None), state_of(sale_row()), TODAY)["route"], "research_hold")
        heir = sale_row(owner_name_records="MCGEE, DOUGLAS & MICHELLE",
                        seed={**sale_row()["seed"], "owner": "CALDWELL, KENNETH H. III"})
        held = sale_card(heir, state_of(heir), TODAY)
        self.assertEqual(held["route"], "research_hold")
        self.assertIn("MCGEE", held["availability"]["conflicts"][0])
        noted = sale_row(seed={**sale_row()["seed"], "hold": "Title passed by affidavit", "verify_next": ["Read the affidavit"]})
        card = sale_card(noted, state_of(noted), TODAY)
        self.assertEqual((card["route"], card["route_reason"], card["availability"]["verify_next"][0]),
                         ("research_hold", "Title passed by affidavit", "Read the affidavit"))
        promoted = sale_card(sale_row(staging_status="promoted"), state_of(sale_row()), TODAY)
        self.assertEqual(promoted["route"], "research_hold")
        company = sale_row(owner_name_records="PLEX LLC", seed={**sale_row()["seed"], "owner": "PLEX LLC"})
        self.assertEqual(sale_card(company, state_of(company), TODAY)["route"], "research_hold")


def appt_row(**raw_over):
    raw = {"instrument": "26098311", "recorded": "2026-11-10", "borrowers": ["RICKARD SAMANTHA"],
           "lenders": ["FREEDOM MORTGAGE CORP S TR"], "substitute_trustees": ["NESTOR SOLUTIONS OF TENNESSEE LLC"],
           "party_basis": "lender_words", "deed_of_trust": {"instrument": "08002870", "year": "2008"},
           "parcel_id": "08902600007", "loan": {"original_amount": 104000.0, "loan_date": "2008-01-04"},
           "dot_checked_at": "2026-11-23",
           COUNTY_KEY: {"roll_owner": "RICKARD SAMANTHA", "parcel_id": "089026  00007", "tax_year": 2026,
                        "checked_on": "2026-11-22", "status": "verified_assessment", "assessed_value": 40000,
                        "assessment_ratio": 0.25, "estimated_appraisal": 160000,
                        "county_source": "https://apps2.shelbycountytrustee.com/Parcel?parcel=089026++00007"}}
    raw.update(raw_over)
    return {"id": "a1", "pipeline_lead_key": "k", "property_address": "6669 Elmore Rd, Memphis, TN 38134",
            "source_url": "https://search.register.shelby.tn.us/pdetail.php?year=2026&instnum=26098311&db=0&book=**0",
            "property_value": None, "property_value_source": None, "raw": raw, "raw_payload": raw}


class AppointmentCardTests(unittest.TestCase):
    def test_verified_early_filing_with_unknown_deadline(self):
        card = appointment_card(appt_row(), {"status": "none"}, TODAY)
        self.assertEqual(card["route"], "first_trace_review")
        self.assertIsNone(card["economics"]["deadline"])
        self.assertIsNone(card["economics"]["runway_days"])
        self.assertIn("Sale date. None advertised yet", card["economics"]["unknowns"])
        self.assertEqual(card["economics"]["known_debt"][0]["kind"], "mortgage_original")
        self.assertEqual(card["asset"]["parcel"], "089026  00007")       # the county bill's spelling
        self.assertEqual(card["property_key"], "tn|shelby|p:08902600007")
        self.assertIn("Taxpayer on the county realty bill and borrower on the appointment", card["decider"]["role"])
        self.assertTrue(card["decider"]["verified"])
        self.assertNotIn("—", str(card))

    def test_index_only_filing_is_research_with_the_attempt_count(self):
        row = appt_row(deed_of_trust=None, parcel_id=None, loan=None, dot_checked_at=None, **{COUNTY_KEY: None})
        row["property_address"] = None
        row["raw"]["recovery"] = {"source": "saved_register_APPT_index", "detail_issue": "HTTP 503",
                                  "last_detail_attempt": "2026-11-22T22:01:14+00:00", "detail_attempts": 2}
        card = appointment_card(row, {"status": "none"}, TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("2 attempt(s), last 2026-11-22", card["route_reason"])
        self.assertEqual(card["decider"]["role"], "Borrower on the appointment (Register index)")
        self.assertFalse(card["decider"]["verified"])
        self.assertIsNone(card["economics"]["deadline"])

    def test_owner_and_borrower_disagree(self):
        county = {**appt_row()["raw"][COUNTY_KEY], "roll_owner": "NEW OWNER HOLDINGS LLC"}
        card = appointment_card(appt_row(**{COUNTY_KEY: county}), {"status": "none"}, TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("different names", card["route_reason"])

    def test_company_borrower_and_missing_value_hold(self):
        county = {**appt_row()["raw"][COUNTY_KEY], "roll_owner": "PLEX LLC"}
        card = appointment_card(appt_row(borrowers=["PLEX LLC"], **{COUNTY_KEY: county}), {"status": "none"}, TODAY)
        self.assertIn("company", card["route_reason"])
        county = {k: v for k, v in appt_row()["raw"][COUNTY_KEY].items() if k != "estimated_appraisal"}
        card = appointment_card(appt_row(**{COUNTY_KEY: county}), {"status": "none"}, TODAY)
        self.assertIn("no sourced value", card["route_reason"])

    def test_linked_appointment_points_at_the_notice_record(self):
        link = link_notice({"instrument": "26098311", "recorded": "2026-11-10", "loan_instrument": "08002870",
                            "parcel": "089026  00007", "address": "6669 Elmore Rd, Memphis, TN 38134"},
                           [notice(staging_id="n7", notice_id="FD117714", loan_instrument="08002870",
                                   cited_appointment="26098311", parcel="089026  00007",
                                   address="6669 Elmore Rd, Memphis, TN 38134", first_advertised="2026-11-20",
                                   sale_date="2026-12-29")])
        card = appointment_card(appt_row(), link, TODAY, notice_carded=True)
        self.assertEqual(card["route"], "removed")
        self.assertIn("Duplicate of the sale notice record", card["route_reason"])
        self.assertIn("10 days before the first advertisement", card["route_reason"])
        alone = appointment_card(appt_row(), link, TODAY, notice_carded=False)
        self.assertEqual((alone["route"], alone["economics"]["deadline"]), ("first_trace_review", "2026-12-29"))

    def test_conflicting_notice_holds(self):
        link = {"status": "conflict", "notice_ids": ["FD1"], "reason": "notice FD1 names parcel X"}
        card = appointment_card(appt_row(), link, TODAY, notice_carded=True)
        self.assertEqual(card["route"], "research_hold")


class FakeClient:
    """Applies apply_staging_enrichment patches to the in-memory rows."""

    def __init__(self, appointments, notices, others):
        self.appointments, self.notices, self.others = appointments, notices, others
        self.calls = []

    def rpc(self, name, params):
        self.calls.append(copy.deepcopy(params["p_rows"]))
        alias = {"pilot_review": "card", TRACK_KEY: "track", EVIDENCE_KEY: "seed"}
        for item in params["p_rows"]:
            for row in self.appointments:
                if row["id"] == item["id"]:
                    row["raw_payload"] = {**row["raw_payload"], **item["raw_patch"]}
            for row in self.notices + self.others:
                if row["id"] == item["id"]:
                    row.update({alias.get(k, k): v for k, v in item["raw_patch"].items()})
        class Result:
            def execute(self):
                return type("R", (), {"data": len(params["p_rows"])})()
        return Result()


class ReviewerRunTests(unittest.TestCase):
    def setUp(self):
        raw = appt_row()["raw"]
        self.appointment = {**appt_row(), "raw_payload": raw}
        self.appointment.pop("raw")
        self.notice = {"id": "n7", "bot_source": "memphis_daily_news", "county": "shelby", "notice_id": "FD117714",
                       "loan_instrument": "08002870", "cited_appointment": {"instrument_number": "26098311"},
                       "property_address": "6669 Elmore Rd, Memphis, TN 38134", "first_notice_date": "11/20/2026",
                       "trustee_sale_date": "2026-12-29", "full_name": "Samantha Rickard", "owner_name_records": "Samantha Rickard",
                       "source_url": "https://www.memphisdailynews.com/x", "staging_status": "pending",
                       "property_value": None, "property_value_source": None,
                       "notice_tracking": {"last_seen_at": READ, "last_seen_by": "memphis_daily_news"}}
        self.brock = {k: v for k, v in sale_row().items() if k != "ranked"}
        self.client = FakeClient([self.appointment], [self.notice], [self.brock])
        self.bot = PreforeclosureReviewBot()
        for name, value in (("_appointments", lambda c, t, only: [r for r in self.client.appointments if only is None or r["id"] in only]),
                            ("_notices", lambda c, t: self.client.notices),
                            ("_sales", lambda c, t, source: [r for r in self.client.others if r["bot_source"] == source]),
                            ("_ranked", lambda c, ids: {"s1", "n7"})):
            self.enterContext(patch.object(self.bot, name, side_effect=value))
        self.lookup = self.enterContext(patch.object(bot_module, "lookup"))

    def run_once(self, **kwargs):
        return self.bot.review(self.client, now=NOW, **kwargs)

    def test_first_run_writes_cards_and_second_run_writes_nothing(self):
        first = self.run_once()
        self.assertEqual((first["rows_changed"], first["cards_written"], first["linked"]), (3, 3, 1))
        self.lookup.assert_not_called()          # the county owner is already verified
        raw = self.appointment["raw_payload"]
        self.assertEqual(raw[LINK_KEY]["days_gained"], 10)
        self.assertEqual(raw["pilot_review"]["route"], "removed")      # duplicate of the notice record
        self.assertIn("10 days before the first advertisement", self.notice["card"]["event"]["what"])
        self.assertEqual(self.brock["card"]["route"], "first_trace_review")
        self.assertEqual(self.brock["track"]["dates"], ["2026-12-18"])
        second = self.run_once()
        self.assertEqual((second["rows_changed"], second["cards_written"], second["unchanged"]), (0, 0, 3))
        self.assertEqual(len(self.client.calls), 1)

    def test_dry_run_plans_the_same_ids_and_writes_nothing(self):
        plan = self.run_once(dry_run=True)
        self.assertEqual(sorted(p["id"] for p in plan["plan"]), ["a1", "n7", "s1"])
        self.assertEqual(self.client.calls, [])
        self.assertNotIn("pilot_review", self.appointment["raw_payload"])

    def test_postponed_then_withdrawn_then_passed_rewrite_one_record(self):
        self.run_once()
        self.brock["trustee_sale_date"] = "2027-01-15"
        moved = self.run_once()
        self.assertEqual(moved["rows_changed"], 1)
        self.assertEqual((self.brock["card"]["event"]["status"], self.brock["card"]["economics"]["deadline"]),
                         ("postponed", "2027-01-15"))
        self.assertEqual(self.brock["track"]["dates"], ["2026-12-18", "2027-01-15"])
        later = datetime(2026, 11, 26, 1, 0, tzinfo=timezone.utc)
        self.client.others.append({**sale_row(id="s2"), "notice_tracking": {"last_seen_at": "2026-11-25T19:48:00+00:00",
                                                                            "last_seen_by": "brock_scott_trustee"}})
        self.bot.review(self.client, now=later)
        self.assertEqual(self.brock["card"]["route"], "research_hold")     # dropped off the list
        self.assertEqual(self.brock["track"]["status"], "withdrawn")
        self.brock["trustee_sale_date"] = "2026-11-20"
        self.bot.review(self.client, now=later)
        self.assertEqual(self.brock["card"]["route"], "removed")
        self.assertEqual({r["id"] for r in self.client.others}, {"s1", "s2"})   # never a new record

    def test_manual_card_is_kept(self):
        self.brock["card"] = {"manual": True, "route": "removed", "reviewed_by": "patrick"}
        result = self.run_once()
        self.assertEqual(result["kept_existing"], 1)
        self.assertEqual(self.brock["card"]["reviewed_by"], "patrick")

    def test_owner_check_is_bounded_and_keeps_an_unavailable_source_due(self):
        raw = self.appointment["raw_payload"]
        raw.pop(COUNTY_KEY)
        self.lookup.return_value = {"status": "source_unavailable"}
        self.run_once()
        self.assertEqual(self.lookup.call_count, 1)
        raw = self.appointment["raw_payload"]
        self.assertNotIn(COUNTY_KEY, raw)
        self.assertEqual(raw["pilot_review"]["route"], "removed")
        self.lookup.return_value = {"status": "verified_assessment", "owner": "RICKARD SAMANTHA", "parcel_id": "089026  00007",
                                    "location": "6669 ELMORE", "source_url": "https://apps2/x", "tax_year": 2026,
                                    "assessed_value": 40000, "assessment_ratio": .25, "estimated_appraisal": 160000}
        self.run_once()
        self.assertEqual(self.lookup.call_args.args[1]["raw_payload"]["extracted"]["parcel"], "08902600007")
        self.assertEqual(self.appointment["raw_payload"][COUNTY_KEY]["roll_owner"], "RICKARD SAMANTHA")
        self.assertEqual(self.run_once(owner_checks=0)["owner_checks"], 0)

    def test_merge_county_keeps_the_earlier_verified_owner(self):
        earlier = {"roll_owner": "RICKARD SAMANTHA", "parcel_id": "089026  00007", "checked_on": "2026-11-22",
                   "notice_source": "https://n"}
        fresh = {"roll_owner": "RICKARD SAMANTHA", "parcel_id": "089026  00007", "checked_on": "2026-11-23",
                 "status": "verified_assessment", "estimated_appraisal": 160000}
        merged = merge_county(earlier, fresh)
        self.assertEqual((merged["checked_on"], merged["notice_source"], merged["estimated_appraisal"]),
                         ("2026-11-22", "https://n", 160000))
        changed = merge_county(earlier, {**fresh, "roll_owner": "SOMEONE ELSE"})
        self.assertEqual((changed["roll_owner"], changed["recheck"]["roll_owner"]), ("RICKARD SAMANTHA", "SOMEONE ELSE"))


if __name__ == "__main__":
    unittest.main()
