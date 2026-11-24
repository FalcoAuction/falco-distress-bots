"""Tests for the company signer answer. No network.

    python -m unittest tests.test_entity_signer -v
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

from src.bots import _entity_signer as es  # noqa: E402
from src.bots._cre_review import SOS_PATH, mailing_index_for, review_row  # noqa: E402
from src.bots._pilot_review import source  # noqa: E402
from src.bots._tax_tract_review import shelby_card  # noqa: E402
from src.bots.cre_review_bot import plan_rows  # noqa: E402

TODAY = date(2026, 11, 23)


def setUpModule():
    # These fixtures model a newspaper notice whose source is still being
    # read. The stopped-source behaviour is covered in test_source_access.
    global _reading
    from unittest import mock
    from src.bots import _source_access
    _reading = mock.patch.dict(_source_access.STOPPED, clear=True)
    _reading.start()


def tearDownModule():
    _reading.stop()
NOTICE = source("Trustee sale notice, nashville_ledger", "https://example.test/notice/9", "2026-11-06")
RECITAL = ("Deed of Trust dated March 1, 2024, of record in Book 100, Page 5, executed by Example Holdings, LLC, a "
           "Tennessee Limited Liability Company; by it's member, Jane Q. Doe AND Jane Q. Doe Individually, conveying "
           "certain property to Tom Trustee, Trustee for First Example Bank.")


def names(found):
    return [(f["name"], f["role"], f["verified"]) for f in found]


class SignerInText(unittest.TestCase):
    def test_recital_names_member(self):
        got = es.signers_in_text(RECITAL, "EXAMPLE HOLDINGS LLC", NOTICE)
        self.assertEqual(names(got), [("Jane Q. Doe", "member", True)])
        self.assertIn("by it's member, Jane Q. Doe", got[0]["quote"])
        self.assertEqual(got[0]["source"]["date"], "2026-11-06")

    def test_other_wordings(self):
        for text, role in (
            ("from Example Holdings, LLC, a Delaware limited liability company, by John Roe, Authorized Signatory, dated May 1, 2022", "authorized signatory"),
            ("EXAMPLE HOLDINGS, L.L.C. By: John Roe Its: Managing Member STATE OF TENNESSEE", "managing member"),
            ("signed by John Roe, President of Example Holdings LLC, on that date", "president"),
            ("Example Holdings, LLC, acting by and through John Roe, its Chief Manager,", "chief manager"),
        ):
            got = es.signers_in_text(text, "Example Holdings Llc", NOTICE)
            self.assertEqual(names(got), [("John Roe", role, True)], text)

    def test_undated_document_is_a_path(self):
        answer = es.find("Example Holdings LLC", texts=[(RECITAL, source("Notice", None, None))])
        self.assertEqual(answer["status"], "path")
        self.assertEqual(answer["signers"], [])

    def test_exact_entity_only(self):
        for other in ("North Example Holdings LLC", "Example Holdings II LLC", "Example Holding LLC", "Example LLC"):
            self.assertEqual(es.signers_in_text(RECITAL, other, NOTICE), [], other)
        longer = RECITAL.replace("Example Holdings, LLC", "North Example Holdings, LLC")
        self.assertEqual(es.signers_in_text(longer, "Example Holdings LLC", NOTICE), [])

    def test_same_surname_is_not_a_signer(self):
        text = ("WHEREAS, Doe Realty, Inc. executed a Deed of Trust. Other interested parties: Marion Doe; "
                "Peoples Bank; Benjamin C. Wilson")
        answer = es.find("DOE REALTY INC", texts=[(text, NOTICE)])
        self.assertEqual(answer["status"], "none")

    def test_interested_party_titles_are_not_signers(self):
        text = ("executed by Example Holdings, LLC to Tom Trustee. Interested parties: Antonio Turner, Manager, The Vale "
                "Apartments, President, Wilson Bank and Trust, Jack Franklin, Manager, Example Holdings Apartments")
        self.assertEqual(es.find("Example Holdings LLC", texts=[(text, NOTICE)])["status"], "none")

    def test_lender_officer_and_trustee_are_not_signers(self):
        text = ("executed by Example Holdings, LLC to Tom Trustee, Trustee, for First Example Bank, by Sam Banker, its "
                "Vice President. David M. Anthony, Substitute Trustee")
        self.assertEqual(es.find("Example Holdings LLC", texts=[(text, NOTICE)])["status"], "none")
        # The same words do name the bank's officer when the bank is the entity asked about.
        self.assertEqual(names(es.signers_in_text(text, "First Example Bank", NOTICE)), [("Sam Banker", "vice president", True)])

    def test_registered_agent_company_is_nothing(self):
        text = "Example Holdings, LLC, by Corporation Service Company, Registered Agent, 2908 Poston Ave Nashville"
        self.assertEqual(es.find("Example Holdings LLC", texts=[(text, NOTICE)])["status"], "none")

    def test_registered_agent_person_is_a_path(self):
        text = "Example Holdings, LLC, by John Roe, Registered Agent, 10 Main St"
        answer = es.find("Example Holdings LLC", texts=[(text, NOTICE)])
        self.assertEqual(answer["status"], "path")
        self.assertEqual(answer["paths"][0]["kind"], "registered_agent")
        self.assertFalse(answer["paths"][0]["verified"])
        self.assertTrue(es.path_lines(answer)[0].startswith("Path to signer (not verified): John Roe, registered agent"))

    def test_guarantor_is_a_path(self):
        text = "executed by Example Holdings, LLC. Interested parties include: Tabor L. Roe, Jaclynn Roe (Limited Guarantor)"
        answer = es.find("Example Holdings LLC", texts=[(text, NOTICE)])
        self.assertEqual((answer["status"], answer["paths"][0]["name"], answer["paths"][0]["kind"]),
                         ("path", "Jaclynn Roe", "guarantor"))

    def test_duplicates_and_two_signers(self):
        two = RECITAL + " Also signed for Example Holdings, LLC by John Roe, its Manager."
        answer = es.find("Example Holdings LLC", texts=[(RECITAL, NOTICE), (two, NOTICE)],
                         mail_lines=[("ATTN JANE Q DOE, PO BOX 9, CHATTANOOGA TN 37405", None)])
        self.assertEqual([s["name"] for s in answer["signers"]], ["Jane Q. Doe", "John Roe"])
        self.assertEqual(answer["paths"], [])           # the mailing name is the verified signer already
        self.assertEqual(answer, es.find("Example Holdings LLC", texts=[(RECITAL, NOTICE), (two, NOTICE)],
                                         mail_lines=[("ATTN JANE Q DOE, PO BOX 9, CHATTANOOGA TN 37405", None)]))


class MailingPaths(unittest.TestCase):
    def test_care_of_lines(self):
        self.assertEqual(es.care_of_names("ATTN DHAVAL PATEL, 6705 SHALLOWFORD RD, CHATTANOOGA TN 37421"),
                         [("attention line", "DHAVAL PATEL")])
        self.assertEqual(es.care_of_names("COWART ST HFP/CHA LLCATTN E GULLEN"), [("attention line", "E GULLEN")])
        self.assertEqual(es.care_of_names("827 W MCKENNIE AVE C/O BE A HELPING HAND FOUNDATION, NASHVILLE TN 37203"),
                         [("care-of line", "BE A HELPING HAND FOUNDATION")])
        self.assertEqual(es.care_of_names("2313 E MAIN ST, CHATTANOOGA TN 37404"), [])

    def test_mailing_name_is_never_verified(self):
        src = source("Hamilton County assessor export", None, "2026-11-20")
        answer = es.find("WILLIAMS REALTY INCORPORATED",
                         mail_lines=[("WILLIAMS REALTY INCORPORATED & C/O JOE V WILLIAMS III", src)])
        self.assertEqual((answer["status"], answer["signers"]), ("path", []))
        self.assertEqual(answer["paths"][0]["name"], "JOE V WILLIAMS III")
        company = es.find("OM LOOKOUT MTN LLC", mail_lines=[("OM LOOKOUT MTN LLC & C/O COUNTRY INN & SUITES", src)])
        self.assertIn("another business or a partial name", company["paths"][0]["note"])

    def test_shared_po_box_only(self):
        src = source("Hamilton County assessor export", None, "2026-11-20")
        index = es.build_mailing_index([
            ("GALAPAGOS INDUSTRIAL PARK LLC", "ATTN JOHN S WISE III, PO BOX 4261, CHATTANOOGA TN 37405", src),
            ("SUITE CO LLC", "ATTN ANN LEE, 30 N GOULD ST STE R, SHERIDAN WY 82801", src)])
        answer = es.find("WISE PROPERTIES TN LLC", mailing="P O BOX 4261, CHATTANOOGA TN37405", mailing_index=index)
        self.assertEqual((answer["status"], answer["paths"][0]["kind"], answer["paths"][0]["name"]),
                         ("path", "shared_mailing", "JOHN S WISE III"))
        self.assertIn("inference", answer["paths"][0]["note"])
        # A shared street address (a registered agent's office) is never a link.
        self.assertEqual(es.find("CAELUM GLOBAL 1 LLC", mailing="30 N GOULD ST STE R, SHERIDAN WY 82801",
                                 mailing_index=index)["status"], "none")
        self.assertEqual(es.find("OTHER LLC", mailing="PO BOX 4261, CHATTANOOGA TN 37401", mailing_index=index)["status"], "none")

    def test_suit_parties(self):
        src = source("Shelby County tax suit publication", "https://example.test/n", None)
        answer = es.find("SMART INC STUDENT MAKING A REWARDED", suit_parties=(
            ["M.L.A. WILLIAMS", "SMART INC STUDENT MAKING A REWARDED", "Smart Inc/ Student Making (A) Rewarded Today Inc.",
             "WILLIAMS MARION"], src))
        self.assertEqual(answer["status"], "path")
        self.assertEqual([p["name"] for p in answer["paths"]], ["WILLIAMS MARION"])
        self.assertIn("does not state the relationship", answer["paths"][0]["note"])


def notice_row(body=RECITAL, **over):
    row = {
        "id": "row-llc", "bot_source": "nashville_ledger", "distress_type": "PRE_FORECLOSURE", "county": "dickson",
        "property_address": "100 Example Dr, Burns, TN 37029", "owner_name_records": "Example Holdings Llc",
        "full_name": "Example Holdings Llc", "property_value": 900000, "property_value_source": "comptroller",
        "trustee_sale_date": "2026-12-04", "source_url": "https://example.test/notice/9",
        "phone_metadata": {"notice_tracking": {"last_seen_at": "2026-11-23T01:00:00+00:00"}},
        "raw_payload": {
            "property_lane": {"lane": "commercial", "grade": "B", "exclude_reason": None, "basis": "comptroller:08 COMMERCIAL"},
            "publication_date": "2026-11-06", "body": body,
            "structured": {"borrower": "Example Holdings Llc", "substitute_trustee": "David M Anthony"},
            "comptroller_parcel": {"owner": "EXAMPLE HOLDINGS LLC", "parcelid": "022 119 03103 000 2026", "appraisal": 900000.0,
                                   "class": "08 COMMERCIAL", "owner_mailing": "2231 N EXAMPLE RD SUITE 202, MOUNT JULIET TN 37122",
                                   "last_sale_date": "20250228"},
        },
    }
    row.update(over)
    return row


class CommercialCards(unittest.TestCase):
    def test_notice_signer_clears_only_the_signer_hold(self):
        card, facts = review_row(notice_row(), None, TODAY)
        self.assertEqual(card["route"], "first_trace_review", card["route_reason"])
        self.assertEqual(card["decider"]["name"], "Jane Q. Doe")
        self.assertTrue(card["decider"]["verified"])
        self.assertIn("member of EXAMPLE HOLDINGS LLC", card["decider"]["role"])
        self.assertEqual(card["decider"]["source"]["date"], "2026-11-06")
        self.assertEqual(card["owner_names"], ["Example Holdings Llc"])     # the owner group is still the company
        self.assertEqual((facts["signer"], facts["entity_signer"]["status"]), ("Jane Q. Doe", "verified"))

    def test_other_holds_stay(self):
        passed = notice_row(trustee_sale_date="2026-11-01")
        self.assertEqual(review_row(passed, None, TODAY)[0]["route"], "research_hold")
        small = notice_row(property_value=120000)
        small["raw_payload"]["comptroller_parcel"]["appraisal"] = 120000.0
        self.assertEqual(review_row(small, None, TODAY)[0]["route"], "research_hold")
        unmatched = notice_row()
        unmatched["raw_payload"].pop("comptroller_parcel")
        self.assertEqual(review_row(unmatched, None, TODAY)[0]["route"], "research_hold")

    def test_entity_only_notice_still_holds(self):
        body = ("WHEREAS, Example Holdings, LLC, a Tennessee limited liability company, executed a Deed of Trust dated "
                "February 28, 2025, and conveyed to Jonathan R. Vinson, as Trustee. Other interested parties: Peoples "
                "Bank; Benjamin C. Wilson. David M. Anthony, Substitute Trustee")
        card, facts = review_row(notice_row(body), None, TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertEqual(card["route_reason"], "The owner of record is a company; no member, manager or officer is named "
                                               "in a primary record, so the signer is unresolved.")
        self.assertFalse(card["decider"]["verified"])
        self.assertIn(SOS_PATH, card["availability"]["verify_next"])
        self.assertNotIn("entity_signer", facts)

    def test_mailing_path_is_labeled_and_holds(self):
        row = notice_row("WHEREAS, Example Holdings, LLC executed a Deed of Trust.")
        row["raw_payload"]["comptroller_parcel"]["owner_mailing"] = "ATTN JOHN ROE, PO BOX 12, MOUNT JULIET TN 37122"
        card, facts = review_row(row, None, TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("1 path to the signer on file, not verified.", card["route_reason"])
        first = card["availability"]["verify_next"][0]
        self.assertTrue(first.startswith("Path to signer (not verified): JOHN ROE, attention line"), first)
        self.assertEqual(card["decider"]["name"], "EXAMPLE HOLDINGS LLC")
        self.assertIsNone(facts["signer"])
        self.assertEqual(facts["entity_signer"]["status"], "path")

    def test_manual_card_kept_and_repeat_run_writes_nothing(self):
        rows = [notice_row()]
        first = plan_rows(rows, lambda r: None, TODAY)
        self.assertEqual(first[0]["route"], "first_trace_review")
        rows[0]["raw_payload"].update(first[0]["patch"])
        self.assertEqual(plan_rows(rows, lambda r: None, TODAY)[0]["patch"], {})
        manual = notice_row()
        manual["raw_payload"]["pilot_review"] = {"manual": True, "route": "research_hold", "reviewed_by": "patrick"}
        plan = plan_rows([manual], lambda r: None, TODAY)
        self.assertTrue(plan[0]["kept_existing_card"])
        self.assertNotIn("pilot_review", plan[0]["patch"])

    def test_sister_company_box(self):
        sister = notice_row("WHEREAS, Example Holdings, LLC executed a Deed of Trust.", id="row-a")
        sister["raw_payload"]["comptroller_parcel"]["owner_mailing"] = "ATTN JOHN ROE, PO BOX 12, MOUNT JULIET TN 37122"
        other = notice_row("WHEREAS, Second Example, LLC executed a Deed of Trust.", id="row-b",
                           owner_name_records="Second Example Llc", full_name="Second Example Llc")
        other["raw_payload"]["structured"]["borrower"] = "Second Example Llc"
        other["raw_payload"]["comptroller_parcel"].update(owner="SECOND EXAMPLE LLC", owner_mailing="P O BOX 12, MOUNT JULIET TN 37122")
        index = mailing_index_for([sister, other])
        card, facts = review_row(other, None, TODAY, index)
        self.assertEqual(card["route"], "research_hold")
        self.assertEqual(facts["entity_signer"]["paths"][0]["kind"], "shared_mailing")


class ShelbyTaxCard(unittest.TestCase):
    def test_suit_party_is_a_path_and_the_hold_stays(self):
        row = {"id": "t1", "property_address": "3242 COMMERCIAL, Shelby County, TN", "property_value": 176500,
               "property_value_source": "shelby_trustee_assessment_estimate", "trustee_sale_date": "2027-04-13",
               "owner_name_records": "SMART INC", "source_url": "https://example.test/p",
               "raw_payload": {"parcel": "078001  00002", "sale_id": "TS2302",
                               "trustee_detail": {"primary_owner": "SMART INC", "estimated_appraisal": 176500,
                                                  "checked_on": "2026-11-21", "total_tax_balance": 100.0,
                                                  "visible_prior_year_balances": {"2024": 100.0}},
                               "publication_evidence": {"url": "https://example.test/n", "parties": ["SMART INC", "WILLIAMS MARION"]}}}
        status = {"status": "listed", "checked_on": "2026-11-23", "last_confirmed_on": "2026-11-23", "sale_date": "2027-04-13",
                  "note": ""}
        card = shelby_card(row, status, TODAY, "https://example.test/list.csv")
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(card["route_reason"].startswith("The owner of record is an entity; no person with authority"))
        self.assertIn("Path to signer (not verified): WILLIAMS MARION", card["availability"]["verify_next"][0])


if __name__ == "__main__":
    unittest.main()
