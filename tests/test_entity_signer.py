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
from src.bots import _signer_lookup as sl  # noqa: E402
from src.bots._cre_review import SOS_PATH, mailing_index_for, review_row  # noqa: E402
from src.bots._pilot_review import source  # noqa: E402
from src.bots._tax_tract_review import shelby_card  # noqa: E402
from src.bots.cre_review_bot import CreReviewBot, plan_rows, plan_signer_lookups  # noqa: E402

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
        self.assertEqual((answer["status"], answer["tier"]), ("named", "named_person"))
        self.assertEqual(answer["signers"], [])

    def test_verified_signer_carries_source_role_date_and_question(self):
        answer = es.find("EXAMPLE HOLDINGS LLC", texts=[(RECITAL, NOTICE)])
        self.assertEqual((answer["status"], answer["tier"]), ("verified", "verified_authority"))
        first = answer["signers"][0]
        self.assertEqual((first["name"], first["role"], first["tier"], first["source"]["date"]),
                         ("Jane Q. Doe", "member", "verified_authority", "2026-11-06"))
        self.assertIn("Confirm with Jane Q. Doe that the member role is still current", first["ask"])
        self.assertTrue(es.signer_basis(answer).startswith(
            "Verified authority: Trustee sale notice, nashville_ledger, dated 2026-11-06 names Jane Q. Doe as member"))

    def test_same_words_different_entity_type(self):
        # An LLC and an Inc with the same words are different companies.
        for other in ("Example Holdings Inc", "Example Holdings Corp", "Example Holdings LP", "Example Holdings"):
            self.assertFalse(es.same_entity("Example Holdings LLC", other), other)
            self.assertEqual(es.find(other, texts=[(RECITAL, NOTICE)])["status"], "none", other)
        inc = RECITAL.replace("Example Holdings, LLC", "Example Holdings, Inc.")
        self.assertEqual(es.find("Example Holdings LLC", texts=[(inc, NOTICE)])["status"], "none")

    def test_named_trustee_or_lender_is_dropped(self):
        text = "executed by Example Holdings, LLC, by David M. Anthony, its Authorized Agent, to First Example Bank"
        self.assertEqual(es.find("Example Holdings LLC", texts=[(text, NOTICE)])["status"], "verified")
        barred = es.find("Example Holdings LLC", texts=[(text, NOTICE)], never=["David M Anthony", None, "First Example Bank"])
        self.assertEqual((barred["status"], barred["signers"], barred["paths"]), ("none", [], []))

    def test_one_person_keeps_the_strongest_tier(self):
        src = source("Hamilton County assessor export", None, "2026-11-20")
        index = es.build_mailing_index([("SISTER LLC", "ATTN JOHN ROE, PO BOX 12, MOUNT JULIET TN 37122", src)])
        answer = es.find("Example Holdings LLC", mail_lines=[("ATTN JOHN ROE, PO BOX 12, MOUNT JULIET TN 37122", src)],
                         mailing="PO BOX 12, MOUNT JULIET TN 37122", mailing_index=index)
        self.assertEqual([(p["name"], p["tier"]) for p in answer["paths"]], [("JOHN ROE", "named_person")])
        self.assertEqual(es.hold_note(answer), "On file: 1 named person worth asking; none is verified authority.")

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
        self.assertEqual((answer["status"], answer["tier"]), ("contact", "mailing_contact"))
        self.assertEqual(answer["paths"][0]["kind"], "registered_agent")
        self.assertFalse(answer["paths"][0]["verified"])
        self.assertTrue(es.path_lines(answer)[0].startswith(
            "Registered agent or mailing contact (not a signer): John Roe, registered agent"))

    def test_guarantor_is_a_path(self):
        text = "executed by Example Holdings, LLC. Interested parties include: Tabor L. Roe, Jaclynn Roe (Limited Guarantor)"
        answer = es.find("Example Holdings LLC", texts=[(text, NOTICE)])
        self.assertEqual((answer["status"], answer["paths"][0]["name"], answer["paths"][0]["kind"]),
                         ("named", "Jaclynn Roe", "guarantor"))

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
        company = es.find("OM LOOKOUT MTN LLC", mail_lines=[("OM LOOKOUT MTN LLC & C/O COUNTRY INN & SUITES", src)])
        self.assertEqual((answer["status"], answer["signers"]), ("named", []))
        self.assertEqual((answer["paths"][0]["name"], answer["paths"][0]["tier"]), ("JOE V WILLIAMS III", "named_person"))
        self.assertEqual(answer["paths"][0]["source"]["date"], "2026-11-20")
        self.assertIn("Ask JOE V WILLIAMS III what their role is with WILLIAMS REALTY INCORPORATED", answer["paths"][0]["ask"])
        self.assertEqual((company["status"], company["paths"][0]["tier"]), ("contact", "mailing_contact"))
        self.assertIn("another business or a partial name", company["paths"][0]["note"])

    def test_shared_po_box_only(self):
        src = source("Hamilton County assessor export", None, "2026-11-20")
        index = es.build_mailing_index([
            ("GALAPAGOS INDUSTRIAL PARK LLC", "ATTN JOHN S WISE III, PO BOX 4261, CHATTANOOGA TN 37405", src),
            ("SUITE CO LLC", "ATTN ANN LEE, 30 N GOULD ST STE R, SHERIDAN WY 82801", src)])
        answer = es.find("WISE PROPERTIES TN LLC", mailing="P O BOX 4261, CHATTANOOGA TN37405", mailing_index=index)
        self.assertEqual((answer["status"], answer["paths"][0]["kind"], answer["paths"][0]["name"]),
                         ("contact", "shared_mailing", "JOHN S WISE III"))
        # A shared box never lifts the person above mailing contact, and never clears a hold.
        self.assertEqual((answer["tier"], answer["paths"][0]["tier"], answer["signers"]), ("mailing_contact", "mailing_contact", []))
        self.assertIn("the link is the mailing address only", answer["paths"][0]["note"])
        self.assertTrue(es.path_lines(answer)[0].startswith(
            "Registered agent or mailing contact (not a signer): JOHN S WISE III, attention name on GALAPAGOS "
            "INDUSTRIAL PARK LLC at the same P.O. box (Hamilton County assessor export, dated 2026-11-20)"))
        self.assertIsNone(es.hold_extras("WISE PROPERTIES TN LLC", mailing="P O BOX 4261, CHATTANOOGA TN37405",
                                         mailing_index=index)[0])
        # The company whose own line carries the name is one tier up, and still not verified.
        own = es.find("GALAPAGOS INDUSTRIAL PARK LLC", mailing="PO BOX 4261, CHATTANOOGA TN 37405", mailing_index=index,
                      mail_lines=[("ATTN JOHN S WISE III, PO BOX 4261, CHATTANOOGA TN 37405", src)])
        self.assertEqual((own["status"], [p["tier"] for p in own["paths"]]), ("named", ["named_person"]))
        # A shared street address (a registered agent's office) is never a link.
        self.assertEqual(es.find("CAELUM GLOBAL 1 LLC", mailing="30 N GOULD ST STE R, SHERIDAN WY 82801",
                                 mailing_index=index)["status"], "none")
        self.assertEqual(es.find("OTHER LLC", mailing="PO BOX 4261, CHATTANOOGA TN 37401", mailing_index=index)["status"], "none")

    def test_suit_parties(self):
        src = source("Shelby County tax suit publication", "https://example.test/n", None)
        answer = es.find("SMART INC STUDENT MAKING A REWARDED", suit_parties=(
            ["M.L.A. WILLIAMS", "SMART INC STUDENT MAKING A REWARDED", "Smart Inc/ Student Making (A) Rewarded Today Inc.",
             "WILLIAMS MARION"], src))
        self.assertEqual(answer["status"], "named")
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
        self.assertIn("On file: 1 named person worth asking; none is verified authority.", card["route_reason"])
        first = card["availability"]["verify_next"][0]
        self.assertTrue(first.startswith("Named person worth asking (authority not verified): JOHN ROE, attention line"), first)
        self.assertIn("Next: Ask JOHN ROE what their role is with EXAMPLE HOLDINGS LLC", first)
        self.assertEqual(card["decider"]["name"], "EXAMPLE HOLDINGS LLC")
        self.assertFalse(card["decider"]["verified"])
        self.assertIsNone(facts["signer"])
        self.assertEqual((facts["entity_signer"]["status"], facts["entity_signer"]["tier"]), ("named", "named_person"))

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


def clerk_page(*records):
    """A County Clerk result page: (business, address, start, close, status, owner, owner address)."""
    if not records:
        return ("<div>Results Found: 0</div><table><tr><td>Business</td><td>Product</td><td>Business Start Date</td>"
                "<td>Business Close Date</td><td>License Date</td><td>Status</td><td>Owner</td>"
                "<tr><td colspan=7>No Records Found</td></tr></table>")
    rows = "".join(
        f"<tr bgcolor=white valign=top><td>{b}<br>{a}\n<br>CHATTANOOGA, TN 37410<br>\n</td><td></td><td>{start}</td>"
        f"<td>{close}</td><td>0000-00-00</td>\n<td>{status}</td><td>{owner}<br>{oa}<br>\nCHATTANOOGA, TN 37410<br></td></tr>"
        for b, a, start, close, status, owner, oa in records)
    return (f"<div>Results Found: {len(records)}</div><table><tr style=\"font-weight:bold\"><td>Business</td><td>Product</td>"
            "<td>Business Start Date</td><td>Business Close Date</td><td>License Date</td><td>Status</td><td>Owner</td>"
            f"<tr>{rows}</table>")


def hamilton_row(owner="EXAMPLE MARKET INC", mailing="202 W 38TH ST, CHATTANOOGA TN 37410", rid="h1", **over):
    row = {"id": rid, "bot_source": "hamilton_tax_delinquent", "distress_type": "TAX_LIEN", "county": "hamilton",
           "property_address": "202 W 38TH ST, CHATTANOOGA, TN", "owner_name_records": owner, "full_name": owner,
           "raw_payload": {"parcel": "155N-A-001", "owner_name_1": owner,
                           "property_lane": {"lane": "commercial", "grade": "B", "exclude_reason": None}}}
    row.update(over)
    return row


def hamilton_evidence(owner="EXAMPLE MARKET INC", mailing="202 W 38TH ST, CHATTANOOGA TN 37410"):
    return {"roll": {"on_roll": True, "balance": 9000.0, "years": [2023, 2024, 2025], "turned_over_years": [2023],
                     "roll_owner": owner, "mortgage_company": None},
            "roll_date": "2026-11-01", "assessor_date": "2026-11-21", "duplicate_parcel": False,
            "assessor": {"owner": owner, "owner_mailing": mailing, "appraised": 400000, "last_sale_date": "2015-01-05",
                         "property_address": "202 W 38TH ST"},
            "sale_list": {}, "sale_list_year": 2026, "sale_list_url": "https://example.test/list.pdf"}


class Response:
    def __init__(self, text, status_code=200):
        self.text, self.status_code = text, status_code


class LicenseLookup(unittest.TestCase):
    """County Clerk business license records: named person at best, never authority."""

    def lookup(self, entity, *records):
        return sl.clerk_result(entity, sl.clerk_query(entity), clerk_page(*records), TODAY)

    def test_parse_and_query(self):
        self.assertEqual(sl.clerk_query("WILLIAMS REALTY INCORPORATED & C/O JOE V WILLIAMS III"), "WILLIAMS REALTY")
        self.assertEqual(sl.clerk_query("WISE PROPERTIES-TN LLC"), "WISE PROPERTIES TN")
        self.assertEqual(sl.clerk_query("SKIN & BROW ROOM LLC"), "SKIN & BROW ROOM")
        got = sl.parse_clerk(clerk_page(("EXAMPLE MARKET", "202 W 38TH ST", "2012-09-10", "2020-07-10", "Closed",
                                         "MARK  ROE", "202 W 38TH ST")))
        self.assertEqual(got, [{"business": "EXAMPLE MARKET", "business_address": "202 W 38TH ST, CHATTANOOGA, TN 37410",
                                "product": None, "start": "2012-09-10", "close": "2020-07-10", "status": "Closed",
                                "owner": "MARK ROE", "owner_address": "202 W 38TH ST, CHATTANOOGA, TN 37410"}])
        self.assertEqual(sl.parse_clerk(clerk_page()), [])

    def test_failed_lookup_is_none(self):
        for page in (None, "", "<html>Just a moment...</html>", "<h1>503 Service Unavailable</h1>"):
            self.assertIsNone(sl.parse_clerk(page))
            self.assertIsNone(sl.clerk_result("EXAMPLE MARKET INC", "EXAMPLE MARKET", page, TODAY))

    def test_robots(self):
        self.assertTrue(sl.robots_allows("# comment only\n"))
        self.assertTrue(sl.robots_allows("User-agent: *\nDisallow: /admin/\n"))
        self.assertFalse(sl.robots_allows("User-agent: *\nDisallow: /\n"))
        self.assertFalse(sl.robots_allows("User-agent: *\nDisallow: /hamilton/public/\n"))
        self.assertFalse(sl.robots_allows("User-agent: FALCO-research\nDisallow: /\n"))
        self.assertTrue(sl.robots_allows("User-agent: GPTBot\nDisallow: /\n"))
        self.assertFalse(sl.robots_allows(None))

    def test_exact_name_owner_is_a_named_person(self):
        stored = self.lookup("EXAMPLE FOAM LLC", ("EXAMPLE FOAM LLC", "3902 TACOMA AVE", "2014-06-30", "2014-12-31", "Closed",
                                                  "CHARLES ROE", "3902 TACOMA AVE"),
                             ("EXAMPLE FOAM INSULATORS LLC", "1 MAIN ST", "2019-01-01", "0000-00-00", "Active", "OTHER MAN", "1 MAIN ST"))
        self.assertEqual([r["business"] for r in stored["records"]], ["EXAMPLE FOAM LLC"])
        answer = es.find("EXAMPLE FOAM LLC", licenses=stored)
        self.assertEqual((answer["status"], answer["signers"]), ("named", []))
        p = answer["paths"][0]
        self.assertEqual((p["name"], p["tier"], p["role"], p["source"]["date"]),
                         ("CHARLES ROE", "named_person", "Owner on the business license (Closed, closed 2014-12-31)", "2014-06-30"))
        self.assertIn("Hamilton County Clerk business license search, license for EXAMPLE FOAM LLC", p["source"]["label"])
        self.assertIn("whether they are a member or manager of EXAMPLE FOAM LLC", p["ask"])

    def test_name_without_type_needs_the_address_and_stays_a_contact(self):
        rec = ("EXAMPLE MARKET", "202 W 38TH ST", "2012-09-10", "2020-07-10", "Closed", "MARK ROE", "202 W 38TH ST")
        stored = self.lookup("EXAMPLE MARKET INC", rec)
        here = es.find("EXAMPLE MARKET INC", licenses=stored, addresses=["202 W 38TH ST, CHATTANOOGA TN 37410"])
        self.assertEqual((here["status"], here["paths"][0]["tier"], here["paths"][0]["kind"]),
                         ("contact", "mailing_contact", "license_address"))
        self.assertEqual(es.find("EXAMPLE MARKET INC", licenses=stored, addresses=["PO BOX 9, CHATTANOOGA TN 37410"])["status"], "none")
        self.assertEqual(es.find("EXAMPLE MARKET INC", licenses=stored, addresses=["900 OTHER RD, CHATTANOOGA TN 37410"])["status"], "none")

    def test_different_entity_type_on_the_license_is_nothing(self):
        stored = self.lookup("EXAMPLE MARKET INC", ("EXAMPLE MARKET LLC", "202 W 38TH ST", "2012-09-10", "0000-00-00", "Active",
                                                    "MARK ROE", "202 W 38TH ST"))
        self.assertEqual(es.find("EXAMPLE MARKET INC", licenses=stored,
                                 addresses=["202 W 38TH ST, CHATTANOOGA TN 37410"])["status"], "none")

    def test_blank_or_company_owner_is_nothing(self):
        stored = self.lookup("EXAMPLE MARKET INC",
                             ("EXAMPLE MARKET INC", "202 W 38TH ST", "2012-09-10", "0000-00-00", "Active", "", "202 W 38TH ST"),
                             ("EXAMPLE MARKET INC", "202 W 38TH ST", "2013-09-10", "0000-00-00", "Active", "ROE HOLDINGS LLC", "X"))
        self.assertEqual(es.find("EXAMPLE MARKET INC", licenses=stored)["status"], "none")

    def test_lookup_stored_for_another_company_is_ignored(self):
        stored = self.lookup("EXAMPLE FOAM LLC", ("EXAMPLE FOAM LLC", "3902 TACOMA AVE", "2014-06-30", "0000-00-00", "Active",
                                                  "CHARLES ROE", "3902 TACOMA AVE"))
        stored["records"][0]["business"] = "OTHER NAME LLC"
        self.assertEqual(es.find("OTHER NAME LLC", licenses=stored)["status"], "none")

    def test_freshness(self):
        stored = self.lookup("EXAMPLE MARKET INC")
        self.assertTrue(sl.is_fresh(stored, "EXAMPLE MARKET INC ETAL", TODAY))
        self.assertFalse(sl.is_fresh(stored, "EXAMPLE MARKET LLC", TODAY))
        self.assertFalse(sl.is_fresh({**stored, "checked_on": "2026-01-01"}, "EXAMPLE MARKET INC", TODAY))
        self.assertFalse(sl.is_fresh(None, "EXAMPLE MARKET INC", TODAY))


class LookupInTheDailyRun(unittest.TestCase):
    """cre_review_bot: one request per company, stored on the row, reused, and nothing on failure."""

    def bot(self, pages, robots="# no rules\n"):
        bot = CreReviewBot(write=False, today=TODAY)
        bot.lookup_gap = 0
        calls = []

        def fetch(url, method="GET", **kw):
            calls.append((method, url, (kw.get("data") or {}).get("bname")))
            if url == sl.CLERK_ROBOTS:
                return robots if robots is None or isinstance(robots, Response) else Response(robots)
            page = pages.get(kw["data"]["bname"])
            return page if page is None or isinstance(page, Response) else Response(page)
        bot.fetch = fetch
        return bot, calls

    def rows(self):
        ev = hamilton_evidence()
        rows = [hamilton_row(rid="h1"), hamilton_row(rid="h2")]
        return rows, (lambda r: ev)

    def test_one_request_per_company_then_none_on_the_repeat_run(self):
        rows, ev = self.rows()
        plan = plan_rows(rows, ev, TODAY)
        self.assertTrue(all(p["route"] == "research_hold" and p["route_reason"].startswith("The owner of record is a company")
                            for p in plan), plan[0]["route_reason"])
        page = clerk_page(("EXAMPLE MARKET", "202 W 38TH ST", "2012-09-10", "2020-07-10", "Closed", "MARK ROE", "202 W 38TH ST"))
        bot, calls = self.bot({"EXAMPLE MARKET": page})
        looked = bot.signer_lookups(rows, plan)
        self.assertEqual([c for c in calls if c[0] == "POST"], [("POST", sl.CLERK_URL, "EXAMPLE MARKET")])
        self.assertEqual(sorted(looked), ["h1", "h2"])
        plan = plan_rows(rows, ev, TODAY, looked)
        for p in plan:
            self.assertEqual(p["route"], "research_hold")                      # a license owner clears nothing
            self.assertIn("On file: 1 mailing contact; none is verified authority.", p["route_reason"])
            self.assertEqual(p["patch"]["signer_lookup"][sl.CLERK_KEY]["records"][0]["owner"], "MARK ROE")
            self.assertTrue(any(line.startswith("Registered agent or mailing contact (not a signer): MARK ROE")
                                for line in p["card"]["availability"]["verify_next"]))
        for row, p in zip(rows, plan):
            row["raw_payload"].update(p["patch"])
        again = plan_rows(rows, ev, TODAY)
        self.assertEqual([p["patch"] for p in again], [{}, {}])
        bot, calls = self.bot({})
        self.assertEqual(bot.signer_lookups(rows, again), {})
        self.assertEqual(calls, [])

    def test_sister_row_reuses_the_stored_answer(self):
        rows, ev = self.rows()
        stored = sl.clerk_result("EXAMPLE MARKET INC", "EXAMPLE MARKET", clerk_page(), TODAY)
        rows[0]["raw_payload"]["signer_lookup"] = {sl.CLERK_KEY: stored}
        todo = plan_signer_lookups(rows, plan_rows(rows, ev, TODAY), TODAY)
        self.assertEqual((todo["fetch"], todo["copy"]), ({}, {"h2": stored}))

    def test_failed_lookup_changes_nothing(self):
        for pages, robots in (({"EXAMPLE MARKET": None}, "# none\n"),
                              ({"EXAMPLE MARKET": Response("error", 500)}, "# none\n"),
                              ({"EXAMPLE MARKET": "<html>Just a moment...</html>"}, "# none\n"),
                              ({"EXAMPLE MARKET": clerk_page()}, None),
                              ({"EXAMPLE MARKET": clerk_page()}, "User-agent: *\nDisallow: /\n")):
            rows, ev = self.rows()
            plan = plan_rows(rows, ev, TODAY)
            bot, calls = self.bot(pages, robots)
            looked = bot.signer_lookups(rows, plan)
            self.assertEqual(looked, {})
            self.assertEqual([p["patch"].get("signer_lookup") for p in plan_rows(rows, ev, TODAY, looked)], [None, None])
            if robots is None or "Disallow: /\n" in str(robots):
                self.assertEqual([c for c in calls if c[0] == "POST"], [])      # nothing is requested past robots.txt

    def test_cap_and_scope(self):
        ev_for = {}
        rows = []
        for n in range(4):
            owner = f"EXAMPLE {'ABCD'[n]} MARKET INC"
            rows.append(hamilton_row(owner=owner, rid=f"h{n}"))
            ev_for[f"h{n}"] = hamilton_evidence(owner)
        person = hamilton_row(owner="ROE MARK", rid="p1")
        ev_for["p1"] = hamilton_evidence("ROE MARK")
        rows.append(person)
        plan = plan_rows(rows, lambda r: ev_for[r["id"]], TODAY)
        todo = plan_signer_lookups(rows, plan, TODAY, limit=3)
        self.assertEqual(len(todo["fetch"]), 3)
        self.assertNotIn("p1", [i for job in todo["fetch"].values() for i in job["ids"]])


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
        self.assertIn("Named person worth asking (authority not verified): WILLIAMS MARION",
                      card["availability"]["verify_next"][0])


if __name__ == "__main__":
    unittest.main()
