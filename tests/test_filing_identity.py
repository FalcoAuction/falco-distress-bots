"""Filing type and identity: each record in the right lane exactly once."""

import copy
import unittest
from datetime import date
from types import SimpleNamespace

from src.bots import _notice_filing as nf
from src.bots import filing_identity_backfill as backfill
from src.bots._pilot_review import build_review, dedupe_worklist, source
from src.bots._preforeclosure_review import appointment_card, name_corrections
from src.bots._register_index import IndexEntry, build_lead, evidence_block, split_parties
from src.bots.nashville_ledger_bot import NashvilleLedgerBot
from src.bots.tn_public_notice_bot import TnPublicNoticeBot

TODAY = date(2026, 11, 24)

STANLEY = ("FORECLOSURE SALE NOTICE WHEREAS, Stanley Village 66, LLC, a Tennessee limited liability company, executed "
           "a Deed of Trust, Assignment of Leases and Security Agreement dated February 28, 2025, of record at Volume "
           "1587, Page 11, Register of Deeds Office for Dickson County, Tennessee, (the “De ...")
GLOBAL = ("SUBSTITUTE TRUSTEE’S NOTICE OF SALE Default having been made in payment of the debts and obligations to be "
          "paid in a certain Tennessee Deed of Trust, Assignment of Rents, Security Agreement and Fixture Filing "
          "executed on October 6, 2023, by Global Midsouth Holding Corp., a Tennessee corpor ...")
BRUSAW = ("NOTICE OF SUBSTITUTE TRUSTEE’S SALE\nWHEREAS, default has occurred in the performance of the covenants, "
          "terms and conditions of a Deed of Trust dated May 15, 2025, executed by MICHAEL A BRUSAW conveying certain "
          "real property therein described to SCOTT R. VALBY, as Trustee, as same appears of record i")
NO_GRANTOR = ("SUBSTITUTE TRUSTEE’S SALE\nSale at public auction will be on 11/12/2026 on or about 12:00 PM, at the At "
              "the Putnam County Courthouse, 421 East Spring Street, Cookeville, TN, Putnam County, Tennessee, conducted "
              "by the Substitute Trustee as identified and set for the herein below, pursuant to Deed of T")
TAX_SALE = ("CHANCERY SALE OF LAND FOR DELINQUENT TAXES In obedience to decrees of the Chancery Court of Wilson County, "
            "Tennessee, at Lebanon, made in the cases of Wilson County vs. Delinquent Taxpayers, Docket No. 2024-CV-55, "
            "I will sell at public auction the following tracts")
AMBIGUOUS = ("IN THE CHANCERY COURT OF HAWKINS COUNTY, TENNESSEE DOCKET NO. 2025-CH-5 WILMINGTON SAVINGS FUND SOCIETY, "
             "FSB, D/B/A CHRISTIANA TRUST, AS TRUSTEE FOR THE CSMC 2017-F ...")
BOTH = ("NOTICE OF SUBSTITUTE TRUSTEE'S SALE WHEREAS, default has occurred under a Deed of Trust dated May 1, 2019, "
        "executed by John Doe, of record in Book 100, Page 5, and the property is also listed in the Clerk and Master's "
        "tax sale against delinquent taxpayers for 2023")
SHERIFF = ("NOTICE OF SHERIFF'S EXECUTION SALE OF PERSONAL PROPERTY WHEREAS, Estate of Christine Joy Koczwara is the "
           "holder of a Judgment in the amount of $145,500.00")
MULTI = ("NOTICE OF SUBSTITUTE TRUSTEE'S SALE WHEREAS, default has occurred under a Deed of Trust dated March 3, 2020, "
         "executed by Riverbend Farms, LLC, a Tennessee limited liability company, of record in Book 2210, Page 77, "
         "Register's Office for Maury County, Tennessee, covering Map and Parcel 045 012.00, Parcel 045 013.00 and "
         "Parcel No. 046 001.01")


def notice_row(excerpt, notice_id="580544", county="davidson", category=("22", "Delinquent Taxes"), **over):
    row = {"id": f"row-{notice_id}", "bot_source": "tn_public_notice", "staging_status": "pending",
           "staged_at": f"2026-11-{int(notice_id) % 20 + 1:02d}T00:00:00+00:00", "pipeline_lead_key": f"key-{notice_id}",
           "county": county, "distress_type": "PRE_FORECLOSURE", "full_name": None, "owner_name_records": None,
           "property_address": None, "trustee_sale_date": None,
           "source_url": f"https://www.tnpublicnotice.com/Details.aspx?ID={notice_id}",
           "raw_payload": {"excerpt": excerpt, "category": category[1], "category_id": category[0], "county": county,
                           "city": "Nashville", "newspaper": "The Nashville Ledger",
                           "publication_date": "Friday, November 13, 2026"}}
    row.update(over)
    return row


def lead(excerpt, distress="TAX_LIEN", label="Delinquent Taxes", county="davidson", notice_id="1"):
    return TnPublicNoticeBot()._build_lead({"notice_id": notice_id, "county": county, "excerpt": excerpt,
                                            "paper": "The Nashville Ledger", "pub_date": "Friday, November 13, 2026",
                                            "city": "Nashville"}, distress, label)


def existing_card(staging_id, name, county, parcel, address, deadline, route="research_hold"):
    src = source("Trustee sale notice", "https://example.test/notice", "2026-11-06")
    card = build_review(
        lane="cre", route=route, route_reason="The owner of record is a company; the signer is unresolved.",
        reviewed_by="cre_review_bot", county=county, owner_names=[name], today=TODAY,
        event={"what": f"Trustee foreclosure sale advertised for {deadline}", "status": "active",
               "checked_on": "2026-11-19", "source": src},
        asset={"description": "Commercial property", "parcel": parcel, "address": address, "tie": "The notice names this street",
               "source": src},
        decider={"name": name.upper(), "role": "owner of record (entity)", "basis": "County parcel record", "source": src},
        economics={"deadline": deadline, "deadline_kind": "trustee sale", "value": 334800.0,
                   "value_basis": "County appraisal", "value_source": src, "unknowns": ["Payoff"]},
        availability={"evidence": "No transfer recorded", "checked_on": "2026-11-24",
                      "first_call_question": "Is the sale still going ahead?", "verify_next": ["Signer path"]})
    return {"id": staging_id, "bot_source": "nashville_ledger", "county": county, "full_name": name,
            "trustee_sale_date": deadline, "staging_status": "pending", "pilot_review": card}


def run_plan(notices, cards=(), bodies=None, ledger=(), register=(), appointments=()):
    return backfill.plan_all(list(notices), list(cards), bodies or {}, list(ledger), list(register),
                             list(appointments), TnPublicNoticeBot(), TODAY)


def write(rows, changes):
    """Apply a plan to in-memory rows the way the database would."""
    by_id = {r["id"]: r for r in rows}
    for change in changes:
        row = by_id[change["id"]]
        for column, old in change["guards"].items():
            assert row.get(column) == old, (change["id"], column)
        row.update(change["columns"])
        row["raw_payload"] = {**row["raw_payload"], **change["raw_patch"]}


class TypeFromTheText(unittest.TestCase):
    def test_foreclosure_filed_under_delinquent_taxes(self):
        for text, name in ((STANLEY, "Stanley Village 66, LLC"), (GLOBAL, "Global Midsouth Holding Corp")):
            built = lead(text)
            self.assertEqual(built.distress_type, "PRE_FORECLOSURE")
            self.assertEqual(built.full_name, name)
            self.assertEqual(built.raw_payload["filed_as"], "TAX_LIEN")
            self.assertEqual(built.raw_payload["filing"]["filed_under"], "Delinquent Taxes")
            self.assertIn("deed of trust", built.raw_payload["filing"]["type_basis"])

    def test_county_is_the_register_named_in_the_notice(self):
        built = lead(STANLEY)
        self.assertEqual(built.county, "dickson")
        self.assertEqual(built.raw_payload["filing"]["publication_county"], "davidson")
        self.assertEqual(built.raw_payload["filing"]["identifiers"]["deed_book"], "1587")
        self.assertEqual(lead(GLOBAL, county="fayette").county, "fayette")   # the text names no county: not guessed

    def test_true_tax_lien_stays_a_tax_lien(self):
        self.assertEqual(nf.classify(TAX_SALE)["type"], "TAX_LIEN")
        filed_as_foreclosure = lead(TAX_SALE, "PRE_FORECLOSURE", "Foreclosures", county="wilson")
        self.assertEqual(filed_as_foreclosure.distress_type, "TAX_LIEN")
        self.assertEqual(filed_as_foreclosure.raw_payload["filed_as"], "PRE_FORECLOSURE")
        self.assertIsNone(filed_as_foreclosure.trustee_sale_date)
        # A foreclosure's "subject to unpaid taxes" clause does not make it a tax sale.
        mention = STANLEY.replace("(the “De ...", "subject to any unpaid taxes and delinquent taxes")
        self.assertEqual(nf.classify(mention)["type"], "PRE_FORECLOSURE")

    def test_ambiguous_notice_is_held_not_guessed(self):
        both = nf.classify(BOTH)
        self.assertIsNone(both["type"])
        self.assertIn("both", both["hold_reason"])
        for text in (AMBIGUOUS, SHERIFF):
            built = lead(text, "PRE_FORECLOSURE", "Foreclosures", county="hawkins")
            self.assertEqual(built.distress_type, nf.RESEARCH)
            self.assertTrue(built.raw_payload["filing"]["hold"]["reason"])
            self.assertIsNone(built.raw_payload["filing"]["type"])
            self.assertIsNone(built.property_address)
        self.assertEqual(lead(BOTH).distress_type, nf.RESEARCH)

    def test_not_a_property_filing_is_not_staged(self):
        for text in ("NOTICE OF SALE Ellis Towing and Recovery LLC will sell the following vehicles on May 15, 2026",
                     "NOTICE OF SALE OF REAL PROPERTY BY SHELBY COUNTY GOVERNMENT Notice is hereby given that Shelby "
                     "County Government has received an Offer to Purchase for the following property"):
            self.assertIsNone(lead(text, "PRE_FORECLOSURE", "Foreclosures", county="shelby"))


class Parties(unittest.TestCase):
    def test_deed_wording_is_never_a_name(self):
        built = lead(BRUSAW, "PRE_FORECLOSURE", "Foreclosures", county="putnam")
        self.assertEqual(built.full_name, "MICHAEL A BRUSAW")
        for bad in ("Michael A Brusaw Conveying Certain Real Property Therein Described To Scott R. Valby",
                    "Archienico V. Martin To Michael A. King", "Cedric Larodney Coppage who acquired title as Cedric Coppage"):
            self.assertIsNone(nf.party_name(bad))
        for good in ("Menenial Property Partner's LLC", "Red River Property Group Llc", "Donna K. Reed"):
            self.assertEqual(nf.party_name(good), good)
        # The grantor is kept and the beneficiary dropped.
        self.assertEqual(nf.party_name("Dustin Lee May In Favor Of Russell E. Roberson"), "Dustin Lee May")

    def test_no_party_is_stored_with_the_reason(self):
        built = lead(NO_GRANTOR, "PRE_FORECLOSURE", "Foreclosures", county="putnam")
        self.assertIsNone(built.full_name)
        self.assertIsNone(built.property_address)            # the only street is the courthouse
        self.assertEqual(built.trustee_sale_date, "2026-11-12")
        self.assertIn("ends before a grantor", built.raw_payload["filing"]["party_reason"])
        self.assertIsNone(built.raw_payload["filing"]["key"])   # no identifier: never merged with another notice

    def test_grantors_in_parentheses(self):
        text = ("SUCCESSOR TRUSTEE'S NOTICE OF SALE WHEREAS, by Deed of Trust dated the 29th day of September, 2022, of "
                "record in Book 1440, Page 443, in the Register's Office for Putnam County, TN, BRADY T. BUSH and LISA "
                "BUSH (the “Grantors”) conveyed to Amelia A. Shreve, Trustee, the hereinafter described real est")
        filing = nf.build(text, category="Foreclosures", category_id="4", publication_county="putnam")
        self.assertEqual(filing["parties"], ["BRADY T. BUSH", "LISA BUSH"])
        self.assertEqual(filing["identifiers"]["deed_date"], "2022-09-29")


class OneFilingOneRecord(unittest.TestCase):
    def test_multi_parcel_notice_is_one_filing(self):
        built = lead(MULTI, "PRE_FORECLOSURE", "Foreclosures", county="maury")
        self.assertEqual(built.raw_payload["filing"]["parcels"], ["045 012.00", "045 013.00", "046 001.01"])
        plan = run_plan([notice_row(MULTI, "700001", "maury", ("4", "Foreclosures"))])
        self.assertEqual(len(plan), 1)
        self.assertEqual(len(plan[0]["raw_patch"]["filing"]["parcels"]), 3)

    def test_republication_takes_the_staged_record(self):
        first, again = lead(STANLEY, notice_id="578632"), lead(STANLEY, notice_id="580544")
        self.assertEqual(first.raw_payload["filing"]["key"], again.raw_payload["filing"]["key"])
        self.assertNotEqual(first.pipeline_lead_key, again.pipeline_lead_key)
        staged = [{"pipeline_lead_key": first.pipeline_lead_key, "key": first.raw_payload["filing"]["key"], "duplicate_of": None}]
        out = nf.merge_republications([again], staged)
        self.assertEqual([x.pipeline_lead_key for x in out], [first.pipeline_lead_key])
        # Two printings read in one run: one lead that lists both.
        out = nf.merge_republications([lead(STANLEY, notice_id="578632"), lead(STANLEY, notice_id="580544")], [])
        self.assertEqual(len(out), 1)
        self.assertEqual([p["notice_id"] for p in out[0].raw_payload["filing"]["publications"]], ["578632", "580544"])
        # A record already merged into another is not staged again.
        own = lead(STANLEY, notice_id="580544")
        merged = [{"pipeline_lead_key": own.pipeline_lead_key, "key": own.raw_payload["filing"]["key"], "duplicate_of": "x"}]
        self.assertEqual(nf.merge_republications([lead(STANLEY, notice_id="580544")], merged), [])
        # No identifier, no merge.
        loose = [lead(NO_GRANTOR, "PRE_FORECLOSURE", "Foreclosures", "putnam", n) for n in ("1", "2")]
        self.assertEqual(len(nf.merge_republications(loose, [])), 2)

    def test_backfill_keeps_one_record_per_notice(self):
        rows = [notice_row(STANLEY, "578632"), notice_row(STANLEY, "580544")]
        plan = run_plan(rows)
        rejected = [c for c in plan if c["after"]["status"] == "rejected"]
        kept = [c for c in plan if c["after"]["status"] == "pending"]
        self.assertEqual((len(rejected), len(kept)), (1, 1))
        self.assertEqual(rejected[0]["columns"]["rejection_reason"], f"republication_of:{kept[0]['id']}")
        self.assertEqual(len(kept[0]["raw_patch"]["filing"]["publications"]), 2)


class ExistingPilotCards(unittest.TestCase):
    def setUp(self):
        self.stanley = existing_card("ae75dc36", "Stanley Village 66 Llc", "dickson", "119    03103 000",
                                     "100 Henry Dr, Burns, TN 37029", "2026-12-04")
        self.body = STANLEY.replace("(the “De ...", "(the “Deed of Trust”) to secure indebtedness owed to KM Developments LLC")
        self.midsouth = existing_card("00281497", "Global Midsouth Holding Corp", "madison", "057 055K E 00100 000 2026",
                                      "620 Carriage House Dr, Jackson, TN 38305", "2026-12-10")
        self.midsouth["bot_source"] = "memphis_daily_news"

    def test_stanley_village_points_at_the_existing_card(self):
        rows = [notice_row(STANLEY, "578632"), notice_row(STANLEY, "580544")]
        plan = run_plan(rows, [self.stanley], {"ae75dc36": self.body})
        write(rows, plan)
        live = [r for r in rows if r["staging_status"] == "pending"]
        self.assertEqual(len(live), 1)
        card = live[0]["raw_payload"]["pilot_review"]
        self.assertEqual((card["lane"], card["route"], live[0]["county"]), ("cre", "removed", "dickson"))
        self.assertIn("ae75dc36", card["route_reason"])
        self.assertIn("book and page", card["route_reason"])
        self.assertEqual(card["owner_group_key"], self.stanley["pilot_review"]["owner_group_key"])
        self.assertEqual(card["property_key"], self.stanley["pilot_review"]["property_key"])
        self.assertEqual(live[0]["raw_payload"]["filing"]["same_sale_as"]["staging_id"], "ae75dc36")
        self.assertEqual(live[0]["raw_payload"]["excerpt"], STANLEY)          # the source text is untouched

    def test_global_midsouth_is_held_on_the_existing_owner_group(self):
        rows = [notice_row(GLOBAL, "573689", "fayette")]
        plan = run_plan(rows, [self.midsouth])
        write(rows, plan)
        card = rows[0]["raw_payload"]["pilot_review"]
        self.assertEqual((card["lane"], card["route"]), ("cre", "research_hold"))
        self.assertIn("00281497", card["route_reason"])
        self.assertEqual(card["owner_group_key"], "tn|madison|E:GLOBAL MIDSOUTH HOLDING")
        self.assertEqual(rows[0]["county"], "fayette")                         # a company match alone moves nothing

    def test_one_owner_group_one_card_in_the_worklist(self):
        first_trace = existing_card("ae75dc36", "Stanley Village 66 Llc", "dickson", "119    03103 000",
                                    "100 Henry Dr, Burns, TN 37029", "2026-12-04", route="first_trace_review")
        rows = [notice_row(STANLEY, "578632"), notice_row(STANLEY, "580544")]
        write(rows, run_plan(rows, [first_trace], {"ae75dc36": self.body}))
        cards = [first_trace["pilot_review"]] + [r["raw_payload"]["pilot_review"] for r in rows
                                                 if r["staging_status"] == "pending"]
        groups = dedupe_worklist(cards)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]["assets"]), 1)
        self.assertEqual(len(groups[0]["assets"][0]["cards"]), 1)

    def test_person_with_the_same_name_is_not_linked_without_an_identifier(self):
        person = existing_card("p1", "Michael A Brusaw", "putnam", "071 001 00100", "1 Main St", "2026-12-01")
        plan = run_plan([notice_row(BRUSAW, "600001", "putnam", ("4", "Foreclosures"))], [person])
        self.assertFalse(any("pilot_review" in c["raw_patch"] for c in plan))

    def test_manual_card_is_never_overwritten(self):
        rows = [notice_row(STANLEY, "580544")]
        rows[0]["raw_payload"]["pilot_review"] = {"manual": True, "route": "first_trace_review", "lane": "cre"}
        plan = run_plan(rows, [self.stanley], {"ae75dc36": self.body})
        self.assertFalse(any("pilot_review" in c["raw_patch"] for c in plan))

    def test_repeat_run_plans_nothing(self):
        rows = [notice_row(STANLEY, "578632"), notice_row(STANLEY, "580544"), notice_row(GLOBAL, "573689", "fayette"),
                notice_row(BRUSAW, "600001", "putnam", ("4", "Foreclosures"),
                           full_name="Michael A Brusaw Conveying Certain Real Property Therein Described To Scott R. Valby",
                           owner_name_records="Michael A Brusaw Conveying Certain Real Property Therein Described To Scott R. Valby"),
                notice_row(TAX_SALE, "600002", "wilson", ("4", "Foreclosures")),
                notice_row(AMBIGUOUS, "600003", "hawkins", ("4", "Foreclosures"), property_address="1 Court St")]
        cards = [self.stanley, self.midsouth]
        before = copy.deepcopy(rows)
        plan = run_plan(rows, cards, {"ae75dc36": self.body})
        self.assertEqual(rows, before)                                          # planning changes nothing
        write(rows, plan)
        by_id = {r["id"]: r for r in rows}
        self.assertEqual(by_id["row-600001"]["full_name"], "MICHAEL A BRUSAW")
        self.assertEqual(by_id["row-600002"]["distress_type"], "TAX_LIEN")
        self.assertEqual(by_id["row-600003"]["distress_type"], nf.RESEARCH)
        self.assertIsNone(by_id["row-600003"]["property_address"])
        self.assertEqual(run_plan(rows, cards, {"ae75dc36": self.body}), [])

    def test_only_pending_rows_change(self):
        for status in ("verified", "promoted", "rejected"):
            self.assertEqual(run_plan([notice_row(TAX_SALE, "600002", "wilson", ("4", "Foreclosures"), staging_status=status)]), [])


class Lender(unittest.TestCase):
    BODY = ("WHEREAS, Stanley Village 66, LLC executed a Deed of Trust to secure the payment of certain indebtedness "
            "(“Indebtedness”) owed to KM Developments LLC (referred to as “Lender”); and WHEREAS, Lender, the owner and "
            "holder of said Indebtedness, has demanded that the real property be advertised and sold in satisfaction of "
            "said Indebtedness and the costs of the foreclosure, in accordance with the terms and provisions of the loan "
            "documents and Deed of Trust.")
    FRAGMENT = ("has demanded that the real property be advertised and sold in satisfaction of said Indebtedness and the "
                "costs of the foreclosure, in accordance with the terms and provisions of the loan documents and Deed of Trust")

    def test_lender_is_a_name_not_a_sentence(self):
        self.assertEqual(NashvilleLedgerBot._extract_lender(self.BODY), "KM Developments LLC")
        self.assertIsNone(NashvilleLedgerBot._extract_lender(
            "WHEREAS, the holder of said Indebtedness, has demanded that the property be sold under the Deed of Trust."))
        self.assertEqual(NashvilleLedgerBot._extract_lender("the Deed of Trust was assigned to First Federal Bank"),
                         "First Federal Bank")

    def test_backfill_replaces_the_fragment_and_keeps_the_body(self):
        row = {"id": "ae75dc36", "bot_source": "nashville_ledger", "staging_status": "pending", "distress_type": "PRE_FORECLOSURE",
               "county": "dickson", "full_name": "Stanley Village 66 Llc",
               "admin_notes": f"Nashville Ledger FL310588 · pub 2026-11-06 · lender={self.FRAGMENT} · parcel=119-031.03-000",
               "raw_payload": {"body": self.BODY, "extracted": {"lender": self.FRAGMENT, "parcel": "119-031.03-000"}}}
        plan = run_plan([], ledger=[row])
        self.assertEqual(plan[0]["raw_patch"]["extracted"]["lender"], "KM Developments LLC")
        self.assertEqual(plan[0]["raw_patch"]["extracted"]["parcel"], "119-031.03-000")
        self.assertIn("lender=KM Developments LLC · parcel", plan[0]["columns"]["admin_notes"])
        self.assertNotIn("body", plan[0]["raw_patch"])
        write([row], plan)
        self.assertEqual(run_plan([], ledger=[row]), [])


class RegisterParties(unittest.TestCase):
    ENTRY = dict(reference="RB 1746-279", recorded="2026-11-10", doc_type="Substitute of Trustee", code="SUBTR")

    def test_lender_in_the_grantee_column_is_not_the_trustee(self):
        entry = IndexEntry(**self.ENTRY, first_parties=["LINDEN THOMAS", "CREDIT HUMAN FEDERAL CREDIT UNION AKA",
                                                        "CU FACTORY BUILT LENDING LP"],
                           second_parties=["CREDIT HUMAN FEDERAL CREDIT UNION"])
        parties = split_parties(entry)
        self.assertEqual(parties["borrowers"], ["LINDEN THOMAS"])
        self.assertEqual(parties["trustees"], [])
        self.assertEqual(parties["lenders"], ["CREDIT HUMAN FEDERAL CREDIT UNION AKA", "CU FACTORY BUILT LENDING LP"])
        built = build_lead(entry, "Putnam", evidence_block("Putnam", "Recording Listing", "2026-11-23"), "recording_listing")
        self.assertEqual(built["raw_payload"]["substitute_trustees"], [])
        self.assertEqual(built["raw_payload"]["grantees_as_printed"], ["CREDIT HUMAN FEDERAL CREDIT UNION"])
        self.assertIn("no substitute trustee printed", built["admin_notes"])

    def test_a_real_trustee_stays_the_trustee(self):
        entry = IndexEntry(**self.ENTRY, first_parties=["MARCUM ALTON R", "NEWREZ LLC"], second_parties=["RUBIN LUBLIN TN PLLC"])
        parties = split_parties(entry)
        self.assertEqual((parties["trustees"], parties["lenders"], parties["notes"]), (["RUBIN LUBLIN TN PLLC"], ["NEWREZ LLC"], []))
        self.assertNotIn("grantees_as_printed", build_lead(
            entry, "Putnam", evidence_block("Putnam", "Recording Listing", "2026-11-23"), "recording_listing")["raw_payload"])

    def test_backfill_moves_the_staged_lender(self):
        row = {"id": "834541a3", "bot_source": "putnam_register_index", "staging_status": "pending",
               "distress_type": "SUBSTITUTION_OF_TRUSTEE", "county": "Putnam", "full_name": "LINDEN THOMAS",
               "admin_notes": "Substitute trustee appointed, recorded 2026-11-10 (Putnam RB 1746-279) by CREDIT HUMAN "
                              "FEDERAL CREDIT UNION AKA; trustee CREDIT HUMAN FEDERAL CREDIT UNION. Index only.",
               "raw_payload": {"book_page": "RB 1746-279", "recorded": "2026-11-10", "doc_type": "Substitute of Trustee",
                               "lenders": ["CREDIT HUMAN FEDERAL CREDIT UNION AKA", "CU FACTORY BUILT LENDING LP"],
                               "borrowers": ["LINDEN THOMAS"], "substitute_trustees": ["CREDIT HUMAN FEDERAL CREDIT UNION"]}}
        plan = run_plan([], register=[row])
        self.assertEqual(plan[0]["raw_patch"]["substitute_trustees"], [])
        self.assertEqual(plan[0]["raw_patch"]["grantees_as_printed"], ["CREDIT HUMAN FEDERAL CREDIT UNION"])
        self.assertEqual(plan[0]["raw_patch"]["borrowers"] if "borrowers" in plan[0]["raw_patch"] else ["LINDEN THOMAS"],
                         ["LINDEN THOMAS"])
        self.assertNotIn("; trustee CREDIT HUMAN", plan[0]["columns"]["admin_notes"])
        write([row], plan)
        self.assertEqual(run_plan([], register=[row]), [])


def appointment_raw(**over):
    raw = {"instrument": "26098520", "recorded": "2026-11-12", "borrowers": ["ENVLY LLC"], "parcel_id": "08800900003",
           "lenders": ["HOMES MORTGAGE TRUST S TR"], "substitute_trustees": ["CLEAR RECON LLC"], "party_basis": "lender_words",
           "deed_of_trust": {"year": "2021", "instrument": "21157143"}, "dot_checked_at": "2026-11-23",
           "loan": {"loan_date": "2021-12-14", "original_amount": 285000.0},
           "recovery": {"loan_grantors": ["ENVOY LLC"], "detail_checked_at": "2026-11-23T13:25:13+00:00"},
           "appointment_county_evidence": {
               "status": "verified_assessment", "roll_owner": "ENVOY LLC", "parcel_id": "088009  00003", "tax_year": 2026,
               "checked_on": "2026-11-23", "assessed_value": 42725, "assessment_ratio": 0.25, "estimated_appraisal": 170900,
               "county_source": "https://apps2.shelbycountytrustee.com/Parcel?parcel=088009++00003"}}
    raw.update(over)
    return raw


class IndexSpelling(unittest.TestCase):
    def test_correction_is_a_separate_field_with_its_evidence(self):
        raw = appointment_raw()
        fixes = name_corrections(raw)
        self.assertEqual(len(fixes), 1)
        fix = fixes[0]
        self.assertEqual((fix["printed"], fix["corrected"]), ("ENVLY LLC", "ENVOY LLC"))
        self.assertIn("21157143", fix["evidence"][0]["document"])
        self.assertEqual(fix["evidence"][0]["date"], "2021-12-14")
        self.assertIn("realty bill", fix["evidence"][1]["document"])
        self.assertEqual(fix["evidence"][1]["date"], "2026-11-23")
        self.assertIn("26098520", fix["tie"])
        self.assertIn("21157143", fix["tie"])
        self.assertEqual(raw["borrowers"], ["ENVLY LLC"])                       # the printed spelling stays

    def test_no_correction_without_both_documents(self):
        self.assertEqual(name_corrections(appointment_raw(recovery={})), [])
        other_owner = appointment_raw()
        other_owner["appointment_county_evidence"]["roll_owner"] = "SOMEONE ELSE LLC"
        self.assertEqual(name_corrections(other_owner), [])
        different = appointment_raw(borrowers=["ACME HOLDINGS LLC"])
        self.assertEqual(name_corrections(different), [])                       # a different name is not a typo

    def test_backfill_writes_the_correction_once(self):
        row = {"id": "54984c5f", "bot_source": "shelby_substitute_trustee", "staging_status": "pending",
               "distress_type": "SUBSTITUTION_OF_TRUSTEE", "county": "Shelby", "full_name": "ENVLY LLC",
               "raw_payload": appointment_raw()}
        plan = run_plan([], appointments=[row])
        self.assertEqual(plan[0]["columns"], {})                                # full_name is not overwritten
        self.assertEqual(plan[0]["raw_patch"]["name_corrections"][0]["corrected"], "ENVOY LLC")
        write([row], plan)
        self.assertEqual(row["full_name"], "ENVLY LLC")
        self.assertEqual(run_plan([], appointments=[row]), [])

    def test_card_names_both_spellings(self):
        row = {"id": "a", "property_address": "4659 STAGE RD, MEMPHIS, TN 38128", "source_url": "https://example.test/a",
               "raw": appointment_raw()}
        card = appointment_card(row, {"status": "none"}, TODAY, ranked=True)
        self.assertIn("ENVLY LLC", card["decider"]["basis"])
        self.assertIn("typing error for ENVOY LLC", card["decider"]["basis"])
        self.assertEqual(card["owner_group_key"], "tn|shelby|E:ENVOY")


class PassedLinkedNotice(unittest.TestCase):
    def test_returns_a_removed_card_instead_of_raising(self):
        raw = appointment_raw(borrowers=["DOE JOHN A"])
        raw["recovery"]["loan_grantors"] = ["DOE JOHN A"]
        raw["appointment_county_evidence"]["roll_owner"] = "DOE JOHN A"
        row = {"id": "a", "property_address": "4659 STAGE RD, MEMPHIS, TN 38128", "source_url": "https://example.test/a",
               "raw": raw}
        link = {"status": "linked", "basis": "The notice forecloses the same deed of trust instrument", "notice_id": "FN1",
                "notice_ids": ["FN1"], "staging_id": "n1", "first_advertised": "2026-09-01",
                "appointment_recorded": "2026-08-20", "days_gained": 12, "sale_date": "2026-10-01"}
        card = appointment_card(row, link, TODAY, notice_carded=False, ranked=True)
        self.assertEqual(card["route"], "removed")
        self.assertIn("2026-10-01", card["route_reason"])
        self.assertIn("passed", card["route_reason"])
        self.assertEqual(card["event"]["status"], "resolved")
        # A sale still ahead is unchanged: verified, ranked and linked is a first-trace card.
        ahead = appointment_card(row, {**link, "sale_date": "2026-12-15"}, TODAY, notice_carded=False, ranked=True)
        self.assertEqual(ahead["route"], "first_trace_review")


if __name__ == "__main__":
    unittest.main()
