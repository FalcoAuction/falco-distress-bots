"""Probate lane review card: estate control of a specific, available parcel.
All names and parcels are invented. No network.

    python -m unittest tests.test_probate_pilot_review -v
"""

from __future__ import annotations

import copy
import os
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import _probate_control as control  # noqa: E402
from src.bots._pilot_review import dedupe_worklist, property_key  # noqa: E402
from src.bots.court_notices_bot import build_lead  # noqa: E402
from src.bots.probate_pilot_review_bot import ProbatePilotReviewBot  # noqa: E402
from tests.test_court_notices import DAVIDSON, STATUTE  # noqa: E402

TODAY = date(2026, 11, 22)
PARCEL = "116 08 0C 205.00"
ADDRESS = "100 SAMPLE PARK W, NASHVILLE, 37205"

CARD_HTML = """<html><body><table>
<tr><td>Map &amp; Parcel:</td><td>{parcel}</td></tr>
<tr><td>Current Owner:</td><td>{owner}</td></tr>
<tr><td>Mailing Address:</td><td>100 SAMPLE PARK W NASHVILLE TN 37205</td></tr>
<tr><td>Location:</td><td>100 SAMPLE PARK W NASHVILLE 37205</td></tr>
<tr><td>Most Recent Sale Date:</td><td>{sale}</td></tr>
<tr><td>Most Recent Sale Price:</td><td>$410,000</td></tr>
<tr><td>Deed Reference:</td><td>{deed}</td></tr>
<tr><td>Assessment Year:</td><td>2026</td></tr>
<tr><td>Total Appraisal Value:</td><td>$612,300</td></tr>
</table></body></html>"""


def estate(row_id="row-1", lines=DAVIDSON, owner="PERSON, EVE F", confidence="middle_initial",
           source_name="court_notices", parcel=PARCEL, day=20, account=501):
    lead = build_lead(source_name, "ledger", "https://example.test", date(2026, 11, day),
                      {"notice_id": "L" + row_id, "lines": lines})
    row = {"id": row_id, "bot_source": source_name, "distress_type": lead.distress_type, "county": lead.county,
           "full_name": lead.full_name, "property_address": ADDRESS if owner else None, "property_value": 612300,
           "source_url": lead.source_url, "staging_status": "pending", "raw_payload": copy.deepcopy(lead.raw_payload)}
    if owner:
        item = {"owner": owner, "parcel": parcel, "address": ADDRESS, "value": 612300, "extra": {"account_id": account}}
        row["raw_payload"]["owner_match"] = {"status": "matched", "confidence": confidence, "county": "davidson",
                                             "parcels": [item], "primary": item}
    return row


def roll(owner="PERSON, EVE F", sale="2019-06-14", deed="20190625-0061461", status="ok"):
    def fetch(county, parcel, today, decedent=""):
        if status is None:
            return None
        if status != "ok":
            return {"status": status, "note": "county source not wired"}
        return {"status": "ok", "owner": owner, "parcel": parcel["parcel"], "address": parcel["address"],
                "value": 612300.0, "value_basis": "Davidson assessor 2026 total appraisal", "last_sale_date": sale,
                "deed_reference": deed, "source_url": "https://portal.padctn.org/OFS/WP/Print/501",
                "checked_on": today.isoformat()}
    return fetch


def plan(rows, fetch=None, today=TODAY):
    return ProbatePilotReviewBot().plan(rows, today, roll_for=fetch or roll())


def write(rows, items):
    """Apply a plan the way the guarded update would."""
    by_id = {r["id"]: r for r in rows}
    for item in items:
        by_id[item["id"]]["raw_payload"] = item["raw_after"]


def only(items, row_id="row-1"):
    return next(i for i in items if i["id"] == row_id)["after"]


PETITION = ["IN THE PROBATE COURT OF SHELBY COUNTY, TENNESSEE", "PR-99589-II", "IN RE: ESTATE OF",
            "CARL MADEUP, DECEASED.", "ORDER OF PUBLICATION",
            "It appearing from the original Petition to Subject Real Property to the Payment of Estate Debts and "
            "Expenses and for Authority to Sell Real Property filed in the cause on October 20, 2026, that the "
            "identity of persons claiming an interest in the real property commonly known as 900 Example Road, "
            "Memphis, Tennessee 38116, are unknown.",
            "Any unknown heir must respond in the Probate Court for Shelby County, Tennessee, on or BEFORE "
            "November 30, 2026 at 9:00 a.m.", "This the 27th day of October, 2026.", "A. CLERK, CLERK"]


class CleanCandidateTests(unittest.TestCase):
    def test_letters_alone_make_an_estate_candidate_hold_not_a_recommendation(self):
        card = only(plan([estate()]))
        self.assertEqual(card["lane"], "probate")
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(card["route_reason"].startswith("Estate candidate: "))
        self.assertIn("still the sole owner of record", card["route_reason"])
        self.assertIn("$612,300", card["route_reason"])
        self.assertEqual(card["availability"]["verify_next"][0],
                         "Read the will/letters at the Davidson County probate clerk (docket 26P9001) for a power "
                         "of sale, or confirm all heirs/devisees")
        self.assertEqual(card["availability"]["conflicts"], [])
        self.assertEqual(card["decider"]["name"], "Fay Fictional")
        self.assertFalse(card["decider"]["verified"])
        self.assertIn("Letters alone do not give authority", card["decider"]["basis"])
        self.assertIn(control.AUTHORITY_UNKNOWN, card["economics"]["unknowns"])
        self.assertIn(control.LISTING_UNVERIFIED, card["availability"]["verify_next"])
        self.assertIn("power to sell", card["availability"]["first_call_question"])
        self.assertEqual(card["asset"]["parcel"], PARCEL)
        self.assertEqual(card["economics"]["value"], 612300.0)
        self.assertEqual(card["economics"]["known_debt"], [])
        self.assertEqual(card["owner_group_key"], "tn|davidson|P:PERSON E")
        # Counsel is recorded as its own route, never as the decider.
        self.assertTrue(any("Estate counsel as printed: Gus Allen Counsel Jr." in v
                            for v in card["availability"]["verify_next"]))

    def authority(self, kind):
        row = estate()
        row["raw_payload"][control.EVIDENCE_KEY] = {
            "recorded_on": "2026-11-22",
            "authority": {"kind": kind, "summary": "Article IV of the will", "date": "2026-11-21",
                          "source": "https://example.test/court-file"}}
        return only(plan([row]))

    def test_recorded_will_power_of_sale_clears_first_trace(self):
        card = self.authority("will_power_of_sale")
        self.assertEqual(card["route"], "first_trace_review")
        self.assertIn("evidenced by a record", card["route_reason"])
        self.assertIn("the will gives the personal representative power of sale", card["decider"]["basis"])
        self.assertEqual(card["decider"]["source"]["url"], "https://example.test/court-file")
        self.assertNotIn(control.AUTHORITY_UNKNOWN, card["economics"]["unknowns"])

    def test_sole_heir_record_clears_and_an_unknown_kind_does_not(self):
        self.assertEqual(self.authority("sole_heir_or_devisee")["route"], "first_trace_review")
        loose = self.authority("letters_issued")
        self.assertEqual(loose["route"], "research_hold")
        self.assertTrue(loose["route_reason"].startswith("Estate candidate: "))

    def test_court_order_on_the_parcel_clears_first_trace(self):
        row = estate()
        row["raw_payload"][control.EVIDENCE_KEY] = {
            "recorded_on": "2026-11-22",
            "order": {"date": "2026-11-19", "summary": "Order authorizes the sale of 100 Sample Park W.",
                      "source": "https://example.test/docket"}}
        card = only(plan([row]))
        self.assertEqual(card["route"], "first_trace_review")
        self.assertIn("a court order on this parcel", card["route_reason"])

    def test_no_em_dashes_in_card_text(self):
        import json
        for row in (estate(), estate(owner="PERSON, EVE F & PERSON, ADAM")):
            self.assertNotIn("—", json.dumps(only(plan([row])), ensure_ascii=False))


class HoldTests(unittest.TestCase):
    def test_surviving_joint_owner_is_held(self):
        for owner in ("PERSON, EVE F & PERSON, ADAM", "PERSON, EVE F. ET UX", "PERSON EVE F TRUSTEE",
                      "OTHER ANN ETAL WITH A RESERVED LIFE ESTATE FOR PERSON EVE F"):
            row = estate(owner=owner)
            card = only(plan([row], roll(owner=owner)))
            self.assertEqual(card["route"], "research_hold", owner)
            self.assertIn("outside probate", card["route_reason"])
            self.assertTrue(card["availability"]["conflicts"])
            self.assertIn("Register of Deeds", card["availability"]["verify_next"][0])

    def test_fiduciary_plainly_on_the_roll_is_a_surviving_co_owner_of_record(self):
        owner = "PERSON, EVE F & FICTIONAL, FAY"
        card = only(plan([estate(owner=owner)], roll(owner=owner)))
        self.assertEqual(card["route"], "first_trace_review")
        self.assertEqual(card["decider"]["name"], "Fay Fictional")
        self.assertEqual(card["decider"]["role"], "surviving co-owner of record")
        self.assertIn("not on the letters", card["decider"]["basis"])
        self.assertEqual(card["decider"]["source"]["url"], "https://portal.padctn.org/OFS/WP/Print/501")
        self.assertEqual(card["economics"]["unknowns"][0], control.CO_OWNERSHIP_UNKNOWN)
        self.assertEqual(card["owner_names"], [owner])
        self.assertEqual(card["availability"]["conflicts"], [])

    def test_truncated_or_staged_only_co_owner_name_stays_held(self):
        # "FICTIONAL, FA" does not plainly name Fay Fictional.
        cut = "PERSON, EVE F & FICTIONAL, FA"
        card = only(plan([estate(owner=cut)], roll(owner=cut)))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("surname of the named fiduciary", card["route_reason"])
        # Named on the staged capture only: the roll could not be read today.
        owner = "PERSON, EVE F & FICTIONAL, FAY"
        staged = only(plan([estate(owner=owner)], roll(status="unavailable")))
        self.assertEqual(staged["route"], "research_hold")
        self.assertIn("staged capture", staged["route_reason"])

    def test_identity_conflict_hold_is_preserved_without_a_lookup(self):
        lines = ["ESTATE OF Kay-Joong Sample, (Deceased)", "Case No. MC-CH-CV-PB-26-999",
                 "Notice to creditors. Letters Testamentary in respect of the Estate of Kay-Joon Sample, "
                 "who died 7/7/2026, were issued by the Probate Court of Montgomery County, Tennessee.",
                 "This 10th day of November, 2026", "Jen Sample Park - Executor",
                 "Attorney for the Estate:", "B. Example Hunt"]
        calls = []

        def fetch(*args, **kwargs):
            calls.append(args)
            return None

        card = only(plan([estate(lines=lines)], fetch))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("two different decedent names", card["route_reason"])
        self.assertEqual(calls, [])

    def test_unclear_signer_role_is_held(self):
        lines = DAVIDSON[:DAVIDSON.index("Fay Fictional")] + ["JANE Q. CLERKPERSON", "Probate Court Clerk"]
        card = only(plan([estate(lines=lines)]))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("No fiduciary is named", card["route_reason"])
        self.assertIsNone(card["decider"]["name"])

    def test_name_only_tie_is_held(self):
        lines = [line.replace("Eve Fictional Person", "Eve Person") for line in DAVIDSON]
        card = only(plan([estate(lines=lines, owner="PERSON, EVE", confidence="name_only")], roll(owner="PERSON, EVE")))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("first and last name only", card["route_reason"])

    def test_roll_owner_with_another_middle_name_is_a_bad_match_not_a_transfer(self):
        card = only(plan([estate(owner="PERSON, EVE D.")], roll(owner="PERSON, EVE D.")))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("does not name the decedent", card["route_reason"])

    def test_county_without_a_free_recheck_is_held_with_the_gap_named(self):
        card = only(plan([estate()], roll(status="unavailable")))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("could not be rechecked today", card["route_reason"])

    def test_sale_recorded_after_death_with_the_decedent_still_named_is_held(self):
        card = only(plan([estate()], roll(sale="2026-11-02")))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("after the date of death", card["route_reason"])


class CourtEvidenceTests(unittest.TestCase):
    def stevens(self, listing_status="Active"):
        lines = ["NOTICE TO CREDITORS", "Docket: PR099053", "In Re the Matter of: Rex Lee Sample II",
                 "Notice is hereby given that on the 15th day of October, 2026, Letters of Administration C.T.A. in "
                 "respect of Rex Lee Sample II who died Sept. 19, 2026, were issued to the undersigned by the "
                 "Probate Court of Shelby County, Tennessee.", *STATUTE, "This 15th day of October, 2026.",
                 "Stacy Elizabeth Sample Administrator C.T.A.", "Attorney for the Estate:", "Matt Example Rhoads",
                 "Oct. 20, 27, 2026 Cd099194"]
        row = estate(lines=lines, owner=None)
        row["raw_payload"]["court_notice"]["representatives"] = [
            {"name": "Stacy Elizabeth Sample", "role": "administrator", "address": []}]
        row["raw_payload"][control.EVIDENCE_KEY] = {
            "recorded_on": "2026-11-22", "docket": "PR099053",
            "parcel": {"parcel_id": "067102 A00068", "address": "5000 Sample Road, Apt 6, Memphis, TN 38119",
                       "owner": "SAMPLE REX L II", "value": 99300,
                       "value_basis": "Shelby 2026 county tax assessment divided by the 25 percent residential "
                                      "ratio (estimate)",
                       "source_url": "https://apps2.shelbycountytrustee.com/Parcel?parcel=067102+A00068",
                       "checked_on": "2026-11-22", "tie": "court_record"},
            "order": {"date": "2026-11-19", "summary": "Order authorizes the administrator to list the unit."},
            "listing": {"brokerage": "Example Realty", "mls": 10232902, "asking_usd": 99000,
                        "status": listing_status, "recorded_on": "2026-11-22"}}
        return row

    def test_broker_listed_estate_stays_held_on_every_run(self):
        rows = [self.stevens()]
        first = plan(rows)
        card = only(first)
        self.assertEqual(card["route"], "research_hold")
        self.assertEqual(card["availability"]["conflicts"], ["Broker listing: Example Realty MLS 10232902 at $99,000"])
        self.assertIn("Court order entered 2026-11-19", card["event"]["what"])
        self.assertTrue(card["economics"]["value_is_estimate"])
        write(rows, first)
        later = plan(rows, today=TODAY + timedelta(days=8))
        self.assertEqual(only(later)["route"], "research_hold")

    def test_sold_listing_is_removed(self):
        self.assertEqual(only(plan([self.stevens("Sold")]))["route"], "removed")

    def test_recorded_county_check_goes_stale(self):
        row = self.stevens()
        del row["raw_payload"][control.EVIDENCE_KEY]["listing"]
        bot = ProbatePilotReviewBot()
        fresh = only(bot.plan([copy.deepcopy(row)], TODAY))
        self.assertEqual(fresh["route"], "first_trace_review")   # court order on this parcel, no listing
        stale = only(bot.plan([copy.deepcopy(row)], TODAY + timedelta(days=45)))
        self.assertEqual(stale["route"], "research_hold")
        self.assertIn("more than 30 days old", stale["route_reason"])

    def test_pending_petition_records_the_next_check_not_a_conclusion(self):
        row = estate(lines=PETITION, owner=None)
        self.assertEqual(row["distress_type"], "COURT_SALE")
        card = only(plan([row]))
        self.assertEqual(card["route"], "research_hold")
        self.assertEqual(card["economics"]["deadline"], "2026-11-30")
        self.assertEqual(card["economics"]["runway_days"], 8)
        check = card["availability"]["verify_next"][0]
        self.assertIn("On or after 2026-11-30, check docket PR-99589-II", check)
        self.assertIn("case_id=PR099589", check)
        self.assertIn("No order granting the sale", card["availability"]["evidence"])

    def test_petition_past_its_response_date_asks_for_the_docket_check(self):
        card = only(plan([estate(lines=PETITION, owner=None)], today=date(2026, 12, 3)))
        self.assertEqual(card["route"], "research_hold")
        self.assertIsNone(card["economics"]["deadline"])
        self.assertIn("Response date 2026-11-30 has passed: check docket", card["availability"]["verify_next"][0])

    def test_ordinal_response_dates_parse(self):
        self.assertEqual(control.petition_deadline("respond on or before December 1st, 2026 at 9:00 a.m."),
                         date(2026, 12, 1))


class TransferAndRepeatTests(unittest.TestCase):
    def test_parcel_out_of_the_decedents_name_is_removed(self):
        card = only(plan([estate()], roll(owner="BUYER, NEW Q", sale="2026-11-10", deed="20261112-0000001")))
        self.assertEqual(card["route"], "removed")
        self.assertIn("BUYER, NEW Q", card["route_reason"])
        self.assertEqual(card["event"]["status"], "resolved")
        self.assertIn("20261112-0000001", card["availability"]["evidence"])

    def test_repeat_run_writes_nothing_then_a_later_transfer_flips_the_card(self):
        rows = [estate()]
        first = plan(rows)
        self.assertTrue(only(first)["route_reason"].startswith("Estate candidate: "))
        write(rows, first)
        self.assertEqual(plan(rows), [])
        self.assertEqual(plan(rows, today=TODAY + timedelta(days=3)), [])
        later = plan(rows, roll(owner="BUYER, NEW Q", sale="2026-11-25"), today=TODAY + timedelta(days=7))
        self.assertEqual(only(later)["route"], "removed")

    def test_new_evidence_is_reviewed_without_waiting_for_the_weekly_recheck(self):
        rows = [estate()]
        write(rows, plan(rows))
        rows[0]["raw_payload"][control.EVIDENCE_KEY] = {
            "recorded_on": "2026-11-23", "listing": {"brokerage": "Example Realty", "status": "Active"}}
        card = only(plan(rows, today=TODAY + timedelta(days=1)))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("Already listed", card["route_reason"])

    def test_a_rule_change_re_reviews_cards_written_under_the_old_rule(self):
        rows = [estate()]
        write(rows, plan(rows))
        rows[0]["raw_payload"][control.CHECK_KEY]["rules"] = "older-rule"
        self.assertEqual(len(plan(rows)), 1)

    def test_failed_county_fetch_leaves_the_existing_card(self):
        rows = [estate()]
        write(rows, plan(rows))
        self.assertEqual(plan(rows, roll(status=None), today=TODAY + timedelta(days=8)), [])

    def test_manual_card_and_decided_rows_are_not_touched(self):
        manual = estate()
        manual["raw_payload"]["pilot_review"] = {"manual": True, "route": "research_hold", "reviewed_by": "patrick"}
        decided = estate(row_id="row-2", lines=[x.replace("Eve Fictional Person", "Ann Other Sample") for x in DAVIDSON],
                         owner="SAMPLE, ANN O")
        decided["staging_status"] = "rejected"
        self.assertEqual(plan([manual, decided]), [])

    def test_another_lanes_stronger_card_is_kept(self):
        row = estate(owner="PERSON, EVE F & PERSON, ADAM")
        other = {"route": "removed", "reviewed_by": "tax_lane", "lane": "tax"}
        row["raw_payload"]["pilot_review"] = other
        item = plan([row], roll(owner="PERSON, EVE F & PERSON, ADAM"))[0]
        self.assertIs(item["after"], other)
        self.assertTrue(item["kept_existing"])


class DuplicateNoticeTests(unittest.TestCase):
    def test_repeat_publications_collapse_onto_one_primary(self):
        primary = estate(row_id="row-1", day=20)
        earlier = estate(row_id="row-0", day=13)
        excerpt = {"id": "row-x", "bot_source": "tn_public_notice", "distress_type": "PROBATE",
                   "county": "Davidson", "full_name": "Eve Fictional Person", "property_address": ADDRESS,
                   "source_url": "https://example.test/x", "staging_status": "pending",
                   "raw_payload": {"excerpt": "Estate of Eve Fictional Person, Deceased",
                                   "owner_match": copy.deepcopy(primary["raw_payload"]["owner_match"])}}
        rows = [earlier, excerpt, primary]
        items = plan(rows)
        self.assertTrue(only(items, "row-1")["route_reason"].startswith("Estate candidate: "))
        for dup in ("row-0", "row-x"):
            card = only(items, dup)
            self.assertEqual(card["route"], "removed")
            self.assertIn("review is on staging row row-1", card["route_reason"])
            self.assertEqual(card["property_key"], only(items, "row-1")["property_key"])
        self.assertEqual(len(dedupe_worklist(i["after"] for i in items)), 0)   # a hold is not on the worklist
        write(rows, items)
        self.assertEqual(plan(rows), [])

    def test_same_name_with_two_dockets_is_two_estates(self):
        a = estate(row_id="row-1")
        b = estate(row_id="row-2", lines=[x.replace("26P9001", "26P9444") for x in DAVIDSON], parcel="117 03 0A 089.00")
        items = plan([a, b])
        self.assertEqual({i["role"] for i in items}, {"primary"})
        self.assertEqual(len(items), 2)

    def test_docket_formats_for_one_case_compare_equal(self):
        from src.bots.probate_pilot_review_bot import _docket
        rows = [{"raw_payload": {"court_notice": {"docket": d, "county": "montgomery", "decedent": "A B"}}}
                for d in ("MC-CH-CV-PB-26-428", "MCCHCVPB260000428")]
        self.assertEqual(_docket(rows[0]), _docket(rows[1]))


class ProfessionalRouteTests(unittest.TestCase):
    def test_bank_fiduciary_is_a_professional_relationship(self):
        lines = [x.replace("Fay Fictional", "Example Bank") for x in DAVIDSON]
        card = only(plan([estate(lines=lines)]))
        self.assertEqual(card["route"], "professional_relationship")
        self.assertEqual(card["decider"]["name"], "Example Bank")
        self.assertEqual(card["decider"]["role"], "corporate fiduciary")

    def test_attorney_fiduciary_with_several_estates_is_one_relationship(self):
        def lines(name, docket):
            out = [x.replace("Eve Fictional Person", name).replace("26P9001", docket) for x in DAVIDSON]
            return [x.replace("Fay Fictional", "Gus Allen Counsel Jr.") for x in out]
        a = estate(row_id="row-1", lines=lines("Eve Fictional Person", "26P9001"))
        b = estate(row_id="row-2", lines=lines("Ann Other Sample", "26P9002"), owner="SAMPLE, ANN O",
                   parcel="117 03 0A 089.00")
        items = plan([a, b], lambda county, parcel, today, decedent="": roll(
            owner="PERSON, EVE F" if parcel["parcel"] == PARCEL else "SAMPLE, ANN O")(county, parcel, today))
        cards = [only(items, "row-1"), only(items, "row-2")]
        self.assertEqual({c["route"] for c in cards}, {"professional_relationship"})
        self.assertEqual(cards[0]["decider"]["role"], "estate counsel")
        self.assertEqual(cards[0]["owner_group_key"], cards[1]["owner_group_key"])
        self.assertIn("26P9001 (Eve Fictional Person); 26P9002 (Ann Other Sample)", cards[0]["availability"]["evidence"])

    def test_counsel_grouping_ignores_hyphens_and_office_lines(self):
        self.assertEqual(control.professional_key("Noah Allen-Darden"), control.professional_key("Noah Allen Darden"))
        self.assertFalse(control._looks_like_counsel("One Belle Meade Place"))
        self.assertTrue(control._looks_like_counsel("Fidelis Law PLLC"))


class CountyRecordTests(unittest.TestCase):
    def test_assessor_record_card_fields(self):
        card = control.parse_padctn_card(CARD_HTML.format(parcel=PARCEL, owner="PERSON, EVE F", sale="6/14/2019",
                                                          deed="20190625-0061461"))
        self.assertEqual(card["owner"], "PERSON, EVE F")
        self.assertEqual(card["parcel"], PARCEL)
        self.assertEqual(card["last_sale_date"], "2019-06-14")
        self.assertEqual(card["deed_reference"], "20190625-0061461")
        self.assertEqual(card["appraisal"], 612300.0)

    def test_davidson_roll_comes_from_the_record_card_and_a_miss_is_not_a_hold(self):
        bot = ProbatePilotReviewBot()

        class Page:
            status_code = 200
            text = CARD_HTML.format(parcel=PARCEL, owner="PERSON, EVE F", sale="6/14/2019", deed="20190625-0061461")

        bot.fetch = lambda url, **kwargs: Page()
        parcel = {"parcel": PARCEL, "address": ADDRESS, "account_id": 501, "value": 1}
        got = bot.roll_for("davidson", parcel, TODAY)
        self.assertEqual((got["status"], got["owner"], got["value"]), ("ok", "PERSON, EVE F", 612300.0))
        self.assertEqual(got["source_url"], "https://portal.padctn.org/OFS/WP/Print/501")
        bot.fetch = lambda url, **kwargs: None
        self.assertIsNone(bot.roll_for("davidson", parcel, TODAY))
        self.assertEqual(bot.roll_for("sumner", parcel, TODAY)["status"], "unavailable")

    def test_comptroller_parcel_and_owner_are_kept_as_sourced_for_cross_lane_keys(self):
        row = estate(owner=None)
        row["county"] = "Robertson"
        row["raw_payload"]["court_notice"]["county"] = "robertson"
        row["raw_payload"]["comptroller_parcel"] = {
            "method": "owner_name", "parcelid": "074 058    12100 000 2027", "address": "EXAMPLE RD 4501",
            "owner": "SAMPLE NAOMI P ETAL & PERSON EVE F", "appraisal": 360700,
            "parcels": [{"parcelid": "074 058    12100 000 2027", "address": "EXAMPLE RD 4501",
                         "owner": "SAMPLE NAOMI P ETAL & PERSON EVE F", "appraisal": 360700,
                         "last_sale_date": "20170815"}]}
        row["raw_payload"]["padctn_owner_match"] = {"parcel": "058    12100 000", "property_address": "EXAMPLE RD 4501",
                                                    "owner": "PERSON EVE"}
        card = only(plan([row], roll(status="unavailable")))
        self.assertEqual(card["route"], "research_hold")
        self.assertEqual(card["asset"]["parcel"], "074 058    12100 000 2027")
        self.assertEqual(card["property_key"], property_key("robertson", "074 058    12100 000"))
        self.assertEqual(card["owner_group_key"], "tn|robertson|P:PERSON E+P:SAMPLE N")


if __name__ == "__main__":
    unittest.main()
