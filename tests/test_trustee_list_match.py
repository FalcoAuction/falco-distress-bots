"""Memphis notices checked against their own trustee's sale list: confirmed
dates, postponements, false positives, duplicates and repeat runs. No network."""

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
from src.bots import rubin_lublin_list  # noqa: E402
from src.bots._preforeclosure_review import EVIDENCE_KEY, TRACK_KEY, sale_card, sale_state  # noqa: E402
from src.bots._trustee_list_match import (LIST_KEY, all_staged_books, check_notice, check_notices, effective_date,  # noqa: E402
                                          firm_source, page_book, split_address, street_identity)
from src.bots.preforeclosure_review_bot import PreforeclosureReviewBot  # noqa: E402

TODAY = date(2026, 11, 23)
NOW = datetime(2026, 11, 23, 18, 0, tzinfo=timezone.utc)
READ = "2026-11-23T06:27:50+00:00"
REPORT = "https://mwzmlaw.com/wp-content/uploads/2026/11/TN-Sale-Report-as-of-11.20.2026.pdf"
BILL = {"status": "verified_assessment", "owner": "JAFFE AARON", "parcel_id": "080008 C00094",
        "estimated_appraisal": 439000, "assessed_value": 109750, "assessment_ratio": 0.25, "tax_year": 2026,
        "checked_on": "2026-11-22", "source_url": "https://apps2.shelbycountytrustee.com/Parcel?parcel=080008+C00094"}


def notice(**over):
    base = {"id": "n1", "bot_source": "memphis_daily_news", "county": "shelby", "notice_id": "FD117227",
            "firm": "Mackie Wolf Zientz & Mann, PC", "property_address": "6845 Corsica Dr, Memphis, TN 38120",
            "trustee_sale_date": "2026-12-04", "publication_date": "2026-10-20", "full_name": "Aaron Jaffe",
            "owner_name_records": "Aaron Jaffe", "loan_instrument": "12095975", "staging_status": "pending",
            "source_url": "https://www.memphisdailynews.com/Search/Details/ViewNotice.aspx?id=FD117227&date=10/20/2026",
            "property_value": None, "property_value_source": None, "trustee_review": dict(BILL), "ranked": True,
            "notice_tracking": {"last_seen_at": READ, "last_seen_by": "memphis_daily_news"}}
    return {**base, **over}


def listing(**over):
    base = {"id": "m1", "bot_source": "mackie_wolf_trustee", "county": "Shelby", "full_name": None,
            "property_address": "6845 Corsica Drive, Memphis, TN 38120", "trustee_sale_date": "2026-12-04",
            "source_url": REPORT, "staging_status": "pending", "owner_name_records": None, "property_value": None,
            "property_value_source": None,
            "notice_tracking": {"last_seen_at": READ, "last_seen_by": "mackie_wolf_trustee", "report_url": REPORT,
                                "case_number": "26-000111-505"}}
    return {**base, **over}


def books(*rows):
    return all_staged_books(list(rows), NOW)


def check(n, *rows):
    return check_notice(n, books(*rows), TODAY.isoformat())


def card_for(n, evidence):
    row = {**n, "list_check": evidence, "trustee_sale_date": effective_date(n, evidence, TODAY.isoformat())}
    state = sale_state(row, None, NOW, TODAY)
    return state, sale_card(row, state, TODAY)


class IdentityTests(unittest.TestCase):
    def test_suffix_direction_and_unit_spellings(self):
        self.assertEqual(street_identity("6845 Corsica Drive"), street_identity("6845 Corsica Dr"))
        self.assertEqual(street_identity("980 North Parkway"), street_identity("980 N Pkwy"))
        self.assertEqual(street_identity("1224 Collierville-Arlington Road"), street_identity("1224 Collierville-Arlington Rd"))
        self.assertNotEqual(street_identity("4883 Saddlehorn Dr"), street_identity("4883 Saddlehorn Cv"))
        self.assertNotEqual(street_identity("1814 Crossflower Cv Unit 99"), street_identity("1814 Crossflower Cv"))
        self.assertEqual(street_identity("1814 Crossflower Cv #99")["unit"], "99")
        self.assertIsNone(street_identity("Lot 4 Smith Subdivision"))

    def test_listing_printed_without_a_comma_before_the_city(self):
        got = split_address("761 Woodland Levee Way Cordova, TN 38018")
        self.assertEqual((got["identity"], got["zip"]), (street_identity("761 Woodland Levee Way"), "38018"))
        self.assertIsNone(split_address("12 Oak St Somewhereelse, TN 38018")["identity"])     # city not separable

    def test_only_the_trustee_named_in_the_notice_is_consulted(self):
        self.assertEqual(firm_source("Mackie Wolf Zientz and Mann, PC"), "mackie_wolf_trustee")
        self.assertEqual(firm_source("Brock & Scott PLLC"), "brock_scott_trustee")
        self.assertEqual(firm_source("Rubin Lublin TN, PLLC"), "rubin_lublin_list")
        self.assertIsNone(firm_source("M. Wayne Mink Jr"))
        # The same street on another firm's list says nothing about this notice.
        self.assertIsNone(check(notice(firm="M. Wayne Mink Jr"), listing()))
        self.assertEqual(check(notice(firm="Brock & Scott PLLC"), listing(),
                               listing(id="b1", bot_source="brock_scott_trustee",
                                       property_address="1 Other St Memphis, TN 38120",
                                       notice_tracking={"last_seen_at": READ, "last_seen_by": "brock_scott_trustee"}))["status"],
                         "not_listed")


class ConfirmationTests(unittest.TestCase):
    def test_confirmed_date_reaches_first_trace_with_all_five_answers(self):
        evidence = check(notice(), listing())
        self.assertEqual((evidence["status"], evidence["date"], evidence["file"]), ("confirmed", "2026-12-04", "26-000111-505"))
        self.assertEqual((evidence["notice_date"], evidence["checked_on"], evidence["list_url"]), ("2026-12-04", "2026-11-23", REPORT))
        state, card = card_for(notice(), evidence)
        self.assertEqual(state["status"], "listed")
        self.assertEqual((card["route"], card["event"]["status"], card["economics"]["deadline"]),
                         ("first_trace_review", "active", "2026-12-04"))
        self.assertEqual(card["event"]["source"]["url"], REPORT)
        self.assertIn("file 26-000111-505", card["event"]["source"]["label"])
        self.assertIn("notice FD117227 advertised 2026-12-04", card["event"]["what"])      # both sources kept
        self.assertEqual(card["asset"]["parcel"], "080008 C00094")
        self.assertTrue(card["decider"]["verified"])

    def test_confirmed_date_alone_does_not_clear_the_other_answers(self):
        evidence = check(notice(), listing())
        _, no_owner = card_for(notice(trustee_review=None), evidence)
        self.assertEqual(no_owner["route"], "research_hold")
        self.assertIn("No county ownership record", no_owner["route_reason"])
        # Same borrower, but the county bill for the street is on another parcel than the notice.
        _, conflict = card_for(notice(trustee_review={"status": "notice_parcel_conflict", "checked_on": "2026-11-23"}), evidence)
        self.assertEqual(conflict["route"], "research_hold")
        _, other_owner = card_for(notice(trustee_review={**BILL, "owner": "NEW OWNER HOLDINGS LLC"}), evidence)
        self.assertEqual(other_owner["route"], "research_hold")
        _, unranked = card_for(notice(ranked=False), evidence)
        self.assertEqual(unranked["route"], "research_hold")

    def test_without_a_list_the_newspaper_date_is_still_held(self):
        state, card = card_for(notice(firm="M. Wayne Mink Jr"), None)
        self.assertEqual(state["status"], "publisher_only")
        self.assertEqual(card["route_reason"], "The sale date rests on the newspaper notice alone. "
                                               "No trustee calendar confirms it is still set")

    def test_same_street_different_unit_is_not_a_match(self):
        evidence = check(notice(), listing(property_address="6845 Corsica Drive Unit 2, Memphis, TN 38120"))
        self.assertEqual(evidence["status"], "identity_hold")
        state, card = card_for(notice(), evidence)
        self.assertEqual((state["status"], card["route"]), ("publisher_only", "research_hold"))
        self.assertIn("not the exact street, suffix and unit", card["route_reason"])

    def test_same_street_name_different_suffix_or_zip_is_not_a_match(self):
        self.assertEqual(check(notice(), listing(property_address="6845 Corsica Cove, Memphis, TN 38120"))["status"],
                         "identity_hold")
        self.assertEqual(check(notice(), listing(property_address="6845 Corsica Drive, Memphis, TN 38119"))["status"],
                         "not_listed")
        self.assertEqual(check(notice(), listing(property_address="6854 Corsica Drive, Memphis, TN 38120"))["status"],
                         "not_listed")

    def test_same_borrower_at_another_property_is_not_confirmed(self):
        other = notice(id="n2", notice_id="FD117999", property_address="100 Jaffe Rental Rd, Memphis, TN 38120")
        got = check_notices([notice(), other], books(listing()), TODAY.isoformat())
        self.assertEqual((got["n1"]["status"], got["n2"]["status"]), ("confirmed", "not_listed"))
        _, card = card_for(other, got["n2"])
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("does not carry this street", card["route_reason"])

    def test_two_listings_with_different_dates_hold(self):
        evidence = check(notice(), listing(), listing(id="m2", trustee_sale_date="2027-01-15"))
        self.assertEqual(evidence["status"], "identity_hold")
        self.assertIn("more than once", evidence["reason"])

    def test_other_counties_are_never_matched(self):
        self.assertEqual(check(notice(), listing(county="Knox"))["status"], "not_listed")


class DateChangeTests(unittest.TestCase):
    def test_postponement_updates_the_date_and_keeps_both_sources(self):
        evidence = check(notice(), listing(trustee_sale_date="2027-01-15"))
        self.assertEqual((evidence["status"], evidence["date"], evidence["prior_date"], evidence["notice_date"]),
                         ("postponed", "2027-01-15", "2026-12-04", "2026-12-04"))
        state, card = card_for(notice(), evidence)
        self.assertEqual((state["status"], state["dates"]), ("postponed", ["2026-12-04", "2027-01-15"]))
        self.assertEqual((card["route"], card["event"]["status"], card["economics"]["deadline"]),
                         ("first_trace_review", "postponed", "2027-01-15"))
        self.assertIn("postponed from 2026-12-04 to 2027-01-15", card["event"]["what"])
        self.assertIn("notice FD117227 advertised 2026-12-04", card["event"]["what"])
        # Once the record carries the new date the earlier date is still remembered.
        moved = notice(trustee_sale_date="2027-01-15", list_check=evidence)
        again = check(moved, listing(trustee_sale_date="2027-01-15"))
        self.assertEqual((again["status"], again["prior_date"]), ("postponed", "2026-12-04"))

    def test_until_the_record_is_updated_a_later_list_date_does_not_confirm_the_old_one(self):
        evidence = check(notice(), listing(trustee_sale_date="2027-01-15"))
        row = {**notice(), "list_check": evidence}                 # date override not applied
        state = sale_state(row, None, NOW, TODAY)
        self.assertEqual(state["status"], "publisher_only")
        self.assertIn("2027-01-15", state["list_note"])

    def test_passed_notice_returns_only_when_the_list_shows_a_future_date(self):
        old = notice(trustee_sale_date="2026-09-17")
        self.assertEqual(check_notices([old], books(listing(trustee_sale_date="2026-09-17",
                                                            notice_tracking={"last_seen_at": "2026-09-16T06:00:00+00:00",
                                                                             "last_seen_by": "mackie_wolf_trustee"}),
                                                    listing(id="m9", property_address="1 Else St, Memphis, TN 38120")),
                                       TODAY.isoformat()), {})
        got = check_notices([old], books(listing(trustee_sale_date="2027-01-07")), TODAY.isoformat())
        self.assertEqual((got["n1"]["status"], got["n1"]["prior_date"], got["n1"]["date"]),
                         ("postponed", "2026-09-17", "2027-01-07"))
        state, card = card_for(old, got["n1"])
        self.assertEqual((state["status"], card["economics"]["deadline"]), ("postponed", "2027-01-07"))

    def test_dropped_from_the_list_is_a_withdrawal_hold(self):
        stale = listing(notice_tracking={"last_seen_at": "2026-11-12T19:51:25+00:00", "last_seen_by": "mackie_wolf_trustee"})
        fresh = listing(id="m2", property_address="1 Else St, Memphis, TN 38120")
        evidence = check(notice(), stale, fresh)
        self.assertEqual((evidence["status"], evidence["last_listed_on"]), ("dropped", "2026-11-12"))
        state, card = card_for(notice(), evidence)
        self.assertEqual((state["status"], card["route"], card["event"]["status"]), ("withdrawn", "research_hold", "unknown"))
        self.assertIn("No longer on", card["route_reason"])

    def test_earlier_or_passed_list_date_is_a_conflict_hold(self):
        earlier = check(notice(), listing(trustee_sale_date="2026-11-30"))
        self.assertEqual(earlier["status"], "date_conflict")
        state, card = card_for(notice(), earlier)
        self.assertEqual((state["status"], card["route"]), ("publisher_only", "research_hold"))
        self.assertIn("earlier than the 2026-12-04 in the notice", card["route_reason"])
        self.assertEqual(check(notice(), listing(trustee_sale_date="2026-11-20"))["status"], "date_conflict")

    def test_manual_status_is_never_rechecked_or_moved(self):
        sold = notice(sale_status={"status": "sold"})
        self.assertEqual(check_notices([sold], books(listing(trustee_sale_date="2027-01-15")), TODAY.isoformat()), {})
        evidence = check(notice(), listing(trustee_sale_date="2027-01-15"))
        self.assertEqual(effective_date(sold, evidence, TODAY.isoformat()), "2026-12-04")


class SourceHealthTests(unittest.TestCase):
    def test_a_list_not_read_recently_confirms_and_withdraws_nothing(self):
        old_read = {"last_seen_at": "2026-11-18T06:00:00+00:00", "last_seen_by": "mackie_wolf_trustee"}
        self.assertIsNone(books(listing(notice_tracking=old_read))["mackie_wolf_trustee"])
        self.assertIsNone(check(notice(), listing(notice_tracking=old_read)))
        prior = check(notice(), listing())
        kept = check(notice(list_check=prior), listing(notice_tracking=old_read))
        self.assertEqual(kept, prior)                               # unchanged, no new write

    def test_old_evidence_stops_counting(self):
        prior = {**check(notice(), listing()), "checked_on": "2026-11-18"}
        state = sale_state({**notice(), "list_check": prior}, None, NOW, TODAY)
        self.assertEqual(state["status"], "publisher_only")

    def test_page_list_that_no_longer_carries_a_confirmed_street(self):
        rows = [{"date": "2026-12-10", "file": "25-02607", "street": "7916 Country Lake Dr", "city": "Bartlett", "zip": "38133"}]
        n = notice(firm="Rubin Lublin TN, PLLC", property_address="7916 Country Lake Dr, Bartlett, TN 38133",
                   trustee_sale_date="2026-12-10")
        book = {"rubin_lublin_list": page_book("rubin_lublin_list", rows, "2026-11-23", rubin_lublin_list.URL)}
        first = check_notice(n, book, "2026-11-23")
        self.assertEqual((first["status"], first["file"]), ("confirmed", "25-02607"))
        empty = {"rubin_lublin_list": page_book("rubin_lublin_list", [], "2026-11-24", rubin_lublin_list.URL)}
        gone = check_notice({**n, "list_check": first}, empty, "2026-11-24")
        self.assertEqual((gone["status"], gone["date"], gone["last_listed_on"]), ("dropped", "2026-12-10", "2026-11-23"))
        never = check_notice(n, empty, "2026-11-24")
        self.assertEqual(never["status"], "not_listed")             # absent from the start is not a withdrawal


class DuplicateTests(unittest.TestCase):
    def test_republished_notice_only_the_newest_record_is_confirmed(self):
        first = notice(id="n0", notice_id="FD116000", publication_date="2026-09-01", trustee_sale_date="2026-10-01")
        got = check_notices([first, notice()], books(listing()), TODAY.isoformat())
        self.assertEqual(sorted(got), ["n1"])
        # Order of the rows does not matter.
        self.assertEqual(sorted(check_notices([notice(), first], books(listing()), TODAY.isoformat())), ["n1"])

    def test_same_listing_seen_in_two_weekly_reports_is_one_listing(self):
        older = listing(id="m0", notice_tracking={"last_seen_at": "2026-11-12T19:51:25+00:00",
                                                  "last_seen_by": "mackie_wolf_trustee", "case_number": "26-000111-505"})
        self.assertEqual(check(notice(), older, listing())["status"], "confirmed")


RL_PAGE = """<html><body><table class="property-listing"><tr><th>Sale Date</th><th>File #</th><th>Property</th>
<th>City</th><th>Zip</th><th>County</th><th>Bid</th></tr>
<tr><td class="date">11/23/2026 (9am - 7pm) </td><td>26-00346 </td><td>5123 Avis Ln </td><td>Knoxville </td><td>37914 </td><td>Knox </td><td>Go to Auction.com </td></tr>
<tr><td class="date">01/19/2027 (9am - 7pm) </td><td>22-01494 </td><td>7331 Woodshire Road </td><td>Memphis </td><td>38125 </td><td>Shelby </td><td>Go to Xome.com </td></tr>
</table></body></html>"""


class RubinLublinTests(unittest.TestCase):
    def test_parse_keeps_shelby_rows(self):
        self.assertEqual(rubin_lublin_list.parse(RL_PAGE), [{"date": "2027-01-19", "file": "22-01494",
                                                              "street": "7331 Woodshire Road", "city": "Memphis", "zip": "38125"}])

    def test_changed_page_raises_instead_of_reading_as_empty(self):
        with self.assertRaises(ValueError):
            rubin_lublin_list.parse("<html><body>Please agree to our terms</body></html>")
        with self.assertRaises(ValueError):
            rubin_lublin_list.parse(RL_PAGE.replace("<th>Bid</th>", "<th>Status</th>"))
        with self.assertRaises(ValueError):
            rubin_lublin_list.parse(RL_PAGE.replace("38125", ""))

    def test_read_is_off_unless_enabled(self):
        class Session:
            def get(self, *a, **k):
                raise AssertionError("no request may be made while the read is off")
        with patch.dict(os.environ, {rubin_lublin_list.ENABLE_ENV: ""}):
            self.assertIsNone(rubin_lublin_list.load(Session()))


class Query:
    """Just enough of the Supabase query builder for _redate."""

    def __init__(self, client, update=None):
        self.client, self.update_values, self.guards = client, update, {}

    def select(self, *_):
        return self

    def update(self, values):
        return Query(self.client, values)

    def eq(self, key, value):
        self.guards[key] = value
        return self

    def filter(self, key, op, value):
        self.guards[key] = value
        return self

    def is_(self, key, value):
        self.guards[key] = None
        return self

    def limit(self, _):
        return self

    def execute(self):
        row = next((r for r in self.client.notices if r["id"] == self.guards.get("id")), None)
        if self.update_values is None:
            data = [{"id": row["id"], "pipeline_lead_key": "k1", "phone_metadata": copy.deepcopy(row["phone_metadata"])}] if row else []
            return type("R", (), {"data": data})()
        self.client.updates.append((self.update_values, dict(self.guards)))
        if row is None or row["trustee_sale_date"] != self.guards["trustee_sale_date"]:
            return type("R", (), {"data": []})()
        row["trustee_sale_date"] = self.update_values["trustee_sale_date"]
        row["phone_metadata"] = self.update_values["phone_metadata"]
        row["notice_tracking"] = self.update_values["phone_metadata"]["notice_tracking"]
        return type("R", (), {"data": [{"id": row["id"]}]})()


class FakeClient:
    def __init__(self, notices, others):
        self.notices, self.others, self.calls, self.updates = notices, others, [], []

    def table(self, _):
        return Query(self)

    def rpc(self, name, params):
        self.calls.append(copy.deepcopy(params["p_rows"]))
        alias = {"pilot_review": "card", TRACK_KEY: "track", EVIDENCE_KEY: "seed", LIST_KEY: "list_check"}
        for item in params["p_rows"]:
            for row in self.notices + self.others:
                if row["id"] == item["id"]:
                    row.update({alias.get(k, k): v for k, v in item["raw_patch"].items()})

        class Result:
            def execute(self):
                return type("R", (), {"data": len(params["p_rows"])})()
        return Result()


class ReviewerRunTests(unittest.TestCase):
    def setUp(self):
        self.notice = {k: v for k, v in notice().items() if k != "ranked"}
        self.notice["phone_metadata"] = {"notice_tracking": dict(self.notice["notice_tracking"])}
        self.listing = listing()
        self.client = FakeClient([self.notice], [self.listing])
        self.bot = PreforeclosureReviewBot()
        for name, value in (("_appointments", lambda c, t, only: []),
                            ("_notices", lambda c, t: self.client.notices),
                            ("_sales", lambda c, t, source: [r for r in self.client.others if r["bot_source"] == source]),
                            ("_ranked", lambda c, ids: {"n1", "m1"})):
            self.enterContext(patch.object(self.bot, name, side_effect=value))
        self.enterContext(patch.object(bot_module, "lookup"))
        self.enterContext(patch.dict(os.environ, {rubin_lublin_list.ENABLE_ENV: ""}))

    def run_once(self, **kwargs):
        return self.bot.review(self.client, now=NOW, **kwargs)

    def test_confirmed_then_repeat_run_writes_nothing(self):
        first = self.run_once()
        self.assertEqual((first["list_checked"], first["list_confirmed"], first["redated"]), (1, 1, 0))
        self.assertEqual(self.notice["list_check"]["status"], "confirmed")
        self.assertEqual(self.notice["card"]["route"], "first_trace_review")
        self.assertEqual(self.notice["track"]["status"], "listed")
        self.assertEqual(self.client.updates, [])
        second = self.run_once()
        self.assertEqual((second["rows_changed"], second["cards_written"]), (0, 0))
        self.assertEqual(len(self.client.calls), 1)

    def test_dry_run_writes_nothing(self):
        self.listing["trustee_sale_date"] = "2027-01-15"
        plan = self.run_once(dry_run=True)
        self.assertEqual((plan["list_postponed"], plan["redated"]), (1, 1))
        self.assertEqual([p["list"] for p in plan["plan"] if p["id"] == "n1"], ["postponed"])
        self.assertEqual((self.client.calls, self.client.updates), ([], []))
        self.assertEqual(self.notice["trustee_sale_date"], "2026-12-04")

    def test_postponement_moves_the_one_record_once(self):
        self.run_once()
        self.listing["trustee_sale_date"] = "2027-01-15"
        moved = self.run_once()
        self.assertEqual((moved["list_postponed"], moved["redated"]), (1, 1))
        self.assertEqual(self.notice["trustee_sale_date"], "2027-01-15")
        values, guards = self.client.updates[0]
        self.assertEqual(guards["trustee_sale_date"], "2026-12-04")                    # compare-and-set on the old date
        self.assertEqual(guards["raw_payload->structured->>tdn_no"], "FD117227")
        self.assertEqual(values["phone_metadata"]["notice_tracking"]["last_seen_by"], "preforeclosure_review")
        card = self.notice["card"]
        self.assertEqual((card["route"], card["event"]["status"], card["economics"]["deadline"]),
                         ("first_trace_review", "postponed", "2027-01-15"))
        self.assertEqual(self.notice["list_check"]["prior_date"], "2026-12-04")
        again = self.run_once()
        self.assertEqual((again["rows_changed"], again["redated"]), (0, 0))
        self.assertEqual(len(self.client.updates), 1)
        self.assertEqual(self.notice["list_check"]["prior_date"], "2026-12-04")
        self.assertEqual(len(self.client.notices), 1)                                 # nothing inserted

    def test_manual_card_is_never_overwritten(self):
        manual = {"manual": True, "route": "research_hold", "route_reason": "Patrick: hold", "reviewed_by": "patrick"}
        self.notice["card"] = dict(manual)
        result = self.run_once()
        self.assertEqual(self.notice["card"], manual)
        self.assertEqual(result["kept_existing"], 1)
        self.assertEqual(self.notice["list_check"]["status"], "confirmed")            # evidence is still recorded

    def test_passed_notice_comes_back_when_the_list_shows_a_later_date(self):
        self.notice["trustee_sale_date"] = "2026-09-17"
        self.listing["trustee_sale_date"] = "2027-01-07"
        result = self.run_once()
        self.assertEqual((result["sales"], result["redated"]), (2, 1))
        self.assertEqual((self.notice["trustee_sale_date"], self.notice["card"]["economics"]["deadline"]),
                         ("2027-01-07", "2027-01-07"))

    def test_dropped_listing_holds_the_notice(self):
        self.run_once()
        self.listing["notice_tracking"] = {**self.listing["notice_tracking"], "last_seen_at": "2026-11-12T19:51:25+00:00"}
        self.client.others.append(listing(id="m2", property_address="1 Else St, Memphis, TN 38120"))
        self.run_once()
        self.assertEqual(self.notice["list_check"]["status"], "dropped")
        self.assertEqual(self.notice["card"]["route"], "research_hold")
        self.assertIn("No longer on", self.notice["card"]["route_reason"])


if __name__ == "__main__":
    unittest.main()
