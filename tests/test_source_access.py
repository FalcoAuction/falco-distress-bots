"""A feed that is stopped, refused or failing reads "not read since DATE".
It is never a withdrawal, a removal or a completed sale. No network.

    python -m unittest tests.test_source_access -v
"""

from __future__ import annotations

import os
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import _source_access as access  # noqa: E402
from src.bots import cheatham_tax_sale_bot as cheatham  # noqa: E402
from src.bots import court_notices_bot, mackie_wolf_trustee_bot, nashville_ledger_bot, tpad_enricher_bot  # noqa: E402
from src.bots._base import FALCO_USER_AGENT, make_session  # noqa: E402
from src.bots._cre_review import review_row  # noqa: E402
from src.bots._preforeclosure_review import sale_card, sale_state  # noqa: E402
from src.bots._tax_tract_review import next_status  # noqa: E402
from src.bots._trustee_list_match import check_notice, staged_book  # noqa: E402

TODAY = date(2026, 11, 24)
NOW = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)
FRESH = datetime(2026, 11, 24, 6, 0)          # this morning's successful read
OLD = datetime(2026, 11, 18, 6, 0)            # the last read before the source went quiet
BAD_WORDS = ("withdrawn", "no longer on", "removed", "cancelled", "sold", "sale was held")


def firm_row(**over):
    base = {"id": "s1", "bot_source": "brock_scott_trustee", "county": "Sumner",
            "property_address": "122 Read Tavern Rd Hendersonville, TN 37075", "full_name": None,
            "owner_name_records": "GERDES KEITH", "property_value": 507800, "property_value_source": "comptroller",
            "trustee_sale_date": "2026-12-18", "source_url": "https://example.test/list", "staging_status": "pending",
            "ranked": True,
            "notice_tracking": {"last_seen_at": "2026-11-18T06:00:00+00:00", "last_seen_by": "brock_scott_trustee",
                                "case_number": "26-1-FC01"},
            "seed": {"owner": "GERDES KEITH", "parcel": "083 137J D 01500 000 2027", "value": 507800,
                     "value_basis": "County roll appraisal", "url": None, "observed_on": "2026-11-22",
                     "label": "Sumner County parcel and assessment record"}}
    return {**base, **over}


def seen(stamp):
    return {"last_seen_at": stamp, "last_seen_by": "brock_scott_trustee", "case_number": "26-1-FC01"}


def card_text(card):
    return " ".join([card["route_reason"], card["event"]["what"], card["availability"]["evidence"]]).lower()


class ReadState(unittest.TestCase):
    def test_current_failing_and_stopped(self):
        self.assertEqual(access.read_state("brock_scott_trustee", FRESH, NOW, 4)["status"], "current")
        failing = access.read_state("brock_scott_trustee", OLD, NOW, 4)
        self.assertEqual((failing["status"], failing["not_read_since"]), ("not_read", "2026-11-18"))
        halted = access.read_state("nashville_ledger", "2026-11-23T23:40:45+00:00", NOW, 4)
        self.assertEqual((halted["status"], halted["not_read_since"], halted["stopped_on"]),
                         ("stopped", "2026-11-23", "2026-11-24"))
        self.assertEqual(access.read_state("brock_scott_trustee", None, NOW, 4)["status"], "not_read")

    def test_the_sentence_says_not_read_and_nothing_else(self):
        text = access.not_read_text("The trustee's list", access.read_state("nashville_ledger", OLD, NOW, 4))
        self.assertEqual(text, "The trustee's list has not been read since 2026-11-18 "
                               "(collection from that source stopped on 2026-11-24).")

    def test_registry_names_the_stopped_paths_with_dates(self):
        for key in ("nashville_ledger", "court_notices:nashville_ledger", "memphis_daily_news:detail"):
            self.assertEqual(access.STOPPED[key]["stopped_on"], "2026-11-24")
            self.assertTrue(access.STOPPED[key]["rule"])
        self.assertIsNone(access.stopped("memphis_daily_news"))      # the allowed index is still read
        self.assertIsNotNone(access.stopped("memphis_daily_news", "detail"))


class TrusteeSaleFreshness(unittest.TestCase):
    """Direct trustee lists (Mackie Wolf, Brock & Scott)."""

    def test_source_failing_reads_not_read_since(self):
        state = sale_state(firm_row(), OLD, NOW, TODAY)
        self.assertEqual((state["status"], state["not_read_since"]), ("unverified", "2026-11-18"))
        card = sale_card(firm_row(), state, TODAY)
        self.assertEqual((card["route"], card["event"]["status"]), ("research_hold", "unknown"))
        self.assertIn("has not been read since 2026-11-18", card["route_reason"])
        self.assertIn("has not been read since 2026-11-18", card["event"]["what"])
        self.assertIn("not a withdrawal or a sale", card["event"]["what"])
        self.assertNotIn("no longer on", card_text(card))

    def test_source_failing_past_the_sale_date_is_not_a_completed_sale(self):
        row = firm_row(trustee_sale_date="2026-11-20")       # date passed while the list was unread
        state = sale_state(row, OLD, NOW, TODAY)
        self.assertEqual(state["status"], "unverified")
        self.assertTrue(state["date_passed"])
        card = sale_card(row, state, TODAY)
        self.assertEqual((card["route"], card["event"]["status"]), ("research_hold", "unknown"))
        self.assertIn("has since passed", card["event"]["what"])
        self.assertNotEqual(card["route"], "removed")

    def test_source_stopped_reads_not_read_since_with_the_stop_date(self):
        halt = {"brock_scott_trustee": {"stopped_on": "2026-11-20", "rule": "test"}}
        with mock.patch.dict(access.STOPPED, halt):
            state = sale_state(firm_row(), OLD, NOW, TODAY)
            card = sale_card(firm_row(), state, TODAY)
        self.assertEqual((state["status"], state["source_stopped_on"]), ("unverified", "2026-11-20"))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("has not been read since 2026-11-18 (collection from that source stopped on 2026-11-20)",
                      card["route_reason"])
        self.assertNotIn("no longer on", card_text(card))

    def test_never_read_source_is_not_a_withdrawal(self):
        state = sale_state(firm_row(notice_tracking={}), None, NOW, TODAY)
        self.assertEqual(state["status"], "unverified")
        self.assertIn("has never been read", sale_card(firm_row(notice_tracking={}), state, TODAY)["route_reason"])

    def test_source_back_and_listing_seen_is_listed_again(self):
        row = firm_row(notice_tracking=seen("2026-11-24T06:00:00+00:00"))
        state = sale_state(row, FRESH, NOW, TODAY)
        self.assertEqual(state["status"], "listed")
        self.assertEqual(sale_card(row, state, TODAY)["route"], "first_trace_review")

    def test_source_back_and_listing_gone_is_a_real_withdrawal(self):
        state = sale_state(firm_row(), FRESH, NOW, TODAY)      # read today, row last seen Nov 18
        self.assertEqual((state["status"], state["source_read_on"]), ("withdrawn", "2026-11-24"))

    def test_date_passed_after_a_read_on_the_sale_day_is_still_passed(self):
        row = firm_row(trustee_sale_date="2026-11-18")
        self.assertEqual(sale_state(row, OLD, NOW, TODAY)["status"], "passed")


class NewspaperNoticeAgainstALapsedList(unittest.TestCase):
    """A Memphis notice whose trustee list went quiet."""

    def row(self, check):
        return {"id": "n1", "bot_source": "memphis_daily_news", "county": "shelby", "notice_id": "FD1",
                "property_address": "12 Main St, Memphis, TN 38116", "full_name": "Jane Doe",
                "owner_name_records": "Jane Doe", "trustee_sale_date": "2026-12-18", "staging_status": "pending",
                "firm": "Mackie Wolf Zientz & Mann, P.C.", "ranked": True, "source_url": "https://example.test/n",
                "notice_tracking": {"last_seen_at": "2026-11-24T06:00:00+00:00", "last_seen_by": "memphis_daily_news"},
                "list_check": check}

    def check(self, status, on):
        return {"source": "mackie_wolf_trustee", "firm": "Mackie Wolf Zientz & Mann", "checked_on": on,
                "status": status, "date": "2026-12-18", "notice_date": "2026-12-18", "list_url": "https://example.test/l"}

    def test_old_confirmation_stops_counting_and_says_not_read(self):
        state = sale_state(self.row(self.check("confirmed", "2026-11-18")), None, NOW, TODAY)
        self.assertEqual(state["status"], "publisher_only")
        self.assertIn("has not been read since 2026-11-18", state["list_note"])
        self.assertIn("not a withdrawal", state["list_note"])
        card = sale_card(self.row(self.check("confirmed", "2026-11-18")), state, TODAY)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("has not been read since 2026-11-18", card["route_reason"])

    def test_old_drop_is_not_a_withdrawal(self):
        state = sale_state(self.row(self.check("dropped", "2026-11-18")), None, NOW, TODAY)
        self.assertEqual(state["status"], "publisher_only")
        self.assertNotIn("source_read_on", state)

    def test_list_back_confirms_again(self):
        state = sale_state(self.row(self.check("confirmed", "2026-11-24")), None, NOW, TODAY)
        self.assertEqual(state["status"], "listed")

    def test_unread_list_gives_no_book_and_keeps_the_earlier_evidence(self):
        staged = [{"id": "m1", "bot_source": "mackie_wolf_trustee", "county": "shelby",
                   "property_address": "12 Main St Memphis, TN 38116", "trustee_sale_date": "2026-12-18",
                   "notice_tracking": {"last_seen_at": "2026-11-18T06:00:00+00:00", "last_seen_by": "mackie_wolf_trustee"}}]
        self.assertIsNone(staged_book("mackie_wolf_trustee", staged, NOW))
        prior = self.check("confirmed", "2026-11-18")
        kept = check_notice(self.row(prior), {"mackie_wolf_trustee": None}, TODAY.isoformat())
        self.assertEqual(kept, prior)          # unchanged: not "dropped", not "not_listed"


class TaxTractFreshness(unittest.TestCase):
    LISTED = {"available": True, "listed": True, "sale_date": "2027-04-13", "source_url": "u", "source_date": "2026-11-22"}
    DOWN = {"available": False, "reason": "county list unavailable"}

    def test_failed_read_is_unknown_never_removed_then_recovers(self):
        listed = next_status(None, self.LISTED, date(2026, 11, 22))
        down = next_status(listed, self.DOWN, date(2026, 11, 23))
        self.assertEqual((down["status"], down["last_known_status"]), ("unknown", "listed"))
        self.assertIn("could not be read on 2026-11-23", down["note"])
        self.assertIn("not evidence of payment", down["note"])
        self.assertNotIn("removed_on", down)
        back = next_status(down, self.LISTED, TODAY)
        self.assertEqual((back["status"], back["last_confirmed_on"]), ("listed", "2026-11-24"))

    def test_cheatham_unreadable_notice_rejects_nothing(self):
        row = {"id": "t1", "staging_status": "pending", "rejection_reason": None, "property_address": None,
               "owner_name_records": "DOE JOHN", "property_value": None, "property_value_source": None,
               "trustee_sale_date": "2026-12-16", "source_url": "https://example.test/notice",
               "raw_payload": {"kind": "cheatham_tax_sale", "docket": "19076", "tract": "A",
                               "parcel_printed": "027-008.00", "published_judgment": 4569.8,
                               "tax_status": {"status": "noticed", "sale_date": "2026-12-16",
                                              "last_confirmed_on": "2026-11-23"}}}
        change, = cheatham.review_rows([row], None, TODAY, "Cheatham County posted sale notice unavailable")
        self.assertEqual(change["columns"], {})
        self.assertEqual((change["status"]["status"], change["status"]["last_known_status"]), ("unknown", "noticed"))
        self.assertNotEqual(change["card"]["route"], "removed")


class CommercialNoticeFromAStoppedSource(unittest.TestCase):
    def row(self):
        return {
            "id": "row-dated", "bot_source": "nashville_ledger", "distress_type": "PRE_FORECLOSURE", "county": "davidson",
            "property_address": "10 Example Ln, Nashville, TN 37027", "owner_name_records": "Jane Q Sample Revocable Trust",
            "full_name": "Jane Q Sample Revocable Trust", "property_value": 2045000,
            "property_value_source": "metro_nashville_parcels", "trustee_sale_date": "2026-12-01",
            "source_url": "https://example.test/notice/1",
            "phone_metadata": {"notice_tracking": {"last_seen_at": "2026-11-23T23:40:45+00:00"}},
            "raw_payload": {
                "property_lane": {"lane": "commercial", "grade": "B", "exclude_reason": None, "basis": "metro:MEDICAL OFFICE"},
                "publication_date": "2026-11-06",
                "body": ("NOTICE OF SUBSTITUTE TRUSTEES SALE\n\nOwner of Property: Jane Q. Sample Revocable Trust, "
                         "Jane Q. Sample, Trustee\n\nProperty ID: 170 00 0 006.00\n"),
                "structured": {"borrower": "Jane Q Sample Revocable Trust", "substitute_trustee": "Example Trustee LLC"},
                "metro_parcel": {"owner": "SAMPLE, JANE Q. REVOCABLE TRUST", "parcel_id": "17000000600",
                                 "total_appraised": 2045000},
            },
        }

    def test_stopped_source_is_a_dated_hold(self):
        card, _ = review_row(self.row(), today=TODAY)
        self.assertEqual((card["route"], card["event"]["status"]), ("research_hold", "unknown"))
        self.assertIn("has not been read since 2026-11-23 (collection from that source stopped on 2026-11-24)",
                      card["route_reason"])
        self.assertIn("not confirmed as current", card["route_reason"])
        self.assertNotIn("last seen in print", card["event"]["what"])
        self.assertEqual(card["event"]["source"]["date"], "2026-11-06")      # the evidence keeps its original date
        for word in BAD_WORDS:
            self.assertNotIn(word, card["route_reason"].lower())

    def test_source_read_again_is_judged_on_its_evidence(self):
        with mock.patch.dict(access.STOPPED, clear=True):
            card, _ = review_row(self.row(), today=TODAY)
        self.assertNotIn("has not been read since", card["route_reason"])
        self.assertIn("last seen in print 2026-11-23", card["event"]["what"])


class NoRequestLeavesForAStoppedSource(unittest.TestCase):
    def test_nashville_ledger_makes_no_request(self):
        with mock.patch("requests.Session.request", side_effect=AssertionError("no request may be sent")):
            result = nashville_ledger_bot.run()
            bot = nashville_ledger_bot.NashvilleLedgerBot()
            self.addCleanup(bot.session.close)
            self.assertEqual(bot.scrape(), [])
            self.assertIsNone(bot.fetch("https://www.tnledger.com/Notices.aspx"))
        self.assertEqual((result["status"], result["stopped_on"], result["staged"]), ("source_stopped", "2026-11-24", 0))

    def test_ledger_is_off_the_schedule(self):
        from src.bots import _run_new
        names = [name for name, _ in _run_new.NEW_BOTS]
        self.assertNotIn("nashville_ledger", names)
        self.assertIn("memphis_daily_news", names)
        self.assertIn("court_notices", names)

    def test_court_notices_reads_only_the_allowed_memphis_index(self):
        hosts = {p["base"] for p in court_notices_bot.PUBLICATIONS}
        self.assertEqual(hosts, {"https://www.memphisdailynews.com"})
        url = court_notices_bot.edition_url(court_notices_bot.PUBLICATIONS[0], date(2026, 11, 20))
        self.assertEqual(url, "https://www.memphisdailynews.com/notices/2026/Nov/20/")
        bot = court_notices_bot.CourtNoticesBot()
        self.addCleanup(bot.session.close)
        with mock.patch.object(bot, "fetch", return_value=None) as fetch:
            self.assertEqual(bot.scrape(), [])
        for call in fetch.call_args_list:
            self.assertTrue(call.args[0].startswith("https://www.memphisdailynews.com/notices/"), call.args[0])


class CheathamTractsFromTheTrusteeNotice(unittest.TestCase):
    NOTICE = {"docket": "19500", "sale_date": "2027-03-10", "document_date": "2027-01-15", "sha256": "abc",
              "source_url": cheatham.NOTICE,
              "tracts": [{"letter": "A", "parcel_printed": "027-008.00", "parcel_long": "05-027-008.00-000",
                          "parcel_forms_agree": True, "location": "101 Spring Creek Road", "owners": "John Doe",
                          "amount": 4569.8},
                         {"letter": "B", "parcel_printed": "031-001.00", "parcel_long": "05-031-001.00-000",
                          "parcel_forms_agree": True, "location": "Spring Creek Road", "owners": "Unknown, possibly heirs",
                          "amount": 812.11}]}

    def test_new_case_is_staged_in_the_shape_the_tax_bot_follows(self):
        a, b = court_notices_bot.cheatham_trustee_tracts(self.NOTICE, date(2027, 1, 20), {"19076"})
        self.assertEqual((a.bot_source, a.county, a.distress_type, a.trustee_sale_date),
                         ("court_notices", "cheatham", "TAX_LIEN", "2027-03-10"))
        self.assertEqual(a.source_url, cheatham.NOTICE)
        self.assertEqual((a.raw_payload["kind"], a.raw_payload["docket"], a.raw_payload["tract"],
                          a.raw_payload["published_judgment"]), ("cheatham_tax_sale", "19500", "A", 4569.8))
        self.assertEqual(a.property_address, "101 Spring Creek Road")
        self.assertIsNone(b.property_address)
        self.assertIsNone(b.owner_name_records)
        self.assertNotEqual(a.pipeline_lead_key, b.pipeline_lead_key)

    def test_case_already_staged_or_sale_passed_stages_nothing(self):
        self.assertEqual(court_notices_bot.cheatham_trustee_tracts(self.NOTICE, date(2027, 1, 20), {"19500"}), [])
        self.assertEqual(court_notices_bot.cheatham_trustee_tracts(self.NOTICE, date(2027, 3, 11), set()), [])
        self.assertEqual(court_notices_bot.cheatham_trustee_tracts(None, date(2027, 1, 20), set()), [])


class HonestAgent(unittest.TestCase):
    def test_one_shared_agent_with_no_browser_string_email_or_name(self):
        self.assertEqual(FALCO_USER_AGENT, "FALCO-research/1.0 (+falco.llc)")
        session = make_session()
        self.addCleanup(session.close)
        self.assertEqual(session.headers["User-Agent"], FALCO_USER_AGENT)
        for bot_class in (mackie_wolf_trustee_bot.MackieWolfTrusteeBot, nashville_ledger_bot.NashvilleLedgerBot,
                          court_notices_bot.CourtNoticesBot):
            bot = bot_class()
            self.addCleanup(bot.session.close)
            agent = bot.session.headers["User-Agent"]
            self.assertEqual(agent, FALCO_USER_AGENT)
            self.assertNotIn("Mozilla", agent)
            self.assertNotIn("@", agent)

    def test_mackie_refusal_is_a_failed_read_with_no_probing(self):
        bot = mackie_wolf_trustee_bot.MackieWolfTrusteeBot()
        self.addCleanup(bot.session.close)
        refused = SimpleNamespace(status_code=403, text="", content=b"")
        with mock.patch.object(bot.session, "request", return_value=refused) as request, \
                mock.patch("src.bots._base.time.sleep"):
            with self.assertRaises(RuntimeError) as raised:
                bot.scrape()
        self.assertIn("source_refused", str(raised.exception))
        self.assertEqual(request.call_count, 1)                  # no fallback probing after a refusal
        self.assertIsNone(bot._report)                           # so refresh_staging writes nothing
        self.assertEqual(bot._write_staging([]), (0, 0))

    def test_tpad_refusal_is_unavailable_not_no_match(self):
        refused = SimpleNamespace(status_code=403)
        with mock.patch("requests.Session.get", return_value=refused):
            session = tpad_enricher_bot._build_session()
        self.addCleanup(session.close)
        self.assertEqual(session.headers["User-Agent"], FALCO_USER_AGENT)
        self.assertTrue(session.tpad_refused)
        with mock.patch.object(session, "post", side_effect=AssertionError("no request after a refusal")):
            with self.assertRaises(tpad_enricher_bot.TpadUnavailable):
                tpad_enricher_bot.search_by_owner(session, "001", "DOE")
        with mock.patch.object(tpad_enricher_bot, "_supabase", return_value=object()), \
                mock.patch.object(tpad_enricher_bot, "_build_session", return_value=session):
            self.assertEqual(tpad_enricher_bot.run()["status"], "source_unavailable")

    def test_tpad_refusal_mid_run_stops_and_parks_nothing(self):
        session = SimpleNamespace(tpad_refused=False, post=mock.Mock(return_value=SimpleNamespace(status_code=403)))
        with self.assertRaises(tpad_enricher_bot.TpadUnavailable):
            tpad_enricher_bot.search_by_owner(session, "001", "DOE")
        self.assertTrue(session.tpad_refused)


if __name__ == "__main__":
    unittest.main()
