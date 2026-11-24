"""Bounded free-notice acquisition, without database writes."""
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch
from src.bots.memphis_daily_news_bot import MemphisDailyNewsBot, MDN_BASE

DAY = date(2026, 11, 20)


def index(fid="FD123", day="11%2f20%2f2026", sale="12/18/2026"):
    return f"<table><tr><td><a href=\"javascript:OpenChildFT2('{fid}','{day}')\">View</a></td><td>Jane Doe</td><td>12 Main St</td><td>{sale}</td><td>11/20/2026</td></tr></table>"


def detail(fid="FD123", **changes):
    return dict(dict(tdn_no=fid, borrower="Jane Doe", address="12 Main St",
                     advertised_auction_date="12/18/2026", body="in Shelby County, Tennessee"), **changes)


class MemphisNoticeTests(unittest.TestCase):
    def bot(self):
        b = MemphisDailyNewsBot()
        self.addCleanup(b.session.close)
        b._recent_dates = lambda days: [DAY]
        b.read_details = True    # the detail parser, kept for a permitted copy of the page
        return b

    def test_index_binds_publication_and_deduplicates(self):
        rows = MemphisDailyNewsBot._parse_index(index()+index()+index("FD456", "11%2f19%2f2026"), DAY)
        self.assertEqual([r["id"] for r in rows], ["FD123"])
        self.assertEqual(rows[0]["sale_date"], "2026-12-18")

    def test_current_notice_end_to_end_and_provenance(self):
        b = self.bot()
        html = "<div id='pnlSummary'><table>" + "".join(
            f"<tr><td>{k}:</td><td>{v}</td></tr>" for k,v in [
                ("TDN No.", "FD123"), ("Borrower", "Jane Doe"), ("Address", "12 Main St"),
                ("Advertised Auction Date", "12/18/2026")]) + "</table></div>"
        html += "<div id='record-details'><p>in Shelby County, Tennessee</p></div>"
        with patch.object(b, "fetch", side_effect=[SimpleNamespace(status_code=200,text=index()), SimpleNamespace(status_code=200,text=html)]) as fetch:
            leads = b.scrape()
        self.assertEqual(len(leads),1)
        self.assertEqual(leads[0].county,"shelby")
        self.assertEqual(leads[0].raw_payload["publication_date"], "2026-11-20")
        self.assertEqual(leads[0].raw_payload["evidence_type"], "published_notice")
        self.assertIn("date=11/20/2026", leads[0].source_url)
        self.assertEqual(fetch.call_args.kwargs["timeout"], (5,15))

    def test_past_notice_not_fetched_or_refreshed(self):
        b = self.bot()
        with patch.object(b,"_source_fetch",return_value=SimpleNamespace(status_code=200,text=index(sale="1/1/2020"))), patch.object(b,"_fetch_detail") as fetch:
            self.assertEqual(b.scrape(),[])
        fetch.assert_not_called()
        self.assertEqual(b.collection_stats["past"],1)

    def test_wrong_identity_or_sale_is_rejected(self):
        for changes in ({"tdn_no":"FD999"},{"borrower":"Other Owner"},{"address":"99 Main St"},{"advertised_auction_date":"1/1/2027"}):
            b = self.bot()
            with patch.object(b,"_fetch_index",return_value=b._parse_index(index(),DAY)), patch.object(b,"_fetch_detail",return_value=detail(**changes)):
                self.assertEqual(b.scrape(),[])
                self.assertEqual(b.collection_stats["invalid"],1)

    def test_failure_does_not_fallback_to_older_version(self):
        b = self.bot(); b._recent_dates=lambda days:[DAY,date(2026,11,19)]
        with patch.object(b,"_fetch_index",return_value=b._parse_index(index(),DAY)), patch.object(b,"_fetch_detail",return_value=None) as fetch:
            self.assertEqual(b.scrape(),[])
        self.assertEqual(fetch.call_count,1)
        self.assertEqual(b.collection_stats["failed"],1)

    def test_budget_returns_completed_work(self):
        b = self.bot(); entries=b._parse_index(index()+index("FD456"),DAY)
        clock=[0]
        def fetch(fid, day):
            clock[0]=500
            return detail(fid)
        with patch("src.bots.memphis_daily_news_bot.time.monotonic",side_effect=lambda:clock[0]), patch.object(b,"_fetch_index",return_value=entries), patch.object(b,"_fetch_detail",side_effect=fetch):
            self.assertEqual(len(b.scrape()),1)
        self.assertTrue(b.collection_stats["partial"])

    def test_access_limit_stops_without_retry(self):
        b=self.bot(); b._recent_dates=lambda days:[DAY,date(2026,11,19)]
        with patch.object(b,"fetch",return_value=SimpleNamespace(status_code=429,text="")) as fetch:
            self.assertEqual(b.scrape(),[])
        self.assertEqual(fetch.call_count,1)
        self.assertTrue(b.collection_stats["partial"])
        self.assertEqual(b.session.get_adapter(MDN_BASE+"/").max_retries.total,0)

    def test_detail_failure_isolated(self):
        b=self.bot(); entries=b._parse_index(index()+index("FD456"),DAY)
        def fetch(fid,day): return None if fid=="FD123" else detail(fid)
        with patch.object(b,"_fetch_index",return_value=entries),patch.object(b,"_fetch_detail",side_effect=fetch):
            self.assertEqual(len(b.scrape()),1)
        self.assertEqual(b.collection_stats["failed"],1)

    def test_partial_run_stages_completed_leads_and_reports_coverage(self):
        b=self.bot(); clock=[0]
        entries=b._parse_index(index()+index("FD456"),DAY)
        def fetch(fid,pub):
            clock[0]=500
            return detail(fid)
        with patch("src.bots.memphis_daily_news_bot.time.monotonic",side_effect=lambda:clock[0]), patch.object(b,"_fetch_index",return_value=entries),patch.object(b,"_fetch_detail",side_effect=fetch),patch.object(b,"_write_staging",return_value=(1,0)) as stage,patch("src.bots._base.BotBase._report_health") as health:
            b.run()
        self.assertEqual(len(stage.call_args.args[0]),1)
        notes=health.call_args.kwargs["notes"]["notice_collection"]
        self.assertTrue(notes["partial"])
        self.assertEqual(notes["detail_attempts"],1)

    def test_daily_rotation_changes_partial_prefix(self):
        first=[]
        for day in [date(2026,11,22),date(2026,11,23)]:
            b=self.bot(); clock=[0]
            entries=b._parse_index(index()+index("FD456"),DAY)
            def fetch(fid,pub):
                first.append(fid);clock[0]=500;return detail(fid)
            with patch("src.bots.memphis_daily_news_bot.date") as today, patch("src.bots.memphis_daily_news_bot.time.monotonic",side_effect=lambda:clock[0]),patch.object(b,"_fetch_index",return_value=entries),patch.object(b,"_fetch_detail",side_effect=fetch):
                today.today.return_value=day
                b.scrape()
        self.assertEqual(set(first),{"FD123","FD456"})


class IndexOnlyTests(unittest.TestCase):
    """Since 2026-11-24 the detail page is disallowed and is never requested."""

    def bot(self):
        b = MemphisDailyNewsBot()
        self.addCleanup(b.session.close)
        b._recent_dates = lambda days: [DAY]
        return b

    def test_detail_page_is_off_by_default_and_never_requested(self):
        b = self.bot()
        self.assertFalse(b.read_details)
        with patch.object(b, "fetch", return_value=SimpleNamespace(status_code=200, text=index())) as fetch:
            leads = b.scrape()
        self.assertEqual(fetch.call_count, 1)
        url = fetch.call_args.args[0]
        self.assertEqual(url, MDN_BASE + "/notices/2026/Nov/20/")
        self.assertNotIn("ViewNotice", url)
        self.assertEqual(len(leads), 1)

    def test_index_lead_keeps_the_notice_key_and_says_what_is_missing(self):
        b = self.bot()
        with patch.object(b, "fetch", return_value=SimpleNamespace(status_code=200, text=index())):
            lead, = b.scrape()
        self.assertEqual(lead.pipeline_lead_key, b.make_lead_key("memphis_daily_news", "FD123"))
        self.assertIsNone(lead.county)
        self.assertEqual(lead.trustee_sale_date, "2026-12-18")
        self.assertEqual(lead.source_url, MDN_BASE + "/notices/2026/Nov/20/")
        raw = lead.raw_payload
        self.assertEqual(raw["structured"]["tdn_no"], "FD123")
        self.assertEqual(raw["structured"]["date_of_first_public_notice"], "11/20/2026")
        self.assertEqual(raw["collection"]["scope"], "index_only")
        self.assertEqual(raw["collection"]["detail_stopped_on"], "2026-11-24")
        self.assertIn("substitute trustee", raw["collection"]["not_collected"])
        self.assertEqual(b.collection_stats["index_only"], 1)
        self.assertEqual(b.collection_stats["detail_attempts"], 0)

    def test_past_notice_is_not_staged_from_the_index(self):
        b = self.bot()
        with patch.object(b, "fetch", return_value=SimpleNamespace(status_code=200, text=index(sale="1/1/2020"))):
            self.assertEqual(b.scrape(), [])

    def test_refused_index_stages_nothing(self):
        b = self.bot()
        with patch.object(b, "fetch", return_value=SimpleNamespace(status_code=403, text="")):
            self.assertEqual(b.scrape(), [])
        self.assertTrue(b.collection_stats["partial"])


if __name__ == "__main__": unittest.main()
