from datetime import date, timedelta
import unittest
from unittest.mock import Mock, patch

from bs4 import BeautifulSoup

from src.bots import hamilton_county_herald_bot as herald


def dom(html):
    return BeautifulSoup(html, "html.parser")


def issue(day, notice):
    stamp = day.strftime("%m/%d/%Y")
    link = f"javascript:OpenChild('{notice}','{stamp.replace('/', '%2f')}')"
    return (f'<span id="ctl00_ContentMain_lblDate">{day:%A, %B %d, %Y}</span>'
            f'<a href="PublicNotices.aspx?date={stamp}">edition</a>'
            f'<h3>Foreclosures</h3><a href="{link}">View</a><a href="{link}">View</a>')


def detail(day, notice, sale, printed):
    def field(id_, label, value):
        return f'<tr><td>{label}:</td><td><span id="{id_}">{value}</span></td></tr>'
    return dom(f'<span id="lblDate">{day:%A, %B %d, %Y}</span><span id="lblTitle">Foreclosures</span>'
               + '<table>' + field("lbl2", "Address", "123 E. Main St")
               + field("lbl3", "", "Chattanooga, TN 37402")
               + field("lbl11", "DR No.", notice) + field("lbl8", "Advertised Auction Date", sale)
               + field("lbl9", "Date of First Public Notice", "01/01/2020") + '</table>'
               + f'<span id="lblBody">Hamilton County, Tennessee. {printed} Fr123456</span>')


class HeraldTests(unittest.TestCase):
    def setUp(self):
        self.bot = herald.HamiltonCountyHeraldBot()
        self.day = date.today()
        self.next = self.day + timedelta(days=20)
        self.printed = f"Sale at public auction will be on {self.next:%B %d, %Y}."

    def notice(self, sale, body=None, nid="1"):
        page = detail(self.day, nid, sale, self.printed if body is None else body)
        with patch.object(self.bot, "_document", return_value=(page, herald.HCH_DETAIL)):
            return self.bot._build_lead(nid, self.day, "Foreclosures")

    def test_reprints_have_new_ids_and_true_editions_but_identical_body(self):
        older = self.day - timedelta(days=7)
        landing = issue(self.day, 42) + f'<a href="PublicNotices.aspx?date={older:%m/%d/%Y}">archive</a>'
        def fetch(url, **params):
            if url == herald.HCH_INDEX:
                return dom(issue(older, 44) if params else landing), url
            d = self.day if params["id"] == "42" else older
            return detail(d, params["id"], self.next.strftime("%m/%d/%Y"), self.printed), url
        with patch.object(self.bot, "_document", side_effect=fetch) as read:
            result = self.bot.scrape()
        self.assertEqual(read.call_count, 4)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].raw_payload["publication_date"], self.day.isoformat())
        self.assertEqual(result[0].trustee_sale_date, self.next.isoformat())
        self.assertEqual(result[0].property_address, "123 E. Main St, Chattanooga, TN 37402")

    def test_unbound_or_future_issue_is_not_relabelled(self):
        for html in ("Error", issue(self.day + timedelta(days=1), 1), issue(self.day - timedelta(days=30), 1)):
            with self.subTest(html=html), patch.object(self.bot, "_document", return_value=(dom(html), herald.HCH_INDEX)):
                with self.assertRaises(ValueError):
                    self.bot.scrape()
        wrong = detail(self.day, "2", "", self.printed)
        with patch.object(self.bot, "_document", return_value=(wrong, herald.HCH_DETAIL)):
            with self.assertRaises(ValueError):
                self.bot._build_lead("1", self.day, "Foreclosures")
        blocked = Mock(status_code=403)
        with patch.object(self.bot, "fetch", return_value=blocked) as fetch:
            with self.assertRaises(RuntimeError):
                self.bot.scrape()
            self.assertEqual(fetch.call_count, 1)

    def test_redirect_to_different_notice_is_not_evidence(self):
        response = Mock(status_code=200, url=herald.HCH_DETAIL + "?id=2&date=11/20/2026")
        with patch.object(self.bot, "fetch", return_value=response), self.assertRaises(ValueError):
            self.bot._document(herald.HCH_DETAIL, id="1", date="11/20/2026")

    def test_conflicting_cancelled_past_and_missing_dates(self):
        for summary, text in (("03/04/2038", self.printed), ("1/1/1900", self.printed),
                              ("", self.printed + " The sale has been cancelled."),
                              ("", "Sale at public auction will be on March 1, 2019."),
                              ("", "The deed was recorded March 1, 2038.")):
            with self.subTest(summary=summary, text=text):
                self.assertIsNone(self.notice(summary, text).trustee_sale_date)

    def test_reprint_identity_preserves_narrative_changes_and_distinct_ads(self):
        key = herald._document_key
        old = "Sale at public auction. Nov. 13, 2026 Fr702325"
        new = "Sale at public auction. Nov. 13, 20, 2026 Fr702325"
        self.assertEqual(key("FL", old, "1"), key("FL", new, "2"))
        for other in (new.replace("Fr702325", "Fr702326"), new.replace("Sale at", "Cancellation of")):
            self.assertNotEqual(key("FL", old, "1"), key("FL", other, "2"))
        self.assertNotEqual(key("FL", "generic text", "1"), key("FL", "generic text", "2"))
        self.assertEqual(self.notice("", self.printed + " If the sale is postponed, notice may be given.").trustee_sale_date,
                         self.next.isoformat())

    def test_court_classification_requires_estate_identity_and_county(self):
        lines = ["In the Probate Court of Hamilton County, Tennessee", "NOTICE TO CREDITORS",
                 "Estate of Jane Sample, Deceased", "Docket No. 26-P-111"]
        lead = self.bot._build_probate_lead("55", self.day, "Courts", lines, "test")
        self.assertIsNotNone(lead)
        self.assertEqual(lead.raw_payload["docket"], "26-P-111")
        self.assertIsNone(lead.owner_name_records)
        self.assertTrue(lead.raw_payload["probate_review"]["contact_hold"])
        for other in (["Hamilton County Juvenile Court", "Order of Publication"],
                      [lines[0].replace("Hamilton", "Bradley"), *lines[1:]]):
            self.assertIsNone(self.bot._build_probate_lead("55", self.day, "Courts", other, "test"))

    def test_new_rows_use_repeatable_ids_and_ignore_insert_races(self):
        lead = self.notice("")
        client = Mock()
        table = client.table.return_value
        table.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value.data = []
        insert = table.upsert.return_value.execute.return_value
        with patch.object(herald, "_supabase", return_value=client):
            insert.data = [{"id": "inserted"}]
            self.assertEqual(self.bot._write_staging([lead, lead]), (1, 1))
            first = table.upsert.call_args.args[0]
            insert.data = []
            self.assertEqual(self.bot._write_staging([lead]), (0, 1))
            self.assertEqual(first[0]["id"], table.upsert.call_args.args[0][0]["id"])
            self.assertEqual(first[0]["pipeline_lead_key"], lead.pipeline_lead_key)
            self.assertTrue(table.upsert.call_args.kwargs["ignore_duplicates"])
            self.assertEqual(table.upsert.call_args.kwargs["on_conflict"], "id")

    def test_existing_keys_and_reprints_cannot_refresh_or_reopen_rows(self):
        lead = self.notice("")
        for existing_key, body in ((lead.pipeline_lead_key, "legacy text"),
                                   ("older issue key", lead.raw_payload["body"]),
                                   ("revised ad key", "changed narrative Fr123456")):
            client = Mock()
            table = client.table.return_value
            table.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value.data = [
                {"id":"existing", "distress_type":"PRE_FORECLOSURE", "body":body, "pipeline_lead_key":existing_key}]
            with patch.object(herald, "_supabase", return_value=client):
                self.assertEqual(self.bot._write_staging([lead]), (0, 1))
            table.update.assert_not_called()
            table.upsert.assert_not_called()
