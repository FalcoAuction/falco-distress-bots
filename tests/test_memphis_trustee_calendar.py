from datetime import date
from unittest.mock import MagicMock, patch

import unittest
from bs4 import BeautifulSoup

from src.bots import memphis_trustee_calendar as cal


DAY = date(2026, 11, 22)
END = date(2027, 3, 22)
NOTICE = {"tdn_no": "FD100", "substitute_trustee": cal.FIRM, "address": "14 Oak Cv E",
          "advertised_auction_date": "12/01/2026"}
ROW = {"id": "row-id", "bot_source": "memphis_daily_news", "raw_payload": {"structured": NOTICE},
       "pipeline_lead_key": cal.BotBase.make_lead_key("memphis_daily_news", "FD100"),
       "trustee_sale_date": "2026-12-01", "property_address": "14 Oak Cv E, Memphis, TN 38125",
       "county": "shelby"}
LISTING = {"address": "14 Oak Cove East", "zip": "38125", "date": "2026-12-01", "prior_date": None}


def check(listings, row=ROW, today=DAY):
    return cal.review(row, listings, today, END)


class CalendarTests(unittest.TestCase):
    def test_corroboration_and_continuances(self):
        assert check([LISTING])["status"] == "corroborated"
        new = dict(LISTING, prior_date="2026-12-01", date="2027-01-05")
        ev = check([new])
        assert ev["status"] == "postponed"
        omitted = check([], dict(ROW, prior=ev), date(2026, 12, 2))
        assert omitted["status"] == "omitted"
        later = dict(new, prior_date="2027-01-05", date="2027-02-04")
        assert check([later], dict(ROW, prior=omitted))["status"] == "postponed"
        assert check([later], dict(ROW, prior=dict(omitted, notice_id="FD101")))["status"] == "unresolved"


    def test_control_conflict(self):
        for change in [{"date": "2026-12-17"}, {"zip": "38002"}, {"address": "14 Oak Cove"},
                       {"address": "14 Oak Cove East #2"}, {"address": "14 Oaks Cove East"}]:
            with self.subTest(change=change):
                self.assertEqual(check([dict(LISTING, **change)])["status"], "unresolved")


    def test_missing_duplicate_elapsed_or_outside_window(self):
        assert check([])["status"] == "omitted"
        assert check([], today=date(2026, 12, 2))["status"] == "expired"
        assert check([LISTING, LISTING])["status"] == "unresolved"
        assert check([LISTING], dict(ROW, trustee_sale_date=None))["status"] == "unresolved"
        assert check([LISTING], dict(ROW, pipeline_lead_key="wrong")) is None
        assert check([LISTING], dict(ROW, county="davidson")) is None


    def test_parser_rejects_partial_or_wrong_geography(self):
        form = (f'<input id="txtRangeBegin" value="{DAY}"><input id="txtRangeEnd" value="{END}">'
                '<select id="ddlState"><option selected value="TN"></select>'
                '<select id="ddlCounty"><option selected value="Shelby"></select>')
        header = "".join(f"<th>{h}</th>" for h in cal.HEADERS)
        cells = ["1/5/2027", "11:00 AM", "12/1/2026 11:00 AM", "14 Oak Cove East", "Cordova",
                 "Shelby", "TN", "38125-1234", "Courthouse", "Auction.com"]
        body = "".join(f"<td>{c}</td>" for c in cells)
        html = form + f'<table id="gvSales"><tr>{header}</tr><tr>{body}</tr></table>'
        def parse(value):
            return cal.parse_calendar(BeautifulSoup(value, "html.parser"), DAY, END)
        result, = parse(html)
        self.assertEqual((result["prior_date"], result["date"], result["zip"]),
                         ("2026-12-01", "2027-01-05", "38125"))
        for bad in [html.replace(">Shelby<", ">Knox<"), html.replace(body, ""),
                    html.replace("12/1/2026 11:00 AM", "TBA"), html.replace("Prior Sale Date", "Other")]:
            with self.assertRaises(ValueError):
                parse(bad)

    def test_source_failure_cannot_mean_omitted(self):
        with self.assertRaises(ValueError):
            cal.parse_calendar(BeautifulSoup("<html/>", "html.parser"), DAY, END)
        with patch.object(cal, "load_calendar", side_effect=ValueError), patch.object(cal, "_supabase") as db:
            with self.assertRaises(ValueError):
                cal.run()
            db.assert_not_called()


    def test_runner_only_merges_calendar_field_and_replay_skips_unchanged(self):
        client = MagicMock()
        query = client.table.return_value.select.return_value
        query.eq.return_value = query.in_.return_value = query
        rows = query.order.return_value.limit.return_value.execute.return_value
        rows.data = [{**ROW, "structured": NOTICE, "prior": None}]
        client.rpc.return_value.execute.return_value.data = 1
        clock = MagicMock()
        clock.now.return_value.astimezone.return_value.date.return_value = DAY
        with patch.object(cal, "load_calendar", return_value=[LISTING]), patch.object(cal, "_supabase", return_value=client), patch.object(cal, "datetime", wraps=cal.datetime) as dt:
            dt.now = clock.now
            assert cal.run()["staged"] == 1
            rpc, args = client.rpc.call_args.args
            assert rpc == "apply_staging_enrichment"
            update, = args["p_rows"]
            assert set(update) == {"id", "raw_patch"} and update["id"] == ROW["id"]
            assert set(update["raw_patch"]) == {cal.KEY}
            rows.data[0]["prior"] = update["raw_patch"][cal.KEY]
            assert cal.run()["staged"] == 0
            client.rpc.assert_called_once()
