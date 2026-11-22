"""The newspaper cites the appointment and the mortgage in different clauses."""
from datetime import date
import unittest
from unittest.mock import MagicMock, patch

from src.bots.memphis_daily_news_bot import MemphisDailyNewsBot

APPT = ("appointed Substitute Trustee by the owner and holder of the Note by instrument "
        "recorded in Deed Book , Page(s) and/or as Instrument Number ")
SALE = "will proceed to sell the below-mentioned property on 12/29/2026, at 12:00 PM"
COUNTY = " in Register's Office of Shelby County, Tennessee; "


class AppointmentTests(unittest.TestCase):
    def setUp(self):
        self.bot = MemphisDailyNewsBot()
        self.addCleanup(self.bot.session.close)

    def lead(self, fid="FD117714", body=None, **overrides):
        detail = dict(tdn_no=fid, borrower="Samantha Rickard", address="6669 Elmore Rd",
                      address_continuation="Memphis, TN 38134", instrument_no="08002870",
                      body=body if body is not None else APPT + "26098311" + COUNTY + SALE)
        return self.bot._build_lead({**detail, **overrides}, fid, date(2026, 11, 20))

    def test_two_recordings_and_missing_summary_date(self):
        for fid, mortgage, appointment, summary, expected in [
            ("FD117695", "22115033", "26097972", "12/18/2026", "2026-12-18"),
            ("FD117714", "08002870", "26098311", None, "2026-12-29"),
        ]:
            body = APPT + appointment + COUNTY
            if not summary:
                body += SALE
            lead = self.lead(fid, body, instrument_no=mortgage, advertised_auction_date=summary)
            raw = lead.raw_payload
            self.assertEqual(raw["structured"]["instrument_no"], mortgage)
            self.assertEqual(raw["substitute_trustee_appointment"]["instrument_number"], appointment)
            self.assertIn(appointment, raw["substitute_trustee_appointment"]["excerpt"])
            self.assertEqual(lead.trustee_sale_date, expected)
            if not summary:
                self.assertEqual(raw["published_sale_clause"]["source_url"], lead.source_url)

    def test_regular_feed_accepts_clause_only_with_blank_or_agreeing_index(self):
        detail = dict(tdn_no="FD117714", borrower="Samantha Rickard", address="6669 Elmore Rd",
                      instrument_no="08002870", body=APPT + "26098311" + COUNTY + SALE)
        for advertised, expected in [(None, 1), ("2026-12-29", 1), ("2026-12-30", 0)]:
            entry = {"id": "FD117714", "borrower": detail["borrower"], "address": detail["address"],
                     "sale_date": advertised}
            with patch.object(self.bot, "_recent_dates", return_value=[date(2026, 11, 20)]), \
                    patch.object(self.bot, "_fetch_index", return_value=[entry]), \
                    patch.object(self.bot, "_fetch_detail", return_value=detail), \
                    patch("src.bots.memphis_daily_news_bot.date") as clock:
                clock.today.return_value = date(2026, 11, 22)
                self.assertEqual(len(self.bot.scrape()), expected)

    def test_loan_only_ambiguous_and_malformed_instruments(self):
        for body in ["Deed of Trust recorded as Instrument No. 26098311",
                     APPT + "08002870", APPT + "26098311-2", APPT + "260983111",
                     APPT + "26098311 and 26098312", APPT + "26098311; " + APPT + "26098312",
                     "Appointment of Substitute Trustee of record. The mortgage is Instrument No. 26098311"]:
            with self.subTest(body=body):
                self.assertNotIn("substitute_trustee_appointment", self.lead(body=body + COUNTY).raw_payload)

    def test_only_one_operative_date(self):
        for body in ["Instrument recorded on 12/29/2026", SALE + "; " + SALE,
                     SALE + "; the sale has been postponed", SALE.replace("12/29", "13/29")]:
            self.assertIsNone(self.lead(body=COUNTY + body).trustee_sale_date)
        lead = self.lead(advertised_auction_date="1/7/2027")
        self.assertNotIn("published_sale_clause", lead.raw_payload)
        self.assertEqual(lead.trustee_sale_date, "2027-01-07")

    def repeat(self, duplicates=False, **stored):
        lead = self.lead()
        client, staging, live = MagicMock(), MagicMock(), MagicMock()
        client.table.side_effect = lambda t: staging if t.endswith("_staging") else live
        row = {"id": "existing", "pipeline_lead_key": lead.pipeline_lead_key, "tdn": "FD117714",
               "loan": "08002870", "county": "shelby", "property_address": lead.property_address,
               "phone_metadata": {"other": "preserved"}, "trustee_sale_date": None, **stored}
        for table, data in [(staging, [row, row] if duplicates else [row]), (live, [])]:
            query = table.select.return_value
            query.eq.return_value = query.in_.return_value = query
            query.execute.return_value.data = data
        q = staging.update.return_value
        q.eq.return_value = q.filter.return_value = q.is_.return_value = q
        with patch("src.bots._base._supabase", return_value=client):
            self.bot._write_staging([lead])
        live.update.assert_not_called()
        return staging.update.call_args.args[0], q

    def test_pending_repeat_null_date_and_metadata_guards(self):
        change, query = self.repeat()
        self.assertEqual(change["trustee_sale_date"], "2026-12-29")
        pm = change["phone_metadata"]
        self.assertEqual(pm["other"], "preserved")
        self.assertEqual(pm["notice_tracking"]["substitute_trustee_appointment"]["instrument_number"], "26098311")
        query.is_.assert_any_call("trustee_sale_date", "null")
        query.eq.assert_any_call("raw_payload->structured->>instrument_no", "08002870")
        query.filter.assert_called_once()

    def test_repeat_conflicts_and_manual_date_controls(self):
        for mismatch in [{"property_address": "Other"}, {"loan": "another"}, {"duplicates": True}]:
            change, _ = self.repeat(**mismatch)
            self.assertNotIn("substitute_trustee_appointment", change["phone_metadata"]["notice_tracking"])
            self.assertNotIn("trustee_sale_date", change)
        for stored in [{"phone_metadata": {"sale_status": {"status": "cancelled"}}},
                       {"trustee_sale_date": "2027-03-22"}]:
            change, _ = self.repeat(**stored)
            self.assertNotIn("trustee_sale_date", change)
        change, _ = self.repeat(substitute_trustee_appointment={"instrument_number": "different"})
        self.assertNotIn("substitute_trustee_appointment", change["phone_metadata"]["notice_tracking"])
