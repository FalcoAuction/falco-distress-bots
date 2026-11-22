import unittest
from dataclasses import replace
from unittest.mock import MagicMock, patch

from src.bots.johnson_city_bdsr_bot import JohnsonCityBdsrBot, Packet


PACKET = Packet("123", "May 28 2026", "Board", "https://johnsoncitytn.civicweb.net/document/may.pdf")
AGENDA = """AGENDA
Thursday, May 28, 2026
3.1 12 First Street- CEPM202600001
First Public Hearing
3.2 44 Oak Rd: CEPM202600002
3. ADJOURNMENT
"""
MINUTES = """MINUTES
Thursday, March 26, 2026
1. 132 Spring St. - CEPM202200220
Motion to rescind and remove 132 Spring St. from the Board's docket.
RESULT: Carried 4-0
"""
SUMMARY = """AGENDA SUMMARY
SUBJECT: 99 Elsewhere Ave - CEPM202600001
Case Opened: February 2, 2024
AGENDA SUMMARY
SUBJECT: 44 Oak Road - CEPM202600002
Case Opened: March 26, 2026
TO: Some Interested Party
"""


class JohnsonCityTests(unittest.TestCase):
    def setUp(self):
        self.bot = JohnsonCityBdsrBot()
        self.leads = self.bot._parse_packet(AGENDA + MINUTES + SUMMARY, PACKET)
        self.lead = self.leads[0]
        self.name = self.bot.name
        self.event = self.lead.raw_payload[self.name]
        old = dict(case_number="CEPM202600001", address="12 First St", meeting_date="Apr 23 2026")
        self.row = dict(id="row", pipeline_lead_key=self.lead.pipeline_lead_key,
                        property_address=self.lead.property_address, staging_status="pending",
                        source_url=PACKET.pdf_url.replace("may", "apr"),
                        admin_notes="BDSR CEPM202600001 | meeting: Apr 23 2026",
                        raw_payload={"assessor": {"value": 25}, self.name: old})

    def test_agenda_boundary_and_exact_subject(self):
        self.assertEqual(len(self.leads), 2)
        self.assertEqual(self.event["hearing"], "First Public Hearing")
        self.assertIsNone(self.event["opened_on"])
        second = self.leads[1]
        self.assertEqual(second.raw_payload[self.name]["opened_on"], "2026-03-26")
        self.assertIsNone(second.owner_name_records)
        self.assertIsNone(second.county)
        self.assertFalse(self.event["sale_authority_verified"])
        self.assertFalse(self.bot._parse_packet(AGENDA, replace(PACKET, meeting_date="Apr 23 2026")))

    def test_minutes_require_carried_vote_and_matching_property(self):
        votes = list(self.bot._minute_removals(MINUTES, PACKET))
        self.assertEqual(votes[0][1]["meeting_on"], "2026-03-26")
        for text in (MINUTES.replace("Carried", "Failed"),
                     MINUTES.replace("remove 132", "remove 134")):
            self.assertFalse(list(self.bot._minute_removals(text, PACKET)))

    def test_refresh_preserves_enrichment_and_is_idempotent(self):
        self.row["admin_notes"] += " \u00b7 TPAD: parcel"
        delta = self.bot._refresh_patch(self.row, self.lead)
        self.assertEqual(delta["raw_payload"]["assessor"], {"value": 25})
        self.assertTrue(delta["admin_notes"].endswith(" \u00b7 TPAD: parcel"))
        self.assertFalse(self.bot._refresh_patch({**self.row, **delta}, self.lead))
        self.row["admin_notes"] = "staff review"
        self.assertNotIn("admin_notes", self.bot._refresh_patch(self.row, self.lead))

    def test_manual_disposition_identity_and_newer_evidence_protected(self):
        for field, value in (("staging_status", "rejected"), ("property_address", "13 First St")):
            self.assertFalse(self.bot._refresh_patch({**self.row, field: value}, self.lead))
        old = self.row["raw_payload"][self.name]
        old["minutes_evidence"] = {"meeting_on": "2026-06-01"}
        self.assertFalse(self.bot._refresh_patch(self.row, self.lead))
        old["minutes_evidence"] = {}
        old["meeting_on"] = "2026-07-01"
        self.assertFalse(self.bot._refresh_patch(self.row, self.lead))

    @patch("src.bots._base.BotBase._write_staging", return_value=(0, 0))
    @patch("src.bots.johnson_city_bdsr_bot._supabase")
    def test_refresh_guards_and_collision(self, client, base):
        table = client.return_value.table.return_value
        query = table.select.return_value.eq.return_value.in_.return_value
        query.execute.return_value.data = [self.row, self.row]
        self.bot._write_staging(self.leads)
        table.update.assert_not_called()
        base.assert_called_with([self.leads[1]])
        self.leads[1].raw_payload[self.name]["stage"] = "docket_removal_reported"
        query.execute.return_value.data = [self.row]
        update = MagicMock()
        update.eq.return_value = update
        table.update.return_value = update
        self.bot._write_staging(self.leads)
        update.eq.assert_any_call("staging_status", "pending")
        base.assert_called_with([])
        update.eq.assert_any_call("id", "row")
        update.filter.assert_called_once()
        self.assertEqual(update.filter.call_args.args[:2], ("raw_payload", "eq"))
