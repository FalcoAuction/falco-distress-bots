import unittest
from unittest.mock import Mock, patch

from src.bots.memphis_codes_bot import FIELDS, MemphisCodesBot, evidence, request_address


DAY = "2026-11-22"


def case(**changes):
    attrs = dict(OBJECTID=1, INCIDENT_NUMBER="123", INCIDENT_ID=111, DEPARTMENT="Code Enforcement",
                 REQUEST_TYPE="CE-Substandard,Derelict Struc", REQUEST_STATUS="Open", Request_Sub_Status="Under Review",
                 Location_Address="10 Main Street, Memphis, Tennessee, 38103", CITY="", STATE="TN",
                 ZipCode="38103", PARCEL_ID="004 00030", REPORTED_DATE=123000)
    return {**attrs, **changes}


def response(features, more=False):
    res = Mock(status_code=200)
    res.json.return_value = {"features": [{"attributes": a} for a in features], "exceededTransferLimit": more}
    return res


class MemphisTests(unittest.TestCase):
    def setUp(self):
        self.bot = MemphisCodesBot()

    def test_exact_confirmation_across_pages_filters_noise_and_conflicts(self):
        self.bot.fetch = Mock(side_effect=[
            response([case(DEPARTMENT=None)], more=True),
            response([case(OBJECTID=2, INCIDENT_NUMBER="456"), case(OBJECTID=3, INCIDENT_NUMBER="789", REQUEST_TYPE="CE-Code Miscellaneous")]),
            response([case(DEPARTMENT=None), case(OBJECTID=2, INCIDENT_NUMBER="456"),
                      case(OBJECTID=4, INCIDENT_NUMBER="456", REQUEST_STATUS="Closed")]),
        ])
        lead, = self.bot.scrape()
        self.assertEqual(lead.property_address, "10 Main Street, Memphis, TN 38103")
        self.assertEqual(lead.pipeline_lead_key, self.bot.make_lead_key("memphis_codes", "123"))
        self.assertEqual(lead.raw_payload["memphis_codes_review"]["parcel_as_reported"], "004 00030")
        self.assertIsNone(lead.owner_name_records)
        self.assertIn("OBJECTID > 1", self.bot.fetch.call_args_list[1].kwargs["params"]["where"])
        self.assertNotIn("CONTACT", FIELDS)

    def test_dispositions_are_request_level_and_absence_is_unknown(self):
        for changes, expected in [
            (dict(REQUEST_STATUS="Closed"), "terminal_request"),
            (dict(REQUEST_STATUS="Resolved"), "terminal_request"),
            (dict(RESOLUTION_SUMMARY="outside city jurisdiction"), "disposition_review"),
            (dict(RESOLVED_DATE=1234), "disposition_review"),
            (dict(DEPARTMENT="Drain Maintenance"), "other_request_type"),
            (dict(REQUEST_TYPE="CE-Code Miscellaneous"), "other_request_type"),
            (dict(REQUEST_STATUS="Pending Litigation"), "structural_request"),
            (dict(REQUEST_STATUS=""), "status_review"),
        ]:
            with self.subTest(changes=changes):
                self.assertEqual(evidence("123", [case(**changes)], DAY)["finding"], expected)
        self.assertEqual(evidence("123", [], DAY)["finding"], "not_returned")
        for attrs in (case(Location_Address="11 Main Street"), case(PARCEL_ID="DIFFERENT"), case(Unit_Address="Unit 2")):
            self.assertEqual(evidence("123", [attrs], DAY, case())["finding"], "identity_conflict")

    def test_location_and_unit_controls(self):
        self.assertEqual(request_address(case(Location_Address="10 Main St", CITY="Memphis", Unit_Address="Unit 2")),
                         "10 Main St, Unit 2, Memphis, TN 38103")
        for attrs in (case(CITY="Cordova"), case(STATE="MS"), case(ZipCode="00000"),
                      case(Location_Address=""), case(Location_Address="10 Main St", CITY="Nashville"),
                      case(Location_Address="10 Main St, #1, Memphis, TN 38103", Unit_Address="#2")):
            self.assertIsNone(request_address(attrs))

    def test_incomplete_snapshot_cannot_write(self):
        for bad in (response([], True), Mock(status_code=503), response([case()], True)):
            self.bot.fetch = Mock(side_effect=[response([case()], True), bad])
            with self.assertRaises((ValueError, RuntimeError)):
                self.bot.scrape()
            with patch("src.bots.memphis_codes_bot._supabase") as db:
                self.assertEqual(self.bot._write_staging([]), (0, 0))
                db.assert_not_called()

    @patch("src.bots.memphis_codes_bot._supabase")
    def test_revisit_preserves_original_decisions_and_other_evidence(self, connect):
        table = connect.return_value.table.return_value
        key = self.bot.make_lead_key("memphis_codes", "123")
        row = {"id": "a", "pipeline_lead_key": key, "number": "123", "checked": None}
        table.select.return_value.eq.return_value.gt.return_value.order.return_value.limit.return_value.execute.return_value.data = [row]
        old = {"memphis_311": case(REQUEST_TYPE="CE-Code Miscellaneous"), "assessor": {"keep": True}}
        table.select.return_value.eq.return_value.in_.return_value.execute.return_value.data = [{**row, "raw_payload": old}]
        self.bot._complete = True
        self.bot.fetch = Mock(return_value=response([case(REQUEST_STATUS="Closed")]))
        self.bot._write_staging([])
        payload = table.update.call_args.args[0]
        self.assertEqual(set(payload), {"raw_payload"})
        self.assertEqual(payload["raw_payload"]["assessor"], old["assessor"])
        self.assertEqual(payload["raw_payload"]["memphis_311"], old["memphis_311"])
        self.assertEqual(payload["raw_payload"]["memphis_codes_review"]["finding"], "terminal_request")
        connect.return_value.table.assert_called_once_with("homeowner_requests_staging")
