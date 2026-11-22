from copy import deepcopy
from datetime import date, timedelta
import unittest
from unittest.mock import MagicMock

from src.bots._land_identity import parcel_control
from src.bots._land_qualification import collect_events, land_gate, qualify, source_event
from src.bots._property_lane import property_lane
from src.bots.decision_engine_bot import DecisionEngineBot
from src.bots.property_lane_bot import _tag, _dedupe
from src.enrichment.shortlist import screen


TODAY = date(2026, 11, 22)
PID = "035 070I D 01700 000 2027"
OWNER = "EXAMPLE ADA HEIRS"


def tract():
    roll = {"county": "hardeman", "parcelid": PID, "owner": OWNER, "class": "Agricultural",
            "land_use": "FARM", "owner_mailing": "20 EXAMPLE LN, JACKSON TN 38301",
            "acres": 35, "appraisal": 200000, "method": "land_lane"}
    land = {"county": "hardeman", "parcel_id": PID, "owner_of_record": OWNER,
            "observed_on": "2026-11-20", "source_refreshed_on": None,
            "source_url": "https://comptroller.tn.gov/", "status": "current", "review_required": False}
    return {"id": "land-a", "county": "hardeman", "bot_source": "land_lane", "distress_type": "HEIRS_PROPERTY",
            "owner_name_records": OWNER, "property_address": "10 Example Road", "property_value": 200000,
            "raw_payload": {"source": "comptroller_roll", "property_lane": "land", "land": land,
                            "comptroller_parcel": roll}}


def tax():
    return {"id": "tax-a", "county": "hardeman", "bot_source": "hardeman_tax_notice",
            "distress_type": "TAX_LIEN", "staging_status": "pending",
            "source_url": "https://www.bulletintimesnews.com/legal.pdf",
            "raw_payload": {"kind": "tax_collection_suit", "parcel": "035 070I D 01700 000",
                            "publication_date": "2026-10-29", "case_number": "1234",
                            "assessment": {"parcel_id": PID[:-4] + "2026"}}}


class LandQualificationTests(unittest.TestCase):
    def test_fixed_width_identity_is_namespace_and_county_scoped(self):
        for parcel in (PID, "043 061    01800 000 2027", "082 098    07600T000 2026"):
            self.assertEqual(parcel_control("comptroller_roll", "Test", parcel), ("comptroller_roll", "test", parcel[:-5]))
            self.assertEqual(parcel_control("county_gis", "Test", parcel)[2], parcel)
        for other in (PID.replace("000 2027", "001 2027"), PID.replace("I D", "D I"), " ".join(PID.split()) + " X"):
            self.assertNotEqual(parcel_control("comptroller_roll", "hardeman", PID),
                                parcel_control("comptroller_roll", "hardeman", other))
        self.assertNotEqual(parcel_control("county_gis", "davidson", "01"), parcel_control("county_gis", "knox", "01"))

    def test_research_blocks_legacy_and_stale_ready_at_paid_and_decision_gates(self):
        row = tract()
        for old in ("land", {"lane": "land", "readiness": {"ready": True}}):
            row["raw_payload"]["property_lane"] = old
            self.assertFalse(land_gate(row))
            self.assertFalse(screen(row, row["raw_payload"], TODAY)[0])
            self.assertEqual(DecisionEngineBot()._fast_rules(row)["action"], "HOLD_FOR_DATA")
        self.assertFalse(property_lane(row)["readiness"]["ready"])
        self.assertTrue(land_gate({"distress_type": "PROBATE", "raw_payload": {}}))

    def test_real_producer_shape_qualifies_independent_notice_without_sale_authority(self):
        row = tract()
        event = source_event(tax(), TODAY)
        lane = property_lane(row, [event])
        review = lane["land_qualification"]
        self.assertEqual(review["status"], "corroborated_opportunity", review)
        self.assertEqual(review["events"][0]["publication_date"], "2026-10-29")
        self.assertFalse(review["sale_authority_verified"])
        self.assertFalse(review["outreach_eligible"])
        self.assertTrue(lane["readiness"]["ready"], lane)
        row["raw_payload"]["property_lane"] = _tag(lane)
        self.assertTrue(land_gate(row, TODAY))
        self.assertTrue(screen(row, row["raw_payload"], TODAY)[0])
        # Last checked ownership is provenance, never a synthetic expiry.
        self.assertTrue(land_gate(row, TODAY + timedelta(days=90)))
        client = MagicMock()
        client.table.return_value.select.return_value.in_.return_value.execute.return_value.data = [
            {**tax(), "staging_status": "rejected", "rejection_reason": "Superseded county record"}]
        self.assertFalse(land_gate(row, TODAY, client))
        row["raw_payload"]["land"]["status"] = "ambiguous_existing_records"
        self.assertFalse(land_gate(row, TODAY))

    def test_ownership_alone_and_assessor_linked_estate_stay_research(self):
        row = tract()
        self.assertEqual(qualify(row, [])["reasons"], ["no_independent_parcel_event"])
        probate = {**tax(), "bot_source": "court_notices", "distress_type": "PROBATE",
                   "raw_payload": {"publication_date": "2026-10-29", "court_notice": {"docket": "PR-26"},
                                   "comptroller_parcel": {**row["raw_payload"]["comptroller_parcel"], "method": "owner_name"}}}
        event = source_event(probate, TODAY)
        self.assertIsNone(event["printed_parcel"])
        self.assertEqual(event["evidence_level"], "assessor_link_only")
        self.assertEqual(qualify(row, [event])["reasons"], ["assessor_link_only"])
        self.assertFalse(property_lane(row, [event])["readiness"]["ready"])
        self.assertEqual(qualify(row, [event, source_event(tax(), TODAY)])["status"], "corroborated_opportunity")

    def test_exact_join_survives_roll_year_but_not_subparcel_owner_or_county_change(self):
        row = tract()
        event = source_event(tax(), TODAY)
        for changed in ({"parcel_id": PID.replace("000 2027", "001 2027")}, {"county": "wilson"},
                        {"parcel_namespace": "county_gis"}, {"owner": "SOMEONE ELSE"}):
            self.assertEqual(qualify(row, [{**event, **changed}])["status"], "research_only")
        changed = tax()
        changed["raw_payload"]["parcel"] += "9"
        self.assertIsNone(source_event(changed, TODAY))
        self.assertFalse(qualify(row, [{**event, "sale_date": "2026-11-21"}])["status"] == "corroborated_opportunity")

    def test_collector_reads_indexed_summaries_then_selected_primary_keys(self):
        table = MagicMock()
        client = MagicMock()
        client.table.return_value = table
        page = table.select.return_value.eq.return_value.gt.return_value.order.return_value.limit.return_value
        summary = {"id": "one", "county": "hardeman", "distress_type": "TAX_LIEN", "method": None, "kind": "tax_collection_suit"}
        page.execute.side_effect = [MagicMock(data=[summary])] + [MagicMock(data=[])] * 5
        details = table.select.return_value.in_.return_value
        details.execute.return_value.data = [tax()]
        events = collect_events(client, TODAY)
        self.assertEqual(len(events[parcel_control("comptroller_roll", "hardeman", PID)]), 1)
        table.select.return_value.in_.assert_called_once_with("id", ["one"])
        self.assertEqual(table.select.return_value.eq.call_count, 6)
        self.assertTrue(all(call.args[0] == "bot_source" for call in table.select.return_value.eq.call_args_list))

    def test_specific_outcomes_preserved(self):
        for outcome in ("marked_paid", "past_sale_date", "past_listing", "withdrawn", "stayed"):
            record = tax()
            record["raw_payload"]["wilson_tax_review"] = {"status": outcome, "source_url": "https://example.test/results"}
            event = source_event(record, TODAY)
            self.assertEqual(outcome, event["status"])
            self.assertIn(outcome, qualify(tract(), [event])["reasons"])
        url = "https://montgomerytn.gov/storage/departments/chancery/result.xlsx"
        record = {"id": "result", "bot_source": "montgomery_tax_sale", "distress_type": "TAX_LIEN",
                  "county": "montgomery", "staging_status": "rejected", "source_url": url,
                  "raw_payload": {"tax_sale": {"sold": True, "sold_for": 4000, "buyer": "Bidder",
                  "owner": "Estate", "parcel": "001", "list_url": url, "sale_date": "2026-10-01"}}}
        self.assertEqual(source_event(record, TODAY)["status"], "sold")

    def test_report_groups_by_control_even_for_same_situs(self):
        a = tract()
        b = deepcopy(a)
        b["id"] = "land-b"
        c = deepcopy(a)
        c["id"] = "another-parcel"
        b["raw_payload"]["comptroller_parcel"]["parcelid"] = PID[:-4] + "2026"
        c["raw_payload"]["comptroller_parcel"]["parcelid"] = PID.replace("01700", "01800")
        rows = [{"row": row, "lane": {"land_qualification": {}, "grade": "B"}} for row in (a, b, c)]
        self.assertEqual(len(_dedupe(rows)), 2)
