from datetime import date, timedelta
import unittest

from src.bots._personal_property import classify, dedupe_key
from src.bots._property_lane import property_lane
from src.bots._warn_gate import is_tn_warn
from src.enrichment.shortlist import screen


def record():
    employer = "Example Beverages"
    return {"distress_type": "BUSINESS_CLOSURE", "bot_source": "warn_notices", "county": "davidson",
            "full_name": employer, "trustee_sale_date": "2099-01-01",
            "raw_payload": {"warn": {"company": employer, "workers": 500, "notice": "test",
                            "effective": "2099-01-01", "letter_url": "https://www.tn.gov/warn/letter.pdf"}}}


def asset_review():
    fact = {"verified_on": date.today().isoformat(), "source_url": "https://example.org/inventory"}
    return {"subject_name": "Example Beverages",
            "assets": [{**fact, "kind": "equipment", "asset_id": "01", "description": "Identified bottling line"}],
            "availability": dict(fact, status="available"), "controller": dict(fact, name="Officer", role="manager", authority="verified"),
            "liens": dict(fact, status="reviewed"), "deadline": dict(fact, status="none_documented")}


class WarnQualificationTests(unittest.TestCase):
    def test_employment_date_and_assessed_worksite_never_become_auction_facts(self):
        row = record()
        row["raw_payload"]["warn"]["worksite_parcel"] = {"county_appraisal": 9648500, "recorded_owner": "Landlord"}
        tag = classify(row)
        self.assertEqual(tag["qualification"], "research")
        for key in ("deadline", "est_value_min", "controller", "route_to"):
            self.assertIsNone(tag[key])
        self.assertFalse(property_lane(row)["readiness"]["ready"])
        self.assertFalse(screen(row, row["raw_payload"], date.today())[0])

    def test_independent_inventory_can_be_researched_and_reviewed(self):
        row = record()
        ev = asset_review()
        row["raw_payload"]["personal_property_evidence"] = ev
        ev["valuation"] = dict(ev["availability"], amount=50000)
        tag = classify(row)
        self.assertEqual(tag["est_value_min"], 50000)
        self.assertTrue(tag["routable"])
        self.assertEqual(tag["controller"], "Officer")
        self.assertIsNone(tag["deadline"])
        ev.pop("controller")
        tag = classify(row)
        self.assertEqual(tag["qualification"], "qualified_prospect")
        self.assertFalse(tag["routable"])
        self.assertIsNone(dedupe_key(tag, row, row["raw_payload"]))
        ev["assets"][0]["source_url"] = row["raw_payload"]["warn"]["letter_url"] + "?copy=1"
        self.assertEqual(classify(row)["qualification"], "research")

    def test_terminal_and_wrong_subject_override_cached_tags(self):
        row = record()
        ev = asset_review()
        row["raw_payload"]["personal_property_evidence"] = ev
        ev["subject_name"] = "Worksite Landlord"
        self.assertFalse(classify(row)["prospect"])
        ev["subject_name"] = row["full_name"]
        row["raw_payload"]["warn"].update(asset_disposition="sold", disposition_source="https://example.org/results")
        row["raw_payload"]["warn_review"] = {"asset_disposition": "unverified"}
        tag = classify(row)
        self.assertEqual(tag["qualification"], "hold")
        self.assertFalse(tag["routable"])
        self.assertEqual(tag["disposition_source"], "https://example.org/results")

    def test_other_proceeding_at_same_company_remains_independent(self):
        row = record()
        row.update(bot_source="bankruptcy_events", distress_type="BANKRUPTCY")
        row.update(full_name="Example Beverages Inc.")
        row["raw_payload"].update(chapter="7")
        row["raw_payload"]["warn"].update(asset_disposition="sold")
        with_notice = classify(row)
        del row["raw_payload"]["warn"]
        self.assertEqual(with_notice, classify(row))
        self.assertEqual(with_notice["lane"], "business_liquidation")
        self.assertFalse(is_tn_warn(row))
        self.assertFalse(is_tn_warn({"distress_type":"BUSINESS_CLOSURE", "bot_source":"fl_sunbiz"}))
