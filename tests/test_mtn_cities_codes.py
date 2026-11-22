from unittest.mock import Mock, patch
import unittest
from src.bots import mtn_cities_codes_bot as mod


def issue(description="The roof is collapsing.", **changes):
    return {"id": 123, "status": "Open", "address": "12 Elm St Mount Juliet, Tennessee, 37122",
            "request_type": {"organization": mod.CODES_ORG, "title": "Property maintenance issue"},
            "created_at": "2026-11-10T12:00:00Z", "description": description, **changes}


class MtJulietTests(unittest.TestCase):
    def test_complaints_require_specific_building_evidence(self):
        bot = mod.MtnCitiesCodesBot()
        assert bot.review(issue())["finding"] == "reported_building_condition"
        for text in ("Repair the bicycle pump.", "Abandoned vehicle", "No evidence of structural damage.", "Trash, debris, rats", "Vacant house for sale. Grass overgrown.", "A dangerous structure being built across the street"):
            assert bot.review(issue(text))["finding"] == "other_request"
        assert bot.review(issue(status="Archived"))["finding"] == "closed_request"
        assert bot.review(issue(address="Mt Juliet TN"))["finding"] == "other_request"
        assert bot.review(issue(request_type={"organization": "Other Juliet"})) is None


    def test_discovery_and_exact_historical_refresh(self):
        bot = mod.MtnCitiesCodesBot()
        page = {"issues": [issue()], "metadata": {"pagination": {"page": 1, "entries": 1, "next_page": None}}}
        bot.fetch = Mock(return_value=Mock(status_code=200, json=lambda:page))
        lead, = bot.scrape()
        assert lead.owner_name_records is lead.trustee_sale_date is None
        assert lead.raw_payload[mod.REVIEW]["sale_authority_verified"] is False
        row = {"id": "one", "bot_source":bot.name, "issue_id":"123","pipeline_lead_key":lead.pipeline_lead_key,
               "county":lead.county,"property_address":lead.property_address,"phone_metadata":{"manual":"keep"}}
        bot.issues.clear()
        bot.fetch = Mock(return_value=Mock(status_code=200, json=lambda:issue(status="Archived")))
        assert bot.existing_review({**row, "property_address":"another property"}) is None
        assert bot.existing_review({**row, "pipeline_lead_key":"another case"}) is None
        table = Mock()
        table.select.return_value.eq.return_value.order.return_value.limit.return_value.execute.return_value.data = [row]
        update = table.update.return_value
        update.eq.return_value = update
        update.filter.return_value.execute.return_value.data = [row]
        with patch.object(mod, "_supabase") as client:
            client.return_value.table.return_value = table
            assert bot._write_staging([]) == (0,0)
        payload = table.update.call_args.args[0]
        assert set(payload) == {"phone_metadata"}
        assert payload["phone_metadata"]["manual"] == "keep"
        assert payload["phone_metadata"][mod.REVIEW]["finding"] == "closed_request"
        bot.fetch.return_value = None
        assert bot.existing_review(row) is None


    def test_partial_feed_never_refreshes_or_inserts(self):
        bot = mod.MtnCitiesCodesBot()
        page = {"issues":[issue()], "metadata":{"pagination":{"page":1,"entries":2,"next_page":2}}}
        bot.fetch = Mock(side_effect=[Mock(status_code=200,json=lambda:page), None])
        with self.assertRaisesRegex(RuntimeError, "page 2"):
            bot.scrape()
        with patch.object(mod,"_supabase") as client:
            assert bot._write_staging([]) == (0,0)
            client.assert_not_called()
