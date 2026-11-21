import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.bots._free_data import county_patch, hmda_principal_repair, notice_phone_repair, entity_eligible, professional_contact_ready
from src.bots.auto_promoter_bot import AutoPromoterBot, _is_defensible, SHARED_COLUMNS
from src.bots.phone_resolver_bot import PhoneResolverBot
from src.bots.hmda_enricher_bot import HmdaEnricherBot
from src.bots.shelby_free_chain_bot import ShelbyFreeChainBot, evidence_stack
from src.bots.free_lien_review_bot import review
from src.enrichment.shortlist import register_system
from tests.test_shelby_register import DOT, RESULTS


class EmptyClient:
    def __getattr__(self, name):
        if name == "execute":
            return lambda: SimpleNamespace(data=[])
        return lambda *a, **kw: self


class FreeDataTests(unittest.TestCase):
    def test_shelby_request_budget_goes_to_queue_first(self):
        class Pages(EmptyClient):
            pages = iter([[{"id": "first", "raw_payload": {}},
                           {"id": "priority", "raw_payload": {"lien_check": {"status": "pending_free_lookup"}}}], []])
            def execute(self):
                return SimpleNamespace(data=next(self.pages))
        bot = ShelbyFreeChainBot(write=False, max_fetches=1)
        seen = []
        def enrich(row):
            seen.append(row["id"])
            bot.fetched += 1
            return {}
        with patch("src.bots.shelby_free_chain_bot._supabase", return_value=Pages()), patch.object(bot, "enrich", side_effect=enrich):
            bot.run()
        self.assertEqual(seen, ["priority"])

    def test_hmda_writer_never_changes_balance(self):
        class Capture(EmptyClient):
            def update(self, payload):
                self.payload = payload
                return self
        client = Capture()
        match = dict(principal=200000, candidate_count=1, lender_name="Example",
                     lei="x", match_year=2020, interest_rate=4, loan_purpose=1,
                     loan_term=360, tract="1", anchor_value=250000, sale_anchored=True)
        HmdaEnricherBot()._write(client, {"id": "test", "__table__": "homeowner_requests_staging",
                                       "mortgage_balance": 150000}, match)
        self.assertNotIn("mortgage_balance", client.payload)
        self.assertEqual(client.payload["phone_metadata"]["mortgage_signal"]["kind"], "original_principal")

    def test_missing_shelby_detail_stays_unresolved(self):
        row = {"raw_payload": {"deed_of_trust": {"instrument": "19001234", "year": "2019"}}}
        bot = ShelbyFreeChainBot(write=False, max_fetches=1)
        with patch.object(bot, "fetch", return_value=SimpleNamespace(status_code=200, text="Detail unavailable")):
            result = bot.enrich(row)
        raw = result["raw_payload"]
        self.assertEqual(raw["shelby_chain"]["status"], "needs_parcel_review")
        self.assertFalse(raw["lien_check"]["stack"]["payoff_verified"])
        self.assertEqual(raw["lien_check"]["stack"]["liens"], [])

    def test_hmda_is_not_current_balance_even_with_high_confidence(self):
        signal = {"source": "hmda_match", "amount": 200000, "confidence": .99, "sale_anchored": True}
        row = {"mortgage_balance": 200000, "phone_metadata": {"mortgage_signal": signal}}
        self.assertEqual(_is_defensible(row["phone_metadata"]), (False, ""))
        self.assertIsNone(hmda_principal_repair(row)["mortgage_balance"])
        row["mortgage_balance"] = 175000
        self.assertEqual(hmda_principal_repair(row), {})
        row["mortgage_balance"] = 200000
        row["phone_metadata"]["rod_lookup"] = {"verified": True}
        self.assertEqual(hmda_principal_repair(row), {})

    def test_county_match_required_and_stronger_values_preserved(self):
        row = {"property_value": None, "raw_payload": {"comptroller_parcel": {
            "method": "owner_name", "appraisal": 200000, "owner": "DOE JANE"}}}
        self.assertEqual(county_patch(row), {})
        row["raw_payload"]["comptroller_parcel"]["method"] = "address"
        self.assertEqual(county_patch(row)["property_value"], 200000)
        row.update(property_value=300000, property_value_source="verified_appraisal")
        self.assertNotIn("property_value", county_patch(row))
        self.assertNotIn("property_value_source", county_patch(row))

    def test_existing_county_value_gets_correct_source(self):
        row = {"property_value": 200000, "raw_payload": {"comptroller_parcel": {
            "method": "land_lane", "appraisal": 200000}}}
        self.assertEqual(county_patch(row)["property_value_source"], "comptroller")
        self.assertIn("property_value_source", SHARED_COLUMNS)
        self.assertIn("year_built", SHARED_COLUMNS)

    def test_permit_cost_is_not_appraisal(self):
        row = {"bot_source": "stalled_build", "property_value": 500000,
               "raw_payload": {"metro_parcel": {"appraisal": 200000}, "permit": {"Const_Cost": 500000}}}
        self.assertEqual(county_patch(row)["property_value_source"], "permit_construction_cost_proxy")

    def test_legacy_notice_number_moves_without_losing_it(self):
        row = {"phone": "+16155551234", "phone_metadata": {"phone_resolver": {
            "phone": "615-555-1234", "source": "notice_body"}}, "raw_payload": {"keep": 1}}
        result = notice_phone_repair(row)
        self.assertIsNone(result["phone"])
        self.assertEqual(result["raw_payload"]["keep"], 1)
        self.assertEqual(result["raw_payload"]["public_contact_candidates"][0]["phone"], row["phone"])
        row["phone_metadata"]["batchdata_skip_trace"] = {"owner_name_matched": True}
        self.assertEqual(notice_phone_repair(row), {})

    def test_entity_and_professional_gate(self):
        self.assertTrue(entity_eligible({"distress_type": "CHURCH_CLOSURE"}))
        self.assertFalse(entity_eligible({"raw_payload": {"property_lane": {
            "lane": "commercial", "readiness": {"ready": False}}}}))
        row = {"raw_payload": {"b2b_contacts": {"trustee": {"name": "Jane", "phone": "+16155551234"}}}}
        self.assertTrue(professional_contact_ready(row))
        self.assertFalse(professional_contact_ready({"raw_payload": {"public_contact_candidates": [{"phone": "123"}]}}))

    def test_no_twilio_initialization_when_parked_or_dry(self):
        for enabled, sample in [("0", "0"), ("1", "1")]:
            with self.subTest(enabled=enabled, sample=sample), patch.dict("os.environ", {
                    "FALCO_PAID_DATA_ENABLED": enabled, "FALCO_AUTO_PROMOTE_SAMPLE": sample}), \
                    patch("src.bots.auto_promoter_bot._supabase", return_value=EmptyClient()), \
                    patch.object(AutoPromoterBot, "_report_health"), \
                    patch("src.bots.auto_promoter_bot.get_twilio_client") as twilio:
                AutoPromoterBot().run()
                twilio.assert_not_called()

    def test_resolver_does_not_discover_homeowner_numbers_from_notes(self):
        row = {"admin_notes": "615-555-1234", "phone": "+16155551234", "raw_payload": {}}
        self.assertEqual(PhoneResolverBot()._gather_candidates(row), [])

    def test_shelby_queue_has_free_processor(self):
        self.assertEqual(register_system("Shelby County"), "shelby_register_free")
        result = review({"county": "Shelby", "raw_payload": {"lien_check": {"status": "queued"}}})
        self.assertEqual(result["raw_payload"]["lien_check"]["next_action"], "shelby_free_chain")
        self.assertFalse(result["raw_payload"]["lien_check"]["spend_authorized"])

    def test_partial_release_does_not_clear_entire_loan(self):
        docs = {"111": {"doc_type": "MORTGAGE", "file_date": "2020-01-01", "mortgage_amount": 200000},
                "222": {"doc_type": "PARTIAL RELEASE", "file_date": "2021-01-01", "detail_read": True,
                        "xrefs": [{"instrument": "111"}]}}
        stack = evidence_stack(docs)
        self.assertNotEqual(stack["liens"][0]["status"], "released")
        self.assertIsNone(stack["liens"][0]["balance_high"])
        self.assertIsNone(stack["equity"])
        docs["222"]["doc_type"] = "RELEASE"
        self.assertEqual(evidence_stack(docs)["liens"][0]["status"], "released")

    def test_shelby_budget_preserves_progress(self):
        row = {"raw_payload": {"deed_of_trust": {"instrument": "19001234", "year": "2019"}},
               "property_address": None}
        bot = ShelbyFreeChainBot(write=False, max_fetches=1)
        with patch.object(bot, "fetch", return_value=SimpleNamespace(status_code=200, text=DOT)):
            result = bot.enrich(row)
        chain = result["raw_payload"]["shelby_chain"]
        self.assertIn("19001234", chain["documents"])
        self.assertFalse(chain["finished"])
        self.assertEqual(bot.fetched, 1)
        self.assertIn("property_address", result)


if __name__ == "__main__":
    unittest.main()
