import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from src.bots import _court_notices as cn
from src.bots._base import BotBase
from src.bots._probate_staging import refresh_patch, remember_notice
from src.bots.court_notices_bot import CourtNoticesBot, build_lead
from src.bots.probate_property_enricher_bot import _lookup_subject, _ambiguous_probate_match
from src.bots.tn_public_notice_bot import TnPublicNoticeBot
from tests.test_court_notices import DAVIDSON, MONTGOMERY_FOR


def notice(lines=DAVIDSON, day=18):
    return build_lead("court_notices", "ledger", "https://example.test", date(2026, 11, day),
                      {"notice_id": "L123", "lines": lines})


class ProbateFollowupTests(unittest.TestCase):
    def test_conflicting_printed_decedents_hold_name_matching(self):
        lines = ["ESTATE OF Kil-Joong Kim, (Deceased)",
                 "Case No. MC-CH-CV-PB-26-307",
                 "Notice to creditors. Letters Testamentary in respect of the Estate of Kil-Joon Kim, "
                 "who died 7/7/2026, were issued by the Probate Court of Montgomery County, Tennessee.",
                 "This 10th day of November, 2026", "Jennifer Juney Park - Executor",
                 "Attorney for the Estate:", "B. Nathan Hunt"]
        lead = notice(lines)
        review = lead.raw_payload["probate_review"]
        self.assertEqual(review["decedent_names_printed"], ["Kil-Joong Kim", "Kil-Joon Kim"])
        self.assertIn("conflicting_decedent_names", review["attorney_draft_hold_reasons"])
        self.assertEqual(review["attorney_draft_status"], "hold")
        self.assertIsNone(_lookup_subject(vars(lead)))
        clean = notice([line.replace("Kil-Joong", "KIL-JOON") for line in lines])
        self.assertNotIn("conflicting_decedent_names", clean.raw_payload["probate_review"]["attorney_draft_hold_reasons"])
        self.assertEqual(_lookup_subject(vars(clean)), "Kil-Joon Kim")
        old = {**lead.raw_payload, "probate_review": clean.raw_payload["probate_review"]}
        self.assertEqual(refresh_patch({"raw_payload": old}, lead)["raw_payload"]["probate_review"], review)

    def test_draft_and_seller_have_separate_scopes(self):
        review = notice().raw_payload["probate_review"]
        self.assertEqual(review["estate_key"], "davidson|26P9001")
        self.assertEqual(review["stage"], "appointment_reported")
        self.assertEqual(review["contact_hold_scope"], "owner_seller_solicitation")
        self.assertTrue(review["contact_hold"])
        self.assertEqual(review["attorney_draft_status"], "requires_verification")
        self.assertEqual(review["attorney_draft_hold_reasons"], [])
        self.assertFalse(review["sale_authority_verified"])

    def test_printed_roles_are_distinct_from_property_authority(self):
        lines = DAVIDSON[:DAVIDSON.index("Fay Fictional")] + [
            "Robin Lane - Administratrix", "Attorney for the Estate:", "Jordan Jones",
            "233-A Example Road", "Clarksville, TN 37041", "Attorney for creditor: Riley Claims",
        ]
        lead = notice(lines)
        fields = lead.raw_payload["court_notice"]
        self.assertEqual([(r["name"], r["role"]) for r in fields["representatives"]],
                         [("Robin Lane", "administrator")])
        self.assertEqual(fields["attorney"], {"names": ["Jordan Jones"],
                                              "address": ["233-A Example Road", "Clarksville, TN 37041"]})
        self.assertIsNone(lead.property_address)
        self.assertFalse(lead.raw_payload["probate_review"]["sale_authority_verified"])
        self.assertTrue(lead.raw_payload["probate_review"]["contact_hold"])
        prior = {**lead.raw_payload, "court_notice": {**fields, "representatives": []}}
        patch = refresh_patch({"raw_payload": prior}, lead)
        self.assertEqual(patch["raw_payload"]["court_notice"], fields)
        self.assertIsNone(refresh_patch({"raw_payload": patch["raw_payload"]}, lead))

    def test_other_signers_do_not_supply_estate_roles(self):
        for lines in (["Morgan Example", "Attorney for the Estate: Riley Counsel"],
                      ["Morgan Example, Executor's agent", "Attorney for the Estate: Riley Counsel"]):
            parsed = cn._parse_signers(lines)
            self.assertEqual(parsed["representatives"], [])
            self.assertEqual(parsed["attorney"]["names"], ["Riley Counsel"])
        for lines in (["Riley Claims, Attorney for Creditor"], ["Attorney for Creditor:", "Riley Claims"],
                      ["Riley Claims", "Attorney for Morgan Example, Executor's agent"]):
            self.assertEqual(cn._parse_signers(lines), {"representatives": [], "attorney": {"names": [], "address": []}})

    def test_actual_closed_expired_and_removed_clauses(self):
        for clause in ("the estate has been closed", "Letters Testamentary were revoked",
                       "the case was dismissed", "Letters of Authority expired", "the executor was discharged"):
            with self.subTest(clause=clause):
                lead = notice(DAVIDSON + [clause])
                review = lead.raw_payload["probate_review"]
                self.assertEqual(review["stage"], "estate_inactive_reported")
                self.assertIn(review["evidence"].lower(), clause.lower())
                self.assertIn("inactive_case_or_representative", review["attorney_draft_hold_reasons"])
                self.assertEqual(review["attorney_draft_status"], "hold")
        self.assertEqual(cn.classify(["Estate of Jo Sample, deceased. The estate is closed."]), cn.KIND_PROBATE)

    def test_sale_stages_do_not_grant_authority(self):
        fields = cn.extract_probate(DAVIDSON)
        for text, stage in [
            ("Notice to creditors", "creditor_notice"),
            ("Petition for permission to sell real property", "sale_authority_requested"),
            ("The court ordered the sale of the real property", "sale_order_reported"),
            ("The Clerk and Master will sell at public auction", "sale_advertised"),
            ("The sale has been stayed", "sale_interrupted"),
        ]:
            with self.subTest(stage=stage):
                review = cn.probate_review([text], fields)
                self.assertEqual(review["stage"], stage)
                self.assertFalse(review["sale_authority_verified"])

    def test_excerpt_preserves_historical_id_and_separates_property_from_people(self):
        bot = TnPublicNoticeBot()
        lead = bot._build_lead({"notice_id": "1234", "county": "coffee", "excerpt":
            "Notice to Creditors. Estate of Jo Sample, Deceased. Correspondence to 123 Lawyer Street."},
            "PROBATE", "Notice to Creditors")
        self.assertEqual(lead.pipeline_lead_key, BotBase.make_lead_key(bot.name, "1234"))
        self.assertEqual(lead.full_name, "Jo Sample")
        self.assertIsNone(lead.property_address)
        self.assertIsNone(lead.owner_name_records)
        self.assertIn("no_named_representative", lead.raw_payload["probate_review"]["attorney_draft_hold_reasons"])
        self.assertIsNone(_lookup_subject(vars(lead)))

    def test_legacy_spaced_case_identity_survives_changes(self):
        lead = notice(MONTGOMERY_FOR)
        expected = BotBase.make_lead_key("court_notice", "probate_notice_to_creditors|montgomery|MCCHCVPB-26-999")
        self.assertEqual(lead.pipeline_lead_key, expected)
        changed = notice(MONTGOMERY_FOR + ["The estate is closed."], 19)
        self.assertEqual(changed.pipeline_lead_key, expected)
        old = {**lead.raw_payload, "owner_match": {"parcel": "example"}}
        merged = refresh_patch({"raw_payload": old}, changed)["raw_payload"]
        self.assertEqual(merged["owner_match"], old["owner_match"])
        self.assertEqual(merged["probate_review"]["stage"], "estate_inactive_reported")
        self.assertIsNone(refresh_patch({"raw_payload": merged}, lead))
        self.assertIsNone(refresh_patch({"raw_payload": merged}, changed))

    def test_name_only_match_uses_distinct_parcels(self):
        estate = {"distress_type": "PROBATE"}
        match = {"confidence": "name_only", "parcels": [{"parcel": "A"}, {"parcel": " a "}]}
        self.assertFalse(_ambiguous_probate_match(estate, match))
        match["parcels"].append({"parcel": "B"})
        self.assertTrue(_ambiguous_probate_match(estate, match))
        self.assertFalse(_ambiguous_probate_match({"distress_type": "PARTITION"}, match))

    def test_closure_wins_over_repeat_in_both_scrape_orders(self):
        closed = notice(DAVIDSON + ["The estate is closed."], 19)
        repeat = notice(day=20)
        for items in ((closed, repeat), (repeat, closed)):
            group = {}
            for item in items:
                remember_notice(group, item)
            self.assertEqual(list(group.values()), [closed])

    def test_historical_terminal_and_live_keys_never_restage(self):
        bot = CourtNoticesBot()
        lead = notice()
        for status in ("promoted", "rejected", "live_only"):
            with self.subTest(status=status), patch("src.bots._probate_staging._supabase") as db, patch.object(
                    BotBase, "_write_staging", return_value=(0, 0)) as base:
                table = MagicMock()
                db.return_value.table.return_value = table
                staging = [] if status == "live_only" else [{"pipeline_lead_key": lead.pipeline_lead_key,
                                                              "staging_status": status, "id": "existing", "raw_payload": lead.raw_payload}]
                live = [{"pipeline_lead_key": lead.pipeline_lead_key}] if status == "live_only" else []
                table.select.return_value.eq.return_value.in_.return_value.execute.side_effect = [
                    MagicMock(data=staging), MagicMock(data=live)]
                self.assertEqual(bot._write_staging([lead, lead]), (0, 2))
                base.assert_called_once_with([])
                table.update.assert_not_called()

    def test_legacy_pending_is_upgraded_in_place_preserving_enrichment(self):
        lead = notice()
        raw = {**lead.raw_payload, "owner_match": {"parcel": "123"}}
        del raw["probate_review"]
        update = refresh_patch({"raw_payload": raw}, lead)
        self.assertEqual(update["raw_payload"]["owner_match"], {"parcel": "123"})
        self.assertEqual(update["raw_payload"]["probate_review"], lead.raw_payload["probate_review"])
        self.assertIsNone(refresh_patch({"raw_payload": update["raw_payload"]}, lead))

    def test_recurring_pending_and_promoted_refresh_are_guarded(self):
        lead = notice()
        for status in ("pending", "promoted"):
            with self.subTest(status=status), patch("src.bots._probate_staging._supabase") as db, patch.object(
                    BotBase, "_write_staging", return_value=(0, 0)) as base:
                table = MagicMock()
                db.return_value.table.return_value = table
                raw = {**lead.raw_payload, "independent_evidence": {"keep": True}}
                raw.pop("probate_review")
                table.select.return_value.eq.return_value.in_.return_value.execute.side_effect = [
                    MagicMock(data=[{"id": "existing", "pipeline_lead_key": lead.pipeline_lead_key,
                                     "staging_status": status, "full_name": "Operator name", "raw_payload": raw}]),
                    MagicMock(data=[])]
                self.assertEqual(CourtNoticesBot()._write_staging([lead]), (0, 1))
                update = table.update.call_args.args[0]
                self.assertEqual(set(update), {"raw_payload", "source_url"})
                self.assertEqual(update["raw_payload"]["independent_evidence"], {"keep": True})
                self.assertEqual(update["raw_payload"]["probate_review"], lead.raw_payload["probate_review"])
                guarded = table.update.return_value.eq.return_value.eq.return_value.eq.return_value.eq.return_value
                guarded.filter.assert_called_once()
                guarded.filter.return_value.execute.assert_called_once()
                base.assert_called_once_with([])

    def test_failed_identity_read_never_reinserts(self):
        with patch("src.bots._probate_staging._supabase") as db, patch.object(
                BotBase, "_write_staging", return_value=(0, 0)) as base:
            db.return_value.table.return_value.select.side_effect = RuntimeError("unavailable")
            CourtNoticesBot()._write_staging([notice()])
            base.assert_called_once_with([])
            db.return_value.table.return_value.update.assert_not_called()

    def test_conflicting_existing_case_identity_cannot_refresh(self):
        lead = notice()
        for field, value in (("decedent", "Another Person"), ("county", "shelby"), ("docket", "26P9999")):
            with self.subTest(field=field):
                raw = {**lead.raw_payload, "court_notice": {**lead.raw_payload["court_notice"], field: value}}
                self.assertIsNone(refresh_patch({"raw_payload": raw}, lead))
