import unittest
from unittest.mock import MagicMock, patch

from src.bots.tn_lis_pendens_bot import TnLisPendensBot, publication_stage
from src.bots.tn_public_notice_bot import CATEGORY_TO_DISTRESS, TnPublicNoticeBot
from src.bots._base import LeadPayload


class PublicationReviewTests(unittest.TestCase):
    def test_sale_requires_sale_language(self):
        for t in ("NOTICE OF SUBSTITUTE TRUSTEE'S SALE", "Foreclosure Sale Notice"):
            self.assertEqual(publication_stage(t), "foreclosure_sale_publication")
        self.assertEqual(publication_stage("Reference to a deed of trust"), "excerpt_unconfirmed")
        self.assertEqual(publication_stage("Notice of public hearing in foreclosure district"), "public_hearing")
        self.assertNotIn("__lp_supplement", CATEGORY_TO_DISTRESS)

    def test_same_url_and_type_dedup_only(self):
        bot = TnLisPendensBot()
        def lead(url):
            return LeadPayload(bot_source=bot.name, pipeline_lead_key=url, source_url=url, distress_type="PRE_FORECLOSURE")
        same, different, extra = [lead(url) for url in ("same", "different", "extra")]
        client = MagicMock()
        client.table.return_value.select.return_value.eq.return_value.in_.return_value.in_.return_value.execute.return_value.data = [
            {"source_url": "same", "distress_type": "PRE_FORECLOSURE"},
            {"source_url": "different", "distress_type": "TAX_LIEN"}]
        with patch("src.bots._base._supabase", return_value=client), patch.object(TnPublicNoticeBot, "_write_staging", return_value=(2, 0)) as base:
            self.assertEqual(bot._write_staging([same, different, extra]), (2, 1))
            base.assert_called_once_with([different, extra])
