import html
import json
import unittest
from unittest.mock import Mock, patch

from src.bots.hud_reo_bot import DETAIL, HudReoBot


class HudHomeStoreTests(unittest.TestCase):
    row = {"propertyCaseNumber": "481-001234", "propertyState": "TN",
           "propertyAddress": "101 Main St", "propertyCity": "Bartlett",
           "listPrice": "120000", "bidOpenDate": "11/23/2026"}

    def page(self, rows, state="TN"):
        payload = html.escape(json.dumps(rows), quote=True)
        return Mock(status_code=200, content=(
            f'<input id="cityStateZip" value="{state}"><input id="available_prop" value="{payload}">'
        ).encode())

    def observe(self, response):
        bot = HudReoBot()
        with patch.object(bot, "fetch", return_value=response), patch.object(bot, "_report_health") as report, \
                patch.object(bot, "_write_staging") as write:
            result = bot.run()
            write.assert_not_called()
            self.assertEqual(result["staged"], 0)
            self.assertEqual(report.call_count, 2)
            return result, report.call_args.kwargs["notes"]

    def test_market_inventory_is_a_health_receipt(self):
        result, receipt = self.observe(self.page([self.row]))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["fetched"], 1)
        listing, = receipt["listings"]
        self.assertEqual(listing["source_url"], DETAIL + "481-001234")
        self.assertEqual(listing["bid_open_date"], "11/23/2026")
        self.assertNotIn("trustee_sale_date", listing)

    def test_explicit_empty(self):
        result, receipt = self.observe(self.page([]))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(receipt["listings"], [])

    def test_unusable_sources_do_not_claim_an_empty_inventory(self):
        for response in (None, Mock(status_code=403), Mock(status_code=200, content=b"maintenance"),
                         self.page([], "VA"), self.page({}), self.page([self.row, self.row]),
                         self.page([dict(self.row, propertyState="MS")]),
                         self.page([dict(self.row, propertyCaseNumber="")])):
            with self.subTest(response=response):
                result, receipt = self.observe(response)
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(receipt)
