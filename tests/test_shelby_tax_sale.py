import unittest
import base64
import json
from datetime import date
from unittest.mock import patch

from src.bots.shelby_tax_sale_bot import EXTRACT, ShelbyTaxSaleBot, parcel_evidence, scheduled_parcels, staging_row


PAGE = f'<a href="{EXTRACT}">CSV</a><table><tr><td>2302</td><td>04/13/2027</td></tr></table>'
HEADER = 'ParcelID,Alt_Parcel,Street Number,Street Name,Tax Sale\n'
ROW = '001001  00001,00100100000010,0,MAIN,TS2302\n'
TODAY = date(2026, 11, 21)


class ShelbyTaxTests(unittest.TestCase):
    def test_dedupe_by_exact_parcel_and_sale(self):
        lead, = scheduled_parcels(PAGE, HEADER + ROW * 2, TODAY)
        self.assertIsNone(lead.property_address)
        self.assertEqual(lead.trustee_sale_date, '2027-04-13')
        self.assertEqual(staging_row(lead, 'one')['id'], staging_row(lead, 'two')['id'])
        self.assertEqual(scheduled_parcels(PAGE, HEADER + ROW, date(2027, 4, 14)), [])

    def test_malformed_snapshot_cannot_retire_existing_records(self):
        for content in (HEADER, HEADER + ROW.replace('TS2302', 'TS9999'), HEADER + ROW + ROW.replace('MAIN', 'ELM')):
            with self.assertRaises(ValueError):
                scheduled_parcels(PAGE, content, TODAY)
        with patch('src.bots.shelby_tax_sale_bot._supabase') as db:
            db.return_value.table.return_value.select.return_value.eq.return_value.gt.return_value                 .order.return_value.limit.return_value.execute.return_value.data = []
            self.assertEqual(ShelbyTaxSaleBot()._write_staging([]), (0, 0))
            db.return_value.table.return_value.update.assert_not_called()
            db.return_value.table.return_value.upsert.assert_not_called()
            db.return_value.rpc.assert_not_called()

    def test_owner_and_balance_require_matching_sale_and_parcel(self):
        html = '<span>Parcel ID:</span>001001  00001<span>Primary Owner</span>Example LLC<span>Total Balance</span>$120.00<span>Tax Sale ID:</span>TS2302'
        facts = {'parcel': '001001  00001', 'sale_id': 'TS2302'}
        e = parcel_evidence(html, facts, TODAY)
        self.assertEqual(e['primary_owner'], 'Example LLC')
        self.assertEqual(e['total_tax_balance'], 120)
        self.assertNotIn('estimated_appraisal', e)
        self.assertFalse(parcel_evidence(html, dict(facts, sale_id='TS2301'), TODAY))
        self.assertFalse(parcel_evidence(html, dict(facts, parcel='other'), TODAY))

    def test_estimate_requires_current_county_realty_bill_for_exact_parcel(self):
        facts = {'sale_id': 'TS2302', 'parcel': '001001  00001', 'alt_parcel': '00100100000010'}
        bill = {'year': '2026', 'altParcelId': facts['alt_parcel'], 'classification': 'R',
                'description': 'MEMPHIS REALTY', 'assessment': 15075}
        def detail():
            encoded = base64.urlsafe_b64encode(json.dumps(bill).encode()).decode().rstrip('=')
            html = ('<span>Parcel ID:</span>001001  00001<span>Tax Sale ID:</span>TS2302'
                    f'<table><tr><td><a href="Transaction?billNumberData={encoded}">123</a></td>'
                    '<td>2026</td><td>Shelby</td><td>$300</td><td>$0</td><td>$0</td>'
                    '<td>$300</td><td>$15,075.00</td></tr></table>')
            return parcel_evidence(html, facts, TODAY)
        self.assertEqual(detail()['estimated_appraisal'], 60300)
        bill['classification'] = 'unknown'
        self.assertNotIn('estimated_appraisal', detail())
        bill.update(classification='R', altParcelId='wrong parcel')
        self.assertNotIn('assessed_value', detail())
