"""Free land acquisition and repeat-run regressions; no network or database service."""
import copy
import json
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from src.bots._land import lane_record, signals
from src.bots._land_gis import rutherford_row
from src.bots.land_lane_bot import LandLaneBot, tract_leads

TODAY = date(2026, 11, 22)


def row(**kw):
    out = rutherford_row({'ParcelID': '001 A 123.00', 'Owner1': 'SMITH JOHN ESTATE',
                          'CALCACRES': 40, 'TotalValue': 300000, 'AccountType': 'AGRICULTURAL',
                          'MailingState': 'TN', 'FormattedLocation': '100 FARM RD'})
    out.update(kw)
    return out


class Client:
    def __init__(self, stored):
        self.stored = stored
        self.writes = []
    def table(self, table):
        self.table_name = table
        self.guards = {}
        return self
    def update(self, values):
        self.values = values
        return self
    def eq(self, key, value):
        self.guards[key] = json.loads(value) if key == 'raw_payload' else value
        return self
    def is_(self, key, value):
        self.guards[key] = None
        return self
    def execute(self):
        if any(self.stored.get(k) != v for k, v in self.guards.items()):
            return SimpleNamespace(data=[])
        self.stored.update(copy.deepcopy(self.values))
        self.writes.append(copy.deepcopy(self.values))
        return SimpleNamespace(data=[self.stored])


class AcquisitionTests(unittest.TestCase):
    def bot(self):
        bot = LandLaneBot()
        bot._gis_complete = {'rutherford'}
        bot._covered = {'RUTHERFORD'}
        bot._index_path = None
        return bot

    def test_blank_and_conflicting_parcels_excluded_before_filters(self):
        bot = self.bot()
        self.assertEqual(tract_leads(None, bot, TODAY, [row(parcelid=''), row(parcelid=None)]), [])
        self.assertEqual(tract_leads(None, bot, TODAY, [row(), row(owner='NEW BUYER LLC')]), [])
        self.assertIn(('county_gis', 'rutherford', '001 A 123.00'), bot._ambiguous)
        self.assertEqual(len(tract_leads(None, bot, TODAY, [row(), row()])), 1)
        self.assertEqual(len(tract_leads(None, bot, TODAY, [row(), row(parcelid='001 A 124.00')])), 2)

    def test_dates_and_mailing_are_evidence_not_motivation(self):
        for stamp in ('1900', '19000101', '1900-01-01', '19991340', 'unknown'):
            r = row(owner='SMITH JOHN ETAL', state='GA', saledate=stamp)
            self.assertEqual(signals(r, TODAY), [])
            self.assertIsNone(lane_record(r, [], 'rutherford')['held_since'])
        for state in ('TENNESSEE', '?', 'XX', ''):
            self.assertEqual(signals(row(owner='SMITH JOHN', state=state, saledate='1988'), TODAY), [])
        self.assertEqual(signals(row(owner='SMITH JOHN', state='GA', saledate='1988'), TODAY), ['absentee'])
        self.assertEqual(signals(row(calcac=float('nan')), TODAY), [])

    def test_provenance_and_current_owner_refresh_preserves_review(self):
        bot = self.bot()
        old = tract_leads(None, bot, TODAY, [row()])[0]
        stored = {'id': 'one', 'pipeline_lead_key': old.pipeline_lead_key, 'county': old.county,
                  'owner_name_records': old.owner_name_records, 'full_name': old.full_name,
                  'property_value': old.property_value, 'raw_payload': copy.deepcopy(old.raw_payload)}
        stored['raw_payload']['land_action_evidence'] = {'review': 'keep'}
        client = Client(stored)
        leads = tract_leads(None, bot, TODAY, [row(owner='NEW BUYER LLC')])
        self.assertEqual(leads, [])
        with patch('src.bots.land_lane_bot._supabase', return_value=client), \
             patch.object(bot, '_existing', side_effect=lambda c, t: [copy.deepcopy(stored)] if t.endswith('_staging') else []):
            bot._write_staging(leads)
        self.assertEqual(stored['owner_name_records'], 'NEW BUYER LLC')
        self.assertEqual(stored['raw_payload']['land']['status'], 'no_longer_matches')
        self.assertIn('owner_of_record', stored['raw_payload']['land']['changed_fields'])
        self.assertEqual(stored['raw_payload']['land_action_evidence'], {'review': 'keep'})
        self.assertTrue(stored['raw_payload']['land']['source_url'].startswith('https://'))
        self.assertIsNotNone(stored['raw_payload']['land']['source_refreshed_on'])

    def test_failed_county_does_not_retire_existing_and_ambiguity_holds(self):
        bot = self.bot()
        old = tract_leads(None, bot, TODAY, [row()])[0]
        stored = {'id': 'one', 'pipeline_lead_key': old.pipeline_lead_key, 'county': old.county,
                  'owner_name_records': old.owner_name_records, 'full_name': old.full_name,
                  'property_value': old.property_value, 'raw_payload': copy.deepcopy(old.raw_payload)}
        client = Client(stored)
        bot._covered = set()
        with patch('src.bots.land_lane_bot._supabase', return_value=client), \
             patch.object(bot, '_existing', side_effect=lambda c, t: [copy.deepcopy(stored)] if t.endswith('_staging') else []):
            bot._write_staging([])
            self.assertEqual(client.writes, [])
            bot._covered = {'RUTHERFORD'}
            tract_leads(None, bot, TODAY, [row(), row(owner='OTHER OWNER')])
            bot._write_staging([])
        self.assertEqual(stored['raw_payload']['land']['status'], 'ambiguous_parcel')
        self.assertEqual(stored['owner_name_records'], old.owner_name_records)

    def test_new_limit_does_not_starve_later_parcels(self):
        bot = self.bot()
        leads = tract_leads(None, bot, TODAY, [row(parcelid=str(i), appraisal=400000-i) for i in range(3)])
        existing = [{'pipeline_lead_key': leads[0].pipeline_lead_key, 'raw_payload': {}}]
        with patch('src.bots.land_lane_bot._supabase', return_value=object()), \
             patch.object(bot, '_existing', return_value=existing), \
             patch('src.bots.land_lane_bot.MAX_LEADS', 1), \
             patch('src.bots.land_lane_bot.BotBase._write_staging', return_value=(1, 0)) as write:
            bot._write_staging(leads)
            chosen = write.call_args.args[0][0].pipeline_lead_key
            self.assertNotEqual(chosen, leads[0].pipeline_lead_key)
            existing.append({'pipeline_lead_key': chosen, 'raw_payload': {}})
            bot._write_staging(leads)
            self.assertNotIn(write.call_args.args[0][0].pipeline_lead_key,
                             {leads[0].pipeline_lead_key, chosen})

    def test_cached_observation_is_not_live_freshness(self):
        bot = self.bot()
        evidence = bot.land_evidence(row(county='SUMNER'), ['heirs'], TODAY)
        self.assertIsNone(evidence['source_refreshed_on'])
        self.assertTrue(evidence['source_url'].endswith('/Sumner.zip'))
        self.assertEqual(evidence['observed_on'], TODAY.isoformat())

    def test_index_duplicate_below_size_filter_still_blocks_staging(self):
        import sqlite3
        con = sqlite3.connect(':memory:')
        sample = row(county='SUMNER')
        fields = list(sample)
        con.execute('CREATE TABLE parcels (' + ','.join('"' + k + '"' for k in fields) + ')')
        for source in (sample, {**sample, 'owner': 'BUYER LLC', 'calcac': 1}):
            con.execute('INSERT INTO parcels VALUES (' + ','.join('?' for _ in fields) + ')',
                        [source[k] for k in fields])
        try:
            self.assertEqual(tract_leads(con, self.bot(), TODAY), [])
        finally:
            con.close()



class RefreshBudgetTests(unittest.TestCase):
    bot = AcquisitionTests.bot
    def stored(self, bot):
        old = tract_leads(None, bot, TODAY, [row()])[0]
        return {'id': 'one', 'pipeline_lead_key': old.pipeline_lead_key, 'county': old.county,
                'owner_name_records': old.owner_name_records, 'full_name': old.full_name,
                'property_value': old.property_value, 'raw_payload': copy.deepcopy(old.raw_payload)}

    def test_same_day_no_write_and_owner_change_stays_flagged(self):
        bot = self.bot()
        stored = self.stored(bot)
        client = Client(stored)
        tract_leads(None, bot, TODAY, [row(owner='NEW BUYER LLC')])
        with patch('src.bots.land_lane_bot._supabase', return_value=client), \
             patch.object(bot, '_existing', side_effect=lambda c, t: [copy.deepcopy(stored)] if t.endswith('_staging') else []):
            bot._write_staging([])
            first = len(client.writes)
            bot._write_staging([])
            self.assertEqual(len(client.writes), first)
            stored['raw_payload']['land']['observed_on'] = '2026-11-20'
            bot._write_staging([])
        self.assertTrue(stored['raw_payload']['land']['review_required'])
        self.assertIn('owner_of_record', stored['raw_payload']['land']['changed_fields'])

    def test_rotation_covers_1332_even_if_every_write_fails(self):
        from datetime import timedelta
        from src.bots.land_lane_bot import refresh_order, REFRESH_LIMIT
        existing = {'staging': [{'id': str(i)} for i in range(1332)]}
        seen = set()
        for day in range(7):
            seen.update(r['id'] for _, r in refresh_order(existing, TODAY + timedelta(days=day))[:REFRESH_LIMIT])
        self.assertEqual(len(seen), 1332)

    def test_large_payload_skips_and_small_row_failure_is_isolated(self):
        bot = self.bot()
        stored = self.stored(bot)
        stored['raw_payload']['other'] = 'x' * 10000
        stored['raw_payload']['land']['observed_on'] = '2000-01-01'
        client = Client(stored)
        with patch('src.bots.land_lane_bot._supabase', return_value=client), \
             patch.object(bot, '_existing', side_effect=lambda c, t: [copy.deepcopy(stored)] if t.endswith('_staging') else []):
            bot._write_staging([])
        self.assertEqual(bot.refresh_stats['oversized'], 1)
        self.assertEqual(client.writes, [])
        del stored['raw_payload']['other']
        with patch('src.bots.land_lane_bot._supabase', return_value=client), \
             patch.object(bot, '_existing', side_effect=lambda c, t: [copy.deepcopy(stored)] if t.endswith('_staging') else []), \
             patch.object(client, 'execute', side_effect=RuntimeError('fixture failure')):
            bot._write_staging([])
        self.assertEqual(bot.refresh_stats['failed'], 1)



class CanonicalAcquisitionTests(unittest.TestCase):
    bot = AcquisitionTests.bot
    def test_roll_year_refresh_does_not_insert_again_or_break_next_run(self):
        bot = self.bot()
        bot._covered = {'HUMPHREYS'}
        old_pid, new_pid = '043 061    01800 000 2026', '043 061    01800 000 2027'
        old = tract_leads(None, bot, TODAY, [row(county='HUMPHREYS', parcelid=old_pid)])[0]
        stored = {'id': 'one', 'pipeline_lead_key': old.pipeline_lead_key, 'county': old.county,
                  'owner_name_records': old.owner_name_records, 'full_name': old.full_name,
                  'property_value': old.property_value, 'raw_payload': copy.deepcopy(old.raw_payload)}
        client = Client(stored)
        leads = tract_leads(None, bot, TODAY, [row(county='HUMPHREYS', parcelid=new_pid)])
        with patch('src.bots.land_lane_bot._supabase', return_value=client), \
             patch.object(bot, '_existing', side_effect=lambda c, t: [copy.deepcopy(stored)] if t.endswith('_staging') else []), \
             patch('src.bots.land_lane_bot.BotBase._write_staging') as insert:
            bot._write_staging(leads)
            insert.assert_not_called()
            self.assertEqual(stored['raw_payload']['land']['parcel_id'], new_pid)
            self.assertEqual(stored['raw_payload']['land']['intake_parcel_id'], old_pid)
            bot._write_staging(leads)
            self.assertEqual(bot.refresh_stats['unchanged'], 1)
            self.assertEqual(stored['pipeline_lead_key'], old.pipeline_lead_key)

    def test_canonical_groups_preserve_map_and_subparcel(self):
        bot = self.bot()
        a = row(county='HUMPHREYS', parcelid='043 061    01800 000 2026')
        b = {**a, 'parcelid': '043 061    01800 000 2027'}
        self.assertEqual(len(tract_leads(None, bot, TODAY, [a, b])), 1)
        self.assertEqual(tract_leads(None, bot, TODAY, [a, {**b, 'owner': 'BUYER LLC'}]), [])
        for pid in ('043 061    01800 001 2027', '043 061K D 01800 000 2027', '043 061    01800T000 2027'):
            self.assertEqual(len(tract_leads(None, bot, TODAY, [a, {**b, 'parcelid': pid}])), 2)
        self.assertEqual(len(tract_leads(None, bot, TODAY, [row(parcelid='native 2026'), row(parcelid='native 2027')])), 2)



class ProductionShapeAndBudgetTests(unittest.TestCase):
    bot = AcquisitionTests.bot

    def test_live_select_uses_source_and_persisted_staging_keys(self):
        calls = []
        class Query:
            def __init__(self, table): self.table_name = table
            def select(self, columns):
                self.columns=columns
                self.filters=[]
                return self
            def eq(self,k,v):
                if self.table_name == 'homeowner_requests':
                    self_test.assertEqual((k,v), ('source','bot'))
                else:
                    self_test.assertEqual((k,v), ('bot_source','land_lane'))
                self.filters.append((k,v)); return self
            def in_(self,k,v): self.filters.append((k,list(v))); return self
            def order(self,k): return self
            def limit(self,n): return self
            def execute(self):
                calls.append((self.table_name,self.columns,self.filters))
                return SimpleNamespace(data=[])
        self_test=self
        client=SimpleNamespace(table=lambda table: Query(table))
        bot=self.bot()
        list(bot._existing(client, 'homeowner_requests_staging'))
        bot._live_keys=[str(i) for i in range(201)]
        list(bot._existing(client, 'homeowner_requests'))
        self.assertEqual(len(calls),4)
        self.assertEqual([len(c[2][1][1]) for c in calls[1:]], [100,100,1])
        self.assertTrue(all('pipeline_lead_key' in c[1] for c in calls))

    def test_1332_rows_never_exceed_200_update_attempts(self):
        bot=self.bot()
        leads=tract_leads(None,bot,TODAY,[row(parcelid=str(i)) for i in range(1332)])
        stored=[]
        for i, lead in enumerate(leads):
            raw=copy.deepcopy(lead.raw_payload)
            raw['land']['observed_on']='2000-01-01'
            stored.append({'id':str(i),'pipeline_lead_key':lead.pipeline_lead_key,'county':lead.county,
                           'owner_name_records':lead.owner_name_records,'full_name':lead.full_name,
                           'property_value':lead.property_value,'raw_payload':raw})
        client=Client(stored[0])
        with patch('src.bots.land_lane_bot._supabase',return_value=client), \
             patch.object(bot,'_existing',side_effect=lambda c,t: stored if t.endswith('_staging') else []), \
             patch.object(client,'execute',return_value=SimpleNamespace(data=[{}])) as execute, \
             patch.object(bot,'logger'):
            bot._write_staging([])
            self.assertEqual(execute.call_count,200)
            self.assertEqual(bot.refresh_stats['attempted'],200)
            execute.reset_mock()
            with patch('src.bots.land_lane_bot.time.monotonic',side_effect=[0,451]):
                bot._write_staging([])
            execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
