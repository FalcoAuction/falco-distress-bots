"""Official Chattanooga replay and guarded recurring status refresh, no network writes."""
import copy
import csv
import io
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from src.bots.chattanooga_codes_bot import (ChattanoogaCodesBot, parse_snapshot, case_review,
    refresh_patch, refresh_candidates, REVIEW_KEY, INACTIVE_REASON, REVIEW_REASON)

MODIFIED = 1795170639000

def record(**kw):
    return dict(case_number='CE-123', record_id='v1', status='INPR', street_number='404',
                street_name='TUNNEL BLVD', state='TN', city='CHATTANOOGA',
                description='EXTERIOR WALLS', description_extended='', comments='',
                flag_dangerous='false', date_corrected='', date_entered='2026-01-01', **kw)

def changed(**kw):
    r=record();r.update(kw);return r

def content(rows):
    stream=io.StringIO(); w=csv.DictWriter(stream,fieldnames=record().keys());w.writeheader();w.writerows(rows)
    return stream.getvalue().encode()

def snapshot(rows=None):
    s=parse_snapshot(content(rows if rows is not None else [record()]),MODIFIED)
    s['checked_on']='2026-11-22';return s

def stored(**kw):
    r={'id':'one','pipeline_lead_key':ChattanoogaCodesBot.make_lead_key('chattanooga_codes','CE-123|404 TUNNEL BLVD'),
       'property_address':'404 TUNNEL BLVD, Chattanooga, TN','county':'Hamilton County',
       'staging_status':'pending','rejection_reason':None,'phone_metadata':{'manual_note':'preserve'},
       'raw_payload':{'chattanooga_codes':{'case_number':'CE-123','rows':[record()]},
                      'hamilton_assessor':{'parcel':'147I-K-028.01','appraised':6685000},
                      'chattanooga_parcel_review':{'status':'ambiguous'}}}
    r.update(kw);return r

class CaseTests(unittest.TestCase):
    def test_schema_empty_and_truncated_sources_rejected(self):
        for data in (b'html error',content([]),content([record()]).rsplit(b',',4)[0]):
            with self.subTest(data=data[:30]), self.assertRaises((ValueError,csv.Error)):
                parse_snapshot(data,MODIFIED)

    def test_statuses_not_conflated(self):
        for official,status in [('C','closed'),('LIT','litigation_review'),('DUP','duplicate'),('OUT','outside_jurisdiction'),('NEW','unknown'),('O','active')]:
            e=case_review(snapshot([changed(status=official)]),'CE-123','404 TUNNEL BLVD')
            self.assertEqual(e['status'],status)
            self.assertEqual(e['current_code_signal'],official=='O')

    def test_corrected_serious_citation_cannot_borrow_open_nuisance_status(self):
        rows=[changed(date_corrected='2026-11-01'),changed(record_id='v2',description='HIGH GRASS')]
        e=case_review(snapshot(rows),'CE-123','404 TUNNEL BLVD')
        self.assertEqual(e['status'],'active');self.assertFalse(e['current_code_signal'])
        self.assertEqual(e['corrected_count'],1)
        self.assertEqual(case_review(snapshot(rows[:1]),'CE-123','404 TUNNEL BLVD')['status'],'corrected')

    def test_comment_alone_is_not_structural_citation(self):
        e=case_review(snapshot([changed(description='GRASS',comments='dangerous roof foundation')]),'CE-123','404 TUNNEL BLVD')
        self.assertFalse(e['current_code_signal'])

    def test_duplicate_observations_deduped_but_conflicts_held(self):
        e=case_review(snapshot([record(),changed(city='Chattanooga')]),'CE-123','404 TUNNEL BLVD')
        self.assertEqual(e['serious_active_count'],1)
        e=case_review(snapshot([record(),changed(status='C')]),'CE-123','404 TUNNEL BLVD')
        self.assertEqual(e['status'],'ambiguous');self.assertFalse(e['current_code_signal'])

    def test_missing_and_different_address_not_closed(self):
        for r,state in [(changed(case_number='OTHER'),'not_found'),(changed(street_number='405'),'identity_mismatch')]:
            result=refresh_patch(stored(),snapshot([r]))
            self.assertEqual(result['phone_metadata'][REVIEW_KEY]['status'],state)
            self.assertEqual(result['rejection_reason'],REVIEW_REASON)

    def test_original_case_key_and_current_property_binding_required(self):
        self.assertIsNone(refresh_patch(stored(pipeline_lead_key='wrong'),snapshot()))
        wrong=stored();wrong['raw_payload']['chattanooga_codes']['rows'][0]['case_number']='OTHER'
        self.assertIsNone(refresh_patch(wrong,snapshot()))
        result=refresh_patch(stored(property_address='405 TUNNEL BLVD, Chattanooga, TN'),snapshot())
        self.assertEqual(result['phone_metadata'][REVIEW_KEY]['status'],'identity_mismatch')

    def test_manual_and_promoted_states_preserved_only_own_holds_reopen(self):
        for state,reason in [('rejected','manual'),('promoted',None),('verified',None)]:
            result=refresh_patch(stored(staging_status=state,rejection_reason=reason),snapshot([changed(status='C')]))
            self.assertNotIn('staging_status',result)
        result=refresh_patch(stored(staging_status='rejected',rejection_reason=INACTIVE_REASON),snapshot())
        self.assertEqual(result['staging_status'],'pending')

    def test_no_owner_parcel_or_entire_complex_inference(self):
        row=stored();original=copy.deepcopy(row);p=refresh_patch(row,snapshot())
        self.assertEqual(row,original);self.assertNotIn('raw_payload',p)
        e=p['phone_metadata'][REVIEW_KEY]
        self.assertFalse(e['owner_verified']);self.assertFalse(e['sale_authority_verified'])
        self.assertEqual(e['evidence_scope'],'individual_case_not_whole_parcel')
        self.assertNotIn('prospect_key',e)
        self.assertEqual(e['serious_active_records'][0]['record_id'],'v1')
        self.assertEqual(p['phone_metadata']['manual_note'],'preserve')

    def test_large_case_stays_refreshable_and_clipping_is_explicit(self):
        from urllib.parse import quote
        import json
        rows=[changed(record_id=str(i)) for i in range(80)]
        row=stored();s=snapshot(rows);p=refresh_patch(row,s)
        e=p['phone_metadata'][REVIEW_KEY]
        self.assertEqual(e['serious_active_count'],80)
        self.assertEqual(e['record_count'],80)
        self.assertTrue(e['evidence_truncated'])
        self.assertGreaterEqual(len(e['serious_active_records']),1)
        self.assertLessEqual(len(quote(json.dumps(p['phone_metadata']),safe='')),5500)
        self.assertIsNone(refresh_patch({**row,**p},s))

    def test_separate_cases_keep_observations_without_multiplying_parcel_value(self):
        rows=[record(),changed(case_number='CE-124')];s=snapshot(rows)
        first=refresh_patch(stored(),s)
        second_row=stored(pipeline_lead_key=ChattanoogaCodesBot.make_lead_key('chattanooga_codes','CE-124|404 TUNNEL BLVD'))
        second_row['raw_payload']['chattanooga_codes']={'case_number':'CE-124','rows':[rows[1]]}
        second=refresh_patch(second_row,s)
        self.assertNotEqual(first['phone_metadata'][REVIEW_KEY]['case_number'],second['phone_metadata'][REVIEW_KEY]['case_number'])
        self.assertNotIn('raw_payload',first);self.assertNotIn('raw_payload',second)

    def test_same_day_noop_and_new_version_refresh(self):
        row=stored();s=snapshot();p=refresh_patch(row,s);row.update(p)
        self.assertIsNone(refresh_patch(row,s))
        s2=snapshot([changed(status='C')]);self.assertIsNone(refresh_patch(row,s2))
        s2['source_modified_on']='2026-11-22T18:00:00+00:00'
        self.assertEqual(refresh_patch(row,s2)['rejection_reason'],INACTIVE_REASON)

    def test_fair_rotation_retries_and_new_source_same_day(self):
        s=snapshot();rows=[{'id':str(i).zfill(4)} for i in range(1000)]
        first=refresh_candidates(rows,s);self.assertEqual(len(first),400)
        for r in first:r['review']={k:s[k] for k in ('checked_on','source_sha256')}
        second=refresh_candidates(rows,s);self.assertFalse({r['id'] for r in first}&{r['id'] for r in second})
        s['source_sha256']='changed';self.assertEqual(len(refresh_candidates(rows,s)),400)

    def test_acquisition_and_failed_source_write_safeguard(self):
        bot=ChattanoogaCodesBot()
        metadata=SimpleNamespace(status_code=200,json=lambda:{'modified':MODIFIED})
        csv_response=SimpleNamespace(status_code=200,content=content([record(),changed(case_number='CLOSED',status='C')]))
        with patch.object(bot,'fetch',side_effect=[metadata,csv_response,metadata]):leads=bot.scrape()
        self.assertEqual(len(leads),1)
        with patch.object(bot,'fetch',return_value=None),self.assertRaises(RuntimeError):bot.scrape()
        with patch('src.bots.chattanooga_codes_bot._supabase') as client:
            self.assertEqual(bot._write_staging([]),(0,0));client.assert_not_called()

    def test_publication_changes_during_download_never_stage_partial_snapshot(self):
        bot=ChattanoogaCodesBot()
        before=SimpleNamespace(status_code=200,json=lambda:{'modified':MODIFIED})
        after=SimpleNamespace(status_code=200,json=lambda:{'modified':MODIFIED+1})
        response=SimpleNamespace(status_code=200,content=content([record()]))
        with patch.object(bot,'fetch',side_effect=[before,response,after]),self.assertRaises(RuntimeError):bot.scrape()
        self.assertIsNone(bot._snapshot)

class Query:
    def __init__(self,db,table):self.db=db;self.table=table;self.filters=[];self.payload=None
    def select(self,*a):return self
    def order(self,*a):return self
    def limit(self,*a):return self
    def eq(self,k,v):self.filters.append((k,v));return self
    def is_(self,k,v):return self.eq(k,None)
    def in_(self,k,v):return self.eq(k,v)
    def filter(self,k,op,v):return self.eq(k,v)
    def update(self,p):self.payload=p;return self
    def execute(self):
        if self.payload is not None:
            self.db.writes.append(self)
            if self.db.fail and self.table.endswith('_staging'):raise RuntimeError('isolated failure')
            return SimpleNamespace(data=[{'id':'ok'}])
        return SimpleNamespace(data=[copy.deepcopy(self.db.stage if self.table.endswith('_staging') else self.db.live)])

class FlowTests(unittest.TestCase):
    def test_real_write_path_stage_and_live_and_isolated_failure(self):
        for fail in (False,True):
            db=SimpleNamespace(stage=stored(),live=stored(id='live'),writes=[],fail=fail)
            del db.live['staging_status'];del db.live['rejection_reason']
            db.table=lambda table:Query(db,table)
            bot=ChattanoogaCodesBot();bot._snapshot=snapshot([changed(status='C')])
            with patch.object(bot,'_candidates',return_value=iter([{'id':'one','pipeline_lead_key':db.stage['pipeline_lead_key']}])),patch('src.bots.chattanooga_codes_bot._supabase',return_value=db):bot._write_staging([])
            self.assertEqual(len(db.writes),2)
            self.assertEqual(bot.refresh_counts['updated'],1 if fail else 2)
            self.assertEqual(bot.refresh_counts['failed'],int(fail))
            self.assertNotIn('staging_status',db.writes[-1].payload)
            self.assertIn(('source','bot'),db.writes[-1].filters)
            self.assertTrue(any(k=='phone_metadata' for k,v in db.writes[-1].filters))
            self.assertNotIn('raw_payload',db.writes[-1].payload)

if __name__=='__main__':unittest.main()
