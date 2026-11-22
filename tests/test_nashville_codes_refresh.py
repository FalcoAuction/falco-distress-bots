"""Nashville case acquisition/revalidation; no production writes or network."""
import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from src.bots.nashville_codes_bot import (NashvilleCodesBot, case_groups, refresh_candidates,
                                         refresh_patch, INACTIVE_REASON, REVIEW_REASON)

TODAY='2026-11-22'
def attrs(**kw):
    return {'ObjectId':1,'Request_Nbr':'26-123','Property_APN':'00102003000',
            'Property_Address':'100 FARM RD','Property_Owner':'JANE DOE','City':'Nashville',
            'Status':'OPEN','Violations_Noted':'UNSAFE STRUCTURE','Reported_Problem':'Vacant building',
            'Last_Activity_Date':1795000000000, **kw}

def stored(**kw):
    return {'id':'one','pipeline_lead_key':NashvilleCodesBot.make_lead_key('nashville_codes','26-123'),
            'staging_status':'pending','rejection_reason':None,
            'raw_payload':{'nashville_codes':attrs(),'unrelated':{'keep':True},'condition':['photo_verified']}, **kw}

class NashvilleRefreshTests(unittest.TestCase):
    def test_keyset_uses_actual_objectid_not_requested_page_size(self):
        bot=NashvilleCodesBot()
        responses=[SimpleNamespace(status_code=200,json=lambda:{'features':[{'attributes':attrs(ObjectId=5)}], 'exceededTransferLimit':True}),
                   SimpleNamespace(status_code=200,json=lambda:{'features':[{'attributes':attrs(ObjectId=11,Request_Nbr='26-124')}], 'exceededTransferLimit':False})]
        with patch.object(bot,'fetch',side_effect=responses) as fetch:
            rows=bot._query("Status='OPEN'")
        self.assertEqual(len(rows),2)
        self.assertIn('ObjectId > 5',fetch.call_args_list[1].kwargs['params']['where'])
        self.assertEqual(fetch.call_args.kwargs['params']['resultRecordCount'],'1000')

    def test_source_errors_never_become_empty_snapshot(self):
        bot=NashvilleCodesBot()
        for data in ({'error':{'code':500}}, {'features':[], 'exceededTransferLimit':True}, {}):
            with self.subTest(data=data), patch.object(bot,'fetch',return_value=SimpleNamespace(status_code=200,json=lambda:data)):
                with self.assertRaises(RuntimeError): bot._query('1=1')

    def test_done_and_closed_hold_pending_without_assuming_repaired(self):
        for status in ('DONE','CLOSED'):
            result=refresh_patch(stored(),case_groups([attrs(Status=status)]),TODAY)
            self.assertEqual(result['rejection_reason'],INACTIVE_REASON)
            self.assertFalse(result['raw_payload']['nashville_codes_review']['current_code_signal'])
            self.assertEqual(result['raw_payload']['unrelated'],{'keep':True})
            self.assertEqual(result['raw_payload']['condition'],['photo_verified'])

    def test_missing_and_ambiguous_are_not_closed(self):
        for records,status in (([], 'not_found'), ([attrs(),attrs(Property_APN='other')], 'ambiguous_case'),
                               ([attrs(Property_APN='other')], 'identity_mismatch')):
            result=refresh_patch(stored(),case_groups(records),TODAY)
            self.assertEqual(result['raw_payload']['nashville_codes_review']['status'],status)
            self.assertEqual(result['raw_payload']['nashville_codes']['Status'],'OPEN')
            self.assertEqual(result['rejection_reason'],REVIEW_REASON)

    def test_only_own_holds_reopen_manual_and_promoted_preserved(self):
        for status,reason in (('rejected','manual do not contact'), ('promoted',None), ('verified',None)):
            result=refresh_patch(stored(staging_status=status,rejection_reason=reason),case_groups([attrs(Status='DONE')]),TODAY)
            self.assertNotIn('staging_status',result)
        result=refresh_patch(stored(staging_status='rejected',rejection_reason=INACTIVE_REASON),case_groups([attrs()]),TODAY)
        self.assertEqual(result['staging_status'],'pending')
        self.assertIsNone(result['rejection_reason'])

    def test_live_evidence_does_not_mutate_stage_or_owner(self):
        row=stored()
        del row['staging_status']; del row['rejection_reason']
        result=refresh_patch(row,case_groups([attrs(Status='DONE',Property_Owner='NEW OWNER')]),TODAY)
        self.assertEqual(set(result),{'raw_payload'})
        self.assertTrue(result['raw_payload']['nashville_codes_review']['owner_changed'])
        result2=refresh_patch({**row,**result},case_groups([attrs(Status='DONE',Property_Owner='NEW OWNER')]),'2026-11-23')
        self.assertTrue(result2['raw_payload']['nashville_codes_review']['owner_changed'])

    def test_no_closed_new_lead_and_conflicting_case_excluded(self):
        bot=NashvilleCodesBot()
        self.assertIsNone(bot._build_lead(attrs(Status='DONE')))
        with patch.object(bot,'_query',return_value=[attrs(),attrs(Property_APN='other')]):
            self.assertEqual(bot.scrape(),[])

    def test_current_same_day_idempotent_and_later_change_recorded(self):
        row=stored()
        result=refresh_patch(row,case_groups([attrs()]),TODAY)
        self.assertIsNone(refresh_patch({**row,**result},case_groups([attrs()]),TODAY))
        result2=refresh_patch({**row,**result},case_groups([attrs(Status='DONE')]),TODAY)
        self.assertEqual(result2['raw_payload']['nashville_codes_changes'][-1]['changes']['Status']['after'],'DONE')

    def test_rotation_and_same_day_retry_advance(self):
        from datetime import date,timedelta
        rows=[{'id':str(i),'case_num':str(i),'checked_on':None} for i in range(5841)]
        seen=set()
        for day in range(30):
            batch=refresh_candidates(rows,(date.fromisoformat(TODAY)+timedelta(days=day)).isoformat())
            self.assertLessEqual(len(batch),200)
            seen.update(r['id'] for r in batch)
        self.assertEqual(len(seen),5841)
        first=refresh_candidates(rows,TODAY)
        for row in first: row['checked_on']=TODAY
        self.assertFalse({r['id'] for r in first}&{r['id'] for r in refresh_candidates(rows,TODAY)})


class RefreshFlowTests(unittest.TestCase):
    def test_discovery_and_refresh_share_citation_rule(self):
        bot=NashvilleCodesBot()
        for citation in ('', 'HIGH WEEDS'):
            record=attrs(Violations_Noted=citation, Reported_Problem='unsafe collapsing building')
            self.assertIsNone(bot._build_lead(record))
            self.assertFalse(refresh_patch(stored(),case_groups([record]),TODAY)['raw_payload']['nashville_codes_review']['current_code_signal'])
        fresh=bot._build_lead(attrs())
        self.assertTrue(fresh.raw_payload['nashville_codes_review']['current_code_signal'])

    def flow(self, failure=False, conflict=False):
        db={'homeowner_requests_staging':[stored()], 'homeowner_requests':[stored(id='live')]}
        db['homeowner_requests'][0].pop('staging_status')
        db['homeowner_requests'][0].pop('rejection_reason')
        class Query:
            def __init__(self,table): self.table=table;self.filters=[];self.values=None
            def select(self,cols): return self
            def eq(self,key,value):
                if key=='raw_payload':
                    import json
                    value=json.loads(value)
                if key not in ('source','bot_source'): self.filters.append((key,value))
                elif self.table=='homeowner_requests':
                    if (key,value)!=('source','bot'): raise AssertionError('wrong live source schema')
                return self
            def in_(self,key,values): self.filters.append((key,values));return self
            def is_(self,key,value): self.filters.append((key,None));return self
            def update(self,values): self.values=values;return self
            def execute(self):
                matched=[r for r in db[self.table] if all(r.get(k) in v if isinstance(v,list) else r.get(k)==v for k,v in self.filters)]
                if self.values is not None:
                    if conflict: return SimpleNamespace(data=[])
                    for r in matched: r.update(copy.deepcopy(self.values))
                return SimpleNamespace(data=copy.deepcopy(matched))
        bot=NashvilleCodesBot()
        candidates=[{'id':'one','pipeline_lead_key':stored()['pipeline_lead_key'],'case_num':'26-123','checked_on':None}]
        with patch('src.bots.nashville_codes_bot._supabase',return_value=SimpleNamespace(table=Query)), \
             patch.object(bot,'_candidates',return_value=candidates), \
             patch.object(bot,'_query',side_effect=RuntimeError('offline') if failure else None,return_value=[attrs(Status='DONE')]), \
             patch('src.bots.nashville_codes_bot.BotBase._write_staging') as insert:
            # A rediscovered OPEN row must not create another copy of an existing case.
            bot._write_staging([bot._build_lead(attrs())])
            insert.assert_not_called()
        return db,bot

    def test_actual_write_path_updates_live_evidence_and_staging_status(self):
        db,bot=self.flow()
        self.assertEqual(db['homeowner_requests_staging'][0]['staging_status'],'rejected')
        self.assertEqual(db['homeowner_requests'][0]['raw_payload']['nashville_codes']['Status'],'DONE')
        self.assertEqual(bot.refresh_counts['updated'],2)

    def test_failed_source_and_concurrent_change_keep_prior_records(self):
        for failure,conflict in ((True,False),(False,True)):
            db,bot=self.flow(failure,conflict)
            self.assertEqual(db['homeowner_requests_staging'][0]['staging_status'],'pending')
            self.assertEqual(db['homeowner_requests'][0]['raw_payload']['nashville_codes']['Status'],'OPEN')
            self.assertEqual(bot.refresh_counts['updated'],0)


if __name__ == '__main__':
    unittest.main()
