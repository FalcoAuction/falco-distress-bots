"""Mackie report completeness/date revalidation; synthetic records, no remote writes."""
import copy
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
from src.bots._base import BotBase,LeadPayload
from src.bots._mackie_refresh import inspect_report,finalize_report,refresh_patch,refresh_staging,protected,same_property

URL='https://mwzmlaw.com/wp-content/uploads/2026/11/TN-Sale-Report-as-of-11.20.2026.pdf'
CASE='26-000049-514'

def lead(**kw):
    return LeadPayload(bot_source='mackie_wolf_trustee',pipeline_lead_key=BotBase.make_lead_key('mackie_wolf',CASE),
                       property_address='1410 Old Stone Road, Lebanon, TN 37087',county='Wilson',
                       trustee_sale_date=kw.pop('trustee_sale_date','2027-01-14'),raw_payload={'file_no':CASE,'borrowers':[]},**kw)

def row(**kw):
    obj=lead()
    return dict(id='one',pipeline_lead_key=obj.pipeline_lead_key,county='Wilson County',
                property_address=obj.property_address,trustee_sale_date='2026-12-03',
                raw_payload={'file_no':CASE,'report_date':'2026-11-17'},phone_metadata={},
                staging_status='promoted',rejection_reason=None,**kw)

def report(text=None, linked=True, today=date(2026,11,22)):
    text=text if text is not None else '1/14/2027 '+CASE+' 1410 Old Stone Road Wilson\nLebanon TN 37087'
    class Pdf:
        pages=[SimpleNamespace(extract_text=lambda:text)]
        def __enter__(self): return self
        def __exit__(self,*a): pass
    extractor=SimpleNamespace(open=lambda stream:Pdf())
    result=inspect_report(b'fixture',URL,linked,extractor,today)
    result['checked_at']='2026-11-22T12:00:00+00:00'
    finalize_report(result,[lead()])
    return result

class MackieRefreshTests(unittest.TestCase):
    def test_current_exact_file_property_changes_date_and_confirms_live(self):
        r=row();r['sale_date_last_seen_at']='2026-11-17T12:00:00+00:00'
        result=refresh_patch(r,report(),lead())
        self.assertEqual(result['trustee_sale_date'],'2027-01-14')
        self.assertEqual(result['sale_date_last_seen_at'],report()['checked_at'])
        self.assertEqual(result['phone_metadata']['notice_tracking']['published_sale_date'],'2027-01-14')

    def test_missing_from_complete_is_hold_never_cancellation_or_date_erase(self):
        snapshot=report();snapshot['file_counts']={};snapshot['complete']=True
        result=refresh_patch(row(),snapshot)
        self.assertEqual(set(result),{'phone_metadata'})
        review=result['phone_metadata']['trustee_notice_review']
        self.assertEqual(review['detail'],'missing_from_current_report')
        self.assertEqual(review['reason'],'stale_notice')
        self.assertNotIn('sale_status',result['phone_metadata'])
        self.assertEqual(review['sale_date'],'2026-12-03')

    def test_printed_malformed_tail_is_not_absence(self):
        snapshot=report('1/14/2027 '+CASE+' malformed line without tail')
        finalize_report(snapshot,[])
        self.assertFalse(snapshot['complete'])
        self.assertEqual(refresh_patch(row(),snapshot)['phone_metadata']['trustee_notice_review']['detail'],'published_unparsed')
        snapshot['file_counts']={}
        self.assertIsNone(refresh_patch(row(),snapshot))

    def test_duplicates_and_blank_pages_prevent_absence(self):
        snapshot=report('1/14/2027 '+CASE+' x\n1/14/2027 '+CASE+' y')
        self.assertFalse(snapshot['complete'])

        self.assertEqual(refresh_patch(row(),snapshot)['phone_metadata']['trustee_notice_review']['detail'],'ambiguous_file')
        snapshot=report();snapshot['pages_readable']=False
        finalize_report(snapshot,[lead()])
        self.assertFalse(snapshot['complete'])

    def test_unreadable_file_number_on_a_dated_row_prevents_absence(self):
        snapshot=report('1/14/2027 '+CASE+' 1410 Old Stone Road Wilson\nLebanon TN 37087\n1/15/2027 broken-file-number property')
        self.assertFalse(snapshot['complete'])

    def test_fallback_or_stale_report_never_confirms_or_removes(self):
        for snapshot in (report(linked=False),report(today=date(2026,12,1))):
            self.assertIsNone(refresh_patch(row(),snapshot,lead()))
            self.assertIsNone(refresh_patch(row(),snapshot))

    def test_manual_status_manual_date_and_newer_report_protected(self):
        for pm in ({'sale_status':{'status':'postponed','updated_by':'operator'}},
                   {'trustee_notice_review':{'reason':'stale_notice','updated_by':'operator'}},
                   {'notice_tracking':{'published_sale_date':'2026-12-10'}},
                   {'notice_tracking':{'report_as_of':'2026-11-23'}}):
            r=row();r['phone_metadata']=pm
            self.assertIsNone(refresh_patch(r,report(),lead()))

    def test_wrong_unit_county_or_file_never_changes_date(self):
        for key,value in (('property_address','1410 Old Stone Road Apt B, Lebanon, TN 37087'),('county','Shelby')):
            r=row();r[key]=value
            result=refresh_patch(r,report(),lead())
            self.assertNotIn('trustee_sale_date',result)
            self.assertEqual(result['phone_metadata']['trustee_notice_review']['detail'],'identity_mismatch')
        r=row();r['pipeline_lead_key']='other-feed-key'
        self.assertIsNone(refresh_patch(r,report(),lead()))

    def test_relisting_clears_only_source_hold(self):
        r=row();r['phone_metadata']={'trustee_notice_review':{'reason':'stale_notice','updated_by':'mackie_wolf_trustee'},'other':{'keep':True}}
        result=refresh_patch(r,report(),lead())
        self.assertNotIn('trustee_notice_review',result['phone_metadata'])
        self.assertEqual(result['phone_metadata']['other'],{'keep':True})

    def test_same_asof_changed_pdf_requires_review_without_overwriting_date(self):
        r=row();r['phone_metadata']={'notice_tracking':{'report_as_of':'2026-11-20','report_sha256':'different'}}
        result=refresh_patch(r,report(),lead())
        self.assertNotIn('trustee_sale_date',result)
        self.assertEqual(result['phone_metadata']['trustee_notice_review']['detail'],'report_version_conflict')

    def test_both_copies_guarded_and_live_manual_blocks_staging(self):
        for manual in (False,True):
            client=MagicMock();staging=MagicMock();live=MagicMock()
            client.table.side_effect=lambda t:staging if t.endswith('_staging') else live
            a=row();b=row();b['id']='live';b['sale_date_last_seen_at']=None
            if manual: b['phone_metadata']={'sale_status':{'status':'postponed'}}
            for table,record in ((staging,a),(live,b)):
                q=table.select.return_value
                q.eq.return_value=q.in_.return_value=q.order.return_value=q.limit.return_value=q
                q.execute.return_value.data=[record]
                u=table.update.return_value
                u.eq.return_value=u.is_.return_value=u.filter.return_value=u
                u.execute.return_value.data=[record]
            bot=SimpleNamespace(_report=report(),logger=MagicMock())
            with patch('src.bots._mackie_refresh._supabase',return_value=client):
                refresh_staging(bot,[lead()])
            for table in (staging,live):
                if manual: table.update.assert_not_called()
                else:
                    q=table.update.return_value
                    q.eq.assert_any_call('trustee_sale_date','2026-12-03')
                    q.filter.assert_called_once_with('phone_metadata','eq','{}')

if __name__=='__main__':unittest.main()
