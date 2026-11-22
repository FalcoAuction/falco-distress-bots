import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from src.bots.knoxville_poh_bot import KnoxvillePohBot,discover_documents,hearing_date,review_patch,REVIEW_KEY

HTML='''<a href="/DocumentCenter/View/1255">View Current Public Officer Hearing Agenda (PDF)</a><a href="/DocumentCenter/View/1253">View Current Better Building Board Agenda (PDF)</a>'''
TEXT='''PUBLIC OFFICER HEARING
MEETING AGENDA
OCTOBER 30, 2026
ADDRESS: 2208 ADAIR AVENUE RECORD NUMBER: CE-26-006926
CLT NUMBER: 070PK007 OFFICER: EXAMPLE
CITY TAXES: $633.85 (2025) FIRST INSPECTION: 09/08/2026
OWNERS AND OTHER INTERESTED PARTIES:
- EXAMPLE OWNER
- EXAMPLE DECEASED PARTY, DECEASED
- EXAMPLE TRUSTEE
VIOLATIONS: STRUCTURAL
ADDRESS: 314 DALLAS STREET RECORD NUMBER: CE-23-001177
CLT NUMBER: 071PH016 OFFICER: EXAMPLE
OWNERS AND OTHER INTERESTED PARTIES:
- ANOTHER PARTY
VIOLATIONS: BURNOUT
'''
CANCEL='THE OCTOBER 29, 2026 BETTER BUILDING BOARD MEETING HAS BEEN CANCELED. THE DECEMBER 3, 2026 MEETING AGENDA WILL BE POSTED ONE WEEK BEFORE THE MEETING.'

def scrape(text=TEXT):
 bot=KnoxvillePohBot()
 responses=[SimpleNamespace(status_code=200,text=HTML),SimpleNamespace(status_code=200,content=b'%PDF cancel'),SimpleNamespace(status_code=200,content=b'%PDF cases')]
 with patch.object(bot,'fetch',side_effect=responses),patch.object(bot,'_extract_text',side_effect=[CANCEL,text]):leads=bot.scrape()
 return bot,leads

def stored(lead):
 return dict(id='one',pipeline_lead_key=lead.pipeline_lead_key,property_address=lead.property_address,county='Knox County',phone_metadata={'operator_note':'keep'},staging_status='rejected',rejection_reason='manual')

class KnoxvilleTests(unittest.TestCase):
 def test_discovery_uses_official_links_and_rejects_external_targets(self):
  links=discover_documents(HTML+'<a href="https://evil.test/DocumentCenter/View/1">Public Officer Agenda</a>')
  self.assertEqual(len(links),2);self.assertTrue(all('/DocumentCenter/View/' in u for _,u in links))
  with self.assertRaises(RuntimeError):discover_documents('<html>No links</html>')

 def test_cover_date_not_inspection_or_next_meeting(self):
  self.assertEqual(hearing_date(TEXT),'2026-10-30');self.assertEqual(hearing_date(CANCEL),'2026-10-29')
  self.assertIsNone(hearing_date('FEBRUARY 31, 2026'))

 def test_modern_cases_have_exact_record_parcel_and_party_evidence(self):
  bot,leads=scrape();self.assertEqual(len(leads),2)
  first=leads[0];raw=first.raw_payload;e=raw[REVIEW_KEY]
  self.assertEqual(raw['case_number'],'CE-26-006926');self.assertEqual(raw['parcel_id'],'070PK007')
  self.assertEqual(len(raw['named_parties']),3);self.assertIsNone(first.owner_name_records)
  self.assertEqual(e['status'],'agenda_outcome_unverified');self.assertFalse(e['current_order_verified'])
  self.assertNotIn('tax delinquent',first.admin_notes.lower())
  self.assertEqual(bot.source_documents[0]['meeting_cancelled'],True)

 def test_one_case_does_not_absorb_next_case_parties(self):
  _,leads=scrape();self.assertEqual(leads[1].raw_payload['named_parties'],['ANOTHER PARTY'])

 def test_case_keys_preserve_multiple_cases_at_one_address(self):
  bot=KnoxvillePohBot();text=TEXT.replace('314 DALLAS STREET','2208 ADAIR AVENUE')
  leads=bot._parse_pdf_text(text,'poh_agenda','official')
  self.assertEqual(len(leads),2);self.assertNotEqual(leads[0].pipeline_lead_key,leads[1].pipeline_lead_key)

 def test_missing_case_or_parcel_not_synthetic_prospect(self):
  _,leads=scrape(TEXT.replace('CLT NUMBER: 070PK007','CLT NUMBER: '));self.assertEqual(len(leads),1)
  _,leads=scrape('PUBLIC OFFICER HEARING OCTOBER 30, 2026\n4521 FAWNIE LANE 10/02/2026')
  self.assertEqual(leads,[])

 def test_current_review_is_idempotent_and_does_not_overwrite_manual_fields(self):
  _,leads=scrape();row=stored(leads[0]);e=leads[0].raw_payload[REVIEW_KEY]
  p=review_patch(row,e);self.assertEqual(set(p),{'phone_metadata'})
  self.assertEqual(p['phone_metadata']['operator_note'],'keep')
  self.assertIsNone(review_patch({**row,**p},e))

 def test_wrong_identity_and_older_agenda_protected(self):
  _,leads=scrape();row=stored(leads[0]);e=leads[0].raw_payload[REVIEW_KEY]
  self.assertIsNone(review_patch({**row,'county':'Other County'},e))
  self.assertIsNone(review_patch({**row,'property_address':'999 WRONG ST'},e))
  self.assertIsNone(review_patch({**row,'pipeline_lead_key':'legacy-address-key'},e))
  previous={**e,'document_kind':'poh_results'};row['phone_metadata'][REVIEW_KEY]=previous
  self.assertIsNone(review_patch(row,e))
  previous['hearing_date']='2026-12-04';self.assertIsNone(review_patch(row,e))

 def test_failure_never_becomes_case_absence_or_close(self):
  bot=KnoxvillePohBot()
  with patch.object(bot,'fetch',return_value=None),self.assertRaises(RuntimeError):bot.scrape()
  with patch('src.bots.knoxville_poh_bot._supabase') as db:
   self.assertEqual(bot._write_staging([]),(0,0));db.assert_not_called()

 def test_latest_hearing_wins_independent_of_address_fetch_order(self):
  bot=KnoxvillePohBot()
  page=HTML.replace('Better Building Board Agenda','Public Officer Results')
  responses=[SimpleNamespace(status_code=200,text=page),SimpleNamespace(status_code=200,content=b'%PDF new'),SimpleNamespace(status_code=200,content=b'%PDF old')]
  with patch.object(bot,'fetch',side_effect=responses),patch.object(bot,'_extract_text',side_effect=[TEXT.replace('OCTOBER 30','DECEMBER 4'),TEXT]):leads=bot.scrape()
  self.assertTrue(all(x.raw_payload[REVIEW_KEY]['hearing_date']=='2026-12-04' for x in leads))

class Query:
 def __init__(self,db,table):self.db=db;self.table=table;self.payload=None;self.filters=[]
 def select(self,*args):return self
 def eq(self,k,v):self.filters.append((k,v));return self
 def in_(self,*args):return self
 def is_(self,*args):return self
 def filter(self,*args):return self
 def update(self,p):self.payload=p;return self
 def execute(self):
  if self.payload is not None:
   self.db.writes.append(self)
   if self.table.endswith('_staging'):raise RuntimeError('isolated write failure')
   return SimpleNamespace(data=[{'id':'ok'}])
  return SimpleNamespace(data=[self.db.row])

class WriterTests(unittest.TestCase):
 def test_recurring_exact_key_refresh_live_and_staging_fail_isolated(self):
  bot,leads=scrape();lead=leads[0];bot._reviews={lead.pipeline_lead_key:lead.raw_payload[REVIEW_KEY]}
  db=SimpleNamespace(row=stored(lead),writes=[]);db.table=lambda table:Query(db,table)
  with patch('src.bots.knoxville_poh_bot._supabase',return_value=db):bot._write_staging([lead])
  self.assertEqual(len(db.writes),2);self.assertEqual(bot.refresh_counts['failed'],1);self.assertEqual(bot.refresh_counts['updated'],1)
  self.assertIn(('source','bot'),db.writes[-1].filters)
  self.assertNotIn('staging_status',db.writes[-1].payload)

if __name__=='__main__':unittest.main()
