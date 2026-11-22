import base64
import copy
import json
import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from src.bots._shelby_trustee_value import select_parcel, parcel_value, lookup, REVIEW, VALUE_SOURCE
from src.bots.memphis_county_value_bot import MemphisCountyValueBot

DAY=date(2026,11,22)

def bill(parcel="036065 A00048", location="1960 PARKWAY", **changes):
    return dict(dict(billYear=2026,billCategory=20,billId=123,billNumber=456,parcel=parcel,billLocation=location,
                     accountsReceivableCode={"description":"MEMPHIS REALTY"},customer={"primaryName":"DOE JANE"}),**changes)

def page(match, amount=25000, code="R", **changes):
    data=dict(assessment=amount,classification=code,paymentType=20,description="MEMPHIS REALTY",parcel=match["parcel_id"],location=match["location"],primaryName=match["owner"],billNumber=match["bill_number"],year="2026")
    data.update(changes);encoded=base64.urlsafe_b64encode(json.dumps(data).encode()).decode()
    cells=[f"<a href='Transaction?billNumberData={encoded}'>456</a>","2026","Shelby","0","0","0","0",f"${amount:,.2f}"]
    return f"<span>Parcel ID:</span><span>{match['parcel_id']}</span><span>Primary Owner</span><span>{match['owner']}</span><span>Location</span><span>{match['location']}</span><table><tr>"+"".join(f"<td>{c}</td>" for c in cells)+"</tr></table>"

def row():
    return dict(id="one",bot_source="memphis_daily_news",pipeline_lead_key="key",property_address="1960 N Parkway Unit 503, Memphis, TN",county="shelby",staging_status="pending",property_value=None,raw_payload={"extracted":{"parcel":"036065-A00048"}},phone_metadata={"manual_note":"preserve","notice_tracking":{"last_seen_at":datetime.now(timezone.utc).isoformat(),"last_seen_by":"memphis_daily_news"}})

class CountyValueTests(unittest.TestCase):
    def match(self):return select_parcel({"parcelData":[bill()]},row()["property_address"],"036065-A00048",DAY)
    def test_condo_uses_full_notice_parcel(self):
        data={"parcelData":[bill("036065 A00046"),bill()]}
        self.assertEqual(select_parcel(data,row()["property_address"],"036065-A00048",DAY)["parcel_id"],"036065 A00048")
        self.assertEqual(select_parcel(data,row()["property_address"],None,DAY)["status"],"unit_requires_parcel")
    def test_wrong_subparcel_never_falls_back_to_owner_or_first_hit(self):
        self.assertEqual(select_parcel({"parcelData":[bill()]},row()["property_address"],"036065-A00049",DAY)["status"],"notice_parcel_conflict")
    def test_multiple_parcels_not_resolved_by_first_hit(self):
        d={"parcelData":[bill(),bill("036065 A00046")]}
        self.assertEqual(select_parcel(d,"1960 N Parkway",None,DAY)["status"],"ambiguous_parcels")
    def test_personalty_old_year_and_wrong_street_excluded(self):
        d={"parcelData":[bill(billCategory=25),bill(billYear=2025),bill(location="1960 POPLAR")]}
        self.assertEqual(select_parcel(d,"1960 N Parkway",None,DAY)["status"],"no_current_match")
    def test_duplicate_conflicting_owner_hold(self):
        d={"parcelData":[bill(),bill(customer={"primaryName":"OTHER OWNER"})]}
        self.assertEqual(select_parcel(d,row()["property_address"],"036065-A00048",DAY)["status"],"conflicting_current_bills")
    def test_assessment_ratio_labeled_and_bound(self):
        m=self.match();e=parcel_value(page(m),m,DAY)
        self.assertEqual(e["estimated_appraisal"],100000)
        self.assertEqual(e["status"],"verified_assessment")
        self.assertEqual(e["valuation_method"],"county_tax_assessment_divided_by_class_ratio")
        self.assertEqual(parcel_value(page(m,code="C"),m,DAY)["estimated_appraisal"],62500)
    def test_bill_identity_year_and_unrecognized_class_rejected(self):
        m=self.match()
        for changes in ({"parcel":"036065 A00046"},{"primaryName":"WRONG"},{"billNumber":999},{"year":"2025"},{"paymentType":25},{"code":"UNKNOWN"},{"location":"99 MAIN"}):
            self.assertEqual(parcel_value(page(m,**changes),m,DAY)["status"],"assessment_unverified")
    def test_visible_assessment_must_equal_payload(self):
        m=self.match();html=page(m).replace("$25,000.00","$30,000.00")
        self.assertEqual(parcel_value(html,m,DAY)["status"],"assessment_unverified")
    def test_wrong_detail_page_rejected(self):
        m=self.match();self.assertEqual(parcel_value(page(m).replace("<span>DOE JANE</span>","<span>OTHER</span>"),m,DAY)["status"],"detail_identity_conflict")
    def test_failed_source_not_a_zero_value(self):
        b=MagicMock();b.fetch.return_value=None
        e=lookup(b,row(),DAY);self.assertEqual(e["status"],"source_unavailable");self.assertNotIn("estimated_appraisal",e)
    def test_persist_preserves_owner_raw_and_manual_metadata(self):
        b=MemphisCountyValueBot();self.addCleanup(b.session.close);client=MagicMock();q=client.table.return_value
        for method in ["update","eq","is_","filter"]:getattr(q,method).return_value=q
        q.execute.return_value=SimpleNamespace(data=[{"id":"one"}]);r=row();original=copy.deepcopy(r);m=self.match();e=parcel_value(page(m),m,DAY)
        self.assertTrue(b._persist(client,r,e));self.assertEqual(r,original)
        data=q.update.call_args.args[0];self.assertEqual(data["property_value_source"],VALUE_SOURCE);self.assertNotIn("owner_name_records",data);self.assertNotIn("raw_payload",data)
        self.assertEqual(data["phone_metadata"]["manual_note"],"preserve")
        self.assertIn((("property_value","null"),),[(c.args,) for c in q.is_.call_args_list])
        self.assertIn((("raw_payload->extracted->>parcel","036065-A00048"),),[(c.args,) for c in q.eq.call_args_list])
    def test_large_cas_payload_isolated(self):
        b=MemphisCountyValueBot();self.addCleanup(b.session.close);r=row();r["phone_metadata"]["big"]="x"*7000;client=MagicMock()
        self.assertFalse(b._persist(client,r,{"status":"no_current_match"}));client.table.assert_not_called()
    def test_same_day_and_stale_sightings_not_requeried(self):
        b=MemphisCountyValueBot();self.addCleanup(b.session.close);client=MagicMock();q=client.table.return_value
        for method in ["select","eq","neq","is_","or_","order","limit"]:getattr(q,method).return_value=q
        a=row();same=row();same["id"]="same";same["phone_metadata"][REVIEW]={"checked_on":date.today().isoformat()};stale=row();stale["id"]="stale";stale["phone_metadata"]["notice_tracking"]["last_seen_at"]="2020-01-01T00:00:00Z"
        q.execute.return_value=SimpleNamespace(data=[a,same,stale]);self.assertEqual([r["id"] for r in b._candidates(client)],["one"])
    def test_scheduled_before_fallback_and_after_notices(self):
        from src.bots import _run_new
        names=[name for name,_ in _run_new.NEW_BOTS]
        self.assertLess(names.index("memphis_daily_news"),names.index("memphis_county_value"))
        self.assertLess(names.index("memphis_county_value"),names.index("shelby_loan_value"))

if __name__=="__main__":unittest.main()
