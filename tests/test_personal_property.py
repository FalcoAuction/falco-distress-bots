"""Tests for the picky personal-property lane rules. No network.

    python -m unittest tests.test_personal_property -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._personal_property import category_of, classify, implied_equipment_value  # noqa: E402

JET = {"n_number": "N722AP", "aircraft": "2018 CIRRUS DESIGN CORP SF50", "value_band": "A",
       "match_type": "address", "confidence": "high"}
BARON = {"n_number": "N4590S", "aircraft": "1975 BEECH 58", "value_band": "B",
         "match_type": "name_middle_bk_district", "confidence": "medium"}
NOTICE = {"county": "davidson", "docket": "26P1785", "decedent": "Jane Doe",
          "attorney": {"names": ["Robert Akin"], "address": ["611 Woodland Street", "Nashville, TN 37206"]},
          "representatives": [{"name": "Michele Smith", "role": "personal_representative",
                               "address": ["512 Alta Loma Rd.", "Goodlettsville, TN 37072"]}]}


def lead(dt, raw=None, **kw):
    return {"distress_type": dt, "raw_payload": raw or {}, **kw}


class CategoryTests(unittest.TestCase):
    def test_categories(self):
        self.assertEqual(category_of("Volunteer Firearms LLC"), "guns")
        self.assertEqual(category_of("Music City Coin & Jewelry"), "collections")
        self.assertEqual(category_of("Elliott & Son Excavating LLC"), "heavy_equipment")
        self.assertEqual(category_of("Engineered Alloys LLC"), "business_equipment")
        self.assertEqual(category_of("James Spearman Aircraft, LLC"), "vehicles")

    def test_false_friends_rejected(self):
        self.assertIsNone(category_of("Oak Arms Apartments LLC"))
        self.assertIsNone(category_of("Southern Air Heating LLC"))
        self.assertIsNone(category_of("799 Auto Repair, LLC"))
        self.assertIsNone(category_of("Bright Consulting Group LLC"))

    def test_implied_value(self):
        # $18,566 over 4 years -> principal 13,647 -> 3,412/yr -> /3.5% /30% ~ $325K
        self.assertEqual(implied_equipment_value(18566, 4), 325000.0)
        self.assertIsNone(implied_equipment_value(None, 3))


class EstateTests(unittest.TestCase):
    def test_ordinary_estate_not_tagged(self):
        self.assertIsNone(classify(lead("PROBATE", {"court_notice": NOTICE}, property_value=310000)))

    def test_affluent_home_is_b_with_contacts(self):
        t = classify(lead("PROBATE", {"court_notice": NOTICE}, property_value=1_400_000))
        self.assertEqual((t["lane"], t["grade"]), ("estate", "B"))
        self.assertEqual(t["contacts"]["attorney"]["names"], ["Robert Akin"])
        self.assertEqual(t["contacts"]["representatives"][0]["name"], "Michele Smith")
        self.assertTrue(t["caveats"])

    def test_two_million_home_is_a(self):
        self.assertEqual(classify(lead("PROBATE", property_value=2_049_200))["grade"], "A")

    def test_decedent_jet_is_a(self):
        t = classify(lead("PROBATE", {"aircraft": [JET]}, property_value=0))
        self.assertEqual((t["grade"], t["categories"], t["est_value_min"]), ("A", ["aircraft"], None))


class LiquidationTests(unittest.TestCase):
    def test_individual_bankruptcy_never_liquidation(self):
        self.assertIsNone(classify(lead("BANKRUPTCY", {"chapter": "7"}, full_name="John Q Trucking")))

    def test_chapter_13_entity_skipped(self):
        self.assertIsNone(classify(lead("BANKRUPTCY", {"chapter": "13"}, full_name="Elliott Excavating LLC")))

    def test_ch7_hard_asset_entity_is_b_with_size_caveat(self):
        t = classify(lead("BANKRUPTCY", {"chapter": "7", "courtlistener_docket_url": "https://cl/1"},
                          full_name="J.R. COLEMAN TRUCKING CO. LLC"))
        self.assertEqual((t["lane"], t["grade"], t["categories"]), ("business_liquidation", "B", ["heavy_equipment"]))
        self.assertIn("Official Form 201", t["caveats"][0])
        self.assertEqual(t["contacts"]["bankruptcy_docket"], "https://cl/1")

    def test_any_chapter_7_business_goes_to_the_trustee(self):
        t = classify(lead("BANKRUPTCY", {"chapter": "7", "trustee": "LEMEH, EVA M"}, full_name="MyEyeris, Inc."))
        self.assertEqual(t["categories"], ["business_assets"])
        self.assertEqual(t["controller"], "chapter 7 trustee")
        self.assertEqual(t["primary_contact"], "Trustee Eva M Lemeh")
        self.assertFalse(t["routable"])
        self.assertIsNone(t["route_to"])

    def test_chapter_11_business_not_tagged(self):
        self.assertIsNone(classify(lead("BANKRUPTCY", {"chapter": "11"}, full_name="MyEyeris, Inc.")))

    def test_florida_and_court_sales_not_routable(self):
        fl = classify(lead("BANKRUPTCY", {"chapter": "7", "state": "FL"}, full_name="ACME TRUCKING LLC"))
        self.assertFalse(fl["routable"])
        self.assertIn("Florida", fl["not_routable_reason"])

    def test_big_warn_without_hard_assets_remains_research(self):
        warn = {"company": "Kroger Fulfillment Network LLC", "workers": 132}
        tag = classify(lead("BUSINESS_CLOSURE", {"warn": warn}))
        self.assertEqual(tag["qualification"], "research")
        self.assertFalse(tag["prospect"])

    def test_large_warn_is_research_only(self):
        warn = {"company": "Smoky Mountain Logistics, LLC", "workers": 100}
        t = classify(lead("BUSINESS_CLOSURE", {"warn": warn}))
        self.assertFalse(t["routable"])
        self.assertEqual((t["lane"], t["grade"]), ("warn_research", None))


class SeizureTests(unittest.TestCase):
    def per(self, owed, years, name):
        return lead("TAX_LIEN", {"parcel": "PER--1", "cumulative_owed": owed, "years_delinquent": years,
                                 "owner_name_1": name})

    def test_small_account_not_tagged(self):
        self.assertIsNone(classify(self.per(1152, 1, "ACME LLC")))

    def test_one_year_late_not_tagged_even_if_large(self):
        self.assertIsNone(classify(self.per(9000, 1, "MAYSE CONSTRUCTION COMPANY")))

    def test_recent_hard_asset_is_a(self):
        t = classify(self.per(18566, 4, "MAYSE CONSTRUCTION COMPANY"))
        self.assertEqual((t["lane"], t["grade"], t["est_value_min"]), ("seizure", "A", 325000.0))

    def test_dormant_needs_bigger_value_and_is_b(self):
        self.assertIsNone(classify(self.per(38131, 7, "AVM ENTERPRISES INC")))   # ~$318K, dormant
        t = classify(self.per(469828, 13, "SHELTON CONSTRUCTION CO INC"))
        self.assertEqual(t["grade"], "B")
        self.assertTrue(any("may be gone" in c for c in t["caveats"]))

    def test_real_property_tax_lien_ignored(self):
        self.assertIsNone(classify(lead("TAX_LIEN", {"parcel": "075014", "cumulative_owed": 90000,
                                                     "years_delinquent": 3})))


class OwnerAssetTests(unittest.TestCase):
    def test_foreclosure_owner_with_jet(self):
        t = classify(lead("PRE_FORECLOSURE", {"aircraft": [JET]}))
        self.assertEqual((t["lane"], t["grade"]), ("owner_asset", "A"))
        self.assertIsNone(t["est_value_min"])
        self.assertIn("FAA registry overlap", t["signals"][0])

    def test_bankrupt_individual_with_baron_is_owner_asset_b(self):
        t = classify(lead("BANKRUPTCY", {"chapter": "13", "aircraft": [BARON]}, full_name="Chad E Conner"))
        self.assertEqual((t["lane"], t["grade"]), ("owner_asset", "B"))

    def test_no_signal_no_tag(self):
        self.assertIsNone(classify(lead("PRE_FORECLOSURE", property_value=2_500_000)))



class EvidenceRoutingTests(unittest.TestCase):
    def fixture(self):
        from datetime import date
        today = date.today().isoformat()
        fact = {'verified_on': today, 'source_url': 'https://example.org/record'}
        ev = {'subject_name': 'Jane Doe',
              'assets': [dict(fact, asset_id='N722AP', kind='aircraft', serial='123',
                              registrant='DOE JANE', description='SF50')],
              'controller': dict(fact, name='Jane Doe', role='owner', authority='verified'),
              'availability': dict(fact, status='available'),
              'liens': dict(fact, status='reviewed'), 'court': dict(fact, status='permitted'),
              'valuation': dict(fact, amount=300000),
              'deadline': dict(fact, status='none_documented')}
        plane = dict(JET, serial='123', registrant='DOE JANE', identity_verified=True,
                     verified_on=today, registry_date=today, registration_status='V')
        return lead('TAX_LIEN', {'aircraft': [plane], 'personal_property_evidence': ev}, full_name='Jane Doe')

    def test_documented_asset_routes_without_title_assumption(self):
        t = classify(self.fixture())
        self.assertTrue(t['routable'])
        self.assertEqual(t['controller'], 'Jane Doe')
        self.assertEqual(t['title_status'], 'not_established')

    def test_changes_and_unknowns_revoke_routing(self):
        for key in ('availability', 'liens', 'controller', 'deadline'):
            with self.subTest(key=key):
                row = self.fixture()
                row['raw_payload']['personal_property_evidence'][key]['verified_on'] = '2000-01-01'
                self.assertFalse(classify(row)['routable'])
        row = self.fixture()
        row['distress_type'] = 'BANKRUPTCY'
        row['raw_payload']['personal_property_evidence']['court']['verified_on'] = '2000-01-01'
        self.assertFalse(classify(row)['routable'])
        for key, value in [('serial', 'other'), ('registrant', 'OTHER'), ('identity_verified', False),
                           ('registration_status', 'D'), ('registry_date', '2000-01-01')]:
            row = self.fixture()
            row['raw_payload']['aircraft'][0][key] = value
            self.assertFalse(classify(row)['routable'])
        row = self.fixture()
        row['raw_payload']['aircraft'] = []
        self.assertFalse(classify(row)['routable'])
        row = self.fixture()
        row['full_name'] = 'Someone Else'
        self.assertFalse(classify(row)['routable'])

    def test_proxies_remain_research(self):
        rows = [lead('PROBATE', property_value=3000000),
                lead('BANKRUPTCY', {'chapter': '7'}, full_name='Construction LLC'),
                lead('PROBATE', {'aircraft': [JET]}),
                lead('PROBATE', {'outbuildings': [{'type': 'detached garage', 'area': 900}]})]
        for row in rows:
            t = classify(row)
            self.assertFalse(t['routable'])
            self.assertEqual(t['qualification'], 'research')

    def test_asset_deadline_not_mortgage_date(self):
        row = self.fixture()
        row['trustee_sale_date'] = '2000-01-01'
        self.assertIsNone(classify(row)['deadline'])
        row['raw_payload']['personal_property_evidence']['deadline']['date'] = '2000-01-01'
        self.assertFalse(classify(row)['routable'])

class FaaRefreshTests(unittest.TestCase):
    def test_current_identity_produces_visible_prospect(self):
        from datetime import date
        from types import SimpleNamespace
        from src.bots.faa_aircraft_bot import evidence_tags
        today = date.today().isoformat()
        index = SimpleNamespace(registry_date=today, match=lambda row: [dict(
            JET, serial='123', registrant='DOE JANE', registration_status='V',
            registration_expires='20990101')])
        row = lead('TAX_LIEN', full_name='Jane Doe')
        row['raw_payload']['aircraft'] = evidence_tags(index, row)
        tag = classify(row)
        self.assertEqual(tag['qualification'], 'qualified_prospect')
        self.assertFalse(tag['routable'])
        self.assertTrue(tag['holds'])
        self.assertEqual(tag['registry_assets'][0]['serial'], '123')
        row['full_name'] = 'Other Person'
        row['raw_payload']['aircraft'] = evidence_tags(index, row)
        self.assertEqual(classify(row)['qualification'], 'research')

    def test_cache_refreshes_next_day_and_rejects_stale_zip(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        from src.bots.faa_aircraft_bot import FaaAircraftBot, _INDEX_CACHE
        bot = FaaAircraftBot()
        _INDEX_CACHE.clear()
        try:
            with patch.object(bot, '_build_index', return_value=SimpleNamespace()) as build:
                bot._load_index()
                bot._load_index()
                self.assertEqual(build.call_count, 1)
                _INDEX_CACHE['key'] = ('2000-01-01', None)
                bot._load_index()
                self.assertEqual(build.call_count, 2)
            zf = SimpleNamespace(getinfo=lambda name: SimpleNamespace(date_time=(2000, 1, 1)))
            with self.assertRaises(ValueError):
                bot._validated_index(zf)
        finally:
            _INDEX_CACHE.clear()

class ScheduledLaneTests(unittest.TestCase):
    def test_overlay_then_lane_changed_and_removed_registry(self):
        import json
        from copy import deepcopy
        from datetime import date
        from types import SimpleNamespace
        from unittest.mock import patch
        from src.bots.faa_aircraft_bot import FaaAircraftBot
        from src.bots.personal_property_lane_bot import PersonalPropertyLaneBot, _RAW_TEXT, _RAW_JSON
        stored = {'id': '1', 'distress_type': 'TAX_LIEN', 'full_name': 'Jane Doe',
                  'raw_payload': {'unrelated': {'keep': True}}}
        class Client:
            def table(self, table):
                self.expected = None
                return self
            def update(self, values):
                self.values = values
                return self
            def eq(self, key, value):
                if key == 'raw_payload':
                    self.expected = json.loads(value)
                return self
            def execute(self):
                if self.expected != stored['raw_payload']:
                    return SimpleNamespace(data=[])
                stored.update(deepcopy(self.values))
                return SimpleNamespace(data=[stored])
        client = Client()
        planes = [dict(JET, serial='123', registrant='DOE JANE', registration_status='V',
                       registration_expires='20990101')]
        index = SimpleNamespace(registry_date=date.today().isoformat(), by_addr={},
                                match=lambda row: deepcopy(planes))
        faa, lane = FaaAircraftBot(), PersonalPropertyLaneBot()
        def faa_rows(client, table):
            return [{**stored, '_aircraft': stored['raw_payload'].get('aircraft')}] if table == 'homeowner_requests' else []
        def lane_rows(client, table):
            if table != 'homeowner_requests':
                return []
            raw = stored['raw_payload']
            return [{**stored, **{k: raw.get(k) for k in _RAW_TEXT + _RAW_JSON}, '_pp': raw.get('personal_property')}]
        with patch('src.bots.faa_aircraft_bot._supabase', return_value=client), \
             patch('src.bots.personal_property_lane_bot._supabase', return_value=client), \
             patch.object(faa, '_load_index', return_value=index), \
             patch.object(faa, '_leads', side_effect=faa_rows), \
             patch.object(lane, '_rows', side_effect=lane_rows), \
             patch.object(faa, '_raw_payload', side_effect=lambda *a: deepcopy(stored['raw_payload'])), \
             patch.object(lane, '_raw_payload', side_effect=lambda *a: deepcopy(stored['raw_payload'])), \
             patch.object(faa, '_report_health'), patch.object(lane, '_report_health'):
            self.assertEqual(faa.run()['status'], 'ok')
            self.assertEqual(lane.run()['status'], 'ok')
            self.assertEqual(stored['raw_payload']['personal_property']['qualification'], 'qualified_prospect')
            planes[0]['registrant'] = 'SOMEONE ELSE'
            faa.run()
            lane.run()
            self.assertEqual(stored['raw_payload']['personal_property']['qualification'], 'research')
            planes.clear()
            faa.run()
            lane.run()
            self.assertNotIn('personal_property', stored['raw_payload'])
            self.assertEqual(stored['raw_payload']['unrelated'], {'keep': True})



class OpportunityDedupeTests(unittest.TestCase):
    def key(self, raw=None, **kw):
        from src.bots._personal_property import dedupe_key
        return dedupe_key({'lane': 'estate'}, {'distress_type': 'PROBATE', **kw}, raw or {})

    def test_blank_or_owner_alone_never_suppressed(self):
        self.assertIsNone(self.key())
        self.assertIsNone(self.key(full_name='Jane Doe'))
        self.assertIsNone(self.key({'case_number': '123'}, full_name='Jane Doe'))
        self.assertIsNone(self.key({'case_number': 'Unknown', 'court_id': 'tnwb'}))

    def test_case_requires_jurisdiction_and_distinguishes_counties(self):
        raw = {'state': 'TN', 'court_notice': {'docket': '26P123', 'county': 'Shelby'}}
        a = self.key(raw)
        self.assertIsNotNone(a)
        self.assertEqual(a, self.key(raw, id='second-newspaper'))
        self.assertNotEqual(a, self.key({'state': 'TN', 'court_notice': {'docket': '26P123', 'county': 'Davidson'}}))
        self.assertNotEqual(self.key({'case_number': '26-123', 'court_id': 'tnwb'}),
                            self.key({'case_number': '26-123', 'court_id': 'tnmb'}))

    def test_assets_dont_collapse_by_owner_or_case(self):
        a = {'aircraft': [{'n_number': 'N123', 'serial': '1'}]}
        b = {'aircraft': [{'n_number': 'N456', 'serial': '2'}]}
        self.assertIsNone(self.key(a))
        self.assertEqual(self.key(a, full_name='Jane Doe'), self.key(a, full_name=' jane doe '))
        self.assertNotEqual(self.key(a, full_name='Jane Doe'), self.key(b, full_name='Jane Doe'))
        self.assertNotEqual(self.key(a, full_name='Jane Doe'), self.key(a, full_name='John Doe'))
        a.update(case_number='26-123', court_id='tnwb')
        b.update(case_number='26-123', court_id='tnwb')
        self.assertNotEqual(self.key(a), self.key(b))
        combined = {'aircraft': a['aircraft'] + b['aircraft']}
        reversed_assets = {'aircraft': list(reversed(combined['aircraft']))}
        self.assertEqual(self.key(combined, full_name='Jane Doe'), self.key(reversed_assets, full_name='Jane Doe'))
        self.assertIsNone(self.key({'court_id': 'tnwb', 'case_number': '26-123',
                                    'aircraft': [{'n_number': 'N999'}]}))


if __name__ == "__main__":
    unittest.main()
