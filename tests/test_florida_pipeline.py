"""Tests for the Florida pipeline parsers and matchers: clerk daily files,
DOR roll records and name matching, Sunbiz fixed-width files, and the FL
rules in the personal-property lane. No network.

    python -m unittest tests.test_florida_pipeline -v
"""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest
from pathlib import Path

os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots._fl_clerk import civil_case, group_cases, probate_case, read_rows  # noqa: E402
from src.bots._fl_roll import (SCHEMA, FlRoll, county_key, load_rows, name_compatible, parcel_tag,  # noqa: E402
                               person_owner_keys)
from src.bots._personal_property import classify  # noqa: E402
from src.bots._sunbiz import (debtor_person_name, parse_cor_line, parse_debtors, parse_events,  # noqa: E402
                              parse_liens)
from src.bots.fl_hillsborough_clerk_bot import _fl_only  # noqa: E402

CIVIL = '''﻿CaseCategory,CaseTypeDescription,CaseNumber,Title,FilingDate,PartyType,FirstName,MiddleName,LastName/CompanyName,PartyAddress,Attorney
"CV","Mortgage Foreclosure - Homestead -2- $50,001 - $249,999","26-CA-1","TRUIST BANK vs HENNING","11/13/2026","Plaintiff","","","TRUIST BANK","PO BOX 1, CHARLOTTE, NC 28201","JANE LAWYER "
"CV","Mortgage Foreclosure - Homestead -2- $50,001 - $249,999","26-CA-1","TRUIST BANK vs HENNING","11/13/2026","Defendant","DAVID","V.","HENNING","5321 Sandy Shell Dr, Apollo Beach, FL 33572","No Attorney"
"CV","Mortgage Foreclosure - Homestead -2- $50,001 - $249,999","26-CA-1","TRUIST BANK vs HENNING","11/13/2026","Defendant","","","UNKNOWN SPOUSE OF DAVID HENNING","5321 Sandy Shell Dr, Apollo Beach, FL 33572","No Attorney"
"CV","Mortgage Foreclosure - Homestead -2- $50,001 - $249,999","26-CA-1","TRUIST BANK vs HENNING","11/13/2026","Defendant","","","BAYSIDE HOMEOWNERS ASSOCIATION INC","1 MAIN ST, TAMPA, FL 33602","No Attorney"
"CV","CC Real Property/Mortgage Foreclosure  $0.00 to  $15,000.00","26-CC-2","VILLAS CONDO vs ROE","11/13/2026","Plaintiff","","","VILLAS CONDOMINIUM ASSOCIATION INC","2 MAIN ST, TAMPA, FL 33602","A LAWYER "
"CV","CC Real Property/Mortgage Foreclosure  $0.00 to  $15,000.00","26-CC-2","VILLAS CONDO vs ROE","11/13/2026","Defendant","MARY","","ROE","7 OAK ST UNIT 4, TAMPA, FL 33610","No Attorney"
"CV","Partition 2 - $50,001 - $249,999","26-CA-3","BUIE vs BUIE","11/13/2026","Plaintiff","BRANDY","","BUIE","2114 W Columbus Dr, Tampa, FL 33607","X "
"CV","Partition 2 - $50,001 - $249,999","26-CA-3","BUIE vs BUIE","11/13/2026","Defendant","ANTWAN","","BUIE","9 ELM ST, ATLANTA, GA 30301","No Attorney"
"CV","Auto Negligence","26-CA-4","A vs B","11/13/2026","Defendant","JOHN","","DOE","1 X ST, TAMPA, FL 33602","No Attorney"
'''

PROBATE = '''CaseCategory,CaseTypeDescription,CaseNumber,Title,FilingDate,PartyType,FirstName,MiddleName,LastName/CompanyName,DateofDeath,PartyAddress,Attorney
"PR","Formal Administration","26-CP-1","TESTA, LIZA M - Estate","11/12/2026","Decedent","LIZA","M","TESTA","08/03/2026","2531 LAKE ELLEN DRIVE, TAMPA, FL 33618","No Attorney"
"PR","Formal Administration","26-CP-1","TESTA, LIZA M - Estate","11/12/2026","Petitioner","LOGAN","","TESTA","","4475 AMBERLY OAKS COURT, TAMPA, FL 33614","ANTHONY PALMA "
"PR","Formal Administration","26-CP-1","TESTA, LIZA M - Estate","11/12/2026","Beneficiary","ANN","","TESTA","","1 A ST, TAMPA, FL 33614","No Attorney"
"PR","GDN Incompetent Person & Property","26-GA-2","X","11/12/2026","Ward","SAM","","WARD","","1 B ST, TAMPA, FL 33614","No Attorney"
"PR","Summary Administration Less Than $1000","26-CP-3","Y","11/12/2026","Decedent","AL","","SMALL","01/01/2026","1 C ST, TAMPA, FL 33614","No Attorney"
'''

NAL_HEADER = ["CO_NO", "PARCEL_ID", "DOR_UC", "JV", "JV_HMSTD", "ACT_YR_BLT", "EFF_YR_BLT", "TOT_LVG_AREA",
              "NO_RES_UNTS", "SALE_PRC1", "SALE_YR1", "SALE_MO1", "OWN_NAME", "OWN_ADDR1", "OWN_ADDR2", "OWN_CITY",
              "OWN_STATE", "OWN_ZIPCD", "PHY_ADDR1", "PHY_CITY", "PHY_ZIPCD"]


def nal(pid, owner, situs, zip5, jv=300000, hmstd=300000, uc="001", mail=None, mail_zip=None):
    vals = ["39", pid, uc, str(jv), str(hmstd), "1990", "1995", "2000", "1", "250000", "2019", "6", owner,
            mail or situs, "", "TAMPA", "FL", mail_zip or zip5, situs, "TAMPA", zip5]
    return dict(zip(NAL_HEADER, vals))


def nap(acct, owner, jv, naics="333120", pen="0"):
    return {"CO_NO": "39", "ACCT_ID": acct, "NAICS_CD": naics, "JV_TOTAL": str(jv), "TAX_VAL": str(jv),
            "PEN_RATE": pen, "OWN_NAM": owner, "OWN_ADDR": "1 IND BLVD", "OWN_CITY": "TAMPA", "OWN_STATE": "FL",
            "OWN_ZIPCD": "33610", "PHY_ADDR": "1 IND BLVD", "PHY_CITY": "TAMPA", "PHY_ZIPCD": "33610"}


def roll():
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA)
    load_rows(db, "NAL", 39, [
        nal("A1", "HENNING DAVID VINCENT & CINDY", "5321 SANDY SHELL DR", "33572", jv=447778),
        nal("A2", "HENRY JAMES", "3202 SATURN CT", "33610"),
        nal("A3", "HENRY JAMES", "3814 N OAK DR", "33611"),
        nal("A4", "ROE MARY", "7 OAK ST UNIT 4", "33610", jv=90000),
        nal("A5", "ROE PAUL", "7 OAK ST UNIT 5", "33610", jv=95000),
        nal("A6", "ACME HOLDINGS LLC", "50 RENTAL RD", "33612", hmstd=0, mail="9 OFFICE PKWY", mail_zip="33602"),
    ])
    load_rows(db, "NAP", 39, [nap("P1", "GULF CRANE SERVICES LLC", 640000, pen="0.15"),
                              nap("P2", "MIKE'S AUTO GLASS", 11300)])
    return FlRoll(db)


class ClerkTests(unittest.TestCase):
    def test_civil_cases(self):
        cases = group_cases(read_rows(CIVIL))
        out = {cn: civil_case(cn, rows) for cn, rows in cases.items()}
        self.assertIsNone(out["26-CA-4"])
        mf = out["26-CA-1"]
        self.assertEqual((mf["kind"], mf["occupancy"], mf["value_tier"], mf["owners"], mf["plaintiff"]),
                         ("mortgage", "homestead", "$50,001 - $249,999", ["DAVID V. HENNING"], "TRUIST BANK"))
        self.assertEqual(mf["plaintiff_attorney"], "JANE LAWYER")
        self.assertEqual((out["26-CC-2"]["kind"], out["26-CC-2"]["owners"]), ("association_lien", ["MARY ROE"]))
        part = out["26-CA-3"]
        self.assertEqual((part["distress_type"], part["owners"], part["owner_address"]),
                         ("PARTITION", ["BRANDY BUIE", "ANTWAN BUIE"], "2114 W Columbus Dr, Tampa, FL 33607"))

    def test_probate_cases(self):
        cases = group_cases(read_rows(PROBATE))
        out = {cn: probate_case(cn, rows) for cn, rows in cases.items()}
        self.assertIsNone(out["26-GA-2"])   # guardianship never staged
        self.assertIsNone(out["26-CP-3"])   # small-estate summary administration
        p = out["26-CP-1"]
        self.assertEqual((p["decedent"], p["date_of_death"], p["petitioner"], p["attorney"], p["beneficiaries"]),
                         ("LIZA M TESTA", "08/03/2026", "LOGAN TESTA", "ANTHONY PALMA", 1))

    def test_out_of_state_address_is_not_the_property(self):
        self.assertIsNone(_fl_only("2347 BIRCH BARK TRAIL, GROVE CITY, OH 43123"))
        self.assertEqual(_fl_only("1 A ST, TAMPA, FL 33614"), "1 A ST, TAMPA, FL 33614")


class RollTests(unittest.TestCase):
    def test_county_key(self):
        self.assertEqual(county_key("Miami-Dade County"), "miami-dade")
        self.assertEqual(county_key("Dade"), "miami-dade")
        self.assertIsNone(county_key("Davidson"))

    def test_parcel_by_address(self):
        r = roll()
        p = r.parcel_at("Hillsborough", "5321 Sandy Shell Dr, Apollo Beach, FL 33572")
        tag = parcel_tag(p)
        self.assertEqual((tag["parcel_id"], tag["just_value"], tag["homestead"], tag["residential"]),
                         ("A1", 447778.0, True, True))

    def test_condo_unit_disambiguates(self):
        r = roll()
        self.assertEqual(r.parcel_at("Hillsborough", "7 OAK ST UNIT 4, TAMPA, FL 33610")["parcel_id"], "A4")
        self.assertIsNone(r.parcel_at("Hillsborough", "7 OAK ST, TAMPA, FL 33610"))

    def test_name_matching_is_strict(self):
        self.assertTrue(name_compatible("David V. Henning", "HENNING DAVID VINCENT & CINDY"))
        self.assertTrue(name_compatible("TESTA, LIZA M", "TESTA LIZA TRUSTEE"))
        self.assertFalse(name_compatible("James Edward Henry", "JAMES EDWARD W III"))
        self.assertFalse(name_compatible("David Q Henning", "HENNING DAVID VINCENT"))
        self.assertEqual(person_owner_keys("David V. Henning"), ["P|DAVID|HENNING"])
        r = roll()
        self.assertEqual(len(r.parcels_of("James Henry", "Hillsborough")), 2)   # two people: enricher refuses

    def test_parcel_id_orlando_order(self):
        db = sqlite3.connect(":memory:")
        db.executescript(SCHEMA)
        row = nal("052229774400160", "SMITH JANE", "5050 CINDERLANE PKWY", "32808")
        row["CO_NO"] = "58"
        load_rows(db, "NAL", 58, [row])
        r = FlRoll(db)
        self.assertEqual(r.parcel_by_id("Orange", "05-2229-7744-00-160")["parcel_id"], "052229774400160")
        self.assertEqual(r.parcel_by_id("Orange", "292205774400160")["parcel_id"], "052229774400160")
        self.assertIsNone(r.parcel_by_id("Orange", "999999774400160"))

    def test_statewide_address_probe(self):
        r = roll()
        self.assertEqual(r.parcel_at(None, "5321 Sandy Shell Dr, Apollo Beach, FL 33572")["parcel_id"], "A1")

    def test_lien_debtor_must_own_the_parcel(self):
        self.assertFalse(name_compatible("PRIMITIVE PARTNERS LLC", "CP MIAMI CENTER LLC"))
        self.assertTrue(name_compatible("SMITH, JOHN A", "SMITH JOHN A & MARY"))

    def test_absentee_flag(self):
        r = roll()
        self.assertTrue(parcel_tag(r.parcels_of("Acme Holdings LLC")[0])["absentee"])

    def test_equipment_lookup(self):
        r = roll()
        self.assertEqual([t["own_name"] for t in r.equipment_of("Gulf Crane Services, LLC")],
                         ["GULF CRANE SERVICES LLC"])
        self.assertEqual([t["own_name"] for t in r.equipment_of("Mike's Auto Glass")], ["MIKE'S AUTO GLASS"])


def fixed(fields, width):
    line = [" "] * width
    for start, text in fields:
        for i, ch in enumerate(text):
            line[start + i] = ch
    return "".join(line)


class SunbizTests(unittest.TestCase):
    def test_events_keep_closures_only(self):
        dis = fixed([(0, "P06000123456"), (17, "CORAPVOLDS"), (37, "VOLUNTARY           DISSOLUTION"),
                     (77, "11122026"), (85, "11132026")], 662)
        rev = fixed([(0, "L20000000001"), (17, "CORLCREVDS"), (37, "LC REVOCATION OF    DISSOLUTION")], 662)
        amd = fixed([(0, "L20000000002"), (17, "CORLCAMND"), (37, "LC AMENDMENT")], 662)
        ev = parse_events("\n".join([dis, rev, amd]))
        self.assertEqual([(e["doc_number"], e["description"], e["filed"]) for e in ev],
                         [("P06000123456", "VOLUNTARY DISSOLUTION", "2026-11-13")])

    def test_lien_files(self):
        deb = fixed([(0, "F"), (1, "26FLR0004180"), (13, "C & F HYATT CONSTRUCTION INC"), (68, "C"),
                     (69, "37180 COUNTY ROAD 452"), (157, "GRAND ISLAND"), (185, "FL"), (187, "327359101"),
                     (205, "A")], 206)
        d = parse_debtors(deb)[0]
        self.assertEqual((d["name"], d["entity"], d["zip"], d["city"]),
                         ("C & F HYATT CONSTRUCTION INC", True, "32735", "GRAND ISLAND"))
        lien = parse_liens("26FLR0004180110920260000100001AF08242026000000000923203600000000010000100001")
        self.assertEqual(lien["26FLR0004180"]["filed"], "2026-11-09")
        self.assertEqual(debtor_person_name("SMITH JOHN A"), "SMITH, JOHN A")

    def test_cor_line(self):
        line = fixed([(0, "P06000123456"), (12, "GULF CRANE SERVICES LLC"), (204, "A"), (220, "1 IND BLVD"),
                      (304, "TAMPA"), (332, "FL"), (334, "33610"), (673, "SMITH JOHN"), (715, "9 HOME ST")], 1440)
        r = parse_cor_line(line)
        self.assertEqual((r["doc_number"], r["name"], r["officer_name"], r["zip"]),
                         ("P06000123456", "GULF CRANE SERVICES LLC", "SMITH JOHN", "33610"))


class FloridaLaneTests(unittest.TestCase):
    EQUIP = [{"county": "hillsborough", "naics": "333120", "equipment_value": 640000, "late_filing_penalty": True,
              "situs": "1 IND BLVD, TAMPA, 33610"}]

    def test_dissolution_with_equipment_is_liquidation(self):
        t = classify({"distress_type": "BUSINESS_CLOSURE", "full_name": "GULF CRANE SERVICES LLC",
                      "raw_payload": {"state": "FL", "fl_tpp": self.EQUIP,
                                      "sunbiz_event": {"description": "VOLUNTARY DISSOLUTION", "filed": "2026-11-13"}}})
        self.assertEqual((t["lane"], t["grade"], t["est_value_min"]), ("business_liquidation", "A", 640000))
        self.assertTrue(any("late filing" in s for s in t["signals"]))

    def test_dissolution_without_equipment_is_dropped(self):
        self.assertIsNone(classify({"distress_type": "BUSINESS_CLOSURE", "full_name": "SMALL CONSULTING LLC",
                                    "raw_payload": {"state": "FL", "sunbiz_event": {
                                        "description": "VOLUNTARY DISSOLUTION", "filed": "2026-11-13"}}}))

    def test_federal_lien_with_equipment_is_seizure(self):
        t = classify({"distress_type": "TAX_LIEN", "full_name": "GULF CRANE SERVICES LLC",
                      "raw_payload": {"source": "sunbiz_flr", "state": "FL", "filed": "2026-11-09",
                                      "fl_tpp": self.EQUIP}})
        self.assertEqual((t["lane"], t["grade"]), ("seizure", "A"))

    def test_hillsborough_probate_contacts(self):
        t = classify({"distress_type": "PROBATE", "property_value": 2_400_000,
                      "raw_payload": {"source": "hillsclerk_daily", "kind": "probate", "state": "FL",
                                      "case_number": "26-CP-1", "attorney": "ANTHONY PALMA",
                                      "petitioner": "LOGAN TESTA", "petitioner_address": "4475 AMBERLY OAKS CT"}})
        self.assertEqual(t["contacts"]["attorney"]["names"], ["ANTHONY PALMA"])
        self.assertEqual(t["contacts"]["representatives"][0]["name"], "LOGAN TESTA")


if __name__ == "__main__":
    unittest.main()
