"""County value and owner for serious code cases: one parcel or nothing.
No network and no database; county answers and the staging table are fakes."""
import base64
import copy
import json
import sqlite3
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.bots import _run_new
from src.bots import code_case_values_bot as ccv
from src.bots._code_severity import code_severity
from src.bots._comptroller import SCHEMA, street_key_from_comptroller
from src.bots._value_guard import value_status

TODAY = "2026-11-26"
SUB = {"m_parcel": ("memphis_311", "PARCEL_ID"), "m_type": ("memphis_311", "REQUEST_TYPE"),
       "c_desc": ("chattanooga_codes", "descriptions"), "j_stage": ("johnson_city_bdsr", "stage"),
       "j_viol": ("johnson_city_bdsr", "violations")}
TOP = {"k_section": "poh_section", "k_case": "case_number", "k_parcel": "parcel_id", "k_roll": "knox_assessor",
       "prior": ccv.KEY}


def selected(db_row):
    """What the slim SELECT returns for a stored row."""
    raw = db_row.get("raw_payload") or {}
    out = {k: v for k, v in db_row.items() if k != "raw_payload"}
    for alias, (outer, inner) in SUB.items():
        out[alias] = (raw.get(outer) or {}).get(inner)
    for alias, key in TOP.items():
        out[alias] = raw.get(key)
    return out


def stored(id_, source, address, raw, **cols):
    county = {"memphis_codes": "Shelby County", "chattanooga_codes": "Hamilton County",
              "knoxville_poh": "Knox County", "johnson_city_bdsr": "Washington County"}[source]
    return {"id": id_, "bot_source": source, "pipeline_lead_key": "key-" + id_, "county": county,
            "property_address": address, "distress_type": "CODE_VIOLATION", "staging_status": "pending",
            "property_value": None, "property_value_source": None, "property_value_as_of": None,
            "full_name": None, "owner_name_records": None, "staged_at": "2026-11-20T00:00:00+00:00",
            "raw_payload": raw, **cols}


def memphis(id_="m1", address="101 N Evergreen St, Memphis, TN 38104", parcel="017043  00017",
            kind="CE-Substandard,Derelict Struc", **cols):
    return stored(id_, "memphis_codes", address, {"memphis_311": {"PARCEL_ID": parcel, "REQUEST_TYPE": kind}}, **cols)


def chattanooga(id_="c1", address="224 HICKORY ST, Chattanooga, TN", **cols):
    return stored(id_, "chattanooga_codes", address,
                  {"chattanooga_codes": {"descriptions": ["DANGEROUS STRUCTURE"], "case_number": "CE1"}}, **cols)


class Query:
    def __init__(self, db):
        self.db, self.filters, self.patch, self.count = db, [], None, None

    def select(self, columns):
        self.db.selects.append(columns)
        return self

    def update(self, values):
        self.patch = values
        return self

    @property
    def not_(self):
        self.negate = True
        return self

    def _add(self, op, *args):
        self.filters.append((("not_" if getattr(self, "negate", False) else "") + op, *args))
        self.negate = False
        return self

    def eq(self, k, v): return self._add("eq", k, v)
    def neq(self, k, v): return self._add("neq", k, v)
    def lt(self, k, v): return self._add("lt", k, v)
    def is_(self, k, v): return self._add("is", k, v)
    def in_(self, k, v): return self._add("in", k, list(v))
    def or_(self, text): return self._add("or", text)
    def order(self, *_, **__): return self

    def limit(self, n):
        self.count = n
        return self

    def _plain(self, row):
        for f in self.filters:
            if f[0] == "eq" and "->" not in f[1] and row.get(f[1]) != f[2]:
                return False
            if f[0] == "neq" and row.get(f[1]) == f[2]:
                return False
            if f[0] == "is" and "->" not in f[1] and row.get(f[1]) is not None:
                return False
            if f[0] == "in" and f[1] == "id" and row["id"] not in f[2]:
                return False
            if f == ("or", "property_value.is.null,owner_name_records.is.null") and not (
                    row.get("property_value") is None or row.get("owner_name_records") is None):
                return False
            if f[0] == "lt" and not (row.get(f[1]) or "") < f[2]:
                return False
        return True

    def execute(self):
        rows = [r for r in self.db.rows.values() if self._plain(r)]
        if self.patch is not None:
            self.db.updates.append((self.patch, list(self.filters)))
            for r in rows:
                r.update(self.patch)
            return SimpleNamespace(data=[{"id": r["id"]} for r in rows])
        self.db.queries.append(list(self.filters) + [("limit", self.count)])
        rows.sort(key=lambda r: r.get("staged_at") or "", reverse=True)
        return SimpleNamespace(data=[selected(r) for r in rows[:self.count]])


class Db:
    """homeowner_requests_staging plus the apply_staging_enrichment procedure."""

    def __init__(self, *rows):
        self.rows = {r["id"]: copy.deepcopy(r) for r in rows}
        self.rpcs, self.updates, self.queries, self.selects = [], [], [], []

    def table(self, name):
        assert name == ccv.TABLE
        return Query(self)

    def rpc(self, name, args):
        assert name == "apply_staging_enrichment"
        self.rpcs.append(copy.deepcopy(args["p_rows"]))
        done = 0
        for e in args["p_rows"]:
            row = self.rows.get(e["id"])
            if row is None:
                continue
            if e.get("property_value") and not row.get("property_value"):
                row.update({k: e.get(k) for k in ("property_value", "property_value_source", "property_value_as_of")})
            row["raw_payload"] = {**(row.get("raw_payload") or {}), **(e.get("raw_patch") or {})}
            done += 1
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=done))

    def writes(self):
        return len(self.rpcs) + len(self.updates)

    def shaped(self, *ids):
        return [ccv.shape(selected(self.rows[i])) for i in (ids or self.rows)]


def bill(parcel="017043  00017", location="101 EVERGREEN", owner="KHOO WEY", **changes):
    return dict(dict(billYear=2026, billCategory=20, billId=1, billNumber=456, parcel=parcel, billLocation=location,
                     accountsReceivableCode={"description": "MEMPHIS REALTY"}, customer={"primaryName": owner}),
                **changes)


def parcel_page(parcel="017043  00017", location="101 EVERGREEN", owner="KHOO WEY", amount=69480, code="C"):
    data = dict(assessment=amount, classification=code, paymentType=20, description="MEMPHIS REALTY", parcel=parcel,
                location=location, primaryName=owner, billNumber=456, year="2026")
    encoded = base64.urlsafe_b64encode(json.dumps(data).encode()).decode()
    cells = [f"<a href='Transaction?billNumberData={encoded}'>456</a>", "2026", "Shelby", "0", "0", "0", "0",
             f"${amount:,.2f}"]
    return (f"<span>Parcel ID:</span><span>{parcel}</span><span>Primary Owner</span><span>{owner}</span>"
            f"<span>Location</span><span>{location}</span><table><tr>"
            + "".join(f"<td>{c}</td>" for c in cells) + "</tr></table>")


def trustee(bills, page=None, status=200):
    """A fetch that answers like the Shelby Trustee site and counts reads."""
    calls = []

    def fetch(url, **_):
        calls.append(url)
        if status != 200:
            return SimpleNamespace(status_code=status, text="", json=lambda: {})
        if "/api/search" in url:
            return SimpleNamespace(status_code=200, json=lambda: {"parcelData": bills})
        return SimpleNamespace(status_code=200, text=page or parcel_page())
    fetch.calls = calls
    return fetch


def county_record(parcel, address, owner="DOE JANE", appraised=171900.0, unit=""):
    return {"parcel": parcel, "owner": owner, "property_address": address, "appraised": appraised,
            "situs_unit": unit}


def bot_with(fetch=None, hamilton=None):
    bot = ccv.CodeCaseValuesBot()
    bot.fetch = fetch or (lambda *a, **k: (_ for _ in ()).throw(AssertionError("no county read expected")))
    if hamilton is not None:
        index = {r["parcel"]: r for r in hamilton}
        exact, loose = ccv.hamilton_streets(index)
        bot._hamilton = (index, exact, loose, index, set(), "2026-11-21")
    return bot


class ShelbyTests(unittest.TestCase):
    def test_exact_parcel_writes_value_with_basis_and_date_and_owner_with_source(self):
        db = Db(memphis())
        out = bot_with(trustee([bill()])).process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["m1"]
        self.assertEqual((row["property_value"], row["property_value_source"], row["property_value_as_of"]),
                         (173700, "shelby_trustee_assessment_estimate", TODAY))
        self.assertEqual(row["owner_name_records"], "KHOO WEY")
        ev = row["raw_payload"][ccv.KEY]
        self.assertEqual((ev["status"], ev["parcel"], ev["match_method"]),
                         ("matched", "017043  00017", "notice_parcel_and_street"))
        self.assertIn("class ratio", ev["value_basis"])
        self.assertEqual((ev["value_as_of"], ev["owner_as_of"]), (TODAY, TODAY))
        self.assertEqual(ev["owner_source"], "Shelby County Trustee current-year realty bill")
        self.assertIn("apps2.shelbycountytrustee.com/Parcel", ev["source_url"])
        status = value_status(row)
        self.assertEqual((status["status"], status["observed_on"]), ("verified", TODAY))
        self.assertEqual(out["counts"]["wrote_value"], 1)

    def test_two_parcels_at_one_address_write_nothing_but_the_reason(self):
        db = Db(memphis(parcel=None))
        bot_with(trustee([bill(), bill(parcel="017043  00018")])).process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["m1"]
        self.assertIsNone(row["property_value"])
        self.assertIsNone(row["owner_name_records"])
        self.assertEqual((row["raw_payload"][ccv.KEY]["status"], row["raw_payload"][ccv.KEY]["reason"]),
                         ("held", "ambiguous_parcels"))
        self.assertNotIn("owner", row["raw_payload"][ccv.KEY])

    def test_reported_parcel_on_another_street_writes_nothing(self):
        db = Db(memphis())
        bot_with(trustee([bill(parcel="017043  00099")])).process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual(db.rows["m1"]["raw_payload"][ccv.KEY]["reason"], "notice_parcel_conflict")
        self.assertIsNone(db.rows["m1"]["property_value"])

    def test_unit_address_is_not_given_the_building_parcel(self):
        db = Db(memphis(address="1797 Evelyn Ave, #1-A, Memphis, TN 38114", parcel="031052  00016"))
        fetch = trustee([bill("031052  00016", "1797 EVELYN")], parcel_page("031052  00016", "1797 EVELYN"))
        bot_with(fetch).process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["m1"]
        self.assertIsNone(row["property_value"])
        self.assertIsNone(row["owner_name_records"])
        self.assertEqual(row["raw_payload"][ccv.KEY]["reason"], "unit_not_matched")

    def test_existing_value_and_owner_are_kept(self):
        db = Db(memphis(property_value=50000, property_value_source="other_source"))
        bot_with(trustee([bill()])).process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["m1"]
        self.assertEqual((row["property_value"], row["property_value_source"]), (50000, "other_source"))
        self.assertEqual(row["owner_name_records"], "KHOO WEY")
        self.assertNotIn("property_value", db.rpcs[0][0])
        db = Db(memphis(owner_name_records="PRIOR OWNER"))
        bot_with(trustee([bill()])).process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual(db.rows["m1"]["owner_name_records"], "PRIOR OWNER")
        self.assertEqual(db.rows["m1"]["property_value"], 173700)
        self.assertEqual(db.updates, [])

    def test_refused_or_failed_county_read_changes_nothing(self):
        for status in (403, 500):
            db = Db(memphis())
            before = copy.deepcopy(db.rows)
            out = bot_with(trustee([bill()], status=status)).process(db, db.shaped(), write=True, today=TODAY)
            self.assertEqual(db.rows, before)
            self.assertEqual(db.writes(), 0)
            self.assertEqual(out["plan"][0]["status"], "unavailable")
        db = Db(memphis())
        bot_with(lambda *a, **k: None).process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual(db.writes(), 0)

    def test_county_is_left_alone_after_repeated_failed_reads(self):
        db = Db(*[memphis(f"m{i}") for i in range(6)])
        fetch = trustee([bill()], status=403)
        out = bot_with(fetch).process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual(len(fetch.calls), ccv.MAX_FAILED_READS)
        self.assertEqual(out["counts"]["unavailable"], 6)
        self.assertEqual(db.writes(), 0)

    def test_value_page_that_cannot_be_verified_gives_owner_only(self):
        db = Db(memphis())
        bot_with(trustee([bill()], parcel_page(code="X"))).process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["m1"]
        self.assertIsNone(row["property_value"])
        self.assertEqual(row["owner_name_records"], "KHOO WEY")
        self.assertNotIn("value", row["raw_payload"][ccv.KEY])

    def test_repeat_run_writes_nothing_and_reads_no_county(self):
        db = Db(memphis(), memphis("m2", parcel=None))
        fetch = trustee([bill(), bill(parcel="017043  00018")])
        bot_with(fetch).process(db, db.shaped(), write=True, today=TODAY)
        writes, reads, before = db.writes(), len(fetch.calls), copy.deepcopy(db.rows)
        self.assertGreater(writes, 0)
        out = bot_with(fetch).process(db, db.shaped(), write=True, today="2026-11-27")
        self.assertEqual((db.writes(), len(fetch.calls)), (writes, reads))
        self.assertEqual(db.rows, before)
        self.assertEqual(out["counts"].get("skipped"), 2)

    def test_matched_check_finishes_a_missed_owner_without_a_county_read(self):
        db = Db(memphis())
        bot_with(trustee([bill()])).process(db, db.shaped(), write=True, today=TODAY)
        db.rows["m1"]["owner_name_records"] = None  # the owner update was lost
        db.updates.clear()
        bot_with().process(db, db.shaped(), write=True, today="2026-11-27")
        self.assertEqual(db.rows["m1"]["owner_name_records"], "KHOO WEY")
        self.assertEqual(len(db.rpcs), 1)  # evidence unchanged: not sent again

    def test_dry_run_writes_nothing(self):
        db = Db(memphis())
        out = bot_with(trustee([bill()])).process(db, db.shaped(), write=False, today=TODAY)
        self.assertEqual(db.writes(), 0)
        self.assertEqual(out["plan"][0]["write"]["property_value"], 173700)
        line = ccv.plan_row(out["plan"][0])
        self.assertEqual((line["parcel"], line["owner"], line["value"], line["observed_on"]),
                         ("017043  00017", "KHOO WEY", 173700, TODAY))
        self.assertTrue(line["basis"] and line["source_url"])

    def test_per_run_cap_holds(self):
        db = Db(*[memphis(f"m{i}") for i in range(5)])
        fetch = trustee([bill()])
        out = bot_with(fetch).process(db, db.shaped(), write=True, cap=2, today=TODAY)
        self.assertEqual(out["counts"]["checked"], 2)
        self.assertEqual(len(fetch.calls), 4)  # search and parcel page, twice
        self.assertEqual(sum(1 for r in db.rows.values() if r["property_value"]), 2)

    def test_nuisance_case_and_rejected_row_are_not_read(self):
        db = Db(memphis(kind="CE-Roof"), memphis("m2", staging_status="rejected"))
        out = bot_with().process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual([e["reason"] for e in out["plan"]], ["not_a_serious_case", "rejected_row"])
        self.assertEqual(db.writes(), 0)


class HamiltonTests(unittest.TestCase):
    def run_one(self, row, records):
        db = Db(row)
        bot_with(hamilton=records).process(db, db.shaped(), write=True, today=TODAY)
        return db.rows[row["id"]]

    def test_exact_street_writes_county_appraisal_and_owner(self):
        row = self.run_one(chattanooga(address="224 N HICKORY ST, Chattanooga, TN"),
                           [county_record("146G-N-039", "224 N HICKORY ST")])
        self.assertEqual((row["property_value"], row["property_value_source"], row["property_value_as_of"]),
                         (171900, "hamilton_assessor", "2026-11-21"))
        self.assertEqual(row["owner_name_records"], "DOE JANE")
        self.assertEqual(row["raw_payload"][ccv.KEY]["match_method"], "exact_county_address")
        self.assertEqual(value_status(row)["status"], "verified")

    def test_omitted_direction_is_accepted_only_when_one_parcel_has_that_street(self):
        row = self.run_one(chattanooga(), [county_record("146G-N-039", "224 N HICKORY ST")])
        self.assertEqual(row["property_value"], 171900)
        self.assertEqual(row["raw_payload"][ccv.KEY]["match_method"], "unique_county_street_direction_prefix_omitted")
        row = self.run_one(chattanooga(), [county_record("146G-N-039", "224 N HICKORY ST"),
                                           county_record("155A-B-001", "224 S HICKORY ST")])
        self.assertIsNone(row["property_value"])
        self.assertIsNone(row["owner_name_records"])
        self.assertEqual(row["raw_payload"][ccv.KEY]["reason"], "ambiguous_address")

    def test_leading_zero_in_numbered_street(self):
        row = self.run_one(chattanooga(address="2401 5TH ST, Chattanooga, TN"),
                           [county_record("146K-G-013", "2401 E 05TH ST")])
        self.assertEqual(row["property_value"], 171900)

    def test_spelled_out_direction_is_a_different_street_name(self):
        row = self.run_one(chattanooga(address="8512 BRAINERD RD, Chattanooga, TN"),
                           [county_record("171B-C-006", "8512 EAST BRAINERD RD")])
        self.assertIsNone(row["property_value"])
        ev = row["raw_payload"][ccv.KEY]
        self.assertEqual((ev["reason"], ev["candidate_address"]), ("street_name_differs", "8512 EAST BRAINERD RD"))

    def test_two_parcels_at_one_address_write_nothing(self):
        row = self.run_one(chattanooga(address="224 N HICKORY ST, Chattanooga, TN"),
                           [county_record("146G-N-039", "224 N HICKORY ST"),
                            county_record("146G-N-039.01", "224 N HICKORY ST", owner="OTHER")])
        self.assertIsNone(row["property_value"])
        self.assertEqual(row["raw_payload"][ccv.KEY]["reason"], "ambiguous_address")

    def test_different_unit_writes_nothing(self):
        row = self.run_one(chattanooga(address="224 N HICKORY ST, Unit 4, Chattanooga, TN"),
                           [county_record("146G-N-039", "224 N HICKORY ST", unit="7")])
        self.assertIsNone(row["property_value"])
        self.assertIsNone(row["owner_name_records"])
        self.assertEqual(row["raw_payload"][ccv.KEY]["reason"], "unit_not_matched")
        row = self.run_one(chattanooga(address="224 N HICKORY ST, Unit 4, Chattanooga, TN"),
                           [county_record("146G-N-039", "224 N HICKORY ST", unit="4")])
        self.assertEqual(row["property_value"], 171900)

    def test_owner_name_alone_never_matches(self):
        row = self.run_one(chattanooga(full_name="DOE JANE"), [county_record("100A-A-001", "900 OAK ST")])
        self.assertIsNone(row["property_value"])
        self.assertIsNone(row["owner_name_records"])
        self.assertEqual(row["raw_payload"][ccv.KEY]["reason"], "no_county_address")

    def test_failed_download_changes_nothing(self):
        db = Db(chattanooga())
        bot = bot_with()
        bot._hamilton = False
        out = bot.process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual(db.writes(), 0)
        self.assertEqual(out["plan"][0]["status"], "unavailable")


class KnoxTests(unittest.TestCase):
    ROLL = {"owner": "THOMPSON GREGORY QUINN", "tax_year": 2026, "parcel_id": "070PB005", "checked_on": "2026-11-22",
            "source_url": "https://propertytax.knoxcountytn.gov/property-tax/bill/070PB-005/2026",
            "case_number": "CE-26-006922", "appraised_value": 299500,
            "property_address": "2937 Whittle Springs Road, Knoxville, TN"}

    def case(self, roll=None, **raw):
        payload = {"poh_section": "case", "case_number": "CE-26-006922", "parcel_id": "070PB005", **raw}
        if roll:
            payload["knox_assessor"] = roll
        return stored("k1", "knoxville_poh", "2937 Whittle Springs Road, Knoxville, TN", payload)

    def test_stored_bill_is_not_used_unless_the_source_is_cleared(self):
        db = Db(self.case(self.ROLL))
        bot_with().process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["k1"]
        self.assertIsNone(row.get("property_value"))
        self.assertIsNone(row.get("owner_name_records"))

    def test_stored_bill_for_the_docket_parcel_supplies_value_and_owner(self):
        db = Db(self.case(self.ROLL))
        with mock.patch("src.bots.code_case_values_bot.KNOX_STORED_BILL_OK", True):
            bot_with().process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["k1"]
        self.assertEqual((row["property_value"], row["property_value_source"], row["property_value_as_of"]),
                         (299500, "knox_trustee_bill", "2026-11-22"))
        self.assertEqual(row["owner_name_records"], "THOMPSON GREGORY QUINN")
        self.assertEqual(value_status(row)["status"], "verified")

    def test_no_stored_record_means_no_read_and_no_value(self):
        db = Db(self.case())
        bot_with().process(db, db.shaped(), write=True, today=TODAY)
        self.assertIsNone(db.rows["k1"]["property_value"])
        self.assertEqual(db.rows["k1"]["raw_payload"][ccv.KEY]["reason"], "no_permitted_county_source")

    @mock.patch("src.bots.code_case_values_bot.KNOX_STORED_BILL_OK", True)
    def test_stored_record_for_another_parcel_or_street_writes_nothing(self):
        for change in ({"parcel_id": "070PB006"}, {"property_address": "2939 Whittle Springs Road, Knoxville, TN"},
                       {"case_number": "CE-26-000001"}):
            db = Db(self.case({**self.ROLL, **change}))
            bot_with().process(db, db.shaped(), write=True, today=TODAY)
            self.assertIsNone(db.rows["k1"]["property_value"])
            self.assertIsNone(db.rows["k1"]["owner_name_records"])
            self.assertEqual(db.rows["k1"]["raw_payload"][ccv.KEY]["reason"],
                             "stored_county_record_does_not_match_case")


class ComptrollerTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.db_path = Path(self.dir.name) / "index.sqlite"
        state = Path(self.dir.name) / "refresh_state.json"
        state.write_text(json.dumps({"Washington": "Mon, 23 Nov 2026 17:54:59 GMT"}), encoding="utf-8")
        for name, value in (("COMPTROLLER_DB", self.db_path), ("COMPTROLLER_STATE", state)):
            p = patch.object(ccv, name, value)
            p.start()
            self.addCleanup(p.stop)

    def load(self, *parcels):
        con = sqlite3.connect(str(self.db_path))
        con.executescript(SCHEMA)
        for parcelid, address, owner in parcels:
            values = {"county": "WASHINGTON", "parcelid": parcelid, "street_key": street_key_from_comptroller(address),
                      "city": "JOHNSON CITY", "owner": owner, "owner2": "", "appraisal": 88000.0, "address": address}
            con.execute(f"INSERT INTO parcels ({','.join(values)}) VALUES ({','.join('?' * len(values))})",
                        list(values.values()))
        con.commit()
        con.close()

    def case(self, address="204 W Maple St, Johnson City, TN"):
        return stored("j1", "johnson_city_bdsr", address,
                      {"johnson_city_bdsr": {"stage": "hearing_listed", "violations": ""}})

    def test_one_parcel_on_the_roll_writes_value_and_owner(self):
        self.load(("090 046K A 00100 000 2026", "W MAPLE ST 204", "ROE RICHARD"))
        db = Db(self.case())
        bot_with().process(db, db.shaped(), write=True, today=TODAY)
        row = db.rows["j1"]
        self.assertEqual((row["property_value"], row["property_value_source"], row["property_value_as_of"]),
                         (88000, "comptroller", "2026-11-23"))
        self.assertEqual(row["owner_name_records"], "ROE RICHARD")
        self.assertEqual(value_status(row)["status"], "verified")

    def test_two_parcels_or_none_write_nothing(self):
        self.load(("P1 000 2026", "W MAPLE ST 204", "ROE RICHARD"), ("P2 000 2026", "W MAPLE ST 204", "ROE RICHARD"))
        db = Db(self.case(), stored("j2", "johnson_city_bdsr", "9 Elm St, Johnson City, TN",
                                    {"johnson_city_bdsr": {"stage": "hearing_listed"}}))
        bot_with().process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual(db.rows["j1"]["raw_payload"][ccv.KEY]["reason"], "ambiguous_address")
        self.assertEqual(db.rows["j2"]["raw_payload"][ccv.KEY]["reason"], "no_county_address")
        self.assertTrue(all(r["property_value"] is None and r["owner_name_records"] is None for r in db.rows.values()))

    def test_missing_roll_changes_nothing(self):
        db = Db(self.case())
        out = bot_with().process(db, db.shaped(), write=True, today=TODAY)
        self.assertEqual(db.writes(), 0)
        self.assertEqual(out["plan"][0]["reason"], "county_roll_not_loaded")


class SelectionTests(unittest.TestCase):
    def test_candidates_are_selected_by_source_in_small_slim_pages(self):
        db = Db(memphis("m1"), memphis("m2", property_value=9, owner_name_records="X"), chattanooga("c1"),
                memphis("m3", staging_status="rejected"))
        rows = bot_with().candidates(db, TODAY, 40)
        self.assertEqual(sorted(r["id"] for r in rows), ["c1", "m1"])
        self.assertEqual(len(db.queries), len(ccv.SOURCES))
        for q in db.queries:
            self.assertIn(("limit", ccv.PAGE), q)
            self.assertTrue(any(f[0] == "eq" and f[1] == "bot_source" for f in q))
            self.assertIn(("or", "property_value.is.null,owner_name_records.is.null"), q)
        for columns in db.selects:
            self.assertNotIn("raw_payload,", columns + ",")
            self.assertNotIn("phone_metadata", columns)

    def test_candidates_stop_at_the_cap_and_take_turns(self):
        db = Db(*[memphis(f"m{i}") for i in range(6)], chattanooga("c1"))
        rows = bot_with().candidates(db, TODAY, 3)
        self.assertEqual(len(rows), 3)
        self.assertIn("c1", [r["id"] for r in rows])

    def test_scheduled_run_respects_the_cap_and_the_switch(self):
        db = Db(*[memphis(f"m{i}") for i in range(5)])
        bot = bot_with(trustee([bill()]))
        bot._report_health = lambda **_: None
        with patch.object(ccv, "_supabase", lambda: db), patch.object(ccv, "MAX_PER_RUN", 2):
            out = bot.run()
            self.assertEqual((out["status"], out["checked"], out["wrote_value"]), ("ok", 2, 2))
            with patch.dict("os.environ", {"CODE_CASE_VALUES_WRITE": "0"}):
                writes = db.writes()
                bot.run()
                self.assertEqual(db.writes(), writes)

    def test_runner_lists_the_step_after_the_county_rolls(self):
        names = [n for n, _ in _run_new.NEW_BOTS]
        self.assertIn("code_case_values", names)
        self.assertGreater(names.index("code_case_values"), names.index("comptroller_parcels"))
        self.assertNotIn("code_case_values", _run_new.CRITICAL_BOTS)


class ApplyPlanTests(unittest.TestCase):
    def plan(self, db):
        out = bot_with(trustee([bill()])).process(db, db.shaped(), write=False, today=TODAY)
        return [ccv.plan_row(e) for e in out["plan"]]

    def test_reviewed_plan_is_written_without_a_county_read(self):
        db = Db(memphis())
        entries = self.plan(db)
        self.assertEqual(ccv.apply_plan(bot_with(), db, entries, write=False)["counts"], {"dry_run": 1,
                         "wrote_evidence": 0, "wrote_value": 0, "wrote_owner": 0})
        self.assertEqual(db.writes(), 0)
        result = ccv.apply_plan(bot_with(), db, entries, write=True)
        self.assertEqual(result["results"][0]["written"], {"evidence": True, "value": True, "owner": True})
        self.assertEqual(db.rows["m1"]["property_value"], 173700)
        writes = db.writes()
        again = ccv.apply_plan(bot_with(), db, entries, write=True)
        self.assertEqual(db.writes(), writes)
        self.assertEqual(again["results"][0]["written"], {"evidence": False, "value": False, "owner": False})

    def test_row_that_changed_since_the_plan_is_left_alone(self):
        db = Db(memphis())
        entries = self.plan(db)
        db.rows["m1"]["property_address"] = "999 Other St, Memphis, TN"
        result = ccv.apply_plan(bot_with(), db, entries, write=True)
        self.assertEqual(result["results"][0]["note"], "row_changed_since_plan")
        self.assertEqual(db.writes(), 0)


class SeverityPortTests(unittest.TestCase):
    """public.lead_code_severity after site migration 20261127000100."""

    def sev(self, **raw):
        return code_severity({"distress_type": "CODE_VIOLATION", "raw_payload": raw})

    def test_other_city_fields(self):
        self.assertEqual(self.sev(memphis_311={"REQUEST_TYPE": "CE-Substandard,Derelict Struc"}), "severe")
        self.assertEqual(self.sev(memphis_311={"REQUEST_TYPE": "CE-Boarded Vacant"}), "severe")
        self.assertEqual(self.sev(memphis_311={"REQUEST_TYPE": "CE-Roof"}), "nuisance")
        self.assertEqual(self.sev(memphis_311={"REQUEST_TYPE": "CE-Dilapidated"}), "nuisance")
        self.assertEqual(self.sev(chattanooga_codes={"descriptions": ["WEEDS", "Dangerous Structure"]}), "severe")
        self.assertEqual(self.sev(chattanooga_codes={"descriptions": ["WEEDS"]}), "nuisance")
        self.assertEqual(self.sev(poh_section="boarding"), "severe")
        self.assertEqual(self.sev(johnson_city_bdsr={"stage": "hearing_listed", "violations": ""}), "severe")
        self.assertEqual(self.sev(johnson_city_bdsr={"stage": "docket_removal_reported", "violations": "burnout"}),
                         "severe")
        self.assertEqual(self.sev(johnson_city_bdsr={"stage": "docket_removal_reported", "violations": "weeds"}),
                         "nuisance")
        self.assertEqual(self.sev(mt_juliet_issue_review={"condition_excerpt": "house is unsafe"}), "severe")

    def test_nashville_boarding_house_stays_nuisance(self):
        self.assertEqual(self.sev(nashville_codes={"Violations_Noted": "BOARDING HOUSE", "Reported_Problem": None}),
                         "nuisance")


if __name__ == "__main__":
    unittest.main()
