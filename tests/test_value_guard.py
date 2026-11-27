"""A value belongs to a parcel, not to a street (see src/bots/_value_guard.py)."""
import copy
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _fake_supabase import project  # noqa: E402

from src.bots import xref_avm_enricher_bot as xref
from src.bots._field_confidence import property_value_trust
from src.bots._value_guard import (address_unit, copy_decision, own_parcel_value, parcel_forms,
                                   require_value_evidence, row_parcels, unverified_label, value_status)
from src.bots.hamilton_assessor_bot import HamiltonAssessorBot, file_day


def tax_row(id_, parcel, alt, value=None, source=None, detail=None, address="3911 WINDYKE, Shelby County, TN"):
    raw = {"parcel": parcel, "alt_parcel": alt, "sale_id": "TS2302"}
    if detail:
        raw["trustee_detail"] = detail
    return {"id": id_, "county": "shelby", "property_address": address, "property_value": value,
            "property_value_source": source, "property_value_as_of": None, "owner_name_records": "KHANLARIAN HAIK",
            "raw_payload": raw, "phone_metadata": None}


def notice_row(id_, address, parcel, value, checked="2026-11-24"):
    return {"id": id_, "county": "shelby", "property_address": address, "property_value": value,
            "property_value_source": "shelby_trustee_assessment_estimate", "property_value_as_of": None,
            "raw_payload": {"extracted": {"parcel": parcel}},
            "phone_metadata": {"shelby_trustee_value_review": {
                "status": "verified_assessment", "parcel_id": parcel, "estimated_appraisal": value,
                "assessed_value": value / 4, "checked_on": checked, "location": address.split(",")[0].upper()}}}


def comptroller_row(id_, value=None, source=None, parcelid="057 055K E 00100 000 2026", appraisal=334800,
                    address="620 Carriage House Dr, Jackson, TN 38305", county="madison", block=True):
    raw = {"extracted": {"parcel": "055K-E-001.00"}}
    if block:
        raw["comptroller_parcel"] = {"parcelid": parcelid, "appraisal": appraisal, "county": county.upper(),
                                     "address": "CARRIAGE HOUSE DR 620", "method": "address",
                                     "version": "comptroller-parcels-2026-11-05"}
    return {"id": id_, "county": county, "property_address": address, "property_value": value,
            "property_value_source": source, "property_value_as_of": None, "owner_name_records": None,
            "raw_payload": raw, "phone_metadata": None}


KHANLARIAN_DONOR = tax_row("donor", "D0242N E00083", "D0242N0E000830", 295700, "shelby_trustee_assessment_estimate",
                           {"estimated_appraisal": 295700, "assessed_value": 73925, "checked_on": "2026-11-22"})
KHANLARIAN_OTHER = tax_row("other", "081101  00011", "08110100000110",
                           detail={"checked_on": "2026-11-21", "total_tax_balance": 7799.65})


class Query:
    def __init__(self, db, table):
        self.db, self.table, self.filters, self.patch, self.window = db, table, [], None, None
        self.columns = "*"

    def select(self, columns="*", *_):
        self.columns = columns
        return self

    def in_(self, key, values):
        self.filters.append(lambda r: r.get(key) in values)
        return self

    def order(self, *_):
        return self

    def range(self, a, b):
        self.window = (a, b)
        return self

    def update(self, patch):
        self.patch = patch
        return self

    def eq(self, key, value):
        self.filters.append(lambda r: r.get(key) == value)
        return self

    def is_(self, key, _null):
        self.filters.append(lambda r: r.get(key) is None)
        return self

    def execute(self):
        rows = [r for r in self.db.tables[self.table] if all(f(r) for f in self.filters)]
        if self.patch is not None:
            for r in rows:
                r.update(copy.deepcopy(self.patch))
            self.db.writes.extend((r["id"], copy.deepcopy(self.patch)) for r in rows)
            return mock.Mock(data=rows)
        rows = sorted(rows, key=lambda r: r["id"])
        a, b = self.window or (0, len(rows))
        return mock.Mock(data=[project(r, self.columns) for r in rows[a:b + 1]])


class Db:
    def __init__(self, staging, live=()):
        self.tables = {"homeowner_requests_staging": copy.deepcopy(list(staging)),
                       "homeowner_requests": copy.deepcopy(list(live))}
        self.writes = []

    def table(self, name):
        return Query(self, name)

    def row(self, id_):
        return next(r for r in self.tables["homeowner_requests_staging"] if r["id"] == id_)


def run_xref(db):
    with mock.patch.object(xref, "_supabase", return_value=db), \
            mock.patch.object(xref.XrefAvmEnricherBot, "_report_health", lambda *a, **k: None), \
            mock.patch.object(xref, "record_field", lambda *a, **k: None):
        return xref.run()


class ParcelForms(unittest.TestCase):
    def test_shelby_two_spellings_are_one_parcel(self):
        self.assertTrue(parcel_forms("D0242N E00083", "shelby") & parcel_forms("D0242N0E000830", "shelby"))
        self.assertTrue(parcel_forms("081101  00011", "shelby") & parcel_forms("08110100000110", "shelby"))
        self.assertFalse(parcel_forms("081101  00011", "shelby") & parcel_forms("D0242N E00083", "shelby"))

    def test_roll_year_is_not_part_of_the_parcel(self):
        self.assertEqual(parcel_forms("057 055K E 00100 000 2026"), parcel_forms("057 055K E 00100 000 2027"))

    def test_tenant_account_is_not_a_parcel(self):
        self.assertEqual(parcel_forms("PER--10026327"), set())

    def test_unit(self):
        self.assertEqual(address_unit("100 Main St Unit 4, Nashville, TN"), "4")
        self.assertEqual(address_unit("100 Main St #12B, Nashville"), "12B")
        self.assertIsNone(address_unit("100 Main St, Nashville, TN"))


class CopyRule(unittest.TestCase):
    def test_same_street_different_parcel_gets_no_value(self):
        """Khanlarian: two Shelby parcels at 3911 Windyke. The one without a
        county assessment must not take the other's."""
        self.assertEqual(copy_decision(KHANLARIAN_DONOR, KHANLARIAN_OTHER)["reason"], "different_parcel")
        db = Db([KHANLARIAN_DONOR, KHANLARIAN_OTHER])
        result = run_xref(db)
        self.assertEqual(result["propagated"], 0)
        self.assertIsNone(db.row("other")["property_value"])
        self.assertEqual(db.writes, [])

    def test_same_street_different_unit_gets_no_value(self):
        donor = comptroller_row("donor", 210000, "comptroller", parcelid="019 093 01200 000 2026", appraisal=210000,
                                address="100 Main St Unit 1, Franklin, TN", county="williamson")
        donor["raw_payload"].pop("extracted")
        rcpt = {"id": "rcpt", "county": "williamson", "property_address": "100 Main St Unit 2, Franklin, TN",
                "property_value": None, "property_value_source": None, "property_value_as_of": None,
                "raw_payload": {"parcel": "019 093 01200 000"}, "phone_metadata": None}
        self.assertEqual(copy_decision(donor, rcpt)["reason"], "different_unit")
        db = Db([donor, rcpt])
        run_xref(db)
        self.assertIsNone(db.row("rcpt")["property_value"])

    def test_other_county_gets_no_value(self):
        donor = comptroller_row("donor", 334800, "comptroller")
        rcpt = comptroller_row("rcpt", block=False, county="fayette")
        rcpt["raw_payload"] = {"parcel": "057 055K E 00100 000"}
        self.assertEqual(copy_decision(donor, rcpt)["reason"], "county_mismatch")

    def test_no_parcel_on_the_recipient_gets_no_value(self):
        donor = comptroller_row("donor", 334800, "comptroller")
        rcpt = {"id": "rcpt", "county": "madison", "property_address": donor["property_address"],
                "property_value": None, "raw_payload": {}, "phone_metadata": None}
        self.assertEqual(copy_decision(donor, rcpt)["reason"], "recipient_has_no_parcel")
        db = Db([donor, rcpt])
        self.assertEqual(run_xref(db)["propagated"], 0)

    def test_undated_or_unbacked_donor_is_refused(self):
        donor = comptroller_row("donor", 334800, "xref_avm_enricher", block=False)
        donor["raw_payload"] = {"parcel": "057 055K E 00100 000"}
        rcpt = comptroller_row("rcpt", block=False)
        rcpt["raw_payload"] = {"parcel": "057 055K E 00100 000"}
        self.assertEqual(copy_decision(donor, rcpt)["reason"], "donor_value_unverified")

    def test_exact_parcel_copy_carries_basis_and_date(self):
        donor = comptroller_row("donor", 334800, "comptroller")
        rcpt = comptroller_row("rcpt", block=False)
        rcpt["raw_payload"] = {"parcel": "057 055K E 00100 000"}
        db = Db([donor, rcpt])
        self.assertEqual(run_xref(db)["propagated"], 1)
        got = db.row("rcpt")
        self.assertEqual(got["property_value"], 334800)
        self.assertEqual(got["property_value_source"], "comptroller")
        self.assertEqual(got["property_value_as_of"], "2026-11-05")
        proof = got["phone_metadata"]["property_value_xref"]
        self.assertEqual((proof["match"], proof["parcel"], proof["county"], proof["observed_on"]),
                         ("exact_parcel", "057 055K E 00100 000 2026", "madison", "2026-11-05"))
        self.assertIn("Comptroller", proof["basis"])
        self.assertEqual(value_status(got)["status"], "verified")
        self.assertEqual(property_value_trust(got).kind, "assessor_value")

    def test_own_parcel_record_supplies_the_value_under_its_own_source(self):
        """Global Midsouth: the row's own Comptroller parcel states $334,800,
        so the value is the Comptroller's, not a cross-reference."""
        donor = comptroller_row("donor", 334800, "comptroller")
        rcpt = comptroller_row("rcpt")
        db = Db([donor, rcpt])
        self.assertEqual(run_xref(db)["propagated"], 1)
        got = db.row("rcpt")
        self.assertEqual((got["property_value"], got["property_value_source"], got["property_value_as_of"]),
                         (334800, "comptroller", "2026-11-05"))
        self.assertNotIn("phone_metadata", [k for k, v in db.writes[0][1].items()])

    def test_duplicates_and_repeat_run(self):
        donor = comptroller_row("donor", 334800, "comptroller")
        twins = []
        for n in (1, 2, 3):
            r = comptroller_row(f"twin{n}", block=False)
            r["raw_payload"] = {"parcel": "057 055K E 00100 000"}
            twins.append(r)
        db = Db([donor] + twins + [KHANLARIAN_DONOR, KHANLARIAN_OTHER])
        self.assertEqual(run_xref(db)["propagated"], 3)
        self.assertEqual({db.row(f"twin{n}")["property_value"] for n in (1, 2, 3)}, {334800})
        before = copy.deepcopy(db.tables)
        again = run_xref(db)
        self.assertEqual(again["propagated"], 0)
        self.assertEqual(db.tables, before)
        self.assertIsNone(db.row("other")["property_value"])


class ExistingValues(unittest.TestCase):
    def test_identical_values_on_different_parcels_are_both_kept(self):
        """Stroud and Morgan: $315,600 each, from each parcel's own county bill."""
        stroud = notice_row("37da9a31", "38 N Alicia Dr, Memphis, TN 38112", "028042 00020", 315600)
        morgan = notice_row("33e68fa4", "6350 Quince Rd, Memphis, TN 38119", "081033 00078", 315600)
        for row, parcel in ((stroud, "028042 00020"), (morgan, "081033 00078")):
            status = value_status(row)
            self.assertEqual((status["status"], status["parcel"], status["observed_on"]),
                             ("verified", parcel, "2026-11-24"))
            self.assertIsNone(unverified_label(row))
        db = Db([stroud, morgan])
        self.assertEqual(run_xref(db)["propagated"], 0)
        self.assertEqual((db.row("37da9a31")["property_value"], db.row("33e68fa4")["property_value"]),
                         (315600, 315600))
        self.assertEqual(db.writes, [])

    def test_address_only_copy_is_labeled_unverified(self):
        leaked = copy.deepcopy(KHANLARIAN_OTHER)
        leaked.update(property_value=295700, property_value_source="xref_avm_enricher",
                      phone_metadata={"property_value_xref": {"source": "xref_avm_enricher", "confidence": 0.65,
                                                              "donor_id": "donor"}})
        status = value_status(leaked)
        self.assertEqual(status["status"], "unverified")
        self.assertIn("street address", unverified_label(leaked))
        self.assertNotIn("—", unverified_label(leaked))

    def test_address_copy_backed_by_the_rows_own_parcel_record_is_verified(self):
        row = comptroller_row("gm", 334800, "xref_avm_enricher")
        row["phone_metadata"] = {"property_value_xref": {"source": "xref_avm_enricher", "donor_id": "x"}}
        status = value_status(row)
        self.assertEqual((status["status"], status["source"], status["observed_on"]),
                         ("verified", "comptroller", "2026-11-05"))

    def test_value_without_a_date_is_unverified(self):
        row = {"county": "hamilton", "property_address": "107 Passons Rd, Chattanooga, TN", "property_value": 200300,
               "property_value_source": "hamilton_assessor", "property_value_as_of": None,
               "raw_payload": {"parcel": "099K-C-015", "hamilton_assessor": {"parcel": "099K-C-015", "appraised": 200300}}}
        self.assertEqual(value_status(row)["reason"], "the parcel record has no observation date")
        row["property_value_as_of"] = "2026-11-21"
        self.assertEqual(value_status(row)["status"], "verified")

    def test_loan_estimate_is_an_estimate_not_a_parcel_value(self):
        row = {"county": "shelby", "property_value": 180000, "property_value_source": "shelby_loan_hpi_estimate"}
        self.assertEqual(value_status(row)["status"], "estimate")
        self.assertIsNone(unverified_label(row))


class WriteGate(unittest.TestCase):
    ROW = {"county": "hamilton", "property_address": "1000 Dayton Blvd, Chattanooga, TN",
           "raw_payload": {"county": "hamilton", "cumulative_owed": 6896.31, "parcel": "126N-B-007"}}

    def patch(self):
        return {"property_value": 222700, "property_value_source": "hamilton_assessor", "owner_name_records": "X"}

    def test_complete_evidence_is_stamped(self):
        p = self.patch()
        self.assertIsNone(require_value_evidence(p, self.ROW, parcel="126N-B-007", county="hamilton",
                                                 observed_on="2026-11-21"))
        self.assertEqual((p["property_value"], p["property_value_as_of"]), (222700, "2026-11-21"))

    def test_each_missing_piece_removes_the_value_and_keeps_the_rest(self):
        cases = (
            (dict(parcel=None, county="hamilton", observed_on="2026-11-21"), "no_parcel"),
            (dict(parcel="126N-B-007", county="knox", observed_on="2026-11-21"), "county_mismatch"),
            (dict(parcel="126N-B-099", county="hamilton", observed_on="2026-11-21"), "parcel_mismatch"),
            (dict(parcel="126N-B-007", county="hamilton", observed_on=None), "no_observation_date"),
        )
        for kwargs, reason in cases:
            p = self.patch()
            self.assertEqual(require_value_evidence(p, self.ROW, **kwargs), reason)
            self.assertEqual(p, {"owner_name_records": "X"})

    def test_unknown_source_has_no_basis(self):
        p = {"property_value": 1, "property_value_source": "somewhere"}
        self.assertEqual(require_value_evidence(p, self.ROW, parcel="126N-B-007", county="hamilton",
                                                observed_on="2026-11-21"), "no_basis")

    def test_unit_needs_the_unit_or_the_named_parcel(self):
        row = {"county": "davidson", "property_address": "100 Main St Unit 4, Nashville, TN", "raw_payload": {}}
        p = {"property_value": 300000, "property_value_source": "davidson_assessor"}
        self.assertEqual(require_value_evidence(p, row, parcel="09301012300", county="davidson",
                                                observed_on="2026-11-24", record_address="100 MAIN ST"),
                         "unit_not_matched")
        p = {"property_value": 300000, "property_value_source": "davidson_assessor"}
        self.assertIsNone(require_value_evidence(p, row, parcel="09301012300", county="davidson",
                                                 observed_on="2026-11-24", record_address="100 MAIN ST UNIT 4"))
        named = dict(row, raw_payload={"parcel": "093 01 0 123.00"})
        p = {"property_value": 300000, "property_value_source": "davidson_assessor"}
        self.assertIsNone(require_value_evidence(p, named, parcel="09301012300", county="davidson",
                                                 observed_on="2026-11-24", record_address="100 MAIN ST"))

    def test_patch_without_a_value_is_left_alone(self):
        p = {"owner_name_records": "X"}
        self.assertIsNone(require_value_evidence(p, self.ROW, parcel=None, county=None, observed_on=None))
        self.assertEqual(p, {"owner_name_records": "X"})


class HamiltonLookup(unittest.TestCase):
    INDEX = {"126N-B-007": {"parcel": "126N-B-007", "property_address": "1000 DAYTON BLVD", "appraised": 222700},
             "145C-A-001": {"parcel": "145C-A-001", "property_address": "200 MARKET ST", "appraised": 100000},
             "145C-A-002": {"parcel": "145C-A-002", "property_address": "200 MARKET ST", "appraised": 900000}}

    def test_street_with_two_parcels_matches_neither(self):
        ambiguous = set()
        addresses = HamiltonAssessorBot._build_address_index(self.INDEX, ambiguous)
        self.assertEqual(ambiguous, {"200 MARKET ST"})
        bot = HamiltonAssessorBot.__new__(HamiltonAssessorBot)
        row = {"property_address": "200 Market St, Chattanooga, TN", "raw_payload": {}}
        self.assertIsNone(bot._lookup(row, self.INDEX, addresses, ambiguous))
        row = {"property_address": "1000 Dayton Blvd, Chattanooga, TN", "raw_payload": {}}
        self.assertEqual(bot._lookup(row, self.INDEX, addresses, ambiguous)["parcel"], "126N-B-007")

    def test_file_day(self):
        self.assertEqual(file_day("Sat, 21 Nov 2026 05:00:00 GMT"), "2026-11-21")
        self.assertRegex(file_day('"etag-abc"'), r"^\d{4}-\d{2}-\d{2}$")


class OwnRecord(unittest.TestCase):
    def test_own_record_in_another_county_is_not_used(self):
        row = comptroller_row("x", county="fayette")
        row["raw_payload"]["comptroller_parcel"]["county"] = "MADISON"
        self.assertIsNone(own_parcel_value(row))
        self.assertNotIn("057055KE00100000", row_parcels(row))


if __name__ == "__main__":
    unittest.main()


class Backfill(unittest.TestCase):
    """plan_row settles one address-copied row. Pure, so no client is needed."""

    def leaked(self, row, donor_id="donor"):
        row = copy.deepcopy(row)
        row["phone_metadata"] = {"property_value_xref": {"source": "xref_avm_enricher", "donor_id": donor_id,
                                                         "donor_table": "homeowner_requests_staging"}}
        row["property_value_source"] = "xref_avm_enricher"
        return row

    def test_cross_parcel_value_is_cleared(self):
        from src.bots.value_parcel_backfill import plan_row
        row = self.leaked(dict(KHANLARIAN_OTHER, property_value=295700))
        plan = plan_row(row, KHANLARIAN_DONOR, "now")
        self.assertEqual(plan["action"], "clear_cross_parcel")
        self.assertEqual(plan["columns"], {"property_value": None, "property_value_source": None,
                                           "property_value_as_of": None})

    def test_own_parcel_record_replaces_the_copy(self):
        from src.bots.value_parcel_backfill import plan_row
        row = self.leaked(comptroller_row("gm", 334800))
        plan = plan_row(row, None, "now")
        self.assertEqual(plan["action"], "replace_with_own_parcel_record")
        self.assertEqual(plan["columns"], {"property_value": 334800, "property_value_source": "comptroller",
                                           "property_value_as_of": "2026-11-05"})

    def test_reviewed_county_record_corrects_a_copied_estimate(self):
        from src.bots.value_parcel_backfill import plan_row
        row = self.leaked({"id": "mw", "county": "Shelby", "property_address": "6845 Corsica Drive, Memphis, TN",
                           "property_value": 439000, "property_value_as_of": None,
                           "raw_payload": {"preforeclosure_evidence": {
                               "parcel": "080008 C00094", "value": 446900, "observed_on": "2026-11-22",
                               "value_is_estimate": True, "value_basis": "2026 Shelby County realty bill"}}})
        plan = plan_row(row, None, "now")
        self.assertEqual(plan["columns"], {"property_value": 446900,
                                           "property_value_source": "shelby_trustee_assessment_estimate",
                                           "property_value_as_of": "2026-11-22"})
        self.assertTrue(plan["value_changes"])

    def test_same_parcel_dated_donor_is_confirmed_and_unproven_is_labeled_once(self):
        from src.bots.value_parcel_backfill import plan_row
        donor = comptroller_row("donor", 334800, "comptroller")
        row = self.leaked(dict(comptroller_row("r", 334800, block=False), raw_payload={"parcel": "057 055K E 00100 000"}))
        plan = plan_row(row, donor, "now")
        self.assertEqual((plan["action"], plan["columns"]["property_value_source"]),
                         ("confirm_exact_parcel", "comptroller"))
        row["phone_metadata"]["property_value_xref"] = plan["meta"]
        row.update(plan["columns"])
        self.assertEqual(plan_row(row, donor, "now")["action"], "already_settled")

        bare = self.leaked({"id": "b", "county": "davidson", "property_address": "1 Main St, Nashville, TN",
                            "property_value": 250000, "raw_payload": {}})
        plan = plan_row(bare, None, "now")
        self.assertEqual((plan["action"], plan["reason"]), ("label_unverified", "donor_missing"))
        bare["phone_metadata"]["property_value_xref"] = plan["meta"]
        self.assertEqual(plan_row(bare, None, "later")["action"], "already_labeled")
