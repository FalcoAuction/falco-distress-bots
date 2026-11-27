"""Slim placeholders for rejected staging rows."""

import copy
import gzip
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from src.bots import _run_new
from src.bots import staging_slim_backfill as slim
from src.bots.tn_public_notice_bot import TnPublicNoticeBot

NOW = datetime(2026, 11, 27, 12, 0, tzinfo=timezone.utc)
STANLEY = ("FORECLOSURE SALE NOTICE WHEREAS, Stanley Village 66, LLC, a Tennessee limited liability company, executed "
           "a Deed of Trust, Assignment of Leases and Security Agreement dated February 28, 2025, of record at Volume "
           "1587, Page 11, Register of Deeds Office for Dickson County, Tennessee, (the “De ...")


# ── An in-memory stand-in for the Supabase client ───────────────────────────


def _path(row, path):
    parts = path.replace("->>", "->").split("->")
    value = row.get(parts[0])
    for part in parts[1:]:
        value = value.get(part) if isinstance(value, dict) else None
    if "->>" in path and isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def _same(stored, wanted):
    if isinstance(stored, (dict, list)) and isinstance(wanted, str):
        try:
            return stored == json.loads(wanted)
        except ValueError:
            return False
    return stored == wanted


class Query:
    def __init__(self, db, table):
        self.db, self.table, self.tests = db, table, []
        self.action, self.cols, self.patch, self.rows, self.cap, self.negate = "select", "*", None, None, None, False

    def select(self, cols="*", **_):
        self.action, self.cols = "select", cols
        return self

    def update(self, patch, **_):
        self.action, self.patch = "update", patch
        return self

    def insert(self, rows, **_):
        self.action, self.rows = "insert", rows if isinstance(rows, list) else [rows]
        return self

    def _add(self, test):
        if self.negate:
            self.negate = False
            self.tests.append(lambda row, t=test: not t(row))
        else:
            self.tests.append(test)
        return self

    @property
    def not_(self):
        self.negate = True
        return self

    def eq(self, col, value):
        return self._add(lambda row: _same(_path(row, col), value))

    def filter(self, col, op, value):
        assert op == "eq"
        return self.eq(col, value)

    def in_(self, col, values):
        values = list(values)
        return self._add(lambda row: _path(row, col) in values)

    def is_(self, col, value):
        assert value == "null"
        return self._add(lambda row: _path(row, col) is None)

    def gt(self, col, value):
        return self._add(lambda row: str(_path(row, col)) > str(value))

    def lt(self, col, value):
        return self._add(lambda row: str(_path(row, col)) < str(value))

    def gte(self, col, value):
        return self._add(lambda row: str(_path(row, col)) >= str(value))

    def neq(self, col, value):
        return self._add(lambda row: _path(row, col) != value)

    def order(self, *_a, **_k):
        return self

    def limit(self, n):
        self.cap = n
        self.db.limits.append(n)
        return self

    def execute(self):
        rows = self.db.tables.setdefault(self.table, [])
        if self.action == "insert":
            for row in self.rows:
                rows.append({"id": f"new-{len(rows)}", **copy.deepcopy(row)})
            self.db.inserts += len(self.rows)
            return mock.Mock(data=copy.deepcopy(self.rows))
        if self.action == "update" and self.table == slim.TABLE and self.db.before_update:
            hook, self.db.before_update = self.db.before_update, None
            hook(self.db)
        if self.table in self.db.broken:
            raise RuntimeError("read failed")
        hits = sorted((r for r in rows if all(t(r) for t in self.tests)), key=lambda r: str(r.get("id")))
        if self.action == "update":
            for row in hits:
                row.update(copy.deepcopy(self.patch))
            if self.table == slim.TABLE:
                self.db.updates += len(hits)
            return mock.Mock(data=copy.deepcopy(hits))
        if self.cap is not None:
            hits = hits[:self.cap]
        if self.cols.strip() == "*":
            return mock.Mock(data=copy.deepcopy(hits))
        out = []
        for row in hits:
            item = {}
            for col in self.cols.split(","):
                name, _, path = col.strip().partition(":")
                item[name] = _path(row, path or name)
            out.append(copy.deepcopy(item))
        return mock.Mock(data=out)


class Db:
    def __init__(self, *rows):
        self.tables = {slim.TABLE: [copy.deepcopy(r) for r in rows]}
        self.updates = self.inserts = 0
        self.limits, self.broken, self.before_update = [], set(), None

    def table(self, name):
        return Query(self, name)

    def staging(self, rid):
        return next(r for r in self.tables[slim.TABLE] if r["id"] == rid)


# ── Rows shaped like the production sample ──────────────────────────────────

FILING = {"key": "f1-d610775a4b3545b020da7d72", "version": "filing-1", "type": "PRE_FORECLOSURE", "county": "shelby",
          "duplicate_of": "a7a201a9-3b51-4910-a8a9-014253ed61c9",
          "keys": [{"of": "tn|shelby|g:DWAYNE VISOR|s:2026-10-01", "key": "f1-d610775a4b3545b020da7d72",
                    "kind": "grantor_and_date"}],
          "parcels": [], "parties": ["Dwayne Visor"], "signals": ["trustee sale wording", "deed of trust named"],
          "type_basis": "trustee sale wording; deed of trust named as the instrument enforced",
          "filed_under": "Foreclosures", "identifiers": {"sale_date": "2026-10-01"},
          "county_basis": "county of the newspaper", "county_from_text": False, "publication_county": "shelby",
          "publications": [{"date": "Friday, September 04, 2026", "newspaper": "The Daily News", "notice_id": "562300"}]}
NOTICE_RAW = {"city": "Memphis", "county": "shelby", "parcel": None, "subject": "Dwayne Visor", "category": "Foreclosures",
              "category_id": "4", "publication": "tn_public_notice", "newspaper": "The Daily News",
              "publication_date": "Friday, September 04, 2026", "excerpt": "SUBSTITUTE TRUSTEE'S NOTICE OF SALE ...",
              "notice_reparse": {"v": 1, "at": "2026-11-20T02:23:06+00:00", "kind": "foreclosure"},
              "borrowers": ["Dwayne Visor"], "filed_as": "TAX_LIEN", "deed_of_trust": {"instrument": "24012345"},
              "lien_check": {"status": "queued"}, "comptroller_parcel": {"method": "owner"},
              "shelby_chain": {"status": "none"}, "padctn": {"status": "not_found"},
              "property_lane": {"lane": "residential"}, "probate_match_attempt": {"at": "x"},
              "court_sale_review": {"status": "x"}, "hamilton_notice_review": {"status": "x"}}
NOTICE_PM = {"decision_engine": {"action": "HOLD_FOR_DATA", "decided_at": "2026-10-09T19:13:05+00:00"},
             "notice_tracking": {"last_seen_at": "2026-10-18T23:39:21+00:00", "last_seen_by": "tn_public_notice"},
             "mortgage_estimate": {"estimated_current_balance": 1}, "distress_stack": {"is_stacked": False},
             "owner_class": "homeowner", "owner_class_evidence": None, "mortgage_signal": {"x": 1},
             "skip_trace": {"is_absentee_owner": False}}


def notice(rid="n1", **over):
    row = {"id": rid, "bot_source": "tn_public_notice", "staging_status": "rejected",
           "rejection_reason": "republication_of:a7a201a9-3b51-4910-a8a9-014253ed61c9", "reviewed_at": None,
           "reviewed_by": None, "staged_at": "2026-09-09T16:23:56.539743+00:00", "pipeline_lead_key": f"key-{rid}",
           "county": "shelby", "distress_type": "PRE_FORECLOSURE", "trustee_sale_date": "2026-10-01",
           "admin_notes": "TN Public Notice 562300", "source_url": "https://www.tnpublicnotice.com/Details.aspx?ID=562300",
           "phone": None, "email": None, "alternate_phones": None, "priority_score": 0,
           "raw_payload": {**copy.deepcopy(NOTICE_RAW), "filing": copy.deepcopy(FILING)},
           "phone_metadata": copy.deepcopy(NOTICE_PM)}
    row.update(over)
    return row


OTHER_SOURCES = {
    "tn_lis_pendens": ({"excerpt": "TRUSTEE'S NOTICE OF SALE ...", "same_notice_staging_id": "1cfc27cf",
                        "publication_stage": "foreclosure_sale_publication", "category_id": "__lp_supplement",
                        "notice_reparse": {"v": 1}, "lien_check": {"status": "queued"}, "borrowers": ["A"]},
                       {"decision_engine": {"action": "HOLD_FOR_DATA"}, "notice_tracking": {"last_seen_at": "x"}}),
    "nashville_codes": ({"nashville_codes": {"Request_Nbr": "CAAC-1", "Status": "CLOSED", "Property_Address": "1 A ST"},
                         "nashville_codes_review": {"checked_on": "2026-11-20", "status": "inactive"},
                         "nashville_codes_changes": [{"checked_on": "2026-11-01"}], "metro_parcel": {"apn": "1"}},
                        {"batchdata_skip_trace": {"phones": []}, "twilio_lookup": {"x": 1}, "decision_engine": {}}),
    "hamilton_tax_delinquent": ({"parcel": "PER--10052585", "cumulative_owed": 1502.62, "owner_name_1": "JMF CONCRETE",
                                 "mailing_address": "3819 OWEDA TER", "hamilton_roll_valuation": {"status": "x"},
                                 "property_lane": {"lane": "commercial"}},
                                {"skip_trace": {"owner_mailing": "x"}, "mortgage_estimate": {"x": 1}}),
    "chattanooga_codes": ({"chattanooga_codes": {"case_number": "C-1", "rows": [{"case_number": "C-1"}]},
                           "hamilton_assessor": {"x": 1}, "chattanooga_parcel_review": {"x": 1}},
                          {"chattanooga_case_review": {"checked_on": "2026-11-20"}}),
}


def other(source, rid="o1"):
    raw, pm = OTHER_SOURCES[source]
    return notice(rid, bot_source=source, rejection_reason="manual", raw_payload=copy.deepcopy(raw),
                  phone_metadata=copy.deepcopy(pm))


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.lines = []

    def go(self, db, sources=("tn_public_notice",), **kw):
        kw.setdefault("now", NOW)
        return slim.execute(db, list(sources), archive_root=self.tmp.name, sleep=lambda _s: None,
                            out=self.lines.append, **kw)

    def archives(self):
        return sorted(Path(self.tmp.name).rglob("*.jsonl.gz"))


class WhatSurvives(Case):
    def test_public_notice_keeps_every_key_a_reader_needs_and_drops_the_filing_detail(self):
        row = notice()
        raw, pm, dropped = slim.slim_columns("tn_public_notice", row["raw_payload"], row["phone_metadata"])
        self.assertEqual(set(raw), set(row["raw_payload"]))                 # no top-level key leaves
        for key in set(NOTICE_RAW):
            self.assertEqual(raw[key], row["raw_payload"][key], key)
        self.assertEqual(pm, row["phone_metadata"])                         # phone_metadata is never touched
        self.assertEqual(raw["filing"], {"key": FILING["key"], "version": "filing-1", "type": "PRE_FORECLOSURE",
                                         "county": "shelby", "duplicate_of": FILING["duplicate_of"]})
        self.assertEqual(sorted(dropped), sorted(f"raw_payload.filing.{k}" for k in slim.FILING_DETAIL if k in FILING))
        self.assertIn("filing", row["raw_payload"]["filing"]["version"])    # the input row is not modified
        self.assertIn("signals", row["raw_payload"]["filing"])

    def test_unknown_filing_keys_survive(self):
        row = notice()
        row["raw_payload"]["filing"].update(republication_of_key="k", hold={"route": "research"}, later_field=1)
        raw, _, _ = slim.slim_columns("tn_public_notice", row["raw_payload"], row["phone_metadata"])
        for key in ("republication_of_key", "hold", "later_field"):
            self.assertIn(key, raw["filing"])

    def test_every_key_survives_on_the_excluded_sources(self):
        for source in OTHER_SOURCES:
            with self.subTest(source=source):
                self.assertIn(source, slim.EXCLUDED)
                row = other(source)
                raw, pm, dropped = slim.slim_columns(source, row["raw_payload"], row["phone_metadata"])
                self.assertEqual((raw, pm, dropped), (row["raw_payload"], row["phone_metadata"], []))
                db = Db(row)
                summary = self.go(db, sources=[source], write=True, expect=0)
                self.assertEqual((summary["planned"], db.updates, self.archives()), (0, 0, []))
                self.assertIn(source, summary["excluded"])
                self.assertEqual(db.staging("o1"), row)


class Eligibility(Case):
    def plan(self, *rows, tables=None, **kw):
        db = Db(*rows)
        db.tables.update(tables or {})
        return self.go(db, **kw), db

    def test_eligible_row_is_planned_and_a_dry_run_writes_nothing(self):
        summary, db = self.plan(notice())
        source = summary["sources"]["tn_public_notice"]
        self.assertEqual((summary["planned"], summary["mode"], db.updates), (1, "dry_run", 0))
        self.assertGreater(source["bytes_before"], source["bytes_after"])
        self.assertEqual(source["keys_dropped"]["raw_payload.filing.signals"], 1)
        self.assertEqual(self.archives(), [])
        report = json.loads(Path(summary["report"]).read_text(encoding="utf-8"))
        self.assertEqual([p["id"] for p in report["plan"]], ["n1"])
        self.assertEqual(db.staging("n1"), notice())

    def test_carded_rows_are_skipped(self):
        for key in ("pilot_review", "pretrace_check"):
            row = notice()
            row["raw_payload"][key] = {"route": "research_hold"}
            summary, _ = self.plan(row)
            self.assertEqual((summary["planned"], summary["sources"]["tn_public_notice"]["skipped"]), (0, {"carded": 1}))

    def test_row_on_the_live_table_is_skipped(self):
        summary, _ = self.plan(notice(), tables={"homeowner_requests": [{"id": "L", "pipeline_lead_key": "key-n1"}]})
        self.assertEqual((summary["planned"], summary["sources"]["tn_public_notice"]["skipped"]), (0, {"on_live_table": 1}))

    def test_ranked_or_contacted_rows_are_skipped(self):
        for table, column, value, reason in (("lead_opportunities", "staging_id", "n1", "ranked_before"),
                                             ("dialer_lead_workflow", "listing_slug", "key-n1", "crm_activity"),
                                             ("sms_outreach_state", "listing_slug", "key-n1", "crm_activity"),
                                             ("dialer_activities", "listing_slug", "key-n1", "crm_activity")):
            summary, _ = self.plan(notice(), tables={table: [{column: value}]})
            self.assertEqual((summary["planned"], summary["sources"]["tn_public_notice"]["skipped"]), (0, {reason: 1}), table)

    def test_recently_rejected_row_is_skipped(self):
        summary, _ = self.plan(notice(reviewed_at="2026-11-10T00:00:00+00:00"))        # staged long ago, rejected 17 days ago
        self.assertEqual((summary["planned"], summary["sources"]["tn_public_notice"]["skipped"]), (0, {"rejected_recently": 1}))
        summary, _ = self.plan(notice(staged_at="2026-11-10T00:00:00+00:00"))          # no review date: staged date counts
        self.assertEqual(summary["planned"], 0)
        summary, _ = self.plan(notice(reviewed_at="2026-11-10T00:00:00+00:00"), min_age_days=7)
        self.assertEqual(summary["planned"], 1)

    def test_row_with_a_phone_or_email_is_skipped(self):
        for column, value in (("phone", "6155550100"), ("email", "a@example.com"), ("alternate_phones", ["6155550101"])):
            summary, _ = self.plan(notice(**{column: value}))
            self.assertEqual((summary["planned"], summary["sources"]["tn_public_notice"]["skipped"]),
                             (0, {"has_phone_or_email": 1}), column)

    def test_only_rejected_rows_are_read(self):
        for status in ("pending", "verified", "promoted"):
            summary, _ = self.plan(notice(staging_status=status))
            self.assertEqual((summary["planned"], summary["sources"]["tn_public_notice"]["scanned"]), (0, 0))

    def test_failed_lookup_stops_the_run(self):
        db = Db(notice())
        db.broken.add("dialer_activities")
        with self.assertRaises(RuntimeError):
            self.go(db, write=True, expect=1)
        self.assertEqual((db.updates, self.archives()), (0, []))

    def test_reads_are_paged_by_id_and_never_above_400(self):
        rows = [notice(f"n{i:03d}") for i in range(7)]
        summary, db = self.plan(*rows, page=3)
        self.assertEqual(summary["planned"], 7)
        self.assertEqual(db.limits, [3, 3, 3])
        summary, db = self.plan(*rows, page=5000)
        self.assertEqual(db.limits, [slim.MAX_PAGE])

    def test_limit_caps_the_plan(self):
        summary, _ = self.plan(*[notice(f"n{i:03d}") for i in range(7)], limit=4)
        self.assertEqual(summary["planned"], 4)


class Writes(Case):
    def test_write_archives_first_then_slims_and_marks_the_row(self):
        db, seen = Db(notice(), notice("n2", reviewed_at="2026-10-01T00:00:00+00:00", reviewed_by="x")), []
        real = slim.verify_archive

        def checked(path, rows):
            seen.append((db.updates, real(path, rows)))
            return seen[-1][1]

        with mock.patch.object(slim, "verify_archive", checked):
            summary = self.go(db, write=True, expect=2)
        self.assertEqual(seen, [(0, True)])                                  # verified before the first update
        self.assertEqual((summary["written"], summary["changed_since_read"], db.updates), (2, 0, 2))
        (archive,) = self.archives()
        self.assertEqual(archive.parent.name, "2026-11-27-staging-slim")
        with gzip.open(archive, "rt", encoding="utf-8") as handle:
            stored = [json.loads(line) for line in handle]
        self.assertEqual(stored, [notice(), notice("n2", reviewed_at="2026-10-01T00:00:00+00:00", reviewed_by="x")])
        row = db.staging("n1")
        marker = dict(row["raw_payload"]["slim"])
        dropped = marker.pop("dropped_keys")
        self.assertEqual(marker, {"version": "slim-1", "on": "2026-11-27", "archive": archive.name})
        self.assertEqual(sorted(dropped), sorted(f"raw_payload.filing.{k}" for k in slim.FILING_DETAIL if k in FILING))
        self.assertIn("raw_payload.filing.signals", row["raw_payload"]["slim"]["dropped_keys"])
        self.assertEqual(row["raw_payload"]["filing"]["duplicate_of"], FILING["duplicate_of"])
        self.assertNotIn("signals", row["raw_payload"]["filing"])
        plain = {k: v for k, v in row.items() if k != "raw_payload"}
        self.assertEqual(plain, {k: v for k, v in notice().items() if k != "raw_payload"})   # every other column as it was

    def test_second_run_plans_zero(self):
        db = Db(notice(), notice("n2"))
        self.assertEqual(self.go(db, write=True, expect=2)["written"], 2)
        again = self.go(db, write=True, expect=0)
        self.assertEqual((again["planned"], again["written"], db.updates), (0, 0, 2))
        self.assertEqual(len(self.archives()), 1)

    def test_expect_mismatch_blocks_the_write(self):
        db = Db(notice(), notice("n2"))
        for expect in (1, 3, None):
            summary = self.go(db, write=True, expect=expect)
            self.assertIn("does not match", summary["write_blocked"])
            self.assertEqual((summary["written"], db.updates, self.archives()), (0, 0, []))
        self.assertEqual(db.staging("n1"), notice())

    def test_failed_archive_check_blocks_the_write(self):
        db = Db(notice())
        with mock.patch.object(slim, "verify_archive", return_value=False):
            summary = self.go(db, write=True, expect=1)
        self.assertIn("archive check failed", summary["write_blocked"])
        self.assertEqual((summary["written"], db.updates), (0, 0))
        self.assertEqual(db.staging("n1"), notice())

    def test_archive_check_reads_the_file_back(self):
        path = Path(self.tmp.name) / "a.jsonl.gz"
        rows = [notice(), notice("n2")]
        slim.write_archive(path, rows)
        self.assertTrue(slim.verify_archive(path, rows))
        self.assertFalse(slim.verify_archive(path, rows[:1]))                 # count
        self.assertFalse(slim.verify_archive(path, [notice(), notice("n3")]))  # ids
        path.write_bytes(path.read_bytes()[:40])
        self.assertFalse(slim.verify_archive(path, rows))                     # unreadable file
        self.assertFalse(slim.verify_archive(Path(self.tmp.name) / "missing.gz", rows))

    def test_unwritable_archive_blocks_the_write(self):
        db = Db(notice())
        with mock.patch.object(slim, "write_archive", side_effect=OSError("disk")):
            summary = self.go(db, write=True, expect=1)
        self.assertIn("archive check failed", summary["write_blocked"])
        self.assertEqual(db.updates, 0)

    def test_row_changed_after_the_read_is_skipped_and_counted(self):
        changes = {
            "status": lambda row: row.update(staging_status="pending", rejection_reason=None),
            "reviewed_at": lambda row: row.update(reviewed_at="2026-11-27T11:59:00+00:00"),
            "raw_payload": lambda row: row["raw_payload"].update(notice_reparse={"v": 2}),
            "phone_metadata": lambda row: row["phone_metadata"]["notice_tracking"].update(last_seen_at="2026-11-27"),
            "phone": lambda row: row.update(phone="6155550100"),
        }
        for name, change in changes.items():
            with self.subTest(changed=name):
                db = Db(notice(), notice("n2"))
                db.before_update = lambda d, change=change: change(d.staging("n1"))
                summary = self.go(db, write=True, expect=2)
                self.assertEqual((summary["written"], summary["changed_since_read"]), (1, 1))
                self.assertNotIn("slim", db.staging("n1")["raw_payload"])
                self.assertIn("signals", db.staging("n1")["raw_payload"]["filing"])
                self.assertIn("slim", db.staging("n2")["raw_payload"])

    def test_row_too_large_to_guard_is_skipped(self):
        row = notice()
        row["raw_payload"]["excerpt"] = "x" * (slim.MAX_GUARD_CHARS + 1)
        summary = self.go(Db(row))
        self.assertEqual((summary["planned"], summary["sources"]["tn_public_notice"]["skipped"]), (0, {"too_large_to_guard": 1}))

    def test_writes_pause_at_least_every_100_rows(self):
        pauses = []
        db = Db(*[notice(f"n{i:03d}") for i in range(5)])
        slim.execute(db, ["tn_public_notice"], write=True, expect=5, batch=2, now=NOW, archive_root=self.tmp.name,
                     sleep=pauses.append, out=self.lines.append)
        self.assertEqual(pauses.count(slim.BATCH_PAUSE), 3)
        self.assertEqual(slim.MAX_BATCH, 100)


class CommandLine(Case):
    def test_write_without_expect_is_refused_before_any_read(self):
        with mock.patch.object(slim, "_supabase") as client, self.assertRaises(SystemExit):
            with mock.patch("sys.stderr"):
                slim.main(["--write"])
        client.assert_not_called()

    def test_dry_run_is_the_default_and_sources_can_be_named(self):
        db = Db(notice(), other("tn_lis_pendens"))
        with mock.patch.object(slim, "_supabase", return_value=db), mock.patch("builtins.print"), \
                mock.patch.object(slim.time, "sleep"):
            code = slim.main(["--sources", "tn_public_notice", "--archive-root", self.tmp.name])
        self.assertEqual((code, db.updates), (0, 0))
        with mock.patch.object(slim, "_supabase", return_value=db), mock.patch("builtins.print"), \
                mock.patch.object(slim.time, "sleep"):
            code = slim.main(["--sources", "tn_public_notice", "--archive-root", self.tmp.name, "--write", "--expect", "9"])
        self.assertEqual((code, db.updates), (1, 0))


# ── The bot's own dedupe path against a slimmed row ─────────────────────────


def built(notice_id):
    return TnPublicNoticeBot()._build_lead({"notice_id": notice_id, "county": "davidson", "excerpt": STANLEY,
                                            "paper": "The Nashville Ledger", "pub_date": "Friday, November 13, 2026",
                                            "city": "Nashville"}, "TAX_LIEN", "Delinquent Taxes")


def stored(lead, rid, **over):
    row = {**notice(rid), **lead.as_db_row("run-1"), "id": rid, "staging_status": "rejected",
           "rejection_reason": "manual", "staged_at": "2026-09-01T00:00:00+00:00"}
    row.update(over)
    return row


class PublicNoticeIsNotStagedAgain(Case):
    def restage(self, db, lead):
        bot = TnPublicNoticeBot()
        bot.logger.disabled = True
        with mock.patch("src.bots._base._supabase", return_value=db):
            kept = bot._one_per_filing([lead])
            staged, dupes = bot._write_staging(kept)
        return [x.pipeline_lead_key for x in kept], staged, dupes, len(db.tables[slim.TABLE])

    def slimmed(self, *rows):
        db = Db(*rows)
        expect = sum(1 for r in rows if r["staging_status"] == "rejected")
        self.assertEqual(self.go(db, write=True, expect=expect)["written"], expect)
        return db

    def test_republication_merged_into_another_record_stays_out(self):
        lead = built("580544")
        self.assertTrue(lead.raw_payload["filing"]["key"])
        row = stored(lead, "dup", rejection_reason="republication_of:kept")
        row["raw_payload"]["filing"]["duplicate_of"] = "kept"
        db = self.slimmed(row)
        self.assertIn("slim", db.staging("dup")["raw_payload"])
        self.assertNotIn("keys", db.staging("dup")["raw_payload"]["filing"])
        # The same notice read again on the next run: dropped before the writer, nothing inserted.
        self.assertEqual(self.restage(db, built("580544")), ([], 0, 0, 1))
        self.assertEqual(self.restage(Db(row), built("580544")), ([], 0, 0, 1))      # same as the full row
        self.assertEqual(db.inserts, 0)

    def test_new_printing_of_a_merged_filing_goes_to_the_kept_record(self):
        first, again = built("578632"), built("580544")
        kept = stored(first, "kept", staging_status="pending", rejection_reason=None)
        dup = stored(again, "dup", rejection_reason="republication_of:kept")
        dup["raw_payload"]["filing"]["duplicate_of"] = "kept"
        db = self.slimmed(kept, dup)
        # A third printing under a new notice number takes the kept record's key and is counted as seen.
        keys, staged, dupes, rows = self.restage(db, built("590001"))
        self.assertEqual((keys, staged, dupes, rows), ([first.pipeline_lead_key], 0, 1, 2))
        self.assertEqual(self.restage(db, built("580544")), ([], 0, 0, 2))             # the slimmed one itself

    def test_slim_row_and_full_row_give_the_writer_the_same_answer(self):
        # A rejected record that is not a republication never blocked the base
        # writer (it looks at pending rows only). Slimming must not change that
        # either way: same lead key chosen, same staged and duplicate counts.
        lead = built("580544")
        row = stored(lead, "rej")
        self.assertIsNone(row["raw_payload"]["filing"].get("duplicate_of"))
        full, thin = Db(row), self.slimmed(row)
        self.assertEqual(self.restage(thin, built("590001")), self.restage(full, built("590001")))
        self.assertEqual(self.restage(thin, built("580544")), self.restage(full, built("580544")))


# ── Scheduled step ──────────────────────────────────────────────────────────


class ScheduledStep(Case):
    def run_step(self, db, env):
        env = {"STAGING_SLIM_ARCHIVE_DIR": self.tmp.name, **env}
        with mock.patch.object(slim, "_supabase", return_value=db), mock.patch("src.bots._base._supabase", return_value=db), \
                mock.patch.dict(os.environ, env, clear=False), mock.patch.object(slim.time, "sleep"):
            for key in ("STAGING_SLIM_WRITE", "STAGING_SLIM_MAX_ROWS", "GITHUB_ACTIONS"):
                if key not in env:
                    os.environ.pop(key, None)
            bot = slim.StagingSlimBot()
            bot.logger.disabled = True
            return bot.run(), db.tables["bot_run_health"][-1]

    def test_registered_in_the_runner_and_not_switched_on_in_the_workflows(self):
        self.assertIn(("staging_slim", slim.run), _run_new.NEW_BOTS)
        self.assertNotIn("staging_slim", _run_new.CRITICAL_BOTS)
        for workflow in (Path(__file__).resolve().parents[1] / ".github" / "workflows").glob("*.yml"):
            self.assertNotIn("STAGING_SLIM_WRITE", workflow.read_text(encoding="utf-8"), workflow.name)

    def test_dry_run_unless_the_switch_is_set(self):
        db = Db(notice(), notice("n2"))
        result, health = self.run_step(db, {})
        self.assertEqual((result["status"], result["planned"], result["written"], db.updates), ("ok", 2, 0, 0))
        self.assertEqual((health["status"], health["notes"]["mode"], health["notes"]["planned"]), ("ok", "dry_run", 2))
        self.assertGreater(health["notes"]["bytes_saved"], 0)
        self.assertEqual(self.archives(), [])

    def test_switch_on_writes_up_to_the_cap_and_reports_counts(self):
        db = Db(*[notice(f"n{i:03d}") for i in range(5)])
        result, health = self.run_step(db, {"STAGING_SLIM_WRITE": "1", "STAGING_SLIM_MAX_ROWS": "3"})
        self.assertEqual((result["planned"], result["written"], db.updates), (3, 3, 3))
        self.assertEqual((health["staged_count"], health["notes"]["written"], health["notes"]["cap"]), (3, 3, 3))
        self.assertEqual(len(self.archives()), 1)
        result, _ = self.run_step(db, {"STAGING_SLIM_WRITE": "1"})
        self.assertEqual((result["written"], db.updates), (2, 5))
        result, _ = self.run_step(db, {"STAGING_SLIM_WRITE": "1"})
        self.assertEqual((result["planned"], result["written"]), (0, 0))

    def test_default_cap_is_200(self):
        db = Db(*[notice(f"n{i:03d}") for i in range(205)])
        result, health = self.run_step(db, {})
        self.assertEqual((result["planned"], health["notes"]["cap"]), (200, 200))

    def test_runner_disk_is_not_an_archive(self):
        db = Db(notice())
        result, health = self.run_step(db, {"STAGING_SLIM_WRITE": "1", "GITHUB_ACTIONS": "true",
                                            "STAGING_SLIM_ARCHIVE_DIR": ""})
        self.assertEqual((result["written"], db.updates), (0, 0))
        self.assertIn("no durable archive directory", health["notes"]["write_refused"])


if __name__ == "__main__":
    unittest.main()
