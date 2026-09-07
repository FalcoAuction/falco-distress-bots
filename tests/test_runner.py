"""Tests for the pipeline runner, the batched staging writer, frozen-source
detection, and the alert rules. No network: the Supabase client is faked.

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import signal
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make sure the tests never touch a real project even if .env is present.
os.environ["NEXT_PUBLIC_SUPABASE_URL"] = ""
os.environ["SUPABASE_SERVICE_ROLE_KEY"] = ""

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bots import _base  # noqa: E402
from src.bots._base import BotBase, BotTimeout, LeadPayload  # noqa: E402
from src.bots import _run_new  # noqa: E402
from src.bots._run_new import run_pipeline, select_bots  # noqa: E402
from src.bots._alert import build_alerts, alert_count  # noqa: E402


# ── Fake Supabase client ─────────────────────────────────────────────────


class _Result:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    def __init__(self, client, table):
        self.client = client
        self.table = table
        self.ops = []
        self.payload = None

    def __getattr__(self, name):
        if name == "not_":
            return self

        def m(*a, **k):
            self.ops.append((name, a))
            return self
        return m

    def insert(self, payload):
        self.payload = payload
        self.ops.append(("insert", ()))
        return self

    def update(self, payload):
        self.payload = payload
        self.ops.append(("update", ()))
        return self

    def execute(self):
        self.client.calls.append((self.table, list(self.ops), self.payload))
        return _Result(self.client.respond(self.table, self.ops, self.payload))


class FakeClient:
    """Answers staging dedupe lookups from `pending_keys`, health lookups
    from `health_notes`, everything else with []."""

    def __init__(self, pending_keys=(), health_notes=None):
        self.pending_keys = set(pending_keys)
        self.health_notes = health_notes or []
        self.calls = []

    def table(self, name):
        return FakeQuery(self, name)

    def respond(self, table, ops, payload):
        names = [o[0] for o in ops]
        if table == "homeowner_requests_staging" and "select" in names:
            keys = next((a[1] for n, a in ops if n == "in_"), [])
            return [{"id": f"id-{k}", "pipeline_lead_key": k, "phone_metadata": None}
                    for k in keys if k in self.pending_keys]
        if table == "bot_run_health" and "select" in names:
            return [{"notes": n} for n in self.health_notes]
        return []


class _FakeSupabase(unittest.TestCase):
    def setUp(self):
        self._orig = _base._SUPABASE_CLIENT

    def tearDown(self):
        _base._SUPABASE_CLIENT = self._orig

    def use(self, client):
        _base._SUPABASE_CLIENT = client
        return client


# ── select_bots ──────────────────────────────────────────────────────────


class SelectBotsTests(unittest.TestCase):
    POOL = [("a", None), ("hamilton_tax_delinquent", None), ("b", None)]

    def test_daily_excludes_heavy(self):
        self.assertEqual([n for n, _ in select_bots(bots=self.POOL)], ["a", "b"])

    def test_include_heavy(self):
        self.assertEqual(
            [n for n, _ in select_bots(bots=self.POOL, include_heavy=True)],
            ["a", "hamilton_tax_delinquent", "b"],
        )

    def test_only_bypasses_heavy_exclusion(self):
        got = select_bots(only=["hamilton_tax_delinquent"], bots=self.POOL)
        self.assertEqual([n for n, _ in got], ["hamilton_tax_delinquent"])

    def test_skip(self):
        self.assertEqual([n for n, _ in select_bots(skip=["a"], bots=self.POOL)], ["b"])

    def test_only_unknown_is_an_error(self):
        with self.assertRaises(SystemExit):
            select_bots(only=["nope"], bots=self.POOL)

    def test_real_list_has_health_monitor_first_and_promoter_after_skiptrace(self):
        names = [n for n, _ in select_bots()]
        self.assertEqual(names[0], "bot_health_monitor")
        self.assertNotIn("hamilton_tax_delinquent", names)
        self.assertLess(names.index("enformion_skip_trace"), names.index("auto_promoter"))
        self.assertLess(names.index("middle_tn_twilio_lookup"), names.index("auto_promoter"))
        for c in _run_new.CRITICAL_BOTS:
            self.assertIn(c, names)


# ── run_pipeline ─────────────────────────────────────────────────────────


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class RunPipelineTests(unittest.TestCase):
    def setUp(self):
        self._orig_close = _run_new.close_open_runs
        self.closed = []
        _run_new.close_open_runs = lambda name, status, msg, since: self.closed.append((name, status)) or 0

    def tearDown(self):
        _run_new.close_open_runs = self._orig_close

    def _bot(self, clock, cost, status="ok"):
        def run():
            clock.t += cost
            return {"status": status, "staged": 1}
        return run

    def test_budget_gate_skips_noncritical_and_still_runs_critical(self):
        clock = FakeClock()
        bots = [
            ("a", self._bot(clock, 100)),
            ("b", self._bot(clock, 100)),
            ("x", self._bot(clock, 100)),
            ("crit", self._bot(clock, 10)),
        ]
        s = run_pipeline(bots, budget_seconds=250, reserve_seconds=100,
                         critical={"crit"}, clock=clock, summary_path=None)
        got = {b["bot"]: b["status"] for b in s["bots"]}
        self.assertEqual(got, {"a": "ok", "b": "ok", "x": "skipped_budget", "crit": "ok"})
        self.assertEqual(s["critical_incomplete"], [])

    def test_reserve_only_protects_critical_bots_still_ahead(self):
        clock = FakeClock()
        # No critical bot selected (the weekly --only case): nothing to
        # reserve for, so a 5 min budget must not skip the one bot.
        s = run_pipeline([("heavy", self._bot(clock, 10))], budget_seconds=300, reserve_seconds=600,
                         critical={"crit"}, clock=clock, summary_path=None)
        self.assertEqual(s["bots"][0]["status"], "ok")
        # Critical bot already ran: the trailing non-critical bot gets the remainder.
        s = run_pipeline([("crit", self._bot(clock, 10)), ("tail", self._bot(clock, 10))],
                         budget_seconds=300, reserve_seconds=600, critical={"crit"}, clock=clock, summary_path=None)
        self.assertEqual([b["status"] for b in s["bots"]], ["ok", "ok"])

    def test_crash_marks_critical_incomplete_and_closes_ghost_row(self):
        clock = FakeClock()

        def boom():
            raise RuntimeError("kaboom")
        s = run_pipeline([("crit", boom), ("after", self._bot(clock, 1))],
                         budget_seconds=1000, critical={"crit"}, clock=clock, summary_path=None)
        got = {b["bot"]: b["status"] for b in s["bots"]}
        self.assertEqual(got["crit"], "crashed")
        self.assertEqual(got["after"], "ok")           # the crash did not stop the run
        self.assertEqual(s["critical_incomplete"], ["crit"])
        self.assertEqual(self.closed, [("crit", "failed")])

    def test_failed_status_from_bot_counts_as_incomplete_for_critical(self):
        clock = FakeClock()
        s = run_pipeline([("crit", self._bot(clock, 1, status="failed"))],
                         budget_seconds=1000, critical={"crit"}, clock=clock, summary_path=None)
        self.assertEqual(s["critical_incomplete"], ["crit"])

    def test_skipped_no_creds_is_complete(self):
        clock = FakeClock()
        s = run_pipeline([("crit", self._bot(clock, 1, status="skipped_no_creds"))],
                         budget_seconds=1000, critical={"crit"}, clock=clock, summary_path=None)
        self.assertEqual(s["critical_incomplete"], [])

    def test_summary_is_flushed_after_every_bot(self):
        import tempfile
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "s.json"
            seen = []

            def peek():
                seen.append(path.exists())
                clock.t += 1
                return {"status": "ok"}
            run_pipeline([("first", peek), ("second", peek)], budget_seconds=100,
                         reserve_seconds=0, clock=clock, summary_path=path)
            # First bot: file not yet written. Second bot: file from first flush exists.
            self.assertEqual(seen, [False, True])
            self.assertTrue(path.exists())

    @unittest.skipUnless(hasattr(signal, "SIGALRM"), "per-bot timeout needs SIGALRM (POSIX)")
    def test_per_bot_timeout_interrupts_and_moves_on(self):
        def slow():
            time.sleep(5)
            return {"status": "ok"}

        def quick():
            return {"status": "ok"}
        s = run_pipeline([("slow", slow), ("quick", quick)], budget_seconds=1000,
                         timeouts={"slow": 1}, critical=set(), summary_path=None)
        got = {b["bot"]: b["status"] for b in s["bots"]}
        self.assertEqual(got, {"slow": "timed_out", "quick": "ok"})
        self.assertEqual(self.closed, [("slow", "timed_out")])
        self.assertLess(s["bots"][0]["elapsed"], 3)

    @unittest.skipUnless(hasattr(signal, "SIGALRM"), "per-bot timeout needs SIGALRM (POSIX)")
    def test_bot_base_run_records_timed_out_when_scrape_is_interrupted(self):
        class Slow(BotBase):
            name = "slow_test"

            def scrape(self):
                time.sleep(5)
                return []
        # No supabase client: run() still returns the status.
        _base._SUPABASE_CLIENT = None
        s = run_pipeline([("slow_test", lambda: Slow().run())], budget_seconds=1000,
                         timeouts={"slow_test": 1}, critical=set(), summary_path=None)
        self.assertEqual(s["bots"][0]["status"], "timed_out")


# ── _write_staging batching ──────────────────────────────────────────────


class WriteStagingTests(_FakeSupabase):
    def _leads(self, n, source="src"):
        return [LeadPayload(bot_source=source, pipeline_lead_key=f"k{i:04d}",
                            property_address=f"{i} Main St, Nashville, TN") for i in range(n)]

    def test_chunks_and_counts(self):
        leads = self._leads(450)
        client = self.use(FakeClient(pending_keys={"k0001", "k0002", "k0300"}))
        staged, dupes = BotBase()._write_staging(leads)
        self.assertEqual((staged, dupes), (447, 3))
        staging_selects = [c for c in client.calls if c[0] == "homeowner_requests_staging" and c[1][0][0] == "select"]
        inserts = [c for c in client.calls if c[0] == "homeowner_requests_staging" and c[1][0][0] == "insert"]
        live_selects = [c for c in client.calls if c[0] == "homeowner_requests"]
        touches = [c for c in client.calls if c[1][0][0] == "update"]
        self.assertEqual(len(staging_selects), 3)      # 450 keys / 200 per chunk
        self.assertEqual(len(inserts), 3)
        self.assertEqual(sum(len(c[2]) for c in inserts), 447)
        self.assertEqual(len(live_selects), 3)
        self.assertEqual(len(touches), 3)              # one per dupe, staging side
        for row in inserts[0][2]:
            self.assertEqual(row["staging_status"], "pending")
            self.assertIn("scraper_run_id", row)

    def test_in_run_duplicates_collapse(self):
        leads = self._leads(3) + self._leads(3)
        self.use(FakeClient())
        staged, dupes = BotBase()._write_staging(leads)
        self.assertEqual((staged, dupes), (3, 3))

    def test_batch_insert_failure_falls_back_per_row(self):
        class Flaky(FakeClient):
            def respond(self, table, ops, payload):
                if [o[0] for o in ops][0] == "insert" and isinstance(payload, list):
                    raise RuntimeError("one bad row")
                return super().respond(table, ops, payload)
        client = self.use(Flaky())
        staged, dupes = BotBase()._write_staging(self._leads(5))
        self.assertEqual((staged, dupes), (5, 0))
        single = [c for c in client.calls if c[1][0][0] == "insert" and isinstance(c[2], dict)]
        self.assertEqual(len(single), 5)

    def test_dedupe_lookup_failure_skips_chunk_instead_of_double_inserting(self):
        class Down(FakeClient):
            def respond(self, table, ops, payload):
                if table == "homeowner_requests_staging" and ops[0][0] == "select":
                    raise RuntimeError("503")
                return super().respond(table, ops, payload)
        client = self.use(Down())
        staged, dupes = BotBase()._write_staging(self._leads(5))
        self.assertEqual((staged, dupes), (0, 0))
        self.assertFalse([c for c in client.calls if c[1][0][0] == "insert"])


# ── fingerprint / frozen_source ──────────────────────────────────────────


class FrozenSourceTests(_FakeSupabase):
    def test_fingerprint_is_order_independent_and_none_when_empty(self):
        a = [LeadPayload("s", "k1"), LeadPayload("s", "k2")]
        b = [LeadPayload("s", "k2"), LeadPayload("s", "k1")]
        self.assertEqual(BotBase.fingerprint(a), BotBase.fingerprint(b))
        self.assertNotEqual(BotBase.fingerprint(a), BotBase.fingerprint(a[:1]))
        self.assertIsNone(BotBase.fingerprint([]))

    def test_frozen_requires_full_window_of_identical_runs(self):
        fp = "abc"
        bot = BotBase()
        bot.frozen_after_runs = 3
        self.use(FakeClient(health_notes=[{"fingerprint": fp}] * 3))
        self.assertTrue(bot._source_frozen(fp))
        self.use(FakeClient(health_notes=[{"fingerprint": fp}] * 2))
        self.assertFalse(bot._source_frozen(fp), "window not full yet")
        self.use(FakeClient(health_notes=[{"fingerprint": fp}, {"fingerprint": "other"}, {"fingerprint": fp}]))
        self.assertFalse(bot._source_frozen(fp))
        self.use(FakeClient(health_notes=[None, {"fingerprint": fp}, {"fingerprint": fp}]))
        self.assertFalse(bot._source_frozen(fp), "pre-deploy rows without notes never match")

    def test_run_reports_frozen_source_instead_of_all_dupes(self):
        class Src(BotBase):
            name = "src"
            frozen_after_runs = 2

            def scrape(self):
                return [LeadPayload("src", "k1"), LeadPayload("src", "k2")]
        fp = BotBase.fingerprint(Src().scrape())
        client = self.use(FakeClient(pending_keys={"k1", "k2"}, health_notes=[{"fingerprint": fp}] * 2))
        out = Src().run()
        self.assertEqual(out["status"], "frozen_source")
        # _report_health upserts by run_id; the fake answers the existence
        # probe with rows, so the terminal write lands as an update.
        final = [c for c in client.calls if c[0] == "bot_run_health"
                 and c[1][0][0] in ("insert", "update") and isinstance(c[2], dict) and c[2].get("notes")]
        self.assertTrue(final)
        self.assertEqual(final[-1][2]["notes"]["fingerprint"], fp)

        client = self.use(FakeClient(pending_keys={"k1", "k2"}, health_notes=[{"fingerprint": "x"}] * 2))
        self.assertEqual(Src().run()["status"], "all_dupes")


# ── alert rules ──────────────────────────────────────────────────────────


class AlertRuleTests(unittest.TestCase):
    NOW = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)

    def _row(self, bot, status, hours_ago, staged=0, err=None):
        st = self.NOW - timedelta(hours=hours_ago)
        return {"bot_source": bot, "status": status, "staged_count": staged,
                "started_at": st.isoformat(), "finished_at": None if status == "running" else st.isoformat(),
                "error_message": err}

    def test_quiet_pipeline_has_no_alerts(self):
        rows = [self._row("hud_reo", "ok", 2, staged=5)]
        summary = {"finished_at": "x", "critical_incomplete": [], "elapsed_seconds": 100,
                   "budget_seconds": 4500, "bots": [{"bot": "hud_reo", "status": "ok"}], "planned": ["hud_reo"]}
        a = build_alerts(rows, summary, now=self.NOW, acknowledged={}, expected={"hud_reo": {"max_silent_days": 14}})
        self.assertEqual(alert_count(a), 0, a)

    def test_missing_summary_and_unfinished_summary(self):
        a = build_alerts([], None, now=self.NOW, acknowledged={}, expected={})
        self.assertIn("runner_incomplete", a)
        a = build_alerts([], {"finished_at": None, "bots": [{"bot": "x", "status": "ok"}], "planned": ["x", "y", "z"]},
                         now=self.NOW, acknowledged={}, expected={})
        self.assertIn("1/3 bots ran, last was x", a["runner_incomplete"][0])

    def test_critical_incomplete_and_overrun(self):
        summary = {"finished_at": "x", "critical_incomplete": ["auto_promoter"], "elapsed_seconds": 5000,
                   "budget_seconds": 4500, "bots": [{"bot": "auto_promoter", "status": "crashed"}], "planned": []}
        a = build_alerts([], summary, now=self.NOW, acknowledged={}, expected={})
        self.assertEqual(a["critical_incomplete"], ["auto_promoter: crashed"])
        self.assertEqual(len(a["overrun"]), 1)

    def test_failed_twice_stuck_frozen_timed_out(self):
        rows = [
            self._row("f", "failed", 1, err="Boom\nstack"), self._row("f", "failed", 13),
            self._row("once", "failed", 1), self._row("once", "ok", 13, staged=1),
            self._row("ghost", "running", 5),
            self._row("fz", "frozen_source", 1),
            self._row("to", "timed_out", 2),
            self._row("to2", "failed", 2, err="BotTimeout: to2 exceeded 600s"),
        ]
        a = build_alerts(rows, {"finished_at": "x", "critical_incomplete": [], "bots": [], "planned": []},
                         now=self.NOW, acknowledged={}, expected={})
        self.assertEqual(a["failed_twice"], ["f: Boom"])
        self.assertEqual(len(a["stuck_running"]), 1)
        self.assertEqual(a["frozen_source"], ["fz"])
        self.assertEqual(sorted(a["timed_out"]), ["to", "to2"])

    def test_silent_uses_expected_thresholds(self):
        rows = [self._row("quiet", "all_dupes", 1), self._row("quiet", "ok", 24 * 20, staged=3),
                self._row("fresh", "ok", 24 * 2, staged=3)]
        exp = {"quiet": {"max_silent_days": 14}, "fresh": {"max_silent_days": 14}, "never": {"max_silent_days": 7}}
        a = build_alerts(rows, {"finished_at": "x", "critical_incomplete": [], "bots": [], "planned": []},
                         now=self.NOW, acknowledged={}, expected=exp)
        self.assertEqual(sorted(a["silent"]), ["never: no runs in 30d", "quiet: last staged 20d ago (limit 14d)"])

    def test_acknowledged_bots_are_listed_not_alerted(self):
        rows = [self._row("bd", "failed", 1), self._row("bd", "failed", 13)]
        a = build_alerts(rows, {"finished_at": "x", "critical_incomplete": [], "bots": [], "planned": []},
                         now=self.NOW, acknowledged={"bd": "paused"}, expected={})
        self.assertEqual(alert_count(a), 0)
        self.assertEqual(a["known"], ["bd: failed (paused)"])


if __name__ == "__main__":
    unittest.main()
