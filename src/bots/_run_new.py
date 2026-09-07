"""
Orchestrator for the new BotBase scrapers (the ones writing to staging).

Existing scrapers (foreclosure_tennessee_bot, etc) keep firing through
src.run_all.py to homeowner_requests directly. The new ones live here
and write to homeowner_requests_staging until promoted via /admin/staging.

Add new scrapers to NEW_BOTS as you build them.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# Load env vars from .env file at repo root (one place for all credentials).
# Searches up from this file's location to find a .env.
try:
    from dotenv import load_dotenv
    # Walk up until we find .env or hit filesystem root
    _here = Path(__file__).resolve()
    for _parent in [_here.parent, *_here.parents]:
        candidate = _parent / ".env"
        if candidate.exists():
            load_dotenv(candidate, override=False)
            break
except ImportError:
    # python-dotenv not installed; rely on env vars set externally
    pass

from ._base import BotTimeout, close_open_runs

from . import hud_reo_bot
from . import nashville_codes_bot
from . import memphis_codes_bot
from . import chattanooga_codes_bot
from . import johnson_city_bdsr_bot
# craigslist_tn_bot removed 2026-05-04 — 100% bad-data scam pond
# (RV lots, $1 land swaps, marketing-copy "addresses"). 0% phone yield.
# from . import craigslist_tn_bot
from . import usda_rhs_bot
from . import knoxville_poh_bot
from . import tn_tax_delinquent_bot
from . import hamilton_tax_delinquent_bot
from . import nashville_ledger_bot
from . import memphis_daily_news_bot
from . import hamilton_county_herald_bot
# Direct substitute-trustee firm scrapers — bypass the newspaper-publication
# requirement of TCA § 35-5-101 by going to the firms' own public sale
# lists. ~10-25 days earlier than newspaper notice. Also surfaces the
# auction-platform tag (AUCTION/HUBZU/HUDMARSH/MWZM) so we know which
# leads end up on Auction.com vs courthouse. Added 2026-05-12.
from . import brock_scott_trustee_bot
from . import mackie_wolf_trustee_bot
from . import tn_probate_bot
from . import courtlistener_bankruptcy_bot
from . import bankruptcy_schedule_d_bot
from . import tn_public_notice_bot
from . import mtn_cities_codes_bot
from . import mtn_lis_pendens_rod_bot
from . import davidson_demolition_bot
from . import mortgage_estimator_bot
from . import notice_enricher_bot
from . import phone_classifier_bot
from . import tpad_enricher_bot
from . import davidson_assessor_bot
from . import williamson_assessor_bot
from . import shelby_assessor_bot
from . import rutherford_assessor_bot
from . import hamilton_assessor_bot
from . import probate_property_enricher_bot
from . import bankruptcy_property_enricher_bot
from . import owner_classifier_bot
from . import xref_avm_enricher_bot
from . import continuance_reaper_bot
from . import trustee_status_reaper_bot
from . import stale_regrade_bot
from . import bot_health_monitor
from . import skip_trace_enricher_bot
from . import phone_resolver_bot
from . import stacked_distress_aggregator_bot
from . import decision_engine_bot
# MTN-focused enrichment chain (added 2026-05-06). These are what
# graduate staged leads into dialer-ready leads:
#   - HMDA enricher: defensible mortgage match via CFPB data
#   - Mortgage amortizer: writes current balance from HMDA signal
#   - Middle-TN skip-trace: BatchData phones for MTN focus counties
#   - Middle-TN Twilio lookup: validates every phone (mobile/landline/voip)
#   - Auto-promoter: final gate — graduates eligible staging → live
from . import hmda_enricher_bot
from . import mortgage_amortizer_bot
from . import middle_tn_skiptrace_bot
from . import enformion_skip_trace_bot
from . import middle_tn_twilio_lookup_bot
from . import auto_promoter_bot
from . import tn_lis_pendens_bot

# Each entry is the module's `run()` function. Add new scrapers here.
# Order matters: lead-source scrapers first; enrichers run AFTER so they
# operate on the latest staged + live inventory.
NEW_BOTS = [
    # Watchdog FIRST. It summarizes the staging table (yesterday's
    # supply), not this run, so nothing is lost by running it early and
    # everything is lost by running it last: on a run that hits the job
    # timeout, last is exactly the slot that never executes.
    ("bot_health_monitor", bot_health_monitor.run),
    # Lead sources
    ("hud_reo", hud_reo_bot.run),
    ("nashville_codes", nashville_codes_bot.run),
    ("memphis_codes", memphis_codes_bot.run),
    ("chattanooga_codes", chattanooga_codes_bot.run),
    ("johnson_city_bdsr", johnson_city_bdsr_bot.run),
    ("knoxville_poh", knoxville_poh_bot.run),
    ("tn_tax_delinquent", tn_tax_delinquent_bot.run),
    ("hamilton_tax_delinquent", hamilton_tax_delinquent_bot.run),
    ("nashville_ledger", nashville_ledger_bot.run),
    ("memphis_daily_news", memphis_daily_news_bot.run),
    ("hamilton_county_herald", hamilton_county_herald_bot.run),
    # Substitute trustee firms — direct scrape, earlier than newspaper.
    # Brock & Scott: ~27 TN listings, static HTML. Mackie Wolf: ~58 TN
    # listings via weekly PDF, includes Auction.com/Hubzu platform tags.
    ("brock_scott_trustee", brock_scott_trustee_bot.run),
    ("mackie_wolf_trustee", mackie_wolf_trustee_bot.run),
    # Lis pendens — keyword-driven full-text search of the TN Public
    # Notice index. Captures lawsuit-stage distress 60-120 days
    # earlier than the trustee-notice-based scrapers. New 2026-05-07.
    ("tn_lis_pendens", tn_lis_pendens_bot.run),
    # tn_probate temporarily disabled 2026-05-06 — even with 1-week scan
    # window + first-page fast-fail, detail fetches against tnledger.com
    # were hanging the GH Actions runner for 30+ min when the source was
    # slow. Re-enable after adding a per-bot wall-clock cap. Volume from
    # tn_public_notice + foreclosure_tennessee_bot covers probate notices
    # via the TN Press Association aggregator in the meantime.
    # ("tn_probate", tn_probate_bot.run),
    ("courtlistener_bankruptcy", courtlistener_bankruptcy_bot.run),
    ("bankruptcy_schedule_d", bankruptcy_schedule_d_bot.run),
    ("tn_public_notice", tn_public_notice_bot.run),
    # Middle TN secondary cities — only Mt Juliet has a public scrapable
    # code-enforcement feed (SeeClickFix). Other 5 cities documented as
    # no-public-access in the bot file. New 2026-05-08.
    ("mtn_cities_codes", mtn_cities_codes_bot.run),
    # Per-county Lis Pendens recordings at the ROD — earliest possible
    # foreclosure signal (60-120 days before any sale notice). NO-OPS
    # without paid subscription creds: FALCO_DAVIDSON_ROD_USER/_PASSWORD,
    # FALCO_WILLIAMSON_ROD_USER/_PASSWORD, FALCO_HAMILTON_ROD_USER/_PASSWORD.
    ("mtn_lis_pendens_rod", mtn_lis_pendens_rod_bot.run),
    # Davidson demolition + fire-damage building permits — owners
    # actively committing to teardown / unable to repair, sourced from
    # data.nashville.gov (ArcGIS Feature Service, free, no auth).
    # ~2 new permits/day, 380 in last 180 days. 2026-05-09.
    ("davidson_demolition", davidson_demolition_bot.run),
    # ("craigslist_tn", craigslist_tn_bot.run),  # disabled — scam pond
    ("usda_rhs", usda_rhs_bot.run),
    # Enrichers (run last — replace paid API calls with free internal logic)
    ("notice_enricher", notice_enricher_bot.run),
    ("phone_classifier", phone_classifier_bot.run),
    ("tpad_enricher", tpad_enricher_bot.run),
    ("davidson_assessor", davidson_assessor_bot.run),
    ("williamson_assessor", williamson_assessor_bot.run),
    ("shelby_assessor", shelby_assessor_bot.run),
    ("rutherford_assessor", rutherford_assessor_bot.run),
    ("hamilton_assessor", hamilton_assessor_bot.run),
    # mortgage_estimator moved AFTER hmda_enricher + mortgage_amortizer
    # (see below) so it acts as a true final fallback. Previously it ran
    # here and pre-filled nulls with 80% LTV before HMDA had a chance to
    # write defensible values; the order was inverted vs the intended
    # chain HMDA -> Ledger -> amortizer -> estimator.
    ("probate_property_enricher", probate_property_enricher_bot.run),
    ("bankruptcy_property_enricher", bankruptcy_property_enricher_bot.run),
    ("owner_classifier", owner_classifier_bot.run),
    ("xref_avm_enricher", xref_avm_enricher_bot.run),
    ("continuance_reaper", continuance_reaper_bot.run),
    # Docket-diff reaper: auto-flag foreclosure leads as cancelled when
    # their notice stops appearing in the daily scrape (typically means
    # the homeowner reinstated / paid off / the sale was withdrawn).
    # Runs after the scrapers so it sees today's last_seen_at touches.
    ("trustee_status_reaper", trustee_status_reaper_bot.run),
    ("stale_regrade", stale_regrade_bot.run),
    ("skip_trace_enricher", skip_trace_enricher_bot.run),
    ("phone_resolver", phone_resolver_bot.run),
    ("stacked_distress_aggregator", stacked_distress_aggregator_bot.run),
    # MTN-focused enrichment chain — graduates staged leads into the dialer.
    # Order matters: HMDA must run before mortgage_amortizer (amortizer reads
    # HMDA's mortgage_signal); skip-trace before Twilio lookup (validates
    # the new phones); auto-promoter LAST (sees fully-enriched leads).
    ("hmda_enricher", hmda_enricher_bot.run),
    ("mortgage_amortizer", mortgage_amortizer_bot.run),
    # mortgage_estimator runs LAST in the mortgage chain so HMDA +
    # amortizer + nashville_ledger_extracted get first crack at
    # writing a defensible value. Only then does the 80% LTV fallback
    # fill remaining nulls — it self-gates on already-set
    # mortgage_balance and won't overwrite higher-confidence sources.
    ("mortgage_estimator", mortgage_estimator_bot.run),
    ("middle_tn_skiptrace", middle_tn_skiptrace_bot.run),
    # Enformion waterfall AFTER BatchData (targets its no-phone misses +
    # cross-verifies unverified primaries). No-ops without
    # FALCO_ENFORMION_AP_NAME/_PASSWORD in env. Charged on match only.
    ("enformion_skip_trace", enformion_skip_trace_bot.run),
    ("middle_tn_twilio_lookup", middle_tn_twilio_lookup_bot.run),
    ("auto_promoter", auto_promoter_bot.run),
    # Autonomous brain — runs LAST so it sees fully-enriched leads
    ("decision_engine", decision_engine_bot.run),
]


# ── Scheduling policy ──────────────────────────────────────────────────────

# Bots the run MUST reach. When the budget runs short, everything not on
# this list is skipped so these still fire. auto_promoter is the only gate
# between staging and the dialer; the two skip-trace bots feed it; the
# reaper keeps withdrawn sales out of the queue; decision_engine grades.
# Before the budget gate existed, ~40% of runs were killed by the job
# timeout before reaching any of them.
CRITICAL_BOTS = {
    "enformion_skip_trace",
    "middle_tn_twilio_lookup",
    "auto_promoter",
    "trustee_status_reaper",
    "decision_engine",
}

# Slow, low-yield, and the source only refreshes weekly. Excluded from the
# daily run; weekly_heavy.yml runs them with --only.
HEAVY_BOTS = {"hamilton_tax_delinquent"}

# Per-bot wall-clock caps (seconds). Default covers every observed median
# with room; overrides are for bots whose healthy median sits near it.
# A bot past its cap is interrupted (BotTimeout via SIGALRM), records
# `timed_out`, and the runner moves on. Losing one bot's partial run is
# cheaper than losing the tail of the pipeline.
DEFAULT_BOT_TIMEOUT = 600
BOT_TIMEOUTS = {
    "hmda_enricher": 900,          # median 5.4m, max 12.6m; 954 leads/14d
    "hamilton_tax_delinquent": 1200,  # weekly only; 2,000-lead CSV
    "davidson_assessor": 480,      # 150 x 1.5s throttle + fetches
}

# Wall clock held back for the critical tail. decision_engine's worst
# observed run is 7.3 min; the other four finish in under a minute.
CRITICAL_RESERVE_SECONDS = 600

SUMMARY_PATH = Path("out/reports/new_bots_summary.json")

# Terminal statuses that mean "the bot ran and reported". Anything else
# from a critical bot fails the job.
COMPLETE_STATUSES = {
    "ok", "all_dupes", "zero_yield", "below_threshold", "frozen_source",
    "skipped_no_creds", "no_supabase",
}


class _Alarm:
    """Per-bot wall clock via SIGALRM. POSIX only, which is what CI is.
    On Windows it degrades to no cap and says so once; the global budget
    gate still works there."""

    supported = hasattr(signal, "SIGALRM")
    _warned = False

    def __init__(self, seconds: int, name: str):
        self.seconds = max(1, int(seconds))
        self.name = name
        self._prev = None

    def __enter__(self):
        if not self.supported:
            if not _Alarm._warned:
                print("[runner] per-bot timeouts unavailable on this platform (no SIGALRM)")
                _Alarm._warned = True
            return self

        def _fire(signum, frame):
            raise BotTimeout(f"{self.name} exceeded {self.seconds}s wall-clock cap")

        self._prev = signal.signal(signal.SIGALRM, _fire)
        signal.alarm(self.seconds)
        return self

    def __exit__(self, *exc):
        if self.supported:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, self._prev)
        return False


def select_bots(
    only: Optional[List[str]] = None,
    skip: Optional[List[str]] = None,
    include_heavy: bool = False,
    bots: Optional[List[Tuple[str, Callable]]] = None,
) -> List[Tuple[str, Callable]]:
    """Apply --only / --skip / heavy exclusion. --only is explicit, so it
    bypasses the heavy exclusion (that's how the weekly job runs them)."""
    pool = bots if bots is not None else NEW_BOTS
    skip_set = set(skip or [])
    if only:
        wanted = set(only)
        unknown = wanted - {n for n, _ in pool}
        if unknown:
            raise SystemExit(f"--only names unknown bots: {sorted(unknown)}")
        return [(n, r) for n, r in pool if n in wanted and n not in skip_set]
    out = []
    for n, r in pool:
        if n in skip_set:
            continue
        if n in HEAVY_BOTS and not include_heavy:
            continue
        out.append((n, r))
    return out


def run_pipeline(
    bots: List[Tuple[str, Callable]],
    budget_seconds: float,
    *,
    reserve_seconds: float = CRITICAL_RESERVE_SECONDS,
    timeouts: Optional[Dict[str, int]] = None,
    default_timeout: int = DEFAULT_BOT_TIMEOUT,
    critical: Optional[set] = None,
    clock: Callable[[], float] = time.monotonic,
    summary_path: Optional[Path] = SUMMARY_PATH,
) -> Dict[str, Any]:
    """Run `bots` in order under a global budget with per-bot caps.

    Budget gate: once the remaining budget drops below `reserve_seconds`,
    non-critical bots are skipped (status `skipped_budget`) so the
    critical tail still runs. Critical bots always run, capped to
    whatever budget is left (floor 60s).

    The summary is rewritten after every bot so a hard kill (job
    timeout) still leaves a partial file for the alert step to read.
    """
    timeouts = timeouts if timeouts is not None else BOT_TIMEOUTS
    critical = critical if critical is not None else CRITICAL_BOTS
    t0 = clock()
    summary: Dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "budget_seconds": budget_seconds,
        "reserve_seconds": reserve_seconds,
        "planned": [n for n, _ in bots],
        "bots": [],
        "critical_incomplete": [],
    }

    def _flush():
        if summary_path is None:
            return
        try:
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        except Exception as e:
            print(f"[runner] could not write summary: {e}")

    print(f"Running {len(bots)} staging scrapers, budget {budget_seconds / 60:.0f} min, "
          f"reserve {reserve_seconds / 60:.0f} min for critical tail")
    print("=" * 70)

    for i, (name, runner) in enumerate(bots):
        elapsed = clock() - t0
        remaining = budget_seconds - elapsed
        is_critical = name in critical
        # The reserve exists to protect critical bots still ahead in the
        # list. Once the last one has run (or none were selected, e.g.
        # `--only hamilton_tax_delinquent`), the rest can use everything.
        critical_ahead = any(n in critical for n, _ in bots[i + 1:])

        if not is_critical and critical_ahead and remaining < reserve_seconds:
            print(f"\n[{name}] SKIPPED (budget: {remaining / 60:.1f} min left, reserve {reserve_seconds / 60:.0f})")
            summary["bots"].append({"bot": name, "status": "skipped_budget", "elapsed": 0.0})
            _flush()
            continue

        cap = timeouts.get(name, default_timeout)
        if remaining < cap:
            cap = max(60, int(remaining))
        bot_started = datetime.now(timezone.utc)
        t1 = clock()
        print(f"\n[{name}] starting (cap {cap}s, {remaining / 60:.1f} min budget left)")
        result: Dict[str, Any]
        try:
            with _Alarm(cap, name):
                result = runner() or {}
            status = result.get("status") or "?"
            print(f"[{name}] -> {status}: {result.get('staged', 0)} staged, "
                  f"{result.get('duplicates', 0)} dupes, {result.get('fetched', 0)} fetched")
        except BotTimeout as e:
            # The bot's own run() either caught this (and recorded
            # timed_out/failed itself) or it escaped. Either way close
            # any `running` row it left behind.
            status = "timed_out"
            result = {"status": status, "error": str(e)}
            print(f"[{name}] TIMED OUT: {e}")
            close_open_runs(name, "timed_out", str(e), since=bot_started)
        except Exception as e:
            status = "crashed"
            result = {"status": status, "error": str(e)}
            print(f"[{name}] CRASHED: {e}")
            traceback.print_exc()
            close_open_runs(name, "failed", f"{type(e).__name__}: {e}", since=bot_started)

        summary["bots"].append({
            "bot": name,
            "status": status,
            "elapsed": round(clock() - t1, 1),
            "staged": result.get("staged", 0),
            "fetched": result.get("fetched", 0),
            "duplicates": result.get("duplicates", 0),
            "error": (result.get("error") or None) and str(result.get("error"))[:500],
        })
        _flush()

    ran = {b["bot"]: b["status"] for b in summary["bots"]}
    summary["critical_incomplete"] = sorted(
        n for n, _ in bots
        if n in critical and ran.get(n) not in COMPLETE_STATUSES
    )
    summary["elapsed_seconds"] = round(clock() - t0, 1)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    _flush()
    return summary


def print_summary(summary: Dict[str, Any]) -> None:
    print("\n" + "=" * 70)
    print("Summary:")
    total_staged = 0
    for b in summary["bots"]:
        total_staged += b.get("staged") or 0
        print(f"  {b['bot']:30s} {b['status']:16s} {b.get('elapsed', 0):7.1f}s  staged={b.get('staged', 0)}")
    print(f"\nTotal staged: {total_staged} | elapsed {summary.get('elapsed_seconds', 0) / 60:.1f} min "
          f"of {summary['budget_seconds'] / 60:.0f}")
    if summary["critical_incomplete"]:
        print(f"CRITICAL BOTS DID NOT COMPLETE: {summary['critical_incomplete']}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Run the staging scrapers under a wall-clock budget.")
    ap.add_argument("--only", help="comma-separated bot names to run (bypasses heavy exclusion)")
    ap.add_argument("--skip", help="comma-separated bot names to skip")
    ap.add_argument("--include-heavy", action="store_true", help=f"also run {sorted(HEAVY_BOTS)}")
    ap.add_argument("--budget-min", type=float,
                    default=float(os.environ.get("FALCO_RUN_BUDGET_MIN", "75")),
                    help="global wall-clock budget in minutes (env FALCO_RUN_BUDGET_MIN, default 75)")
    ap.add_argument("--list", action="store_true", help="print the selected bot order and exit")
    args = ap.parse_args(argv)

    only = [x.strip() for x in args.only.split(",") if x.strip()] if args.only else None
    skip = [x.strip() for x in args.skip.split(",") if x.strip()] if args.skip else None
    bots = select_bots(only=only, skip=skip, include_heavy=args.include_heavy)

    if args.list:
        for n, _ in bots:
            tag = " [critical]" if n in CRITICAL_BOTS else (" [heavy]" if n in HEAVY_BOTS else "")
            print(f"{n}{tag}")
        return 0

    summary = run_pipeline(bots, args.budget_min * 60)
    print_summary(summary)
    # The job goes red only when a critical bot didn't complete. A source
    # scraper failing is an alert (see _alert.py), not a red build; the
    # BatchData bot fails every run by design while it's paused.
    return 1 if summary["critical_incomplete"] else 0


if __name__ == "__main__":
    sys.exit(main())
