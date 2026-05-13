"""
Orchestrator for the new BotBase scrapers (the ones writing to staging).

Existing scrapers (foreclosure_tennessee_bot, etc) keep firing through
src.run_all.py to homeowner_requests directly. The new ones live here
and write to homeowner_requests_staging until promoted via /admin/staging.

Add new scrapers to NEW_BOTS as you build them.
"""

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path
from typing import List, Type

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
from . import middle_tn_twilio_lookup_bot
from . import auto_promoter_bot
from . import tn_lis_pendens_bot

# Each entry is the module's `run()` function. Add new scrapers here.
# Order matters: lead-source scrapers first; enrichers run AFTER so they
# operate on the latest staged + live inventory.
NEW_BOTS = [
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
    ("middle_tn_twilio_lookup", middle_tn_twilio_lookup_bot.run),
    ("auto_promoter", auto_promoter_bot.run),
    # Autonomous brain — runs LAST so it sees fully-enriched leads
    ("decision_engine", decision_engine_bot.run),
    # Health monitor runs after everything else — surfaces silent
    # bot failures (Shelby ASSR_ASMT, etc.) and AVM coverage gaps.
    ("bot_health_monitor", bot_health_monitor.run),
]


def main() -> int:
    print(f"Running {len(NEW_BOTS)} new (staging) scrapers")
    print("=" * 70)

    summary = []
    for name, runner in NEW_BOTS:
        print(f"\n[{name}] starting")
        try:
            result = runner() or {}
            print(f"[{name}] -> {result.get('status')}: {result.get('staged', 0)} staged, "
                  f"{result.get('duplicates', 0)} dupes, {result.get('fetched', 0)} fetched")
            summary.append((name, result))
        except Exception as e:
            print(f"[{name}] CRASHED: {e}")
            traceback.print_exc()
            summary.append((name, {"status": "crashed", "error": str(e)}))

    print("\n" + "=" * 70)
    print("Summary:")
    total_staged = 0
    total_failed = 0
    for name, r in summary:
        status = r.get("status", "?")
        staged = r.get("staged", 0)
        total_staged += staged
        if status in ("failed", "crashed"):
            total_failed += 1
        print(f"  {name:25s} {status:12s} staged={staged}")
    print(f"\nTotal staged: {total_staged} · failed: {total_failed}")
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
