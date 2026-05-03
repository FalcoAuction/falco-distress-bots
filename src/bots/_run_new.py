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
from . import craigslist_tn_bot
from . import usda_rhs_bot
from . import knoxville_poh_bot
from . import tn_tax_delinquent_bot
from . import hamilton_tax_delinquent_bot
from . import nashville_ledger_bot
from . import memphis_daily_news_bot
from . import hamilton_county_herald_bot
from . import tn_probate_bot
from . import courtlistener_bankruptcy_bot
from . import bankruptcy_schedule_d_bot
from . import tn_public_notice_bot
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
from . import skip_trace_enricher_bot
from . import phone_resolver_bot
from . import stacked_distress_aggregator_bot
from . import decision_engine_bot

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
    ("tn_probate", tn_probate_bot.run),
    ("courtlistener_bankruptcy", courtlistener_bankruptcy_bot.run),
    ("bankruptcy_schedule_d", bankruptcy_schedule_d_bot.run),
    ("tn_public_notice", tn_public_notice_bot.run),
    ("craigslist_tn", craigslist_tn_bot.run),
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
    ("mortgage_estimator", mortgage_estimator_bot.run),
    ("probate_property_enricher", probate_property_enricher_bot.run),
    ("bankruptcy_property_enricher", bankruptcy_property_enricher_bot.run),
    ("owner_classifier", owner_classifier_bot.run),
    ("skip_trace_enricher", skip_trace_enricher_bot.run),
    ("phone_resolver", phone_resolver_bot.run),
    ("stacked_distress_aggregator", stacked_distress_aggregator_bot.run),
    # Autonomous brain — runs LAST so it sees fully-enriched leads
    ("decision_engine", decision_engine_bot.run),
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
