"""
Orchestrator for the new BotBase scrapers (the ones writing to staging).

Existing scrapers (foreclosure_tennessee_bot, etc) keep firing through
src.run_all.py to homeowner_requests directly. The new ones live here
and write to homeowner_requests_staging until promoted via /admin/staging.

Add new scrapers to NEW_BOTS as you build them.
"""

from __future__ import annotations

import sys
import traceback
from typing import List, Type

from . import hud_reo_bot
from . import nashville_codes_bot
from . import memphis_codes_bot
from . import craigslist_tn_bot
from . import usda_rhs_bot

# Each entry is the module's `run()` function. Add new scrapers here.
NEW_BOTS = [
    ("hud_reo", hud_reo_bot.run),
    ("nashville_codes", nashville_codes_bot.run),
    ("memphis_codes", memphis_codes_bot.run),
    ("craigslist_tn", craigslist_tn_bot.run),
    ("usda_rhs", usda_rhs_bot.run),
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
