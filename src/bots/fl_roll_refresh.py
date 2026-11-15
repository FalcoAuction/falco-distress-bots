"""
Refresh the Florida caches the FL bots read (data/fl_dor/roll.sqlite):

  1. DOR NAL + NAP files for the FALCO Florida counties, re-downloaded only
     when the portal shows a newer file (July preliminary, October final).
  2. The Sunbiz equipment-owner index (doc number -> name, officer) from
     the state's quarterly corporate file, rebuilt only when a new quarter
     is posted.

A cold build is ~15 minutes and ~2 GB of downloads, so this runs as its
own workflow step with the cache persisted between runs, never inside
_run_new's per-bot time caps. Warm runs make three listing requests.

    python -m src.bots.fl_roll_refresh
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from ._fl_roll import open_roll
from ._sunbiz import public_login
from .fl_sunbiz_bot import FlSunbizBot


def run() -> dict:
    bot = FlSunbizBot()
    log = logging.getLogger("fl_roll_refresh")
    roll = open_roll(bot.session, log, datetime.now(timezone.utc).year, refresh=True)
    if roll is None:
        log.error("FL roll could not be built")
        return {"status": "failed"}
    login = public_login(bot.session)
    if login:
        bot._corp_index(login, roll)
    else:
        log.warning("Sunbiz public login not found; equipment-owner index not refreshed")
    counts = roll.counts()
    counts["corp"] = roll.db.execute("SELECT count(*) FROM corp").fetchone()[0] if login else None
    log.info(f"FL caches: {counts}")
    return {"status": "ok", **counts}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    result = run()
    print(result)
    sys.exit(0 if result.get("status") == "ok" else 1)
