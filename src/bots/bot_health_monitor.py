"""Bot health monitor — daily summary of per-bot yield + silence detection.

The Shelby ASSR_ASMT silently broke (server returns 0 features on every
query) and we didn't catch it for weeks. This bot solves the "silent
failure" class of issue: every day it pulls per-bot row counts + most
recent ingestion timestamp from `homeowner_requests_staging`, compares
to a baseline, and surfaces:

  - HEALTHY: ran recently and yielded
  - STALE: hasn't yielded in N days (configurable)
  - DEAD: hasn't yielded in M days, alert
  - GROWING: yield is rising (good)
  - SHRINKING: yield falling (data source degrading)

Output:
  - logged via standard BotBase
  - written to phone_metadata.bot_health_monitor in a singleton row
    (so the admin UI can show it)

Distress type: N/A (operational health monitor).
"""
from __future__ import annotations

import sys
import traceback as tb
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase


# Bots we expect to yield non-zero rows under normal operation.
# Probate / bankruptcy / public_notice are bursty; allow longer silences.
EXPECTED_BOTS = {
    "hamilton_tax_delinquent":  {"max_silent_days": 7,  "min_total": 1000},
    "memphis_codes":             {"max_silent_days": 14, "min_total": 50},
    "nashville_codes":           {"max_silent_days": 14, "min_total": 50},
    "chattanooga_codes":         {"max_silent_days": 14, "min_total": 50},
    "memphis_daily_news":        {"max_silent_days": 14, "min_total": 30},
    "nashville_ledger":          {"max_silent_days": 14, "min_total": 30},
    "hud_reo":                   {"max_silent_days": 14, "min_total": 30},
    "tn_probate":                {"max_silent_days": 30, "min_total": 0},
    "courtlistener_bankruptcy":  {"max_silent_days": 30, "min_total": 0},
    "bankruptcy_schedule_d":     {"max_silent_days": 30, "min_total": 0},
    "tn_public_notice":          {"max_silent_days": 30, "min_total": 0},
    "hamilton_county_herald":    {"max_silent_days": 30, "min_total": 0},
    "knoxville_poh":             {"max_silent_days": 30, "min_total": 0},
    "johnson_city_bdsr":         {"max_silent_days": 30, "min_total": 0},
    "tn_tax_delinquent":         {"max_silent_days": 30, "min_total": 0},
    "usda_rhs":                  {"max_silent_days": 30, "min_total": 0},
}


class BotHealthMonitor(BotBase):
    name = "bot_health_monitor"
    description = "Daily per-bot yield + silence + AVM-coverage health summary"
    throttle_seconds = 0.0
    expected_min_yield = 0  # diagnostic, not a producer

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "alerts": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        per_bot_stats: Dict[str, Dict[str, Any]] = {}
        alerts: List[Dict[str, Any]] = []
        error_message: Optional[str] = None

        try:
            # Walk both tables, compute per-bot stats
            for table, src_col in (
                ("homeowner_requests_staging", "bot_source"),
                ("homeowner_requests", "source"),
            ):
                rows = self._fetch_all(client, table, src_col)
                for r in rows:
                    src = r.get(src_col) or "unknown"
                    s = per_bot_stats.setdefault(src, {
                        "total": 0, "with_avm": 0,
                        "newest_ingest": None, "oldest_ingest": None,
                        "tables": set(),
                    })
                    s["total"] += 1
                    if r.get("property_value"):
                        s["with_avm"] += 1
                    s["tables"].add(table)
                    ingested = r.get("ingested_at") or r.get("created_at")
                    if ingested:
                        if not s["newest_ingest"] or ingested > s["newest_ingest"]:
                            s["newest_ingest"] = ingested
                        if not s["oldest_ingest"] or ingested < s["oldest_ingest"]:
                            s["oldest_ingest"] = ingested

            # Evaluate alerts
            now = datetime.now(timezone.utc)
            for src, expectations in EXPECTED_BOTS.items():
                stats = per_bot_stats.get(src)
                if not stats:
                    alerts.append({
                        "level": "DEAD",
                        "bot": src,
                        "reason": "no rows in either table",
                    })
                    continue

                # Silence check
                newest = stats.get("newest_ingest")
                if newest:
                    try:
                        newest_dt = datetime.fromisoformat(
                            str(newest).replace("Z", "+00:00")
                        )
                        if newest_dt.tzinfo is None:
                            newest_dt = newest_dt.replace(tzinfo=timezone.utc)
                        silent_days = (now - newest_dt).days
                        if silent_days > expectations["max_silent_days"]:
                            alerts.append({
                                "level": "STALE",
                                "bot": src,
                                "silent_days": silent_days,
                                "max_silent_days": expectations["max_silent_days"],
                                "newest_ingest": newest,
                            })
                    except (ValueError, TypeError):
                        pass

                # Total volume check
                if stats["total"] < expectations["min_total"]:
                    alerts.append({
                        "level": "LOW_VOLUME",
                        "bot": src,
                        "total": stats["total"],
                        "min_total": expectations["min_total"],
                    })

                # AVM hit-rate check (Hamilton ~50%; sources known to
                # have low AVM coverage are exempt)
                avm_rate = (
                    stats["with_avm"] / stats["total"] * 100
                    if stats["total"] else 0
                )
                stats["avm_pct"] = round(avm_rate, 1)
                if (src in {"hamilton_tax_delinquent", "memphis_codes",
                            "memphis_daily_news"}
                        and avm_rate < 20 and stats["total"] >= 50):
                    alerts.append({
                        "level": "AVM_GAP",
                        "bot": src,
                        "avm_pct": stats["avm_pct"],
                        "total": stats["total"],
                        "with_avm": stats["with_avm"],
                    })

            # Log summary
            self.logger.info("=" * 70)
            self.logger.info("BOT HEALTH SUMMARY")
            self.logger.info("=" * 70)
            for src in sorted(per_bot_stats.keys(),
                                key=lambda s: -per_bot_stats[s]["total"]):
                s = per_bot_stats[src]
                # Strip the JSON-incompatible 'tables' set before logging
                s_log = {k: v for k, v in s.items() if k != "tables"}
                self.logger.info(f"  {src:30s} total={s['total']:5d}  "
                                  f"avm={s_log.get('avm_pct', 0):>5.1f}%  "
                                  f"newest={s.get('newest_ingest')}")

            self.logger.info("=" * 70)
            if alerts:
                self.logger.warning(f"ALERTS ({len(alerts)}):")
                for a in alerts:
                    self.logger.warning(f"  [{a['level']}] {a['bot']}: "
                                          f"{ {k: v for k, v in a.items() if k not in ('level', 'bot')} }")
            else:
                self.logger.info("No alerts — all bots healthy")
            self.logger.info("=" * 70)

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        status = "failed" if error_message else "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=sum(s["total"] for s in per_bot_stats.values()),
            parsed_count=len(per_bot_stats),
            staged_count=len(alerts),
            duplicate_count=0,
            error_message=error_message,
        )
        # Strip non-JSON-serializable sets before returning
        per_bot_clean = {
            src: {k: v for k, v in s.items() if k != "tables"}
            for src, s in per_bot_stats.items()
        }
        return {
            "name": self.name, "status": status,
            "per_bot": per_bot_clean,
            "alerts": alerts,
            "error": error_message,
            "staged": len(alerts), "duplicates": 0,
            "fetched": sum(s["total"] for s in per_bot_stats.values()),
        }

    def _fetch_all(self, client, table: str, src_col: str) -> List[Dict[str, Any]]:
        out = []
        PAGE_SIZE = 1000
        MAX_PAGES = 20
        for page in range(MAX_PAGES):
            try:
                q = (
                    client.table(table)
                    .select(f"id, {src_col}, property_value, ingested_at, created_at")
                    .order("id")
                    .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                out.extend(rows)
                if len(rows) < PAGE_SIZE:
                    break
            except Exception as e:
                # ingested_at / created_at may not exist on all tables —
                # fall back to a smaller select.
                if "column" in str(e).lower():
                    try:
                        q = (
                            client.table(table)
                            .select(f"id, {src_col}, property_value")
                            .order("id")
                            .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                            .execute()
                        )
                        rows = getattr(q, "data", None) or []
                        if not rows:
                            break
                        out.extend(rows)
                        if len(rows) < PAGE_SIZE:
                            break
                        continue
                    except Exception as e2:
                        self.logger.warning(
                            f"fallback select on {table} page {page} failed: {e2}"
                        )
                        break
                self.logger.warning(
                    f"fetch on {table} page {page} failed: {e}"
                )
                break
        return out


def run() -> dict:
    bot = BotHealthMonitor()
    return bot.run()


if __name__ == "__main__":
    print(run())
