"""
Post-run alerting for the daily pipeline.

Reads bot_run_health (last 30 days) plus the runner's summary JSON,
decides what a human needs to know, prints it, and emails it through
Resend when RESEND_API_KEY is set and there is at least one alert.
No alerts, no email: silence means healthy. Never fails the job.

Before this existed nothing notified anyone. `continue-on-error: true`
on both pipeline steps meant the workflow could not go red, so BatchData
403'd for two months and a WAF blocked mackie_wolf for weeks while every
run looked fine.

Rules (each becomes a heading in the report):
  runner_incomplete    summary missing or never finished (job timeout hit)
  critical_incomplete  a critical bot did not reach a terminal status
  overrun              runner used more than its budget
  timed_out            bot hit its per-bot cap in the last 24h
  failed_twice         a bot's last two runs both failed or crashed
  stuck_running        a `running` row older than 3h that nobody closed
  frozen_source        latest status is frozen_source (identical output
                       for frozen_after_runs consecutive runs)
  silent               expected producer hasn't staged a row within its
                       max_silent_days (thresholds from bot_health_monitor)
  skipped_budget       bots the budget gate skipped (informational)

Bots in ACKNOWLEDGED are listed under "known" with the reason and never
alert. Add a bot there when its failure is a decision, not a bug.

    python -m src.bots._alert            # prints; emails if RESEND_API_KEY set
    python -m src.bots._alert --dry-run  # prints only
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
    _here = Path(__file__).resolve()
    for _parent in [_here.parent, *_here.parents]:
        if (_parent / ".env").exists():
            load_dotenv(_parent / ".env", override=False)
            break
except ImportError:
    pass

from ._base import _supabase
from .bot_health_monitor import EXPECTED_BOTS

SUMMARY_PATH = Path("out/reports/new_bots_summary.json")
RESEND_URL = "https://api.resend.com/emails"
ALERT_FROM = "FALCO Pipeline <falco@falco.llc>"
DEFAULT_ALERT_TO = "falco@falco.llc"
STUCK_AFTER = timedelta(hours=3)
LOOKBACK_DAYS = 30

# Failures that are decisions, not bugs. Listed, never alerted.
ACKNOWLEDGED: Dict[str, str] = {
    "middle_tn_skiptrace": "BatchData paused until after the license exam (Sep 2026)",
    "mtn_lis_pendens_rod": "ROD subscriptions not purchased; skipped_no_creds by design",
    "tn_probate": "disabled 2026-05-06 pending a per-bot cap; the cap exists now, re-enabling is a decision",
}

FAIL_STATUSES = {"failed", "crashed", "timed_out"}


def _parse_ts(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        s = str(v).replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def load_rows(days: int = LOOKBACK_DAYS) -> List[Dict[str, Any]]:
    client = _supabase()
    if client is None:
        raise RuntimeError("no supabase client (missing creds)")
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    res = (
        client.table("bot_run_health")
        .select("bot_source,status,staged_count,started_at,finished_at,error_message")
        .gte("started_at", since)
        .order("started_at", desc=True)
        .limit(6000)
        .execute()
    )
    return getattr(res, "data", None) or []


def load_summary(path: Path = SUMMARY_PATH) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_alerts(
    rows: List[Dict[str, Any]],
    summary: Optional[Dict[str, Any]],
    now: Optional[datetime] = None,
    acknowledged: Optional[Dict[str, str]] = None,
    expected: Optional[Dict[str, Dict[str, int]]] = None,
) -> Dict[str, List[str]]:
    """Pure: rows (newest first) + summary -> {heading: [lines]}.
    Acknowledged bots go under "known" only."""
    now = now or datetime.now(timezone.utc)
    ack = ACKNOWLEDGED if acknowledged is None else acknowledged
    exp = EXPECTED_BOTS if expected is None else expected
    out: Dict[str, List[str]] = defaultdict(list)

    by_bot: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_bot[r.get("bot_source") or "?"].append(r)
    for runs in by_bot.values():
        runs.sort(key=lambda r: _parse_ts(r.get("started_at")) or now, reverse=True)

    # ── runner-level ──
    if summary is None:
        out["runner_incomplete"].append("no summary file: the runner never started or was killed before its first flush")
    else:
        if not summary.get("finished_at"):
            done = len(summary.get("bots") or [])
            planned = len(summary.get("planned") or [])
            last = (summary.get("bots") or [{}])[-1].get("bot") if done else "none"
            out["runner_incomplete"].append(
                f"runner killed before finishing: {done}/{planned} bots ran, last was {last} (job timeout?)"
            )
        for name in summary.get("critical_incomplete") or []:
            st = next((b.get("status") for b in summary.get("bots") or [] if b.get("bot") == name), "not reached")
            out["critical_incomplete"].append(f"{name}: {st}")
        el = summary.get("elapsed_seconds") or 0
        bud = summary.get("budget_seconds") or 0
        if bud and el > bud:
            out["overrun"].append(f"runner took {el / 60:.1f} min against a {bud / 60:.0f} min budget")
        skipped = [b["bot"] for b in summary.get("bots") or [] if b.get("status") == "skipped_budget"]
        if skipped:
            out["skipped_budget"].append(", ".join(skipped))

    # ── per-bot ──
    day_ago = now - timedelta(hours=24)
    for bot, runs in sorted(by_bot.items()):
        latest = runs[0]
        if bot in ack:
            out["known"].append(f"{bot}: {latest.get('status')} ({ack[bot]})")
            continue

        recent = [r for r in runs if (_parse_ts(r.get("started_at")) or now) >= day_ago]
        if any(r.get("status") == "timed_out" or "BotTimeout" in (r.get("error_message") or "") for r in recent):
            out["timed_out"].append(bot)

        last_two = [r for r in runs if r.get("status") != "running"][:2]
        if len(last_two) == 2 and all(r.get("status") in FAIL_STATUSES for r in last_two):
            err = (last_two[0].get("error_message") or "").strip().splitlines()
            out["failed_twice"].append(f"{bot}: {err[0][:120] if err else last_two[0].get('status')}")

        for r in runs:
            if r.get("status") == "running":
                st = _parse_ts(r.get("started_at"))
                if st and now - st > STUCK_AFTER:
                    out["stuck_running"].append(f"{bot}: running since {st.strftime('%m-%d %H:%M')} UTC")
                    break

        if latest.get("status") == "frozen_source":
            out["frozen_source"].append(bot)

        cfg = exp.get(bot)
        if cfg:
            last_yield = next(
                (_parse_ts(r.get("started_at")) for r in runs if (r.get("staged_count") or 0) > 0), None
            )
            limit = timedelta(days=cfg.get("max_silent_days", 14))
            if last_yield is None:
                out["silent"].append(f"{bot}: nothing staged in {LOOKBACK_DAYS}d (limit {limit.days}d)")
            elif now - last_yield > limit:
                out["silent"].append(f"{bot}: last staged {(now - last_yield).days}d ago (limit {limit.days}d)")

    # Expected producers with no rows at all in the window never ran.
    for bot, cfg in exp.items():
        if bot not in by_bot and bot not in ack:
            out["silent"].append(f"{bot}: no runs in {LOOKBACK_DAYS}d")

    return dict(out)


ALERT_HEADINGS = [
    "runner_incomplete", "critical_incomplete", "overrun", "timed_out",
    "failed_twice", "stuck_running", "frozen_source", "silent",
]
INFO_HEADINGS = ["skipped_budget", "known"]


def alert_count(alerts: Dict[str, List[str]]) -> int:
    return sum(len(alerts.get(h) or []) for h in ALERT_HEADINGS)


def format_report(alerts: Dict[str, List[str]], summary: Optional[Dict[str, Any]], now: datetime) -> str:
    lines = [f"FALCO pipeline report {now.strftime('%Y-%m-%d %H:%M')} UTC", ""]
    n = alert_count(alerts)
    lines.append(f"{n} alert(s)" if n else "no alerts")
    for h in ALERT_HEADINGS:
        items = alerts.get(h) or []
        if items:
            lines.append("")
            lines.append(f"== {h} ({len(items)})")
            lines.extend(f"  - {x}" for x in items)
    for h in INFO_HEADINGS:
        items = alerts.get(h) or []
        if items:
            lines.append("")
            lines.append(f"-- {h}")
            lines.extend(f"  - {x}" for x in items)
    if summary and summary.get("bots"):
        lines.append("")
        lines.append(f"-- this run ({(summary.get('elapsed_seconds') or 0) / 60:.1f} min)")
        for b in summary["bots"]:
            lines.append(f"  {b.get('bot', '?'):30s} {b.get('status', '?'):16s} {b.get('elapsed', 0):7.1f}s  staged={b.get('staged', 0)}")
    return "\n".join(lines)


def send_email(subject: str, text: str) -> bool:
    key = (os.environ.get("RESEND_API_KEY") or "").strip()
    if not key:
        print("[alert] RESEND_API_KEY not set; not emailing")
        return False
    to = (os.environ.get("FALCO_ALERT_TO") or DEFAULT_ALERT_TO).strip()
    try:
        import requests
        res = requests.post(
            RESEND_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"from": ALERT_FROM, "to": [to], "subject": subject, "text": text},
            timeout=20,
        )
        print(f"[alert] resend -> {res.status_code} {res.text[:200]}")
        return res.ok
    except Exception as e:
        print(f"[alert] resend failed: {e}")
        return False


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print, never email")
    ap.add_argument("--summary", default=str(SUMMARY_PATH))
    args = ap.parse_args(argv)

    now = datetime.now(timezone.utc)
    summary = load_summary(Path(args.summary))
    try:
        rows = load_rows()
        alerts = build_alerts(rows, summary, now=now)
    except Exception as e:
        # Supabase itself is down or creds are wrong. That IS the alert.
        alerts = {"runner_incomplete": [f"could not read bot_run_health: {type(e).__name__}: {str(e)[:200]}"]}
        if summary is None:
            alerts["runner_incomplete"].append("and no summary file")

    report = format_report(alerts, summary, now)
    print(report)

    n = alert_count(alerts)
    if n and not args.dry_run:
        send_email(f"FALCO pipeline: {n} alert(s) {now.strftime('%Y-%m-%d')}", report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
