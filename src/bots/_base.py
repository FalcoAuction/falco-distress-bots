"""
Base scraper framework for FALCO lead-gen bots.

Every new scraper inherits from `BotBase`. The base class handles:

  - HTTP fetch with retry/backoff/timeout discipline
  - Standardized output contract (the LeadPayload dataclass)
  - Auto-write to homeowner_requests_staging (NOT the live table)
  - Per-run health reporting to bot_run_health (catches silent failures)
  - Polite scraping defaults (User-Agent, throttle)

The staged leads sit in homeowner_requests_staging until promoted via
`promote_staged_lead()` (single) or `promote_staged_batch()` (whole bot
source). This keeps unverified data sources out of Chris's queue.

Why staging-first: the audit caught one of our existing scrapers
silently producing zero leads for weeks because a CSS selector broke.
Staging + health reporting + zero-yield alerts prevents that.
"""

from __future__ import annotations

import os
import sys
import time
import uuid
import hashlib
import logging
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from supabase import create_client, Client
except ImportError:
    print(
        "[bot-base] ERROR: supabase-py not installed. Run: pip install supabase>=2.0.0",
        file=sys.stderr,
    )
    raise

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("[bot-base] ERROR: requests not installed. Run: pip install requests", file=sys.stderr)
    raise


class BotTimeout(Exception):
    """Raised (from a SIGALRM handler in _run_new) when a bot blows its
    per-bot wall-clock cap. Deliberately an Exception, not BaseException:
    bots that wrap their own run() in a bare `except Exception` then
    record a terminal health row instead of leaving a `running` ghost
    that nobody ever closes."""


# ─────────────────────────── Standardized output ────────────────────────────


@dataclass
class LeadPayload:
    """The standard shape every scraper produces, ONE row per discovered lead.

    `bot_source` and `pipeline_lead_key` are required. Everything else is
    optional but enrichment downstream needs at least property_address +
    distress_type to be useful.
    """

    bot_source: str                     # "hud_reo" | "fannie_homepath" | etc
    pipeline_lead_key: str              # stable per-source ID; sha40 of source URL is fine
    property_address: Optional[str] = None
    county: Optional[str] = None
    full_name: Optional[str] = None
    owner_name_records: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    property_value: Optional[float] = None
    mortgage_balance: Optional[float] = None
    trustee_sale_date: Optional[str] = None  # ISO YYYY-MM-DD
    distress_type: Optional[str] = None      # PRE_FORECLOSURE | LIS_PENDENS | TAX_LIEN | PROBATE | BANKRUPTCY | EVICTION | FSBO | REO | CODE_VIOLATION | etc
    admin_notes: Optional[str] = None        # free-form, anything we want to remember
    raw_payload: Optional[Dict[str, Any]] = None  # full raw scraper output for audit
    source_url: Optional[str] = None              # original public URL

    def as_db_row(self, scraper_run_id: str) -> Dict[str, Any]:
        d = asdict(self)
        d["scraper_run_id"] = scraper_run_id
        d["staging_status"] = "pending"

        # Normalize property_address before persisting — strips
        # CRLF/tab noise, "Property Address:" prefix junk, duplicate
        # "City, ST, City, ST" runs, and tags parcel-only addresses
        # (street #0 / 00) for human review downstream. Failure here
        # never blocks the staging write — fall back to raw value.
        if d.get("property_address"):
            try:
                from . import _address  # local import to avoid cycles
                result = _address.normalize_address(d["property_address"])
                if result.normalized:
                    d["property_address"] = result.normalized
                # Surface parcel-only flag inside admin_notes so the
                # /admin/staging review surfaces what won't AVM. Also
                # record changes for audit (only when something changed).
                tags: List[str] = []
                if result.needs_resolution:
                    tags.append("[NORMALIZER: parcel_only_address]")
                if result.changes and result.changes != ["parcel_only_address"]:
                    cleaned = [c for c in result.changes if c != "parcel_only_address"]
                    if cleaned:
                        tags.append(f"[NORMALIZER: {','.join(cleaned)}]")
                if tags:
                    existing = d.get("admin_notes") or ""
                    suffix = " ".join(tags)
                    if suffix not in existing:
                        d["admin_notes"] = (
                            existing + " " if existing else ""
                        ) + suffix
            except Exception:
                # Normalizer is best-effort; never block a scrape on it.
                pass

        # Strip Nones so default values in DB take effect
        return {k: v for k, v in d.items() if v is not None}


# ─────────────────────────── Supabase client ─────────────────────────────────


_SUPABASE_CLIENT: Optional[Client] = None


def _supabase() -> Optional[Client]:
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is not None:
        return _SUPABASE_CLIENT
    url = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        print(
            "[bot-base] WARNING: Missing SUPABASE creds. Health + staging writes will be no-ops.",
            file=sys.stderr,
        )
        return None
    _SUPABASE_CLIENT = create_client(url, key)
    return _SUPABASE_CLIENT


# Keys per Supabase round trip in _write_staging. PostgREST `in` filters
# go in the URL; 200 sha40 keys is ~9KB, well under the 16KB header cap.
CHUNK = 200


# ─────────────────────────── HTTP session helper ────────────────────────────


def make_session(user_agent: str = "FALCO-Lead-Research/1.0 (+ops@falco.llc)") -> requests.Session:
    """Build a requests Session with retry/backoff baked in.

    Retries on 5xx + 429, exponential backoff. 30s default timeout via
    a wrapper. Identifies as FALCO so site owners can contact us if
    they have concerns.
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2.0,           # 2s, 4s, 8s between retries
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


# ─────────────────────────────── BotBase ────────────────────────────────────


class BotBase:
    """Parent class for every FALCO scraper.

    Subclass and implement `scrape()` to return a list of LeadPayload.
    Then call `run()` from `run_all.py` (or your test harness). The base
    class handles staging writes + health reporting automatically.

    Usage:

      class HudReoBot(BotBase):
          name = "hud_reo"
          throttle_seconds = 1.0

          def scrape(self) -> list[LeadPayload]:
              # ... your scraping logic ...
              return [LeadPayload(bot_source=self.name, pipeline_lead_key=..., ...)]

      bot = HudReoBot()
      bot.run()
    """

    # Subclass overrides
    name: str = "unnamed_bot"
    throttle_seconds: float = 1.0           # seconds between requests to same host
    expected_min_yield: int = 1             # zero-yield warning threshold
    description: str = ""                   # human-readable

    # Some sources sit behind a WAF that rejects non-browser agents from
    # datacenter IPs (which is what CI runs on). A bot that needs to look
    # like a browser sets this; everything else keeps the honest default.
    user_agent: Optional[str] = None

    # A source whose lead-key set is identical for this many consecutive
    # finished runs reports `frozen_source` instead of `all_dupes`. Two
    # runs a day, so 6 = three days of byte-identical output. Sources
    # that genuinely move slowly (monthly lists) should raise this.
    frozen_after_runs: int = 6

    def __init__(self):
        self.run_id = str(uuid.uuid4())
        self.session = (
            make_session(self.user_agent) if self.user_agent else make_session()
        )
        self.logger = logging.getLogger(f"bot.{self.name}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(f"[%(asctime)s] [{self.name}] %(message)s"))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        self._last_request_at: Dict[str, float] = {}

    # ── Subclass implements this ────────────────────────────────────────────

    def scrape(self) -> List[LeadPayload]:
        """Return all leads discovered in this run. Don't worry about
        dedup or storage — the framework handles those."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement scrape()")

    # ── Helpers subclasses can use ──────────────────────────────────────────

    def fetch(
        self,
        url: str,
        method: str = "GET",
        timeout: float = 30.0,
        **kwargs,
    ) -> Optional[requests.Response]:
        """Polite fetch — throttles per-host, follows retry policy on
        the session, returns None on permanent failure (logged)."""
        from urllib.parse import urlparse
        host = urlparse(url).netloc
        last = self._last_request_at.get(host, 0)
        elapsed = time.time() - last
        if elapsed < self.throttle_seconds:
            time.sleep(self.throttle_seconds - elapsed)
        self._last_request_at[host] = time.time()

        try:
            res = self.session.request(method, url, timeout=timeout, **kwargs)
            if res.status_code >= 500:
                self.logger.warning(f"fetch {method} {url} -> {res.status_code}")
                return None
            return res
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"fetch {method} {url} failed: {e}")
            return None

    @staticmethod
    def make_lead_key(source: str, identifier: str) -> str:
        """Stable sha40 lead key from a source + identifier (URL, ID, etc)."""
        h = hashlib.sha1(f"{source}|{identifier}".encode("utf-8")).hexdigest()
        return h  # 40 chars hex

    # ── Run loop ────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """Run the scraper end-to-end: scrape → write to staging → report health.

        Returns a summary dict. Never raises — all errors are caught and
        logged to bot_run_health so a single bot crash doesn't kill the
        whole pipeline.
        """
        started_at = datetime.now(timezone.utc)
        self._report_health(
            status="running",
            started_at=started_at,
            finished_at=None,
            fetched_count=0,
            parsed_count=0,
            staged_count=0,
            duplicate_count=0,
        )

        leads: List[LeadPayload] = []
        error_message: Optional[str] = None
        timed_out = False
        try:
            self.logger.info(f"START run_id={self.run_id}")
            leads = self.scrape() or []
            self.logger.info(f"scrape() returned {len(leads)} leads")
        except BotTimeout as e:
            timed_out = True
            error_message = f"BotTimeout: {e}"
            self.logger.error(f"TIMED OUT: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        staged_count, duplicate_count = self._write_staging(leads)
        finished_at = datetime.now(timezone.utc)

        # Decide final status — distinguish three cases:
        #   "failed"     — scraper crashed, error_message captured
        #   "zero_yield" — scraper RAN but produced no leads at all (likely broken,
        #                  needs investigation; alerting threshold)
        #   "all_dupes"  — scraper found leads but all already-staged (healthy
        #                  but no new supply this run; not an alert)
        #   "ok"         — scraper produced new leads above expected_min_yield
        #   "frozen_source" — all_dupes AND the lead-key set has been
        #                  byte-identical for frozen_after_runs runs. A stale
        #                  page and a quiet week look the same as all_dupes;
        #                  this is the one that needs a human.
        #   "timed_out"  — killed by the runner's per-bot wall-clock cap
        fp = self.fingerprint(leads)
        if timed_out:
            status = "timed_out"
        elif error_message:
            status = "failed"
        elif len(leads) == 0:
            status = "zero_yield"
        elif staged_count == 0 and duplicate_count > 0:
            status = "frozen_source" if self._source_frozen(fp) else "all_dupes"
        elif staged_count < self.expected_min_yield:
            status = "below_threshold"
        else:
            status = "ok"

        self._report_health(
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            fetched_count=len(leads),
            parsed_count=len(leads),
            staged_count=staged_count,
            duplicate_count=duplicate_count,
            error_message=error_message,
            notes={"fingerprint": fp, "key_count": len(leads)} if fp else None,
        )

        return {
            "name": self.name,
            "run_id": self.run_id,
            "status": status,
            "fetched": len(leads),
            "staged": staged_count,
            "duplicates": duplicate_count,
            "error": error_message,
        }

    # ── Internal: staging writes ────────────────────────────────────────────

    def _write_staging(self, leads: List[LeadPayload]) -> tuple[int, int]:
        """Write leads to homeowner_requests_staging. Returns (staged, dupes).

        Also TOUCHES phone_metadata.notice_tracking.last_seen_at on both
        the staging row (if it exists as a dupe) AND the live
        homeowner_requests row (if it exists). This gives the trustee-
        status reaper a reliable "is this notice still being
        republished?" signal — when a sale gets withdrawn, the notice
        stops re-appearing in our scrapes and last_seen_at goes stale.

        Lookups and inserts are chunked (one round trip per CHUNK keys)
        rather than one select per lead. The per-lead version cost a
        2,000-lead source ~8 minutes of the pipeline's 90 on nothing but
        Supabase latency. Touches stay per-row: each one merges a
        different jsonb blob.
        """
        client = _supabase()
        if client is None:
            self.logger.warning(f"no supabase client — would have staged {len(leads)} leads")
            return (0, 0)
        if not leads:
            return (0, 0)

        staged = 0
        dupes = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        def _touch_last_seen(table: str, row_id: str, existing_pm: Any) -> None:
            """Update phone_metadata.notice_tracking on a single row."""
            try:
                pm = existing_pm if isinstance(existing_pm, dict) else {}
                tracking = pm.get("notice_tracking") if isinstance(pm.get("notice_tracking"), dict) else {}
                tracking["last_seen_at"] = now_iso
                tracking["last_seen_by"] = self.name
                pm["notice_tracking"] = tracking
                client.table(table).update({"phone_metadata": pm}).eq("id", row_id).execute()
            except Exception as e:
                self.logger.warning(f"notice_tracking touch failed for {table}/{row_id}: {e}")

        # A source can list the same notice twice in one run (republished
        # in two sections). Keep the first; the old per-lead path would
        # have inserted the first and counted the second as a dupe.
        unique: List[LeadPayload] = []
        seen: set = set()
        for lead in leads:
            k = (lead.bot_source, lead.pipeline_lead_key)
            if k in seen:
                dupes += 1
                continue
            seen.add(k)
            unique.append(lead)

        by_source: Dict[str, List[LeadPayload]] = {}
        for lead in unique:
            by_source.setdefault(lead.bot_source, []).append(lead)

        for source, group in by_source.items():
            for i in range(0, len(group), CHUNK):
                chunk = group[i:i + CHUNK]
                keys = [l.pipeline_lead_key for l in chunk]

                # Which of these are already pending in staging?
                try:
                    res = (
                        client.table("homeowner_requests_staging")
                        .select("id, pipeline_lead_key, phone_metadata")
                        .eq("bot_source", source)
                        .in_("pipeline_lead_key", keys)
                        .eq("staging_status", "pending")
                        .execute()
                    )
                    pending = {r["pipeline_lead_key"]: r for r in (getattr(res, "data", None) or [])}
                except Exception as e:
                    # Can't tell what's new. Skip the chunk rather than
                    # blindly inserting 200 possible duplicates; the next
                    # run re-scrapes the same notices anyway.
                    self.logger.warning(f"staging dedupe lookup failed, skipping {len(chunk)} leads: {e}")
                    continue

                new_rows = []
                for lead in chunk:
                    hit = pending.get(lead.pipeline_lead_key)
                    if hit:
                        _touch_last_seen("homeowner_requests_staging", hit["id"], hit.get("phone_metadata"))
                        dupes += 1
                    else:
                        new_rows.append(lead.as_db_row(scraper_run_id=self.run_id))

                if new_rows:
                    try:
                        client.table("homeowner_requests_staging").insert(new_rows).execute()
                        staged += len(new_rows)
                    except Exception as e:
                        # One bad row fails the whole batch insert. Fall
                        # back to per-row so the other 199 still land.
                        self.logger.warning(f"batch insert of {len(new_rows)} failed ({e}); retrying per-row")
                        for row in new_rows:
                            try:
                                client.table("homeowner_requests_staging").insert(row).execute()
                                staged += 1
                            except Exception as e2:
                                self.logger.warning(f"staging insert failed for {row.get('pipeline_lead_key')}: {e2}")

                # Touch live homeowner_requests rows for any lead already
                # promoted, so the reaper sees today's sighting.
                try:
                    live = (
                        client.table("homeowner_requests")
                        .select("id, pipeline_lead_key, phone_metadata")
                        .eq("source", "bot")
                        .in_("pipeline_lead_key", keys)
                        .execute()
                    )
                    for r in (getattr(live, "data", None) or []):
                        _touch_last_seen("homeowner_requests", r["id"], r.get("phone_metadata"))
                except Exception as e:
                    self.logger.warning(f"live touch lookup failed for chunk: {e}")

        self.logger.info(f"staged {staged} new leads, {dupes} dupes skipped (last_seen_at touched on all)")
        return (staged, dupes)

    @staticmethod
    def fingerprint(leads: List[LeadPayload]) -> Optional[str]:
        """sha1 of the sorted lead-key set. Two runs with the same
        fingerprint saw byte-identical supply. Used to tell a frozen
        source (stale cache, dead page, WAF interstitial parsed as
        empty) from a quiet week."""
        if not leads:
            return None
        keys = sorted({f"{l.bot_source}|{l.pipeline_lead_key}" for l in leads})
        return hashlib.sha1("\n".join(keys).encode("utf-8")).hexdigest()

    def _source_frozen(self, fp: Optional[str]) -> bool:
        """True when the last `frozen_after_runs` finished runs of this
        bot all carry the same fingerprint as this one."""
        if not fp or self.frozen_after_runs <= 0:
            return False
        client = _supabase()
        if client is None:
            return False
        try:
            res = (
                client.table("bot_run_health")
                .select("notes")
                .eq("bot_source", self.name)
                .neq("run_id", self.run_id)
                .not_.is_("finished_at", "null")
                .order("started_at", desc=True)
                .limit(self.frozen_after_runs)
                .execute()
            )
        except Exception as e:
            self.logger.warning(f"frozen-source lookup failed: {e}")
            return False
        rows = getattr(res, "data", None) or []
        if len(rows) < self.frozen_after_runs:
            return False
        return all(((r.get("notes") or {}).get("fingerprint") == fp) for r in rows)

    # ── Internal: health reporting ──────────────────────────────────────────

    def _report_health(
        self,
        status: str,
        started_at: datetime,
        finished_at: Optional[datetime],
        fetched_count: int,
        parsed_count: int,
        staged_count: int,
        duplicate_count: int,
        error_message: Optional[str] = None,
        notes: Optional[Dict[str, Any]] = None,
    ) -> None:
        client = _supabase()
        if client is None:
            return
        row: Dict[str, Any] = {
            "bot_source": self.name,
            "run_id": self.run_id,
            "started_at": started_at.isoformat(),
            "status": status,
            "fetched_count": fetched_count,
            "parsed_count": parsed_count,
            "staged_count": staged_count,
            "duplicate_count": duplicate_count,
        }
        if finished_at is not None:
            row["finished_at"] = finished_at.isoformat()
        if error_message is not None:
            row["error_message"] = error_message[:5000]
        if notes is not None:
            row["notes"] = notes
        try:
            # Upsert by run_id so the "running" row gets updated to final status
            existing = (
                client.table("bot_run_health")
                .select("id")
                .eq("run_id", self.run_id)
                .limit(1)
                .execute()
            )
            if getattr(existing, "data", None):
                client.table("bot_run_health").update(row).eq("run_id", self.run_id).execute()
            else:
                client.table("bot_run_health").insert(row).execute()
        except Exception as e:
            # Don't let health-report failure crash the bot
            self.logger.warning(f"health report failed: {e}")


def close_open_runs(bot_source: str, status: str, error_message: str,
                    since: datetime) -> int:
    """Close any `running` bot_run_health rows for `bot_source` started at
    or after `since`. The runner calls this after it kills a bot: the bot
    never got to write its own terminal row, and a `running` row that is
    never closed is exactly the ghost the stuck-run alert looks for."""
    client = _supabase()
    if client is None:
        return 0
    try:
        res = (
            client.table("bot_run_health")
            .select("id")
            .eq("bot_source", bot_source)
            .eq("status", "running")
            .gte("started_at", since.isoformat())
            .execute()
        )
        ids = [r["id"] for r in (getattr(res, "data", None) or [])]
        for rid in ids:
            client.table("bot_run_health").update({
                "status": status,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "error_message": error_message[:5000],
            }).eq("id", rid).execute()
        return len(ids)
    except Exception as e:
        print(f"[bot-base] close_open_runs({bot_source}) failed: {e}", file=sys.stderr)
        return 0
