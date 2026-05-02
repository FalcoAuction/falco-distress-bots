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

    def __init__(self):
        self.run_id = str(uuid.uuid4())
        self.session = make_session()
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
        try:
            self.logger.info(f"START run_id={self.run_id}")
            leads = self.scrape() or []
            self.logger.info(f"scrape() returned {len(leads)} leads")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        staged_count, duplicate_count = self._write_staging(leads)
        finished_at = datetime.now(timezone.utc)

        # Decide final status
        if error_message:
            status = "failed"
        elif len(leads) == 0 or staged_count < self.expected_min_yield:
            status = "zero_yield"
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
        """Write leads to homeowner_requests_staging. Returns (staged, dupes)."""
        client = _supabase()
        if client is None:
            self.logger.warning(f"no supabase client — would have staged {len(leads)} leads")
            return (0, 0)
        if not leads:
            return (0, 0)

        staged = 0
        dupes = 0
        for lead in leads:
            row = lead.as_db_row(scraper_run_id=self.run_id)
            try:
                # Check for existing staged row with same lead_key+bot_source still pending
                # (avoid re-staging same lead from same bot in same week)
                existing = (
                    client.table("homeowner_requests_staging")
                    .select("id")
                    .eq("bot_source", lead.bot_source)
                    .eq("pipeline_lead_key", lead.pipeline_lead_key)
                    .eq("staging_status", "pending")
                    .limit(1)
                    .execute()
                )
                if getattr(existing, "data", None):
                    dupes += 1
                    continue
                client.table("homeowner_requests_staging").insert(row).execute()
                staged += 1
            except Exception as e:
                self.logger.warning(f"staging insert failed for {lead.pipeline_lead_key}: {e}")

        self.logger.info(f"staged {staged} new leads, {dupes} dupes skipped")
        return (staged, dupes)

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
