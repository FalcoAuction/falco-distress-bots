"""Twilio dial probe — places a brief outbound call with Answering Machine
Detection, captures whether the line rings to a human/voicemail/dead.

Goal: prove which Middle TN phones are actually live before the dialer
team starts cold-calling. Each call dials the lead, listens for AMD up
to ~5s, then immediately hangs up via inline TwiML.

What we capture per phone:
  - status: 'completed', 'no-answer', 'busy', 'failed', 'canceled'
  - answered_by: 'human', 'machine_start', 'machine_end_beep',
                  'machine_end_silence', 'machine_end_other', 'fax',
                  'unknown', None
  - duration: seconds (always tiny since we hang up)

Eligibility (the survivor funnel after Twilio Lookup):
  - county in Middle TN focus set
  - has phone_metadata.twilio_lookup.valid == True
  - line_type in {'mobile', 'landline', 'fixed_line_or_mobile'}
    (skip pure VoIP — they're often forwarders/spam-routers and probing
    them gives unreliable signal AND wastes call cost)
  - phone NOT on BatchData DNC (skip federally-flagged numbers)
  - dial_probe missing OR older than 60 days

TCPA / state law: this is an outbound call to a residential number.
Tennessee follows TCPA (8am–9pm local). The workflow's cron is set to
fire 10am CST so all calls fall well within the legal window.

Run via:
  python -m src.bots.middle_tn_dial_probe_bot

Env:
  TWILIO_ACCOUNT_SID
  TWILIO_AUTH_TOKEN
  TWILIO_FROM_NUMBER (E.164, your purchased TN number)
  FALCO_MAX_DIAL_PROBES_PER_RUN  (default 200)
  FALCO_DIAL_PROBE_SAMPLE        (=1 to plan calls without placing them)
"""
from __future__ import annotations

import os
import re
import sys
import time
import traceback as tb
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase

try:
    from twilio.rest import Client as TwilioClient
    from twilio.base.exceptions import TwilioRestException
except ImportError:
    TwilioClient = None  # type: ignore[assignment, misc]
    TwilioRestException = Exception  # type: ignore[assignment, misc]


CORE_COUNTIES = {"davidson", "williamson", "sumner", "rutherford", "wilson"}
STRETCH_COUNTIES = {"maury", "montgomery"}
FOCUS_COUNTIES = CORE_COUNTIES | STRETCH_COUNTIES

DEFAULT_MAX_PER_RUN = 200
PROBE_TTL_DAYS = 60
ALLOWED_LINE_TYPES = {"mobile", "landline", "fixed_line_or_mobile"}
HANGUP_TWIML = "<Response><Hangup/></Response>"

# AMD parameters tuned for short probe (we don't need to leave a message)
AMD_PARAMS = {
    "machine_detection": "Enable",       # detect human vs machine
    "async_amd": False,                   # block call until AMD result
    "machine_detection_timeout": 8,        # seconds — keep short
    "machine_detection_speech_threshold": 2400,
    "machine_detection_speech_end_threshold": 1200,
    "machine_detection_silence_timeout": 3000,
}


def _normalize_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


def _normalize_phone(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def _is_stale(probe_meta: Dict[str, Any]) -> bool:
    ts = probe_meta.get("checked_at")
    if not ts:
        return True
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return True
    return (datetime.now(timezone.utc) - dt) > timedelta(days=PROBE_TTL_DAYS)


def _is_dnc(lead: Dict[str, Any]) -> bool:
    """Honor BatchData's DNC flag for the primary phone."""
    pm = lead.get("phone_metadata") or {}
    if not isinstance(pm, dict):
        return False
    bd = pm.get("batchdata_skip_trace") or {}
    if isinstance(bd, dict) and bd.get("primary_dnc") is True:
        return True
    # Also check all_phones for the matching number's DNC flag
    aps = bd.get("all_phones") or [] if isinstance(bd, dict) else []
    primary_digits = re.sub(r"\D", "", str(lead.get("phone") or ""))
    for ap in aps:
        if not isinstance(ap, dict):
            continue
        ap_digits = re.sub(r"\D", "", str(ap.get("phone") or ""))
        if ap_digits and ap_digits == primary_digits and ap.get("dnc") is True:
            return True
    return False


class MiddleTnDialProbeBot(BotBase):
    name = "middle_tn_dial_probe"
    description = (
        "Twilio outbound dial probe with AMD on Middle TN leads whose "
        "phone passed Twilio Lookup validation."
    )
    throttle_seconds = 1.0  # ~1 call per second to stay polite + below
                             # Twilio's 1 call/sec default for new accts
    expected_min_yield = 0
    max_leads_per_run = DEFAULT_MAX_PER_RUN

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        if TwilioClient is None:
            return self._fail(started, "twilio package not installed")

        sid = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
        token = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
        from_number = os.environ.get("TWILIO_FROM_NUMBER", "").strip()
        if not sid or not token or not from_number:
            return self._fail(
                started,
                "TWILIO_ACCOUNT_SID/AUTH_TOKEN/FROM_NUMBER not set",
            )

        client = _supabase()
        if client is None:
            return self._fail(started, "no_supabase_client")

        twilio = TwilioClient(sid, token)
        max_per_run = int(
            os.environ.get(
                "FALCO_MAX_DIAL_PROBES_PER_RUN", DEFAULT_MAX_PER_RUN,
            )
        )
        sample = os.environ.get("FALCO_DIAL_PROBE_SAMPLE") == "1"

        candidates = self._candidates(client, max_per_run)
        self.logger.info(
            f"{len(candidates)} Middle TN leads eligible for dial probe"
        )

        attempted = 0
        completed = 0
        humans = 0
        machines = 0
        no_answer = 0
        busy_or_failed = 0
        errors = 0

        try:
            for lead in candidates:
                if attempted >= max_per_run:
                    break
                phone_e164 = _normalize_phone(lead.get("phone"))
                if not phone_e164:
                    continue

                if sample:
                    self.logger.info(
                        f"  SAMPLE would probe id={lead.get('id')} "
                        f"phone={phone_e164} county={lead.get('county')!r}"
                    )
                    attempted += 1
                    continue

                attempted += 1
                try:
                    call = twilio.calls.create(
                        to=phone_e164,
                        from_=from_number,
                        twiml=HANGUP_TWIML,
                        **AMD_PARAMS,
                    )
                except TwilioRestException as e:
                    self.logger.warning(
                        f"  call create failed id={lead.get('id')} "
                        f"phone={phone_e164}: {e.code} {e.msg}"
                    )
                    errors += 1
                    self._write_probe(client, lead, phone_e164, {
                        "status": "create_failed",
                        "error_code": e.code,
                        "error_message": e.msg,
                    })
                    continue
                except Exception as e:
                    self.logger.warning(
                        f"  call create error id={lead.get('id')}: {e}"
                    )
                    errors += 1
                    continue

                # Poll for completion (calls usually finish within 15s
                # because of AMD timeout + immediate hangup TwiML)
                final = self._poll_call(twilio, call.sid, max_seconds=30)
                payload = self._summarize(call.sid, final)

                # Tally
                status = payload.get("status")
                ab = payload.get("answered_by")
                if status == "completed":
                    completed += 1
                    if ab == "human":
                        humans += 1
                    elif ab and ab.startswith("machine"):
                        machines += 1
                elif status == "no-answer":
                    no_answer += 1
                elif status in ("busy", "failed", "canceled"):
                    busy_or_failed += 1

                self._write_probe(client, lead, phone_e164, payload)
                self.logger.info(
                    f"  id={lead.get('id')} {phone_e164} "
                    f"-> status={status} answered_by={ab} "
                    f"duration={payload.get('duration')}s"
                )
                # Throttle between calls
                time.sleep(self.throttle_seconds)

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
            return self._wrap(
                started, attempted, completed, humans, machines,
                no_answer, busy_or_failed, errors,
                status="failed", error=error_message,
            )

        self.logger.info(
            f"attempted={attempted} completed={completed} "
            f"humans={humans} machines={machines} "
            f"no_answer={no_answer} busy_failed={busy_or_failed} "
            f"errors={errors}"
        )
        return self._wrap(
            started, attempted, completed, humans, machines,
            no_answer, busy_or_failed, errors,
        )

    # ── candidates ────────────────────────────────────────────────────────
    def _candidates(self, client, max_per_run: int) -> List[Dict[str, Any]]:
        out = []
        PAGE = 1000
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            page_idx = 0
            while True:
                try:
                    r = (
                        client.table(table)
                        .select(
                            "id, county, phone, phone_metadata, "
                            "priority_score, owner_name_records, full_name"
                        )
                        .not_.is_("phone", "null")
                        .order("priority_score", desc=True)
                        .range(page_idx * PAGE, (page_idx + 1) * PAGE - 1)
                        .execute()
                    )
                    rows = getattr(r, "data", None) or []
                    if not rows:
                        break
                    for row in rows:
                        if _normalize_county(row.get("county")) not in FOCUS_COUNTIES:
                            continue
                        if not _normalize_phone(row.get("phone")):
                            continue
                        if _is_dnc(row):
                            continue
                        pm = row.get("phone_metadata") or {}
                        if not isinstance(pm, dict):
                            continue
                        # Require a successful Twilio Lookup with an
                        # eligible line type
                        tl = pm.get("twilio_lookup") or {}
                        if not isinstance(tl, dict):
                            continue
                        if not tl.get("valid"):
                            continue
                        if tl.get("line_type") not in ALLOWED_LINE_TYPES:
                            continue
                        # Skip if probe is fresh
                        existing = pm.get("dial_probe") or {}
                        if isinstance(existing, dict) and not _is_stale(existing):
                            continue
                        row["__table__"] = table
                        out.append(row)
                    if len(rows) < PAGE:
                        break
                    page_idx += 1
                except Exception as e:
                    self.logger.warning(
                        f"candidate query on {table}: {e}"
                    )
                    break
        return out[:max_per_run]

    # ── poll + summarize ──────────────────────────────────────────────────
    @staticmethod
    def _poll_call(twilio, sid: str, max_seconds: int = 30):
        """Poll the call resource until it reaches a terminal state."""
        terminal = {"completed", "no-answer", "busy", "failed", "canceled"}
        deadline = time.time() + max_seconds
        last = None
        while time.time() < deadline:
            try:
                last = twilio.calls(sid).fetch()
            except Exception:
                time.sleep(1)
                continue
            if last.status in terminal:
                return last
            time.sleep(1)
        return last

    @staticmethod
    def _summarize(sid: str, call) -> Dict[str, Any]:
        if call is None:
            return {
                "sid": sid, "status": "unknown",
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
        return {
            "sid": sid,
            "status": getattr(call, "status", None),
            "answered_by": getattr(call, "answered_by", None),
            "duration": getattr(call, "duration", None),
            "start_time": str(getattr(call, "start_time", "") or ""),
            "end_time": str(getattr(call, "end_time", "") or ""),
            "from": getattr(call, "from_", None),
            "to": getattr(call, "to", None),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── DB write ──────────────────────────────────────────────────────────
    def _write_probe(
        self, client, lead: Dict[str, Any], phone_e164: str,
        payload: Dict[str, Any],
    ) -> None:
        table = lead["__table__"]
        pm = lead.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}
        pm["dial_probe"] = {"phone": phone_e164, **payload}
        try:
            client.table(table).update(
                {"phone_metadata": pm}
            ).eq("id", lead["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  update failed id={lead['id']}: {e}")

    # ── status helpers ────────────────────────────────────────────────────
    def _fail(self, started, msg: str) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status="failed", started_at=started, finished_at=finished,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
            error_message=msg,
        )
        return {
            "name": self.name, "status": "failed", "error": msg,
            "completed": 0, "staged": 0, "duplicates": 0, "fetched": 0,
        }

    def _wrap(
        self, started, attempted, completed, humans, machines,
        no_answer, busy_or_failed, errors,
        status: str = "ok", error: Optional[str] = None,
    ) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=attempted, parsed_count=attempted,
            staged_count=completed, duplicate_count=0,
            error_message=error,
        )
        return {
            "name": self.name, "status": status,
            "attempted": attempted, "completed": completed,
            "humans": humans, "machines": machines,
            "no_answer": no_answer, "busy_or_failed": busy_or_failed,
            "errors": errors,
            "error": error,
            "staged": completed, "duplicates": 0, "fetched": attempted,
        }


def run() -> dict:
    bot = MiddleTnDialProbeBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
