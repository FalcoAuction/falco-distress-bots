"""Twilio Lookup pass on every Middle TN lead with a phone.

Twilio Lookup v2 (with line_type_intelligence) returns:
  - validation pass/fail
  - line_type: 'mobile', 'landline', 'fixedVoip', 'nonFixedVoip',
                'fixed_line_or_mobile', 'tollFree', 'voicemail', etc.
  - carrier name + MCC/MNC

Cost: ~$0.005 per lookup. ~422 phones = ~$2.10.

Stores result in `phone_metadata.twilio_lookup` (separate key from the
existing offline `phone_classifier_bot` output, which lives at the top
level of phone_metadata as `valid` + `carrier` + `line_type`).

Eligibility:
  - county in Middle TN focus set
  - lead has a phone
  - phone_metadata.twilio_lookup is missing OR older than 30 days

Run via:
  python -m src.bots.middle_tn_twilio_lookup_bot

Env:
  TWILIO_ACCOUNT_SID
  TWILIO_AUTH_TOKEN
  FALCO_MAX_TWILIO_LOOKUP_PER_RUN  (default 500)
  FALCO_TWILIO_LOOKUP_SAMPLE       (=1 for dry-run)
"""
from __future__ import annotations

import os
import re
import sys
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
# Align with auto_promoter_bot.STRETCH_COUNTIES so every promoted lead
# gets Twilio-validated. Previously cheatham/robertson/dickson were
# being promoted to live but skipped by Twilio validation — leaving
# the dialer with unvalidated phones in those counties.
STRETCH_COUNTIES = {"maury", "montgomery", "cheatham", "robertson", "dickson"}
FOCUS_COUNTIES = CORE_COUNTIES | STRETCH_COUNTIES

DEFAULT_MAX_PER_RUN = 500
LOOKUP_TTL_DAYS = 30


def _normalize_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


def _normalize_phone(raw: Optional[str]) -> Optional[str]:
    """Return E.164 (+1XXXXXXXXXX) for a US phone, or None."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def _is_stale(lookup_meta: Dict[str, Any]) -> bool:
    ts = lookup_meta.get("checked_at")
    if not ts:
        return True
    try:
        # Supports ISO with or without offset
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return True
    return (datetime.now(timezone.utc) - dt) > timedelta(days=LOOKUP_TTL_DAYS)


class MiddleTnTwilioLookupBot(BotBase):
    name = "middle_tn_twilio_lookup"
    description = (
        "Twilio Lookup v2 (line_type_intelligence) on every Middle TN "
        "lead with a phone — produces validated carrier+line_type."
    )
    throttle_seconds = 0.0  # Twilio's rate limit is 100 req/s; we'll
                             # naturally throttle below that
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
        if not sid or not token:
            return self._fail(
                started, "TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN not set"
            )

        client = _supabase()
        if client is None:
            return self._fail(started, "no_supabase_client")

        twilio = TwilioClient(sid, token)
        max_per_run = int(
            os.environ.get(
                "FALCO_MAX_TWILIO_LOOKUP_PER_RUN", DEFAULT_MAX_PER_RUN,
            )
        )
        sample = os.environ.get("FALCO_TWILIO_LOOKUP_SAMPLE") == "1"

        candidates = self._candidates(client, max_per_run)
        self.logger.info(f"{len(candidates)} Middle TN leads with phone "
                          f"to Twilio Lookup")

        attempted = 0
        validated = 0
        skipped_existing = 0
        errors = 0

        try:
            for lead in candidates:
                if attempted >= max_per_run:
                    break
                phone_e164 = _normalize_phone(lead.get("phone"))
                if not phone_e164:
                    continue

                pm = lead.get("phone_metadata") or {}
                if not isinstance(pm, dict):
                    pm = {}
                existing = pm.get("twilio_lookup") or {}
                if isinstance(existing, dict) and not _is_stale(existing):
                    skipped_existing += 1
                    continue

                attempted += 1
                try:
                    result = twilio.lookups.v2.phone_numbers(phone_e164).fetch(
                        fields="line_type_intelligence"
                    )
                except TwilioRestException as e:
                    self.logger.warning(
                        f"  Twilio Lookup failed id={lead.get('id')} "
                        f"phone={phone_e164}: {e.code} {e.msg}"
                    )
                    errors += 1
                    # Cache the failure so we don't retry next run
                    self._write_lookup_failure(
                        client, lead, phone_e164, str(e.code), str(e.msg)
                    )
                    continue
                except Exception as e:
                    self.logger.warning(
                        f"  Twilio Lookup error id={lead.get('id')}: {e}"
                    )
                    errors += 1
                    continue

                payload = self._extract(result)
                if sample:
                    self.logger.info(
                        f"  SAMPLE id={lead.get('id')} phone={phone_e164} "
                        f"valid={payload['valid']} line={payload['line_type']} "
                        f"carrier={payload['carrier_name']!r}"
                    )
                    validated += 1
                    continue

                self._write_lookup(client, lead, phone_e164, payload)
                validated += 1

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
            return self._wrap(
                started, attempted, validated, skipped_existing, errors,
                status="failed", error=error_message,
            )

        self.logger.info(
            f"attempted={attempted} validated={validated} "
            f"skipped_recent={skipped_existing} errors={errors}"
        )
        return self._wrap(started, attempted, validated, skipped_existing, errors)

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
        # Cap at 3x to give the run loop room to skip already-fresh entries
        return out[: max_per_run * 3]

    # ── lookup result extraction ──────────────────────────────────────────
    @staticmethod
    def _extract(result) -> Dict[str, Any]:
        """Pull the fields we care about from a Twilio Lookup v2 response."""
        lti = getattr(result, "line_type_intelligence", None) or {}
        return {
            "valid": bool(getattr(result, "valid", True)),
            "country_code": getattr(result, "country_code", None),
            "phone_number": getattr(result, "phone_number", None),
            "national_format": getattr(result, "national_format", None),
            "line_type": lti.get("type") if isinstance(lti, dict) else None,
            "carrier_name": (
                lti.get("carrier_name") if isinstance(lti, dict) else None
            ),
            "mobile_country_code": (
                lti.get("mobile_country_code") if isinstance(lti, dict) else None
            ),
            "mobile_network_code": (
                lti.get("mobile_network_code") if isinstance(lti, dict) else None
            ),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── DB write ──────────────────────────────────────────────────────────
    def _write_lookup(
        self, client, lead: Dict[str, Any], phone_e164: str,
        payload: Dict[str, Any],
    ) -> None:
        table = lead["__table__"]
        pm = lead.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}
        pm["twilio_lookup"] = {
            "phone": phone_e164,
            **payload,
        }
        try:
            client.table(table).update(
                {"phone_metadata": pm}
            ).eq("id", lead["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  update failed id={lead['id']}: {e}")

    def _write_lookup_failure(
        self, client, lead: Dict[str, Any], phone_e164: str,
        code: str, msg: str,
    ) -> None:
        table = lead["__table__"]
        pm = lead.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}
        pm["twilio_lookup"] = {
            "phone": phone_e164,
            "valid": False,
            "error_code": code,
            "error_message": msg,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
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
            "validated": 0, "staged": 0, "duplicates": 0, "fetched": 0,
        }

    def _wrap(
        self, started, attempted, validated, skipped_existing, errors,
        status: str = "ok", error: Optional[str] = None,
    ) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=attempted, parsed_count=validated + errors,
            staged_count=validated, duplicate_count=skipped_existing,
            error_message=error,
        )
        return {
            "name": self.name, "status": status,
            "attempted": attempted, "validated": validated,
            "skipped_recent": skipped_existing, "errors": errors,
            "error": error,
            "staged": validated, "duplicates": skipped_existing,
            "fetched": attempted,
        }


def run() -> dict:
    bot = MiddleTnTwilioLookupBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
