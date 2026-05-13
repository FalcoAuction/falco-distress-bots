"""Reusable Twilio Lookup v2 helper.

Extracted from middle_tn_twilio_lookup_bot so other places (notably
auto_promoter, when promoting a lead to live) can synchronously
validate a phone before the lead goes into the dialer.

Pattern:

    from ._twilio_lookup import (
        get_twilio_client,
        normalize_phone_e164,
        is_lookup_stale,
        lookup_phone,
    )

    twilio = get_twilio_client()  # None if env not set
    e164 = normalize_phone_e164(lead["phone"])
    if twilio and e164:
        existing = (lead.get("phone_metadata") or {}).get("twilio_lookup") or {}
        if is_lookup_stale(existing):
            try:
                payload = lookup_phone(twilio, e164)
                # payload includes line_type, carrier_name, valid, checked_at
            except Exception:
                pass  # graceful degradation

The payload shape exactly matches what middle_tn_twilio_lookup_bot
writes so downstream consumers see a single contract.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

try:
    from twilio.rest import Client as TwilioClient
    from twilio.base.exceptions import TwilioRestException
except ImportError:
    TwilioClient = None  # type: ignore[assignment, misc]
    TwilioRestException = Exception  # type: ignore[assignment, misc]


logger = logging.getLogger("bot.twilio_lookup")

LOOKUP_TTL_DAYS = 30


def get_twilio_client() -> Optional[Any]:
    """Build a Twilio REST client from env or return None.

    Returns None when:
      - twilio package not installed
      - TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN env not set
    Callers should treat None as "skip Twilio gracefully" rather than
    error out.
    """
    if TwilioClient is None:
        return None
    sid = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
    token = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
    if not sid or not token:
        return None
    return TwilioClient(sid, token)


def normalize_phone_e164(raw: Optional[str]) -> Optional[str]:
    """US phone → E.164 (+1XXXXXXXXXX), or None if unparseable."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def is_lookup_stale(lookup_meta: Any) -> bool:
    """Return True if the cached twilio_lookup is missing / older than TTL."""
    if not isinstance(lookup_meta, dict):
        return True
    ts = lookup_meta.get("checked_at")
    if not ts:
        return True
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return True
    return (datetime.now(timezone.utc) - dt) > timedelta(days=LOOKUP_TTL_DAYS)


def lookup_phone(twilio_client: Any, phone_e164: str) -> Dict[str, Any]:
    """Synchronously fetch line_type_intelligence for one phone.

    Raises on Twilio error so callers can decide whether to record the
    failure shape (same dict structure as success but valid=False).
    Use `lookup_phone_safe` if you don't want exceptions.
    """
    result = twilio_client.lookups.v2.phone_numbers(phone_e164).fetch(
        fields="line_type_intelligence"
    )
    lti = getattr(result, "line_type_intelligence", None) or {}
    return {
        "phone": phone_e164,
        "valid": bool(getattr(result, "valid", True)),
        "country_code": getattr(result, "country_code", None),
        "phone_number": getattr(result, "phone_number", None),
        "national_format": getattr(result, "national_format", None),
        "line_type": lti.get("type") if isinstance(lti, dict) else None,
        "carrier_name": lti.get("carrier_name") if isinstance(lti, dict) else None,
        "mobile_country_code": (
            lti.get("mobile_country_code") if isinstance(lti, dict) else None
        ),
        "mobile_network_code": (
            lti.get("mobile_network_code") if isinstance(lti, dict) else None
        ),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


def lookup_phone_safe(twilio_client: Any, phone_e164: str) -> Dict[str, Any]:
    """Like lookup_phone but never raises. Returns a payload either way.

    On Twilio API failure returns a payload with valid=False and the
    error fields filled. Suitable for caching so we don't retry the
    same bad number every cron.
    """
    try:
        return lookup_phone(twilio_client, phone_e164)
    except TwilioRestException as e:
        logger.warning(f"twilio lookup error {phone_e164}: {e.code} {e.msg}")
        return {
            "phone": phone_e164,
            "valid": False,
            "error_code": str(getattr(e, "code", "")),
            "error_message": str(getattr(e, "msg", str(e))),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.warning(f"twilio lookup unexpected error {phone_e164}: {e}")
        return {
            "phone": phone_e164,
            "valid": False,
            "error_code": "exception",
            "error_message": str(e),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
