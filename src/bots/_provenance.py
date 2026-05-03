"""
Lightweight provenance writer for the lead_field_provenance table.

Each enricher that writes a field to homeowner_requests should ALSO
record provenance via record_field() so the dialer can show Chris
which source filled which field, and operators can compare per-source
quality over time.

Usage from inside an enricher:

    from ._provenance import record_field

    record_field(
        client,
        lead_id=row["id"],
        field_name="property_value",
        value=str(hit["appraised"]),
        source="davidson_assessor",
        confidence=1.0,
        metadata={"account_id": hit["account_id"], "parcel": hit["parcel"]},
    )

The function is fire-and-forget — it logs failures but never raises,
so a provenance write hiccup never blocks an enrichment write. v1
writes per-call (no batching); the per-row overhead is tiny relative
to the assessor HTTP fetches the enrichers already do.

Schema (created by migrations/0042_lead_field_provenance.sql):

    CREATE TABLE lead_field_provenance (
        id BIGSERIAL PRIMARY KEY,
        lead_id UUID NOT NULL REFERENCES homeowner_requests(id),
        field_name TEXT NOT NULL,
        value TEXT,
        source TEXT NOT NULL,
        confidence DOUBLE PRECISION DEFAULT 1.0,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        metadata JSONB
    );

Confidence convention:
    1.0  = strict-match enricher (exactly-1 hit, government data source)
    0.7..0.9 = high-confidence heuristic (assessor lookup with multiple
              candidates, picked best match)
    0.5..0.7 = medium (regex extraction from notice body)
    0.3..0.5 = low (legacy import, untrusted source)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Union

logger = logging.getLogger("bot.provenance")


def record_field(
    client,
    lead_id: str,
    field_name: str,
    value: Optional[Union[str, int, float, bool]],
    source: str,
    confidence: float = 1.0,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Write one provenance row. Returns True on success.

    Never raises — logs and returns False on any failure so callers
    don't have to wrap every call in try/except.
    """
    if client is None:
        return False
    if not lead_id or not field_name or not source:
        return False

    # Coerce value to text for storage
    if value is None:
        value_str = None
    elif isinstance(value, bool):
        value_str = "true" if value else "false"
    elif isinstance(value, (int, float)):
        value_str = str(value)
    elif isinstance(value, (dict, list)):
        try:
            value_str = json.dumps(value, default=str)[:8000]
        except Exception:
            value_str = repr(value)[:8000]
    else:
        value_str = str(value)[:8000]

    payload = {
        "lead_id": lead_id,
        "field_name": field_name,
        "value": value_str,
        "source": source,
        "confidence": max(0.0, min(1.0, float(confidence))),
    }
    if metadata is not None:
        try:
            payload["metadata"] = json.loads(json.dumps(metadata, default=str))
        except Exception:
            pass

    try:
        client.table("lead_field_provenance").insert(payload).execute()
        return True
    except Exception as e:
        # The table might not exist yet (pre-migration) — silently fail
        # rather than spamming the logs. Real callers can check the
        # return value if they care.
        logger.debug(f"provenance insert failed: {e}")
        return False


def record_fields(
    client,
    lead_id: str,
    fields: Dict[str, Any],
    source: str,
    confidence: float = 1.0,
    metadata: Optional[Dict[str, Any]] = None,
) -> int:
    """Convenience wrapper to record many fields from one source at once.
    Returns count of successful writes."""
    if client is None or not fields:
        return 0
    ok = 0
    for field_name, value in fields.items():
        if value is None:
            continue
        if record_field(client, lead_id, field_name, value, source, confidence, metadata):
            ok += 1
    return ok
