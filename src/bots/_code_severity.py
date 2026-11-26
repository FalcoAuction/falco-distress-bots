"""Code-case severity, a line-for-line port of the database rule.

The ranking SQL (public.lead_code_severity) marks a CODE_VIOLATION or
DEMOLITION row 'severe' when its case text matches one expression and
'nuisance' otherwise. Nuisance rows never become leads, so a bot whose rows
carry text the rule can read keeps them out of staging and counts them.

Keep this file identical to the SQL function. If the function changes,
change the expression and the field list here in the same release.

Only NEW rows are filtered. A case that is already staged is refreshed by
its bot exactly as before, and a case whose text later turns severe is
staged on the run where that happens, because it was never stored.

Turn the filter off without a deploy: CODE_NUISANCE_DOOR_FILTER=0
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, List, Optional, Tuple

# ON unless the environment says otherwise.
DOOR_FILTER_ON = (os.environ.get("CODE_NUISANCE_DOOR_FILTER", "1").strip().lower()
                  not in ("0", "false", "off", "no"))

CODE_TYPES = ("CODE_VIOLATION", "DEMOLITION")

# Copied from public.lead_code_severity. Case-sensitive, searched anywhere
# in the upper-cased text, like the SQL `~` operator.
SEVERE_RX = re.compile(
    r"(DILAPIDATED (?!SIGN)|STRUCTURAL|UNSAFE|OPEN AND ACCESSIBLE|OPEN & ACCESSIBLE|CONDEMN"
    r"|UNFIT FOR|UNINHABIT|FIRE DAMAGE|FIRE-DAMAGE|DEMOLI|COLLAPS|BOARD UP|BOARDED|DERELICT"
    r"|VACANT (HOUSE|HOME|BUILDING|STRUCTURE|DWELLING)|VACANT AND OPEN"
    r"|ABANDONED (HOUSE|HOME|BUILDING|STRUCTURE|DWELLING))"
)


def _text(value: Any) -> Optional[str]:
    """jsonb ->> : a JSON null or a missing key is NULL, anything else is text."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _field(container: Any, key: str) -> Optional[str]:
    """jsonb -> then ->> : NULL unless the container is an object."""
    return _text(container.get(key)) if isinstance(container, dict) else None


def _concat_ws(parts: List[Optional[str]]) -> str:
    """concat_ws(' ', ...): NULLs are skipped, empty strings are kept."""
    return " ".join(p for p in parts if p is not None)


def _get(lead: Any, key: str) -> Any:
    return lead.get(key) if isinstance(lead, dict) else getattr(lead, key, None)


def severity_text(raw: Any) -> str:
    """The exact text the SQL rule tests, before upper-casing."""
    raw = raw if isinstance(raw, dict) else {}
    codes = raw.get("nashville_codes")
    issue = raw.get("issue")
    # The inner concat_ws is '' (not NULL) for a Florida row with neither field.
    florida = (_concat_ws([_field(raw, "case_type"), _field(raw, "case_name")])
               if _field(raw, "state") == "FL" else None)
    return _concat_ws([
        _field(codes, "Violations_Noted"), _field(codes, "Reported_Problem"), florida,
        _field(issue, "summary"), _field(issue, "description"),
    ])


def code_severity(lead: Any) -> Optional[str]:
    """'severe', 'nuisance' or None, for a LeadPayload or a row-shaped dict."""
    if str(_get(lead, "distress_type") or "").upper() not in CODE_TYPES:
        return None
    return "severe" if SEVERE_RX.search(severity_text(_get(lead, "raw_payload")).upper()) else "nuisance"


def door_filter(leads: List[Any]) -> Tuple[List[Any], int]:
    """Leads to stage and the number of nuisance cases left out.

    Call this only on rows that would be new inserts, and only from a bot
    whose rows carry text the rule reads.
    """
    if not DOOR_FILTER_ON:
        return list(leads), 0
    kept = [lead for lead in leads if code_severity(lead) != "nuisance"]
    return kept, len(leads) - len(kept)
