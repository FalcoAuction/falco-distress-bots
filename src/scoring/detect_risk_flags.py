from __future__ import annotations
from typing import Any, Dict, List

def detect_risk_flags(fields: Dict[str, Any]) -> List[str]:
    """
    Conservative placeholder. Returns a list of risk flag strings based on available fields.
    Existing bots may call this; keep stable and non-throwing.
    """
    flags: List[str] = []
    # Minimal checks (safe):
    addr = str(fields.get("address") or "").strip()
    if not addr:
        flags.append("MISSING_ADDRESS")
    return flags
