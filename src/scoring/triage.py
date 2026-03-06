from __future__ import annotations
from typing import Any, Dict

def triage(fields: Dict[str, Any]) -> str:
    """
    Conservative placeholder triage bucket.
    """
    # Default to WATCHLIST unless clearly ready.
    return "WATCHLIST"
