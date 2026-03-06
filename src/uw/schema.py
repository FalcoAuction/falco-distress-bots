# src/uw/schema.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class UWSubmission:
    # Gate
    uw_ready: int = 0                       # 0/1
    uw_confidence: Optional[int] = None     # 1-5
    uw_blocker: Optional[str] = None        # title/bankruptcy/occupancy/condition/liens/other/none

    # Reality checks
    occupancy: Optional[str] = None         # unknown/owner/tenant/vacant
    condition: Optional[str] = None         # unknown/light/medium/heavy
    title_notes: Optional[str] = None       # free text

    # Numbers (your actual "bid guidance" truth layer)
    manual_arv: Optional[float] = None
    manual_bid_cap: Optional[float] = None
    repair_estimate: Optional[float] = None
    lien_estimate_total: Optional[float] = None

    # Thesis
    exit_strategy: Optional[str] = None     # auction_retail/wholesale/investor/flip/hold
    partner_action: Optional[str] = None    # 1-3 bullets, plain text

    # meta
    operator: Optional[str] = None
    ts: str = ""                            # set at submit time

    def to_json_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        if not d.get("ts"):
            d["ts"] = _now_z()
        # normalize uw_ready
        d["uw_ready"] = 1 if int(d.get("uw_ready") or 0) == 1 else 0
        return d
