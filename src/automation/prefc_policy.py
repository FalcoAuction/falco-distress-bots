from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path


def normalize_prefc_county(county: str | None) -> str:
    return " ".join(str(county or "").strip().lower().split())


@lru_cache(maxsize=1)
def _load_latest_autonomy() -> dict:
    path = Path(__file__).resolve().parents[2] / "out" / "reports" / "latest_autonomy.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def prefc_county_directive(county: str | None) -> str | None:
    normalized = normalize_prefc_county(county)
    counties = ((_load_latest_autonomy().get("marketAllocation") or {}).get("counties") or [])
    for row in counties:
        if normalize_prefc_county(row.get("county")) == normalized:
            directive = str(row.get("directive") or "").strip().lower()
            return directive or None
    return None


def prefc_source_directive(distress_type: str | None) -> str | None:
    normalized = str(distress_type or "").strip().upper()
    if normalized in {"SUBSTITUTION_OF_TRUSTEE"}:
        normalized = "SOT"
    if normalized in {"FORECLOSURE", "FORECLOSURE_TN"}:
        normalized = "FORECLOSURE_NOTICE"
    sources = ((_load_latest_autonomy().get("marketAllocation") or {}).get("sources") or [])
    for row in sources:
        source = str(row.get("source") or "").strip().upper()
        if source == normalized:
            directive = str(row.get("directive") or "").strip().lower()
            return directive or None
    return None


def prefc_overlap_priority(signals: list[str] | tuple[str, ...] | set[str] | None) -> int:
    signal_set = {str(item or "").strip().lower() for item in (signals or []) if str(item or "").strip()}
    if not signal_set:
        return 3
    if "stacked_notice_path" in signal_set and "tax_overlap" in signal_set:
        return 0
    if "stacked_notice_path" in signal_set:
        return 1
    if "tax_overlap" in signal_set or "reopened_timing" in signal_set:
        return 2
    return 3


def prefc_is_special_situation(signals: list[str] | tuple[str, ...] | set[str] | None) -> bool:
    signal_set = {str(item or "").strip().lower() for item in (signals or []) if str(item or "").strip()}
    return bool(signal_set.intersection({"stacked_notice_path", "tax_overlap", "reopened_timing"}))


def prefc_county_tier(county: str | None) -> str:
    normalized = normalize_prefc_county(county)
    if normalized in {"hamilton county", "rutherford county"}:
        return "PRIMARY"
    if normalized in {"montgomery county", "davidson county", "wilson county", "sumner county"}:
        return "SECONDARY"
    if normalized in {"knox county"}:
        return "WATCH"
    return "OTHER"


def prefc_county_priority(county: str | None) -> int:
    tier = prefc_county_tier(county)
    if tier == "PRIMARY":
        priority = 0
    elif tier == "SECONDARY":
        priority = 1
    elif tier == "WATCH":
        priority = 2
    else:
        priority = 3

    directive = prefc_county_directive(county)
    if directive == "push_harder":
        return max(0, priority - 1)
    if directive == "deprioritize":
        return priority + 1
    return priority


def prefc_county_is_active(county: str | None) -> bool:
    return prefc_county_tier(county) in {"PRIMARY", "SECONDARY"}


def prefc_county_is_watch(county: str | None) -> bool:
    return prefc_county_tier(county) == "WATCH"


def prefc_source_priority(distress_type: str | None) -> int:
    normalized = str(distress_type or "").strip().upper()
    if normalized in {"SOT", "SUBSTITUTION_OF_TRUSTEE"}:
        priority = 0
    elif normalized == "LIS_PENDENS":
        priority = 1
    elif normalized in {"FORECLOSURE", "FORECLOSURE_TN"}:
        priority = 2
    else:
        priority = 3

    directive = prefc_source_directive(distress_type)
    if directive in {"push_harder", "expand_selectively"}:
        return max(0, priority - 1)
    if directive == "deprioritize":
        return priority + 1
    return priority
