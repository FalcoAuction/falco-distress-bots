from __future__ import annotations


def normalize_prefc_county(county: str | None) -> str:
    return " ".join(str(county or "").strip().lower().split())


def prefc_county_tier(county: str | None) -> str:
    normalized = normalize_prefc_county(county)
    if normalized in {"hamilton county", "rutherford county"}:
        return "PRIMARY"
    if normalized in {"montgomery county"}:
        return "SECONDARY"
    if normalized in {"knox county", "davidson county"}:
        return "WATCH"
    return "OTHER"


def prefc_county_priority(county: str | None) -> int:
    tier = prefc_county_tier(county)
    if tier == "PRIMARY":
        return 0
    if tier == "SECONDARY":
        return 1
    if tier == "WATCH":
        return 2
    return 3


def prefc_county_is_active(county: str | None) -> bool:
    return prefc_county_tier(county) in {"PRIMARY", "SECONDARY"}


def prefc_county_is_watch(county: str | None) -> bool:
    return prefc_county_tier(county) == "WATCH"


def prefc_source_priority(distress_type: str | None) -> int:
    normalized = str(distress_type or "").strip().upper()
    if normalized in {"SOT", "SUBSTITUTION_OF_TRUSTEE"}:
        return 0
    if normalized == "LIS_PENDENS":
        return 1
    if normalized in {"FORECLOSURE", "FORECLOSURE_TN"}:
        return 2
    return 3
