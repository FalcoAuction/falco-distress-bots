# src/gating/convertibility.py

_INSTITUTIONAL_PREFIXES = (
    "mackie wolf",
    "western progressive",
    "winchester sellers foster & steele",
)


def is_institutional(payload: dict) -> bool:
    return payload.get("status_flag") == "INSTITUTIONAL"


def apply_convertibility_gate(payload: dict) -> dict:
    fields = (
        payload.get("trustee_attorney", "") or "",
        payload.get("contact_info", "") or "",
        payload.get("raw_snippet", "") or "",
    )
    for field in fields:
        if any(field.strip().lower().startswith(p) for p in _INSTITUTIONAL_PREFIXES):
            payload["status_flag"] = "INSTITUTIONAL"
            if payload.get("raw_snippet"):
                payload["raw_snippet"] = payload["raw_snippet"] + " [INSTITUTIONAL]"
            return payload
    return payload
