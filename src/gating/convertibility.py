# src/gating/convertibility.py

import re as _re

_INSTITUTIONAL_TOKENS = (
    "mackie wolf",
    "western progressive",
    "winchester sellers foster & steele",
    "auction.com",
    "hubzu",
    "xome",
)

_MIXED_TOKENS = (
    "burr & forman",
    "rochelle mcculloch",
)

_SCAN_KEYS = (
    "trustee_attorney",
    "contact_info",
    "raw_snippet",
    "ft_trustee_firm",
    "ft_trustee_name_raw",
    "ft_trustee_person",
    "notice_trustee_firm",
    "notice_trustee_name_raw",
)


def _norm(s: str) -> str:
    """Lowercase and collapse runs of whitespace to a single space."""
    return _re.sub(r"\s+", " ", s).strip().lower()


def is_institutional(payload: dict) -> bool:
    return payload.get("status_flag") == "INSTITUTIONAL"


def apply_convertibility_gate(payload: dict) -> dict:
    haystacks = [
        _norm(payload.get(k) or "")
        for k in _SCAN_KEYS
        if payload.get(k)
    ]

    for token in _INSTITUTIONAL_TOKENS:
        needle = _norm(token)
        if any(needle in h for h in haystacks):
            payload["status_flag"] = "INSTITUTIONAL"
            if payload.get("raw_snippet") and "[INSTITUTIONAL]" not in payload["raw_snippet"]:
                payload["raw_snippet"] = payload["raw_snippet"] + " [INSTITUTIONAL]"
            return payload

    for token in _MIXED_TOKENS:
        needle = _norm(token)
        if any(needle in h for h in haystacks):
            payload["status_flag"] = "MIXED_ROUTING"
            if payload.get("raw_snippet") and "[MIXED_ROUTING]" not in payload["raw_snippet"]:
                payload["raw_snippet"] = payload["raw_snippet"] + " [MIXED_ROUTING]"
            return payload

    return payload
