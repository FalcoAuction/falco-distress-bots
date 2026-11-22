"""A Tennessee employment notice is not a real-estate distress or sale event."""


def is_tn_warn(row):
    raw = row.get("raw_payload")
    raw = raw if isinstance(raw, dict) else {}
    if row.get("bot_source") == "warn_notices":
        return True
    if row.get("bot_source") not in (None, "", "bot") or row.get("distress_type") != "BUSINESS_CLOSURE":
        return False
    return (raw.get("source") == "tn_dol_warn" or row.get("land_source") == "tn_dol_warn"
            or isinstance(raw.get("warn"), dict) or isinstance(raw.get("warn_review"), dict)
            or row.get("warn_notice") is not None)
