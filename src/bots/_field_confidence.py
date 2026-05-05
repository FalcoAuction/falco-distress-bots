"""Shared trust/confidence helpers for lead fields.

The pipeline stores compact fields such as ``mortgage_balance`` and
``property_value`` because the site/dialer need simple columns. Those
columns are not all equal: one value might be a county assessor match,
another might be a statewide median fallback. Keep the semantics here so
scoring, regrading, and math-sheet handoff use the same rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional


SELLER_MATH_CONFIDENCE = 0.70
HARD_EQUITY_CONFIDENCE = 0.70
SCORING_EQUITY_CONFIDENCE = 0.40

NON_PAYOFF_KINDS = {
    "original_principal",
    "default_amount",
    "delinquent_amount",
    "principal_or_arrears",
    "unverified_secured_claim",
    "legacy_unattributed",
}


@dataclass(frozen=True)
class FieldTrust:
    value: Any
    source: str
    confidence: float
    kind: str
    trusted_for_seller_math: bool
    trusted_for_hard_gate: bool
    requires_confirmation: bool
    note: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "source": self.source,
            "confidence": round(max(0.0, min(1.0, float(self.confidence))), 3),
            "kind": self.kind,
            "trusted_for_seller_math": self.trusted_for_seller_math,
            "trusted_for_hard_gate": self.trusted_for_hard_gate,
            "requires_confirmation": self.requires_confirmation,
            "note": self.note,
        }


def as_metadata(row_or_meta: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(row_or_meta, Mapping):
        return {}
    if "phone_metadata" in row_or_meta:
        meta = row_or_meta.get("phone_metadata") or {}
    else:
        meta = row_or_meta
    return dict(meta) if isinstance(meta, Mapping) else {}


def deep_merge_dict(base: Optional[Mapping[str, Any]], patch: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base) if isinstance(base, Mapping) else {}
    for key, value in patch.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = deep_merge_dict(out[key], value)
        else:
            out[key] = value
    return out


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _same_money(a: Any, b: Any, tolerance: float = 1.0) -> bool:
    a_n = safe_float(a)
    b_n = safe_float(b)
    if a_n is None or b_n is None:
        return False
    return abs(a_n - b_n) <= tolerance


def _raw(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw = row.get("raw_payload") or {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def mortgage_balance_trust(row: Mapping[str, Any]) -> FieldTrust:
    """Classify the trust level of ``mortgage_balance`` for this row."""
    mb = row.get("mortgage_balance")
    pm = as_metadata(row)

    estimate = pm.get("mortgage_estimate") or {}
    if isinstance(estimate, Mapping) and estimate:
        conf = safe_float(estimate.get("confidence")) or 0.0
        source = str(estimate.get("source") or "mortgage_estimator")
        kind = "mortgage_estimate"
        note = str(estimate.get("note") or "")
        trusted = conf >= SELLER_MATH_CONFIDENCE and source != "avm_only_tn_median"
        hard = conf >= HARD_EQUITY_CONFIDENCE and source != "avm_only_tn_median"
        return FieldTrust(
            mb,
            source,
            conf,
            kind,
            trusted_for_seller_math=trusted,
            trusted_for_hard_gate=hard,
            requires_confirmation=not trusted,
            note=note,
        )

    signal = pm.get("mortgage_signal") or {}
    if isinstance(signal, Mapping) and signal:
        conf = safe_float(signal.get("confidence")) or 0.0
        kind = str(signal.get("kind") or "mortgage_signal")
        source = str(signal.get("source") or "mortgage_signal")
        trusted = conf >= SELLER_MATH_CONFIDENCE and kind not in NON_PAYOFF_KINDS
        hard = conf >= HARD_EQUITY_CONFIDENCE and kind not in NON_PAYOFF_KINDS
        return FieldTrust(
            mb or signal.get("amount"),
            source,
            conf,
            kind,
            trusted_for_seller_math=trusted,
            trusted_for_hard_gate=hard,
            requires_confirmation=not trusted,
            note=str(signal.get("note") or ""),
        )

    raw = _raw(row)
    if raw.get("schedule_d_extracted") and _same_money(mb, raw.get("schedule_d_primary_balance")):
        return FieldTrust(
            mb,
            "bankruptcy_schedule_d",
            0.65,
            "unverified_secured_claim",
            trusted_for_seller_math=False,
            trusted_for_hard_gate=False,
            requires_confirmation=True,
            note="Largest Schedule D secured claim; verify it is the home mortgage.",
        )

    extracted = raw.get("extracted") or {}
    if isinstance(extracted, Mapping) and _same_money(mb, extracted.get("original_principal")):
        return FieldTrust(
            mb,
            "foreclosure_notice_original_principal",
            0.35,
            "original_principal",
            trusted_for_seller_math=False,
            trusted_for_hard_gate=False,
            requires_confirmation=True,
            note="Original principal is not current payoff.",
        )

    if _same_money(mb, raw.get("delinquent_amount")):
        return FieldTrust(
            mb,
            "foreclosure_notice_delinquent_amount",
            0.25,
            "delinquent_amount",
            trusted_for_seller_math=False,
            trusted_for_hard_gate=False,
            requires_confirmation=True,
            note="Default/arrears amount is not current payoff.",
        )

    if mb is None:
        return FieldTrust(
            None, "missing", 0.0, "missing", False, False, True,
            "No mortgage/payoff value on the lead.",
        )

    return FieldTrust(
        mb,
        "legacy_unattributed",
        0.35,
        "legacy_unattributed",
        trusted_for_seller_math=False,
        trusted_for_hard_gate=False,
        requires_confirmation=True,
        note="No source/confidence metadata found for mortgage_balance.",
    )


def property_value_trust(row: Mapping[str, Any]) -> FieldTrust:
    value = row.get("property_value")
    if value is None:
        return FieldTrust(None, "missing", 0.0, "missing", False, False, True)

    source = str(row.get("property_value_source") or "").strip()
    pm = as_metadata(row)
    xref = pm.get("property_value_xref") or {}
    if isinstance(xref, Mapping) and xref:
        conf = safe_float(xref.get("confidence")) or 0.60
        return FieldTrust(
            value,
            str(xref.get("source") or "xref_avm_enricher"),
            conf,
            "address_propagated_avm",
            trusted_for_seller_math=conf >= SELLER_MATH_CONFIDENCE,
            trusted_for_hard_gate=conf >= HARD_EQUITY_CONFIDENCE,
            requires_confirmation=conf < SELLER_MATH_CONFIDENCE,
            note="Propagated by address cross-reference; verify parcel before seller-facing math.",
        )

    raw = _raw(row)
    inferred = source
    for key, label in (
        ("padctn", "davidson_assessor"),
        ("williamson_inigo", "williamson_assessor"),
        ("tpad", "tpad_enricher"),
        ("hamilton_assessor", "hamilton_assessor"),
        ("shelby_arcgis", "shelby_assessor"),
        ("rutherford_arcgis", "rutherford_assessor"),
    ):
        if not inferred and isinstance(raw.get(key), Mapping):
            inferred = label
            break

    value_n = safe_float(value)
    if value_n is not None and (value_n < 20000 or value_n > 5000000):
        return FieldTrust(value, inferred or "anomalous_value", 0.10, "anomalous_avm", False, False, True)

    if inferred:
        return FieldTrust(
            value,
            inferred,
            0.80,
            "assessor_value",
            trusted_for_seller_math=True,
            trusted_for_hard_gate=True,
            requires_confirmation=False,
            note="County assessor/appraised value; use as triage ARV, confirm comps before signing.",
        )

    return FieldTrust(
        value,
        "legacy_unattributed",
        0.50,
        "legacy_property_value",
        trusted_for_seller_math=False,
        trusted_for_hard_gate=False,
        requires_confirmation=True,
        note="No source/confidence metadata found for property_value.",
    )


def phone_trust(row: Mapping[str, Any]) -> FieldTrust:
    phone = row.get("phone")
    pm = as_metadata(row)

    batch = pm.get("batchdata_skip_trace") or {}
    if isinstance(batch, Mapping) and batch:
        dnc = bool(batch.get("primary_dnc"))
        conf = safe_float(batch.get("primary_confidence")) or 0.0
        trusted = bool(phone) and not dnc and conf >= 0.75
        return FieldTrust(
            phone,
            "batchdata_skip_trace",
            0.0 if dnc else conf,
            "skip_trace_phone",
            trusted_for_seller_math=trusted,
            trusted_for_hard_gate=trusted,
            requires_confirmation=not trusted,
            note="DNC" if dnc else "BatchData phone; still needs dial outcome feedback.",
        )

    resolver = pm.get("phone_resolver") or {}
    if isinstance(resolver, Mapping) and resolver:
        conf = safe_float(resolver.get("confidence")) or 0.0
        source = str(resolver.get("source") or "phone_resolver")
        if source == "notice_body":
            conf = min(conf, 0.30)
        trusted = bool(phone) and conf >= 0.75 and source != "notice_body"
        return FieldTrust(
            phone,
            f"phone_resolver:{source}",
            conf,
            "resolved_phone",
            trusted_for_seller_math=trusted,
            trusted_for_hard_gate=trusted,
            requires_confirmation=not trusted,
            note="Notice-body phones are usually trustee/attorney phones." if source == "notice_body" else "",
        )

    if phone:
        return FieldTrust(phone, "legacy_unattributed", 0.40, "legacy_phone", False, False, True)
    return FieldTrust(None, "missing", 0.0, "missing", False, False, True)


def equity_trust(row: Mapping[str, Any]) -> Dict[str, Any]:
    mortgage = mortgage_balance_trust(row)
    value = property_value_trust(row)
    min_conf = min(mortgage.confidence, value.confidence)
    return {
        "mortgage": mortgage,
        "property_value": value,
        "scoring_allowed": min_conf >= SCORING_EQUITY_CONFIDENCE,
        "hard_gate_allowed": mortgage.trusted_for_hard_gate and value.trusted_for_hard_gate,
        "seller_math_allowed": mortgage.trusted_for_seller_math and value.trusted_for_seller_math,
        "min_confidence": min_conf,
    }
