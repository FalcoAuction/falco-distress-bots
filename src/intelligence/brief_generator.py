# src/intelligence/brief_generator.py
#
# Structured intelligence brief generator for auction operator review.
# LLM call isolated in _call_llm(); swap that function to change providers.
# Deterministic fallback used when no API key is configured or any call fails.

import json
import os
from typing import Any, Dict, List

# ─── Required output keys ─────────────────────────────────────────────────────

_BRIEF_KEYS = (
    "executive_summary",
    "auction_positioning",
    "liquidity_analysis",
    "risk_analysis",
    "pricing_strategy",
    "operator_notes",
)

# ─── System prompt ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a senior auction-operations analyst at an institutional real estate investment firm.
Your role is to produce structured intelligence briefs for auction operators reviewing distressed property leads.

ABSOLUTE CONSTRAINTS — any violation makes the output unusable:
- Do NOT speculate on equity position or lien balance. If relevant, state "equity position unknown".
- Do NOT reference property ownership, occupant identity, or title-holder information.
- Do NOT name any data provider, data service, software vendor, or technology platform.
- Do NOT imply the brief or underlying data will be resold, redistributed, or shared externally.
- Do NOT use the words "guarantee", "certified", or "verified" in any form.
- Do NOT use promotional, speculative, or forward-looking language unsupported by provided data.
- ONLY reference facts explicitly present in the DATA block. Do not infer or estimate beyond it.

TONE: Institutional, analytical, auction-operator focused. Conservative. Terse. No fluff.

ANALYTICAL DEPTH REQUIREMENTS — each section must go beyond restating data:
- executive_summary: State key metrics, then interpret what the DTS figure means operationally
  (e.g., whether timeline creates urgency, narrows due-diligence window, or is within standard range).
- auction_positioning: Assess readiness classification and diamond status, then draw a reasoned
  conclusion about likely bidder competitiveness — whether spread, value, and timing together
  support strong, moderate, or thin buyer demand. Do not invent data; reason from what is present.
- liquidity_analysis: Reference the specific AVM price band and county (if present) when assessing
  market depth. Comment on whether the value tier and spread width structurally support or limit
  competitive bidding at this location.
- pricing_strategy: Explain the structural reason anchoring to the AVM low is conservative —
  specifically that it reflects the lower bound of modeled value uncertainty, not a distressed
  discount, and that reserve strategy should account for unknown lien exposure.
- risk_analysis: Identify data gaps and flags, then state what the operator must independently
  obtain before the sale proceeds (e.g., title search, lien payoff, physical condition report).
- operator_notes: Address workflow concerns directly — flag timeline pressure if DTS is tight,
  note documentation gaps that require action before listing, and confirm enrichment status
  as it affects what information is available to bidders.

OUTPUT FORMAT:
Return ONLY a valid JSON object with exactly these six string keys:
  executive_summary
  auction_positioning
  liquidity_analysis
  risk_analysis
  pricing_strategy
  operator_notes

Each value must be a plain string. Maximum 120 words per section.
No markdown. No code fences. No keys outside the six above. No commentary.\
"""

# ─── Formatting helpers ───────────────────────────────────────────────────────

def _fmt_cur(v: Any) -> str:
    if v is None:
        return "Unavailable"
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return str(v)


def _fmt_pct(v: Any) -> str:
    if v is None:
        return "N/A"
    try:
        return f"{float(v) * 100:.1f}%"
    except (TypeError, ValueError):
        return str(v)


# ─── Context builder ──────────────────────────────────────────────────────────

def _resolve_avm(fields: dict) -> tuple:
    """
    Returns (low, mid, high) as floats or None.
    Prefers value_anchor_* keys; falls back to avm_low / avm_value / avm_high.
    """
    def _coerce(v: Any) -> Any:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    low  = _coerce(fields.get("value_anchor_low"))  or _coerce(fields.get("avm_low"))
    mid  = _coerce(fields.get("value_anchor_mid"))  or _coerce(fields.get("avm_value"))
    high = _coerce(fields.get("value_anchor_high")) or _coerce(fields.get("avm_high"))
    return low, mid, high


def _build_context(fields: dict) -> str:
    """
    Converts normalized fields into a compact plain-text DATA block for the prompt.
    Never exposes provider names or raw internal keys.
    Only emits lines for data that is actually present.
    """
    low, mid, high = _resolve_avm(fields)
    dts            = fields.get("dts_days")
    spread_pct     = fields.get("spread_pct")
    spread_band    = (fields.get("spread_band") or "").upper()
    county         = (fields.get("county") or "").strip()
    state          = (fields.get("state") or "").strip()

    # ── Always-present lines ──────────────────────────────────────────────────
    addr_parts = [fields.get("address") or "Unknown"]
    if county:
        addr_parts.append(f"{county} County")
    if state:
        addr_parts.append(state)
    lines: List[str] = [
        f"Address: {', '.join(addr_parts)}",
        f"Days to Sale: {dts if dts is not None else 'Unknown'}",
        f"Auction Readiness: {(fields.get('auction_readiness') or 'UNKNOWN').upper()}",
        f"Enrichment Status: {fields.get('attom_status') or 'Unknown'}",
    ]

    # ── AVM values — only if at least one is present ──────────────────────────
    if any(v is not None for v in (low, mid, high)):
        lines.append(
            f"AVM Low / Mid / High: {_fmt_cur(low)} / {_fmt_cur(mid)} / {_fmt_cur(high)}"
        )
    else:
        lines.append("AVM Values: Not available")

    # ── Spread — only if computed ─────────────────────────────────────────────
    if spread_pct is not None:
        band_label = f" ({spread_band})" if spread_band and spread_band != "UNKNOWN" else ""
        lines.append(f"AVM Spread: {_fmt_pct(spread_pct)}{band_label}")

    # ── Diamond proxy — only if key is set ───────────────────────────────────
    if "diamond_proxy" in fields:
        lines.append(f"Diamond Screening: {'PASS' if fields['diamond_proxy'] else 'FAIL'}")

    # ── Score — only if present ───────────────────────────────────────────────
    score = fields.get("falco_score_internal")
    if score is not None:
        lines.append(f"Internal Score (0-100): {score}")

    # ── Equity band with mandatory lien caveat ────────────────────────────────
    equity_band = fields.get("equity_band")
    if equity_band:
        lines.append(
            f"Equity Band: {equity_band} "
            f"(NOTE: lien balance is NOT present — do not infer equity position)"
        )

    lines.append(f"Property Detail Available: {'Yes' if fields.get('attom_detail') else 'No'}")

    # ── Optional property characteristics ────────────────────────────────────
    for key, label in (
        ("property_type",      "Property Type"),
        ("year_built",         "Year Built"),
        ("building_area_sqft", "Building Area (sqft)"),
        ("beds",               "Bedrooms"),
        ("baths",              "Bathrooms"),
        ("city",               "City"),
        ("zip",                "ZIP"),
    ):
        v = fields.get(key)
        if v is not None:
            lines.append(f"{label}: {v}")

    return "\n".join(lines)


# ─── Isolated LLM call ────────────────────────────────────────────────────────

def _call_llm(prompt: str) -> str:
    """
    Single point of contact with the LLM provider.
    Replace this function entirely to swap models or providers.
    Raises on any network, auth, or API error — caller handles fallback.
    """
    import requests as _req

    api_key = os.getenv("FALCO_OPENAI_API_KEY", "").strip()
    model   = os.getenv("FALCO_OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"

    resp = _req.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            "max_tokens":      950,
            "temperature":     0.2,
            "response_format": {"type": "json_object"},
        },
        timeout=25,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


# ─── Deterministic fallback ───────────────────────────────────────────────────

def _fallback_brief(fields: dict) -> dict:
    """
    Rule-based brief. Used when no API key is set or the LLM call fails.
    Conservative, compliant, no speculative language.
    """
    addr        = fields.get("address") or "Subject property"
    dts         = fields.get("dts_days")
    readiness   = (fields.get("auction_readiness") or "UNKNOWN").upper()
    low         = fields.get("value_anchor_low")
    mid         = fields.get("value_anchor_mid")
    high        = fields.get("value_anchor_high")
    spread_pct  = fields.get("spread_pct")
    spread_band = (fields.get("spread_band") or "UNKNOWN").upper()
    score       = fields.get("falco_score_internal")
    diamond     = bool(fields.get("diamond_proxy"))
    has_detail  = bool(fields.get("attom_detail"))

    dts_txt      = f"{dts} days"  if dts is not None   else "timeline pending"
    score_txt    = f"{score}/100" if score is not None  else "unscored"
    spread_conf  = {"TIGHT": "high", "NORMAL": "moderate", "WIDE": "low"}.get(spread_band, "unknown")
    readiness_lbl = {"GREEN": "favorable", "YELLOW": "moderate", "RED": "elevated-risk"}.get(readiness, "unclassified")
    in_window    = dts is not None and 21 <= int(dts) <= 60

    return {
        "executive_summary": (
            f"{addr} presents a {readiness_lbl} auction profile with {dts_txt} to sale. "
            f"Internal score: {score_txt}. AVM range: {_fmt_cur(low)}–{_fmt_cur(high)} "
            f"(spread {_fmt_pct(spread_pct)}, {spread_band}). "
            f"Diamond screening: {'PASS' if diamond else 'FAIL'}. "
            "Lien balance and equity position are unknown; independent title research required."
        ),
        "auction_positioning": (
            f"Readiness is {readiness}. "
            f"{'All diamond screening criteria met.' if diamond else 'Diamond criteria partially met — see Qualification Matrix (Appendix).'} "
            f"DTS {dts_txt} is {'within' if in_window else 'outside'} the optimal 21–60 day window. "
            f"Auction timing is {'actionable' if in_window else 'suboptimal — monitor for schedule changes'}."
        ),
        "liquidity_analysis": (
            f"AVM confidence is {spread_conf} based on a {_fmt_pct(spread_pct)} spread ({spread_band}). "
            f"Value anchor range: {_fmt_cur(low)} (floor) to {_fmt_cur(high)} (ceiling), mid {_fmt_cur(mid)}. "
            f"{'Wide spread limits pricing precision; use low anchor as conservative floor only.' if spread_band == 'WIDE' else 'Spread is within range for auction pricing purposes.'}"
        ),
        "risk_analysis": (
            "Equity position is unknown. Lien balance data is absent from this dataset. "
            f"{'Property detail record is unavailable; physical characteristics are unconfirmed.' if not has_detail else 'Property detail record is present.'} "
            f"Readiness classification is {readiness}. "
            "Independent lien search, title review, and physical inspection are required before capital commitment."
        ),
        "pricing_strategy": (
            f"Anchor opening bid at AVM low of {_fmt_cur(low)} as a conservative floor. "
            f"Reserve may target mid-point of {_fmt_cur(mid)}. "
            "Do not factor in equity assumptions — lien balance is unconfirmed. "
            "Adjust reserve downward if local buyer-pool depth or market absorption is uncertain."
        ),
        "operator_notes": (
            f"Internal score: {score_txt}. Diamond: {'PASS' if diamond else 'FAIL'}. "
            f"Enrichment status: {fields.get('attom_status', 'unknown')}. "
            f"Detail record: {'present' if has_detail else 'absent'}. "
            "All capital decisions require independent due diligence. "
            "This brief is produced from automated pipelines and does not constitute investment advice."
        ),
    }


# ─── Public entry point ───────────────────────────────────────────────────────

def generate_brief(fields: dict) -> dict:
    """
    Generate a structured intelligence brief from a normalized fields dict.

    Returns a dict with keys:
        executive_summary, auction_positioning, liquidity_analysis,
        risk_analysis, pricing_strategy, operator_notes

    Uses the LLM when FALCO_OPENAI_API_KEY is set in the environment.
    Falls back to deterministic templates on any failure or absent key.
    """
    api_key = os.getenv("FALCO_OPENAI_API_KEY", "").strip()
    if not api_key:
        return _fallback_brief(fields)

    context = _build_context(fields)
    prompt  = (
        "Generate a structured intelligence brief for the following property lead.\n\n"
        f"DATA:\n{context}"
    )

    try:
        raw = _call_llm(prompt)

        # Strip accidental markdown fences from models that ignore format instructions
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw   = "\n".join(lines[1:])
            if raw.rstrip().endswith("```"):
                raw = raw.rstrip()[:-3].strip()

        result   = json.loads(raw)
        fallback = _fallback_brief(fields)

        # Guarantee all keys are present and non-empty
        return {k: str(result.get(k) or fallback[k]) for k in _BRIEF_KEYS}

    except Exception as exc:
        print(f"[BRIEF_GENERATOR][WARN] LLM call failed ({type(exc).__name__}: {exc}), using fallback.")
        return _fallback_brief(fields)
