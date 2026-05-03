"""
Decision engine — autonomous lead-grading + action-recommendation
brain powered by OpenAI gpt-5-mini.

This is the autonomous front-to-back orchestrator. It runs daily AFTER
all enrichers, walks every staged + live lead, and decides:

  - whether to PROMOTE or HOLD or REJECT
  - what priority_score to assign
  - which outreach path to use (call / mail / email / sms / door-knock)
  - whether to ESCALATE to Patrick for edge cases

The LLM is given full context per lead (raw_payload + all enrichments
+ provenance + history) and a FALCO operating manual (~5K tokens of
system prompt). OpenAI's automatic prompt caching makes the system
prompt nearly-free across calls (cached prefix tokens billed at
discounted rate).

Per-lead cost via gpt-5-mini (reasoning model — completion includes
hidden reasoning_tokens billed at the same rate as visible output):
  - ~2K input × $0.25/M (cached: $0.025/M after first call) = $0.0001
  - ~80 reasoning + 200 output completion × $2/M = $0.0006
  - Total: ~$0.0007/lead with prompt caching active
  - First-run total for 2,901 leads (no cache yet): ~$3.20
  - Steady-state at 1,500 leads/day with cache: ~$1.05/day

Why LLM and not pure rules:
  - Rules handle the 90% of leads that fit clean patterns (auto-
    promote high-equity foreclosure, auto-reject business owners,
    etc). The decision engine still applies those first as fast
    filters.
  - LLM handles the 10% gray-area cases where the rule confidence
    is 60-75%: a homeowner with multiple distress signals but a
    business-sounding name, a probate executor who shares the
    decedent's surname, an out-of-state owner whose mailing is a
    PO box (could be lawyer or could be relative), etc.
  - LLM also catches *anomalies* the rules don't know about:
    "this address keeps appearing in 3 different bot sources at
    once — duplicate or stacked-distress?" The engine flags
    these for human review.

Architecture:

    1. Pull batch of unscored / re-score-due leads
    2. Apply fast rules → labels: AUTO_PROMOTE / AUTO_REJECT /
       NEEDS_LLM_REVIEW
    3. For NEEDS_LLM_REVIEW: build prompt with cached system +
       per-lead context, call Haiku
    4. Parse JSON action response, write to lead row + action_log
    5. Update priority_score per result
    6. Trigger downstream automation (skip-trace queue, mail
       dispatch, dialer hot-25)

Prompt cache strategy:
  - System prompt = FALCO operating manual + scoring rules + action
    enum + safety rails (cached, ~30K tokens, 5-min TTL refresh)
  - Per-call user message = lead JSON + recent history (~2K tokens)
  - Output = strict JSON action object (~200 tokens)

Distress type: N/A (autonomous decision engine).
"""

from __future__ import annotations

import json
import os
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from ._provenance import record_field

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


# ─── Cached system prompt ──────────────────────────────────────────────────


SYSTEM_PROMPT = """You are FALCO's autonomous lead-grading engine.
FALCO is a Tennessee distressed-real-estate intelligence + auction-
routing platform. You grade homeowner leads for whether they're worth
calling/mailing today, and decide what action to take.

# Your job

For each lead, output a single JSON object with one of these actions:

  PROMOTE_HOT       — priority_score 80+, clear distress, contact
                      ready, dial today
  PROMOTE_WARM      — priority_score 50-79, has actionable signal,
                      queue for outreach cycle
  PROMOTE_COLD      — priority_score 20-49, weak but trackable, low
                      priority, send mail only
  HOLD_FOR_DATA     — missing required field (no address, no AVM,
                      no owner name); retry once enrichers backfill
  REJECT_BUSINESS   — owner is LLC/Inc/Trust/Government — out of
                      scope for the homeowner pilot
  REJECT_NO_EQUITY  — mortgage_balance estimate ≥ 95% of property_value
                      → underwater, can't convert
  REJECT_NO_CONTACT — no phone, no mailing address, no representative
                      contact — no path to reach the owner at any cost
  REJECT_FORECLOSED — trustee_sale_date already past + 30+ days
  REJECT_DUPLICATE  — same address + owner already exists at a higher
                      priority
  ESCALATE_TO_PATRICK — anomaly, edge case, regulatory question, or
                      data inconsistency that needs human review

# Required output schema

{
  "action": "PROMOTE_HOT" | "PROMOTE_WARM" | "PROMOTE_COLD" |
            "HOLD_FOR_DATA" | "REJECT_BUSINESS" | "REJECT_NO_EQUITY" |
            "REJECT_NO_CONTACT" | "REJECT_FORECLOSED" |
            "REJECT_DUPLICATE" | "ESCALATE_TO_PATRICK",
  "priority_score": 0-100,
  "reasoning": "1-2 sentence audit-trail explanation",
  "suggested_outreach": "cold_call" | "mail" | "email" | "sms" |
                        "door_knock" | "none",
  "confidence": 0.0-1.0,
  "flags": []  // optional list of strings: ["VACANT", "ABSENTEE",
              //  "STACKED_DISTRESS", "URGENT_SALE", "OUT_OF_STATE",
              //  "JOINT_OWNER", "CONTACT_VIA_REP_ONLY"]
}

# Scoring framework

priority_score factors (you weight them):
  - DTS urgency: trustee_sale_date within 7 days → +25; 14d → +15;
    30d → +8; >60d or null → 0
  - Equity: equity_pct ≥ 0.50 → +20; 0.30-0.49 → +14; 0.15-0.29 → +6;
    <0.15 → 0; unknown → use mortgage_estimator confidence to decide
  - Distress severity: BANKRUPTCY (Ch.13) → +18; PRE_FORECLOSURE → +16;
    TAX_LIEN with cumulative_owed > $5K → +12; PROBATE → +12; FSBO → +8;
    CODE_VIOLATION → +6; REO → +4
  - Stacked-distress bonus: signal_count ≥ 3 → +15; 2 → +8
  - Absentee + out-of-state: out_of_state_owner=true → +10;
    distance_owner_to_property_miles > 200 → +5
  - Phone freshness: verified ≤ 7d → +8; ≤ 30d → +5; null → 0
  - Mortgage confidence: foreclosure_notice or schedule_d → +5;
    amortization_estimate confidence ≥ 0.5 → +3

Cap at 100. Subtract 20 if owner_class != "homeowner". Subtract 30 if
trustee_sale_date is in the past.

# Safety rails (override LLM judgment)

  1. If distress_type missing or unknown → HOLD_FOR_DATA
  2. If property_address missing → HOLD_FOR_DATA
  3. If owner_name_records contains LLC/INC/CORP/TRUST/etc keywords
     AND owner_class != "homeowner" → REJECT_BUSINESS regardless of
     other signals
  4. If property_value ≤ $20,000 (residential floor) or ≥ $5M
     (residential ceiling) → ESCALATE_TO_PATRICK (likely data error)
  5. If lead is already in homeowner_requests with same address +
     same owner → REJECT_DUPLICATE
  6. If trustee_sale_date is more than 30 days past → REJECT_FORECLOSED

# Outreach routing (suggested_outreach)

  - cold_call: have phone (any confidence) AND owner_class=homeowner
  - mail: have property_address or owner_mailing AND priority_score ≥ 70
    AND no phone (or phone confidence < 0.5)
  - email: have email
  - sms: have phone AND distress_type in (FSBO, CODE_VIOLATION) where
    SMS-consent is implicit (owner posted phone publicly)
  - door_knock: priority_score ≥ 90 AND DTS < 14d AND no phone path
  - none: REJECT_* actions

# Tone

Be direct, no hedging. The reasoning field should read like an
analyst's audit comment — what made you decide. Not "this lead may be
interesting"; instead "Ch.13 bankruptcy filed 2026-04-22 + Davidson
property w/ $148K equity, debtor's attorney is a callable lead."
"""


def build_user_message(lead: Dict[str, Any]) -> str:
    """Compact lead context for per-call user message."""
    raw = lead.get("raw_payload") or {}
    if not isinstance(raw, dict):
        raw = {}
    pm = lead.get("phone_metadata") or {}
    if not isinstance(pm, dict):
        pm = {}

    summary = {
        "id": lead.get("id"),
        "property_address": lead.get("property_address"),
        "county": lead.get("county"),
        "owner": lead.get("owner_name_records") or lead.get("full_name"),
        "owner_class": pm.get("owner_class", {}) if isinstance(pm.get("owner_class"), dict) else pm.get("owner_class"),
        "distress_type": lead.get("distress_type"),
        "property_value": lead.get("property_value"),
        "mortgage_balance": lead.get("mortgage_balance"),
        "trustee_sale_date": lead.get("trustee_sale_date"),
        "phone": lead.get("phone"),
        "phone_confidence": (pm.get("phone_resolver") or {}).get("confidence"),
        "absentee_distress_score": (pm.get("skip_trace") or {}).get("absentee_distress_score"),
        "is_out_of_state_owner": (pm.get("skip_trace") or {}).get("is_out_of_state_owner"),
        "distress_stack": pm.get("distress_stack"),
        "mortgage_estimate": pm.get("mortgage_estimate"),
        "raw_summary": {
            "bot_source": lead.get("bot_source"),
            "admin_notes": lead.get("admin_notes"),
        },
    }
    # Trim Nones for token efficiency
    summary = {k: v for k, v in summary.items() if v is not None and v != {}}

    return (
        "Grade this FALCO lead. Output the strict JSON action object only.\n\n"
        + json.dumps(summary, default=str, indent=2)
    )


# ─── Decision engine ──────────────────────────────────────────────────────


class DecisionEngineBot(BotBase):
    name = "decision_engine"
    description = "Autonomous lead-grading + action-recommendation via Claude Haiku"
    throttle_seconds = 0.0
    expected_min_yield = 1
    max_leads_per_run = 500   # ~$0.40/run at Haiku

    model = "gpt-5-mini"
    # gpt-5-mini is a reasoning model — ~60-100 hidden reasoning tokens
    # per call before visible output. Budget = reasoning + JSON output.
    # JSON action object is ~150-250 visible tokens; 800 gives headroom.
    max_output_tokens = 800

    def __init__(self):
        super().__init__()
        self._client: Optional[Any] = None
        self._llm_calls = 0
        self._llm_input_tokens_total = 0
        self._llm_output_tokens_total = 0

    def scrape(self) -> List[Any]:
        return []

    def _ensure_client(self) -> bool:
        if self._client is not None:
            return True
        if OpenAI is None:
            self.logger.error("openai SDK not installed; pip install openai")
            return False
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            self.logger.error("OPENAI_API_KEY not set in env")
            return False
        self._client = OpenAI(api_key=api_key)
        return True

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "decided": 0, "errors": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        if not self._ensure_client():
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="openai_unavailable",
            )
            return {"name": self.name, "status": "no_openai",
                    "decided": 0, "errors": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        decided = 0
        errors = 0
        rule_decided = 0
        action_breakdown: Dict[str, int] = {}
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(f"{len(candidates)} leads to grade")

            for row in candidates[:self.max_leads_per_run]:
                # Step 1: try fast rules first (no LLM)
                fast_result = self._fast_rules(row)
                if fast_result:
                    rule_decided += 1
                    self._write_decision(client, row["id"], fast_result, source="rule_engine")
                    action_breakdown[fast_result["action"]] = (
                        action_breakdown.get(fast_result["action"], 0) + 1
                    )
                    continue

                # Step 2: gray-area → LLM
                try:
                    decision = self._llm_decide(row)
                except Exception as e:
                    self.logger.warning(f"  LLM call failed id={row['id']}: {e}")
                    errors += 1
                    continue

                if not decision:
                    errors += 1
                    continue

                self._write_decision(client, row["id"], decision, source="llm_haiku")
                action_breakdown[decision["action"]] = (
                    action_breakdown.get(decision["action"], 0) + 1
                )
                decided += 1

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif decided + rule_decided == 0 and errors == 0:
            status = "zero_yield"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=decided + rule_decided + errors,
            parsed_count=decided + rule_decided,
            staged_count=decided + rule_decided, duplicate_count=0,
            error_message=error_message,
        )
        self.logger.info(
            f"rule_decided={rule_decided} llm_decided={decided} errors={errors} "
            f"input_tokens={self._llm_input_tokens_total} "
            f"output_tokens={self._llm_output_tokens_total} "
            f"breakdown={action_breakdown}"
        )
        return {
            "name": self.name, "status": status,
            "rule_decided": rule_decided, "llm_decided": decided,
            "errors": errors,
            "input_tokens": self._llm_input_tokens_total,
            "output_tokens": self._llm_output_tokens_total,
            "action_breakdown": action_breakdown,
            "error": error_message,
            "staged": decided + rule_decided, "duplicates": 0,
            "fetched": decided + rule_decided + errors,
        }

    # ── Candidates ──────────────────────────────────────────────────────────

    def _candidates(self, client) -> List[Dict[str, Any]]:
        """Pull staged + live leads needing a fresh decision.

        Schema-tolerant: bot_source column only exists on the staging
        table, so we omit it for the live-table query.

        Idempotency: skip leads that already have a priority_score set
        (decision_engine wrote one previously). Re-grading happens only
        when priority_score is NULL — i.e., a fresh staged lead, or a
        lead whose priority_score was explicitly cleared (e.g., by an
        enricher that significantly changed the data).
        """
        STAGING_FIELDS = (
            "id, property_address, county, owner_name_records, full_name, "
            "distress_type, property_value, mortgage_balance, "
            "trustee_sale_date, phone, raw_payload, phone_metadata, "
            "admin_notes, bot_source, priority_score"
        )
        LIVE_FIELDS = (
            "id, property_address, county, owner_name_records, full_name, "
            "distress_type, property_value, mortgage_balance, "
            "trustee_sale_date, phone, raw_payload, phone_metadata, "
            "admin_notes, source, priority_score"
        )
        out = []
        for table, fields in (
            ("homeowner_requests_staging", STAGING_FIELDS),
            ("homeowner_requests", LIVE_FIELDS),
        ):
            try:
                q = (
                    client.table(table)
                    .select(fields)
                    .is_("priority_score", "null")  # only ungraded
                    .limit(2500)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                for r in rows:
                    r["__table__"] = table
                    # Normalize 'source' → 'bot_source' for live rows so
                    # downstream code can reference one field name
                    if "source" in r and "bot_source" not in r:
                        r["bot_source"] = r["source"]
                    out.append(r)
            except Exception as e:
                self.logger.warning(f"candidate query on {table} failed: {e}")
        return out

    # ── Fast rules (no LLM) ─────────────────────────────────────────────────

    def _fast_rules(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply the explicit safety rails. Returns a decision dict if a
        rule matches, None if the lead is gray-area and needs LLM."""
        pm = row.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}
        owner_class_meta = pm.get("owner_class")
        if isinstance(owner_class_meta, dict):
            owner_class = owner_class_meta.get("class") or owner_class_meta.get("value")
        else:
            owner_class = owner_class_meta

        # Rule 1: missing required fields
        if not row.get("distress_type"):
            return self._mk_decision("HOLD_FOR_DATA", 0, "Missing distress_type", "none", 1.0)
        if not row.get("property_address") and row.get("distress_type") not in (
            "BANKRUPTCY", "PROBATE",
        ):
            return self._mk_decision("HOLD_FOR_DATA", 0,
                                       "Missing property_address (non-court lead)", "none", 1.0)

        # Rule 2: business owner
        owner = (row.get("owner_name_records") or row.get("full_name") or "").upper()
        if owner_class in ("business", "government", "religious_or_education", "healthcare"):
            return self._mk_decision("REJECT_BUSINESS", 0,
                                       f"owner_class={owner_class}", "none", 1.0,
                                       flags=["NON_HOMEOWNER"])
        # Even without owner_class set: catch obvious LLC/INC suffixes
        if any(suffix in f" {owner} " for suffix in (
            " LLC ", " INC ", " CORP ", " CO ", " COMPANY ", " LP ", " LIMITED ",
        )):
            return self._mk_decision("REJECT_BUSINESS", 0,
                                       "Owner name contains LLC/INC/Corp suffix",
                                       "none", 0.95, flags=["NON_HOMEOWNER"])

        # Rule 3: AVM out of residential range
        pv = row.get("property_value")
        if pv is not None:
            try:
                pv_n = float(pv)
                if pv_n < 20000 or pv_n > 5000000:
                    return self._mk_decision(
                        "ESCALATE_TO_PATRICK", 25,
                        f"property_value=${pv_n:,.0f} outside residential range",
                        "none", 0.9, flags=["ANOMALOUS_AVM"],
                    )
            except (ValueError, TypeError):
                pass

        # Rule 4: foreclosed in past
        sale_date = row.get("trustee_sale_date")
        if sale_date:
            try:
                sale_dt = datetime.fromisoformat(str(sale_date)[:10])
                if sale_dt.date() < datetime.now(timezone.utc).date():
                    days_past = (datetime.now(timezone.utc).date() - sale_dt.date()).days
                    if days_past > 30:
                        return self._mk_decision(
                            "REJECT_FORECLOSED", 0,
                            f"trustee_sale_date {days_past}d past",
                            "none", 1.0, flags=["FORECLOSURE_PASSED"],
                        )
            except (ValueError, TypeError):
                pass

        # Rule 5: underwater
        mb = row.get("mortgage_balance")
        if pv is not None and mb is not None:
            try:
                pv_n, mb_n = float(pv), float(mb)
                if pv_n > 0 and (mb_n / pv_n) >= 0.95:
                    return self._mk_decision(
                        "REJECT_NO_EQUITY", 0,
                        f"underwater: ${mb_n:,.0f} / ${pv_n:,.0f} = {mb_n/pv_n:.0%}",
                        "none", 0.85, flags=["UNDERWATER"],
                    )
            except (ValueError, TypeError):
                pass

        # Rule 6: very-clear high-equity hot lead, full data — auto-promote
        skip_trace = pm.get("skip_trace") or {}
        if (pv and mb and float(pv) > 0 and float(mb) / float(pv) <= 0.50
                and row.get("phone")
                and owner_class == "homeowner"
                and row.get("distress_type") in ("PRE_FORECLOSURE", "BANKRUPTCY", "PROBATE", "TAX_LIEN")):
            score = self._compute_priority(row)
            if score >= 80:
                return self._mk_decision(
                    "PROMOTE_HOT", score,
                    f"high-equity {row['distress_type']} + verified phone + clean homeowner",
                    "cold_call", 0.95,
                    flags=self._derive_flags(row),
                )

        # Otherwise — gray area, ask LLM
        return None

    @staticmethod
    def _mk_decision(action: str, score: int, reasoning: str,
                      outreach: str, confidence: float,
                      flags: Optional[List[str]] = None) -> Dict[str, Any]:
        return {
            "action": action,
            "priority_score": score,
            "reasoning": reasoning,
            "suggested_outreach": outreach,
            "confidence": confidence,
            "flags": flags or [],
        }

    @staticmethod
    def _derive_flags(row: Dict[str, Any]) -> List[str]:
        flags = []
        pm = row.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            return flags
        skip = pm.get("skip_trace") or {}
        stack = pm.get("distress_stack") or {}
        if skip.get("is_absentee_owner"):
            flags.append("ABSENTEE")
        if skip.get("is_out_of_state_owner"):
            flags.append("OUT_OF_STATE")
        if stack.get("is_stacked"):
            flags.append("STACKED_DISTRESS")
        sale_date = row.get("trustee_sale_date")
        if sale_date:
            try:
                days_to_sale = (datetime.fromisoformat(str(sale_date)[:10]).date()
                                - datetime.now(timezone.utc).date()).days
                if 0 <= days_to_sale <= 14:
                    flags.append("URGENT_SALE")
            except Exception:
                pass
        return flags

    def _compute_priority(self, row: Dict[str, Any]) -> int:
        """Quick rule-based priority_score (0-100)."""
        score = 0
        pm = row.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}

        # DTS urgency
        sale_date = row.get("trustee_sale_date")
        if sale_date:
            try:
                d = (datetime.fromisoformat(str(sale_date)[:10]).date()
                     - datetime.now(timezone.utc).date()).days
                if 0 <= d <= 7:
                    score += 25
                elif 0 <= d <= 14:
                    score += 15
                elif 0 <= d <= 30:
                    score += 8
            except Exception:
                pass

        # Equity
        pv, mb = row.get("property_value"), row.get("mortgage_balance")
        if pv and mb:
            try:
                pv_n, mb_n = float(pv), float(mb)
                if pv_n > 0:
                    eq = (pv_n - mb_n) / pv_n
                    if eq >= 0.50:
                        score += 20
                    elif eq >= 0.30:
                        score += 14
                    elif eq >= 0.15:
                        score += 6
            except Exception:
                pass

        # Distress severity
        dt = row.get("distress_type")
        score += {
            "BANKRUPTCY": 18, "PRE_FORECLOSURE": 16, "TAX_LIEN": 12,
            "PROBATE": 12, "FSBO": 8, "CODE_VIOLATION": 6, "REO": 4,
        }.get(dt, 0)

        # Stacked
        stack = pm.get("distress_stack") or {}
        if stack.get("signal_count", 0) >= 3:
            score += 15
        elif stack.get("signal_count", 0) >= 2:
            score += 8

        # Absentee
        skip = pm.get("skip_trace") or {}
        if skip.get("is_out_of_state_owner"):
            score += 10
        elif skip.get("is_absentee_owner"):
            score += 5

        # Phone freshness
        if row.get("phone"):
            score += 5

        # Mortgage confidence boost
        me = pm.get("mortgage_estimate") or {}
        if me.get("confidence", 0) >= 0.7:
            score += 3

        return min(100, max(0, score))

    # ── LLM call ────────────────────────────────────────────────────────────

    def _llm_decide(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        msg = build_user_message(row)
        # OpenAI chat completion with JSON-mode response.
        # gpt-5-mini auto-caches the system-prompt prefix (no explicit
        # cache_control directive needed); identical system prompts
        # across calls are billed at the cached-input rate.
        resp = self._client.chat.completions.create(
            model=self.model,
            max_completion_tokens=self.max_output_tokens,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": msg},
            ],
        )
        self._llm_calls += 1
        usage = getattr(resp, "usage", None)
        if usage is not None:
            self._llm_input_tokens_total += getattr(usage, "prompt_tokens", 0) or 0
            self._llm_output_tokens_total += getattr(usage, "completion_tokens", 0) or 0

        # Parse the JSON from the response
        choice = resp.choices[0] if getattr(resp, "choices", None) else None
        if choice is None or not choice.message or not choice.message.content:
            self.logger.warning("  LLM returned empty response")
            return None
        text = choice.message.content.strip()
        # JSON-mode usually returns clean JSON, but strip markdown fences just in case
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text
        if text.endswith("```"):
            text = text.rsplit("\n", 1)[0] if "\n" in text else text
        text = text.strip()
        try:
            decision = json.loads(text)
        except json.JSONDecodeError:
            import re
            m = re.search(r"\{[\s\S]+\}", text)
            if not m:
                self.logger.warning(f"  LLM returned non-JSON: {text[:200]}")
                return None
            try:
                decision = json.loads(m.group(0))
            except json.JSONDecodeError:
                self.logger.warning(f"  LLM JSON parse failed: {text[:200]}")
                return None

        # Validate required keys
        required = ("action", "priority_score", "reasoning",
                    "suggested_outreach", "confidence")
        if not all(k in decision for k in required):
            self.logger.warning(f"  LLM response missing keys: {decision}")
            return None

        # Clamp priority_score to 0-100
        try:
            decision["priority_score"] = max(0, min(100, int(decision["priority_score"])))
        except (TypeError, ValueError):
            decision["priority_score"] = 50
        decision.setdefault("flags", [])
        return decision

    # ── Persistence ─────────────────────────────────────────────────────────

    def _write_decision(self, client, lead_id: str, decision: Dict[str, Any],
                         source: str) -> None:
        """Write decision to lead row + record provenance."""
        # Find which table
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            try:
                # Update phone_metadata.decision_engine + priority_score column
                # if it exists. Otherwise stash in phone_metadata only.
                update: Dict[str, Any] = {}
                # priority_score column may or may not exist yet. If migration
                # has been applied, write to it. If not, the update will fail
                # silently and we still have phone_metadata.
                # Try the full update first; fall back if priority_score doesn't exist.
                meta_payload = {
                    "decision_engine": {
                        "action": decision["action"],
                        "priority_score": decision["priority_score"],
                        "reasoning": decision["reasoning"],
                        "suggested_outreach": decision["suggested_outreach"],
                        "confidence": decision["confidence"],
                        "flags": decision.get("flags", []),
                        "source": source,
                        "decided_at": datetime.now(timezone.utc).isoformat(),
                    }
                }
                # Read existing meta + merge
                existing = (
                    client.table(table)
                    .select("phone_metadata")
                    .eq("id", lead_id)
                    .limit(1)
                    .execute()
                )
                rows = getattr(existing, "data", None) or []
                if not rows:
                    continue
                em = rows[0].get("phone_metadata") or {}
                if not isinstance(em, dict):
                    em = {}
                em.update(meta_payload)
                update["phone_metadata"] = em

                # Try with priority_score column (may not exist)
                try:
                    update_with_score = {**update, "priority_score": decision["priority_score"]}
                    client.table(table).update(update_with_score).eq("id", lead_id).execute()
                except Exception:
                    # Column not present — fall back to just phone_metadata
                    client.table(table).update(update).eq("id", lead_id).execute()

                if table == "homeowner_requests":
                    record_field(client, lead_id, "decision_engine_action",
                                  decision["action"], source,
                                  confidence=decision.get("confidence", 0),
                                  metadata={"score": decision["priority_score"],
                                            "reasoning": decision["reasoning"]})
                return
            except Exception as e:
                self.logger.debug(f"  write attempt on {table} failed: {e}")
                continue


def run() -> dict:
    bot = DecisionEngineBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
