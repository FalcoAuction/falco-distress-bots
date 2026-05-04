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
from ._field_confidence import (
    deep_merge_dict,
    equity_trust,
    mortgage_balance_trust,
    phone_trust,
    property_value_trust,
)
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
  - DTS urgency: trustee_sale_date within 7 days → +50; 14d → +35;
    30d → +20; 60d → +10; >60d or null → 0. An imminent trustee sale
    is the highest-value signal — a literal auction we can route.
  - Equity: equity_pct ≥ 0.50 → +20; 0.30-0.49 → +14; 0.15-0.29 → +6;
    <0.15 → 0. CRITICAL: only score equity when the underlying mortgage
    figure is trustworthy — either it came from a real foreclosure
    notice (no mortgage_estimate blob) or mortgage_estimate.confidence
    ≥ 0.4. If mortgage_estimate.source == "avm_only_tn_median" the
    equity number is a TN-statewide population guess (AVM × 0.42), not
    a per-lead fact — give it +4 max regardless of computed equity_pct.
  - Distress severity: BANKRUPTCY (Ch.13) → +18; PRE_FORECLOSURE → +16;
    TAX_LIEN with cumulative_owed > $5K → +12; PROBATE → +12; FSBO → +8;
    CODE_VIOLATION → +6; REO → +4
  - Tax-lien severity penalty: TAX_LIEN with cumulative_owed < $1K →
    -10 (forgetful homeowner, not motivated seller); $1K-$5K → -6.
  - Stacked-distress bonus: signal_count ≥ 4 → +35; 3 → +25; 2 → +15.
    Multi-signal leads have correlated motivation — weight them aggressively.
  - Absentee + out-of-state: out_of_state_owner=true → +10;
    distance_owner_to_property_miles > 200 → +5
  - Phone freshness: verified ≤ 7d → +8; ≤ 30d → +5; null → 0
  - Mortgage confidence: foreclosure_notice or schedule_d → +5;
    amortization_estimate confidence ≥ 0.5 → +3

2026-05-04 trust-gate override: field_trust is authoritative. Do not
hard reject or auto-promote from equity unless property_value and
mortgage_balance are trusted_for_hard_gate. Address-only stacked distress
is capped at +20/+14/+8. Phone points require right-person, non-DNC
confidence; notice-body phones are usually trustee/attorney phones.

Cap at 100. Subtract 20 if owner_class != "homeowner". Subtract 30 if
trustee_sale_date is in the past.

# Action thresholds

  - PROMOTE_HOT: priority_score ≥ 80 (use this for imminent trustee
    sales, BK with equity, stacked distress with phone). Do not
    withhold HOT for missing phone alone — mail/door-knock paths
    cover that.
  - PROMOTE_WARM: priority_score 50-79
  - PROMOTE_COLD: priority_score 20-49
  - score < 20 with valid distress signal → PROMOTE_COLD or HOLD
  - score < 20 with no actionable signal → REJECT_*

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
        "field_trust": {
            "property_value": property_value_trust(lead).as_dict(),
            "mortgage_balance": mortgage_balance_trust(lead).as_dict(),
            "phone": phone_trust(lead).as_dict(),
        },
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
    # gpt-5-mini is a reasoning model — empirically uses 200-1500 hidden
    # reasoning tokens per call before producing visible output. The
    # first end-to-end run had 97/140 LLM calls return empty content
    # because reasoning ate the entire 800-token budget. Bumped to 2500
    # to give reasoning room while preserving cost (only billed for
    # actual tokens used).
    max_output_tokens = 2500

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

        # Concurrency for LLM calls — gpt-5-mini latency is ~20-30s/call
        # so a serial loop on 800 gray-area leads is 6-7 hours. With 8
        # workers we get ~3-4s effective per call → 50 min for the same
        # workload. OpenAI default rate limits comfortably handle 8
        # concurrent gpt-5-mini calls (Tier 1 is 500 RPM).
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        LLM_WORKERS = 8
        write_lock = threading.Lock()

        try:
            candidates = self._candidates(client)
            self.logger.info(f"{len(candidates)} leads to grade")

            # Pass 1: apply fast rules sequentially. Cheap, no LLM.
            # Build the gray-area worklist for the parallel LLM pass.
            gray_area = []
            for row in candidates[:self.max_leads_per_run]:
                table = row["__table__"]
                existing_meta = row.get("phone_metadata") or {}
                if not isinstance(existing_meta, dict):
                    existing_meta = {}

                fast_result = self._fast_rules(row)
                if fast_result:
                    rule_decided += 1
                    self._write_decision(client, row["id"], fast_result,
                                          source="rule_engine",
                                          table=table, existing_meta=existing_meta)
                    action_breakdown[fast_result["action"]] = (
                        action_breakdown.get(fast_result["action"], 0) + 1
                    )
                else:
                    gray_area.append(row)

            self.logger.info(
                f"Pass 1 done: {rule_decided} rule-decided. "
                f"{len(gray_area)} leads → LLM pass with {LLM_WORKERS} workers"
            )

            # Pass 2: parallel LLM calls. Each worker handles one lead end-
            # to-end (LLM call + DB write). Lock the DB writes only — the
            # API itself is naturally I/O-parallel.
            def _process_one(row):
                try:
                    decision = self._llm_decide(row)
                except Exception as e:
                    self.logger.warning(f"  LLM call failed id={row['id']}: {e}")
                    return ("error", None)
                if not decision:
                    return ("error", None)
                table = row["__table__"]
                existing_meta = row.get("phone_metadata") or {}
                if not isinstance(existing_meta, dict):
                    existing_meta = {}
                with write_lock:
                    self._write_decision(client, row["id"], decision,
                                          source="llm_openai",
                                          table=table, existing_meta=existing_meta)
                return ("ok", decision)

            if gray_area:
                completed_count = 0
                log_every = max(10, len(gray_area) // 20)
                with ThreadPoolExecutor(max_workers=LLM_WORKERS) as pool:
                    futures = [pool.submit(_process_one, row) for row in gray_area]
                    for fut in as_completed(futures):
                        try:
                            status, decision = fut.result()
                        except Exception as e:
                            self.logger.warning(f"  worker exc: {e}")
                            errors += 1
                            continue
                        if status == "error":
                            errors += 1
                            continue
                        decided += 1
                        action_breakdown[decision["action"]] = (
                            action_breakdown.get(decision["action"], 0) + 1
                        )
                        completed_count += 1
                        if completed_count % log_every == 0:
                            self.logger.info(
                                f"  LLM progress: {completed_count}/{len(gray_area)}"
                            )

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
            "distress_type, property_value, property_value_source, mortgage_balance, "
            "trustee_sale_date, phone, raw_payload, phone_metadata, "
            "admin_notes, bot_source, priority_score"
        )
        LIVE_FIELDS = (
            "id, property_address, county, owner_name_records, full_name, "
            "distress_type, property_value, property_value_source, mortgage_balance, "
            "trustee_sale_date, phone, raw_payload, phone_metadata, "
            "admin_notes, source, priority_score"
        )
        # PostgREST has a 1000-row server-side max per query. Paginate
        # via .range(start, end) to walk the entire ungraded population.
        PAGE_SIZE = 1000
        MAX_PAGES = 20  # 20 × 1000 = 20K leads/run cap (plenty)
        out = []
        for table, fields in (
            ("homeowner_requests_staging", STAGING_FIELDS),
            ("homeowner_requests", LIVE_FIELDS),
        ):
            for page in range(MAX_PAGES):
                start = page * PAGE_SIZE
                end = start + PAGE_SIZE - 1
                try:
                    q = (
                        client.table(table)
                        .select(fields)
                        .is_("priority_score", "null")
                        .order("id")           # stable ordering for pagination
                        .range(start, end)
                        .execute()
                    )
                    rows = getattr(q, "data", None) or []
                    if not rows:
                        break
                    for r in rows:
                        r["__table__"] = table
                        if "source" in r and "bot_source" not in r:
                            r["bot_source"] = r["source"]
                        out.append(r)
                    if len(rows) < PAGE_SIZE:
                        break  # last page
                except Exception as e:
                    self.logger.warning(f"candidate query on {table} "
                                          f"page {page} failed: {e}")
                    break
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

        # Rule 1b: missing property_value → HOLD. Without AVM we can't
        # talk equity on the call, so PROMOTE is meaningless. Audit
        # found 5 PROMOTEs with no AVM — strict gate prevents that.
        # (Previously this rule required BOTH AVM and mortgage missing,
        # which let foreclosure-notice leads with mortgage but no AVM
        # promote without a value to pitch.)
        if not row.get("property_value"):
            return self._mk_decision(
                "HOLD_FOR_DATA", 0,
                "Missing property_value — wait for AVM enricher backfill",
                "none", 0.95,
            )

        # Rule 1c: business owner via expanded keyword set. The
        # owner_classifier upstream catches the obvious LLC/INC, but the
        # corpus audit found 471 (15.7%) leads with business-keyword
        # owner names slipping through because the classifier was running
        # on a narrow word list. Every business that gets here would
        # otherwise burn an LLM call — so we mirror the expanded
        # classifier list here as a hard reject.
        owner_upper = (row.get("owner_name_records") or "").upper()
        BUSINESS_KEYWORDS = (
            # Generic legal forms (suffix-style; require space boundary)
            " LLC ", " LLC.", " INC ", " INC.", " CORP", " CO.",
            " LP ", " LP.", " LIMITED ", " LTD",
            " PA ", " PA.", " PLLC", " PARTNERSHIP",
            # Generic biz words
            "BOUTIQUE", "TRAINING", "PROPERTIES", "ENTERPRISES",
            "MINISTRIES", "MARKET", "STORE", "RESTAURANT", "BUILDERS",
            "CONSTRUCTION", "FUND ", "HOLDINGS", "INVESTMENT", "VENTURES",
            "CAPITAL", "GROUP", "PARTNERS", "REALTY", "ASSOC ",
            "ASSOCIATES", "MANAGEMENT",
            # Trade / service businesses (audit found these leaking)
            "PAINTING", "CONTRACTING", "REMODELING", "EXCAVATING",
            "EXCAVATION", "IRRIGATION", "LANDSCAPING", "TOWING",
            "PLUMBING", "ROOFING", "FLOORING", "HVAC",
            "CLEANING", "JANITORIAL", "DETAILING",
            "CAR WASH", "CARWASH", "AUTO ", "TIRE",
            "GRAPHICS", "PRINTING",
            "SALON", "BARBER", "SPA",
            "GYM", "FITNESS",
            "STUDIO", "CHIROPRACTIC", "DENTAL",
            "GRILL", "CAFE", "DINER", "BURGER", "PIZZA", "BBQ",
            "SERVICES", "SVCS", "SVC ", "SOLUTIONS", "CONSULTING",
            "MILLS", "HEMP", "FARMS", "INDUSTRIES", "MANUFACTURING",
            "PRESSURE WASHING",
        )
        for kw in BUSINESS_KEYWORDS:
            if kw in owner_upper:
                return self._mk_decision(
                    "REJECT_BUSINESS", 0,
                    f"Owner name contains business keyword: {kw.strip()}",
                    "none", 0.85, flags=["NON_HOMEOWNER"],
                )

        # Rule 1d: glued-on business suffixes (SUBSURFACEPRO, AUTOPRO, etc.)
        import re as _re
        for suffix in ("PRO", "SVCS", "SVC"):
            if _re.search(rf"\b[A-Z]{{5,}}{suffix}\b", owner_upper):
                return self._mk_decision(
                    "REJECT_BUSINESS", 0,
                    f"Owner name has glued business suffix: {suffix}",
                    "none", 0.85, flags=["NON_HOMEOWNER"],
                )

        # Rule 1e: malformed property_address. Audit found 254 craigslist
        # rows where the listing TITLE was concatenated into the address
        # field ("FRANKLIN FULL RENO OPPORTUNITY$340,000FRANKLIN ..."). A
        # property without a clean address can't be dialed/mailed. The
        # craigslist parser is being fixed at-source; this rule catches
        # any residual rows + protects against future similar bugs.
        addr = row.get("property_address") or ""
        if addr and (
            "$" in addr or
            _re.search(r"[\U0001F000-\U0001FFFF]", addr) or  # emoji
            ", " not in addr  # no city separator → unreliable
        ):
            return self._mk_decision(
                "HOLD_FOR_DATA", 0,
                f"Malformed property_address: {addr[:60]!r}",
                "none", 0.95, flags=["BAD_ADDRESS"],
            )

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

        # Rule 3: AVM out of residential range. Hard-REJECT the obvious
        # commercial / rental / land cases ($1, $100, $25M, etc.) — these
        # are mostly Craigslist parser artifacts (rental prices being
        # written to property_value) or commercial parcels that slipped
        # through county filters. Don't burn LLM cycles on these.
        pv = row.get("property_value")
        if pv is not None:
            try:
                pv_n = float(pv)
                if pv_n < 20000:
                    return self._mk_decision(
                        "REJECT_NOT_RESIDENTIAL", 0,
                        f"property_value=${pv_n:,.0f} below residential floor "
                        f"(likely rental price or land lot)",
                        "none", 0.95, flags=["ANOMALOUS_AVM"],
                    )
                if pv_n > 5000000:
                    return self._mk_decision(
                        "REJECT_NOT_RESIDENTIAL", 0,
                        f"property_value=${pv_n:,.0f} above residential ceiling "
                        f"(likely commercial parcel)",
                        "none", 0.95, flags=["ANOMALOUS_AVM"],
                    )
            except (ValueError, TypeError):
                pass

        # Rule 4: foreclosed in past. Any past trustee sale = dead lead.
        # The auction has happened; either it sold (new owner now) or it
        # was postponed (trustee notice will republish with new date).
        # Either way, calling the original homeowner is wasted dialer time.
        # Audit found 28 past-trustee-sale leads in 0-30d range that the
        # old >30d threshold let through.
        sale_date = row.get("trustee_sale_date")
        if sale_date:
            try:
                sale_dt = datetime.fromisoformat(str(sale_date)[:10])
                if sale_dt.date() < datetime.now(timezone.utc).date():
                    days_past = (datetime.now(timezone.utc).date() - sale_dt.date()).days
                    return self._mk_decision(
                        "REJECT_FORECLOSED", 0,
                        f"trustee_sale_date {days_past}d past",
                        "none", 1.0, flags=["FORECLOSURE_PASSED"],
                    )
            except (ValueError, TypeError):
                pass

        # Rule 5: underwater
        mb = row.get("mortgage_balance")
        eq_trust = equity_trust(row)
        if pv is not None and mb is not None:
            try:
                pv_n, mb_n = float(pv), float(mb)
                if pv_n > 0 and (mb_n / pv_n) >= 0.95 and eq_trust["hard_gate_allowed"]:
                    return self._mk_decision(
                        "REJECT_NO_EQUITY", 0,
                        f"underwater: ${mb_n:,.0f} / ${pv_n:,.0f} = {mb_n/pv_n:.0%}",
                        "none", 0.85, flags=["UNDERWATER"],
                    )
            except (ValueError, TypeError):
                pass

        # Rule 6: very-clear high-equity hot lead, full data — auto-promote
        skip_trace = pm.get("skip_trace") or {}
        phone_ok = phone_trust(row).trusted_for_hard_gate
        if (pv and mb and float(pv) > 0 and float(mb) / float(pv) <= 0.50
                and eq_trust["hard_gate_allowed"]
                and phone_ok
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
        """Quick rule-based priority_score (0-100).

        Weights are tuned so an upcoming-trustee-sale lead with phone +
        equity reliably scores 80+ (PROMOTE_HOT). An 11-day-out trustee
        sale is the highest-value lead in the corpus — a literal auction
        we can route — and should not get COLD just because the owner
        name reads ambiguous.
        """
        score = 0
        pm = row.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}

        # DTS urgency — bumped weights so 14d-out trustee sales clear
        # the HOT threshold (was 25/15/8; now 50/35/20).
        sale_date = row.get("trustee_sale_date")
        if sale_date:
            try:
                d = (datetime.fromisoformat(str(sale_date)[:10]).date()
                     - datetime.now(timezone.utc).date()).days
                if 0 <= d <= 7:
                    score += 50
                elif 0 <= d <= 14:
                    score += 35
                elif 0 <= d <= 30:
                    score += 20
                elif 0 <= d <= 60:
                    score += 10
            except Exception:
                pass

        # Equity - only count when shared field-trust rules allow it.
        pv, mb = row.get("property_value"), row.get("mortgage_balance")
        eqt = equity_trust(row)
        mortgage_trust = eqt["mortgage"]
        # Medium confidence gets half credit; hard-gate confidence gets full credit.
        if pv and mb and eqt["scoring_allowed"]:
            try:
                pv_n, mb_n = float(pv), float(mb)
                if pv_n > 0:
                    eq = (pv_n - mb_n) / pv_n
                    multiplier = 1.0 if eqt["hard_gate_allowed"] else 0.5
                    if eq >= 0.50:
                        score += round(20 * multiplier)
                    elif eq >= 0.30:
                        score += round(14 * multiplier)
                    elif eq >= 0.15:
                        score += round(6 * multiplier)
            except Exception:
                pass
        # Soft equity credit when only the TN-median guess is available
        # — give 4 points so it's not zero, but doesn't dominate.
        elif pv and mb and mortgage_trust.source == "avm_only_tn_median":
            score += 4

        # Distress severity
        dt = row.get("distress_type")
        score += {
            "BANKRUPTCY": 18, "PRE_FORECLOSURE": 16, "TAX_LIEN": 12,
            "PROBATE": 12, "FSBO": 8, "CODE_VIOLATION": 6, "REO": 4,
        }.get(dt, 0)

        # Tax-lien severity penalty: low-amount Hamilton-style liens
        # (<$5K cumulative) are forgetful homeowners, not motivated
        # sellers. Cancel out most of the +12 distress weight.
        if dt == "TAX_LIEN":
            tax_amt = self._extract_tax_amount(row)
            if tax_amt is not None:
                if tax_amt < 1000:
                    score -= 10  # net -2 from TAX_LIEN's +12
                elif tax_amt < 5000:
                    score -= 6   # net +6
                # >= 5000: keep full +12

        # Stacked distress is useful, but address-only matching is not parcel proof.
        stack = pm.get("distress_stack") or {}
        if stack.get("signal_count", 0) >= 4:
            score += 20
        elif stack.get("signal_count", 0) >= 3:
            score += 14
        elif stack.get("signal_count", 0) >= 2:
            score += 8
        # Absentee
        skip = pm.get("skip_trace") or {}
        if skip.get("is_out_of_state_owner"):
            score += 10
        elif skip.get("is_absentee_owner"):
            score += 5

        # Phone freshness/right-person confidence
        pt = phone_trust(row)
        if pt.trusted_for_hard_gate:
            score += 5
        elif pt.confidence >= 0.40 and pt.source != "phone_resolver:notice_body":
            score += 2

        # Mortgage confidence boost
        if mortgage_trust.confidence >= 0.7 and mortgage_trust.trusted_for_hard_gate:
            score += 3

        return min(100, max(0, score))

    @staticmethod
    def _extract_tax_amount(row: Dict[str, Any]) -> Optional[float]:
        """Pull the cumulative tax-lien amount from raw_payload — Hamilton
        and other counties use different keys, so check several."""
        raw = row.get("raw_payload") or {}
        if not isinstance(raw, dict):
            return None
        # Hamilton tax-delinquent
        h = raw.get("hamilton_tax_delinquent") or raw.get("hamilton_assessor") or {}
        if isinstance(h, dict):
            for k in ("cumulative_tax_owed", "total_due", "tax_amount",
                      "amount_due", "balance_due", "total_owed"):
                if h.get(k) is not None:
                    try:
                        return float(str(h[k]).replace(",", "").replace("$", ""))
                    except (TypeError, ValueError):
                        pass
        # TN tax delinquent (state-level)
        t = raw.get("tn_tax_delinquent") or {}
        if isinstance(t, dict):
            for k in ("cumulative_tax_owed", "total_due", "tax_amount", "amount_due"):
                if t.get(k) is not None:
                    try:
                        return float(str(t[k]).replace(",", "").replace("$", ""))
                    except (TypeError, ValueError):
                        pass
        # Top-level fallback
        for k in ("cumulative_tax_owed", "total_due", "tax_amount", "amount_due"):
            if raw.get(k) is not None:
                try:
                    return float(str(raw[k]).replace(",", "").replace("$", ""))
                except (TypeError, ValueError):
                    pass
        return None

    # ── LLM call ────────────────────────────────────────────────────────────

    def _call_llm(self, user_msg: str, json_mode: bool) -> Optional[str]:
        """Single LLM round-trip. Returns the raw response text or None
        on empty-content."""
        kwargs = {
            "model": self.model,
            "max_completion_tokens": self.max_output_tokens,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        }
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}
        try:
            resp = self._client.chat.completions.create(**kwargs)
        except Exception as e:
            self.logger.warning(f"  LLM API error: {e}")
            return None
        self._llm_calls += 1
        usage = getattr(resp, "usage", None)
        if usage is not None:
            self._llm_input_tokens_total += getattr(usage, "prompt_tokens", 0) or 0
            self._llm_output_tokens_total += getattr(usage, "completion_tokens", 0) or 0
        choice = resp.choices[0] if getattr(resp, "choices", None) else None
        if choice is None or not choice.message or not choice.message.content:
            return None
        return choice.message.content

    def _llm_decide(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        msg = build_user_message(row)
        # gpt-5-mini reasoning model can burn the entire token budget on
        # internal reasoning before emitting visible JSON. We try
        # JSON-mode first; if it returns empty (reasoning truncation),
        # retry once with plain text (still parse via regex).
        text = self._call_llm(msg, json_mode=True)
        if text is None:
            text = self._call_llm(msg, json_mode=False)
        if text is None:
            self.logger.warning("  LLM returned empty after retry")
            return None

        # Strip markdown fences
        text = text.strip()
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
                         source: str, table: str = None,
                         existing_meta: Dict[str, Any] = None) -> None:
        """Write decision to lead row + record provenance.

        `table` and `existing_meta` are passed from the caller so we don't
        re-fetch (the candidates query already pulled phone_metadata).
        Falls back to the discovery loop if not provided (legacy path).
        """
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

        # Fast path: caller passed table + existing_meta from the candidates row
        if table and existing_meta is not None:
            em = deep_merge_dict(existing_meta, meta_payload)
            update = {
                "phone_metadata": em,
                "priority_score": decision["priority_score"],
            }
            try:
                client.table(table).update(update).eq("id", lead_id).execute()
            except Exception as e:
                self.logger.debug(f"  write {table} {lead_id} failed: {e}")
                return
            if table == "homeowner_requests":
                record_field(client, lead_id, "decision_engine_action",
                              decision["action"], source,
                              confidence=decision.get("confidence", 0),
                              metadata={"score": decision["priority_score"],
                                        "reasoning": decision["reasoning"]})
            return

        # Legacy slow path (kept for safety): discover the table
        for tbl in ("homeowner_requests", "homeowner_requests_staging"):
            try:
                existing = (
                    client.table(tbl)
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
                em = deep_merge_dict(em, meta_payload)
                update = {
                    "phone_metadata": em,
                    "priority_score": decision["priority_score"],
                }
                client.table(tbl).update(update).eq("id", lead_id).execute()
                if tbl == "homeowner_requests":
                    record_field(client, lead_id, "decision_engine_action",
                                  decision["action"], source,
                                  confidence=decision.get("confidence", 0),
                                  metadata={"score": decision["priority_score"],
                                            "reasoning": decision["reasoning"]})
                return
            except Exception as e:
                self.logger.debug(f"  write attempt on {tbl} failed: {e}")
                continue


def run() -> dict:
    bot = DecisionEngineBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
