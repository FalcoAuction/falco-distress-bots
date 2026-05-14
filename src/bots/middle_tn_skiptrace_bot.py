"""Middle-TN-focused BatchData skip-trace.

Wraps BatchDataSkipTraceBot but constrains candidate selection to the
Middle TN core counties — Davidson, Williamson, Sumner, Rutherford,
Wilson — plus stretch counties Maury, Montgomery, Cheatham, Robertson,
Dickson.

Distress-type gate (2026-05-14): restricted to FORECLOSURE-FAMILY only
by default. Patrick's call after a 60-day-window cost audit — CV /
demolition / probate / FSBO / tax-lien convert at much lower rates and
were burning BatchData credit disproportionately to their value. Set
FALCO_SKIPTRACE_ALL_DISTRESS=1 to disable the filter and skip-trace
the entire MTN pool (e.g. after a cost-budget reset).

Goal: get a phone on every active-foreclosure-family focus lead so the
downstream Twilio Lookup + dial-probe can run on a clean superset.

Run via:
  python -m src.bots.middle_tn_skiptrace_bot

Env knobs:
  FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN  (default 300 — enough for
                                          all 281 missing-phone focus
                                          leads, with margin)
  FALCO_BATCHDATA_SKIPTRACE_SAMPLE       (=1 to dry-run without writes)
  FALCO_SKIPTRACE_ALL_DISTRESS           (=1 to remove foreclosure-only
                                          gate; default off)
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

from .batchdata_skip_trace_bot import BatchDataSkipTraceBot


CORE_COUNTIES = {"davidson", "williamson", "sumner", "rutherford", "wilson"}
STRETCH_COUNTIES = {"maury", "montgomery", "cheatham", "robertson", "dickson"}
FOCUS_COUNTIES = CORE_COUNTIES | STRETCH_COUNTIES

# Foreclosure-family distress types — only these consume BatchData
# credit unless FALCO_SKIPTRACE_ALL_DISTRESS=1.
FORECLOSURE_DISTRESS = {
    "PRE_FORECLOSURE", "PREFORECLOSURE", "TRUSTEE_NOTICE",
    "LIS_PENDENS", "SOT", "SUBSTITUTION_OF_TRUSTEE",
    "NOD", "NOTICE_OF_DEFAULT", "FORECLOSURE",
}


def _normalize_county(c: str) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


class MiddleTnSkipTraceBot(BatchDataSkipTraceBot):
    name = "middle_tn_skiptrace"
    description = (
        "BatchData skip-trace constrained to Middle TN focus counties "
        "(Davidson, Williamson, Sumner, Rutherford, Wilson, +Maury/"
        "Montgomery)"
    )

    def _candidates(self, client, max_per_run: int) -> List[Dict[str, Any]]:
        """Custom candidate query — filter to focus counties + foreclosure
        family at the DB level (county still needs Python normalization
        because of inconsistent 'Davidson' vs 'Davidson County' casing).

        Foreclosure-only gate is on by default (Patrick 2026-05-14).
        Set FALCO_SKIPTRACE_ALL_DISTRESS=1 to disable.
        """
        foreclosure_only = os.environ.get("FALCO_SKIPTRACE_ALL_DISTRESS") != "1"

        out: List[Dict[str, Any]] = []
        PAGE = 1000
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            page = 0
            while True:
                try:
                    builder = (
                        client.table(table)
                        .select(
                            "id, property_address, owner_name_records, "
                            "full_name, county, priority_score, "
                            "phone_metadata, distress_type"
                        )
                        .is_("phone", "null")
                        .not_.is_("owner_name_records", "null")
                        .not_.is_("property_address", "null")
                    )
                    if foreclosure_only:
                        builder = builder.in_(
                            "distress_type", list(FORECLOSURE_DISTRESS)
                        )
                    q = (
                        builder
                        .order("priority_score", desc=True)
                        .range(page * PAGE, (page + 1) * PAGE - 1)
                        .execute()
                    )
                    rows = getattr(q, "data", None) or []
                    if not rows:
                        break
                    for r in rows:
                        if _normalize_county(r.get("county")) not in FOCUS_COUNTIES:
                            continue
                        pm = r.get("phone_metadata") or {}
                        if isinstance(pm, dict):
                            owner_class = pm.get("owner_class")
                            if isinstance(owner_class, dict):
                                owner_class = (
                                    owner_class.get("class")
                                    or owner_class.get("value")
                                )
                            if owner_class in (
                                "business", "government",
                                "religious_or_education", "healthcare",
                            ):
                                continue
                            if pm.get("batchdata_skip_trace"):
                                continue
                        # Skip deceased/estate
                        addr_upper = (r.get("property_address") or "").upper()
                        owner_upper = (
                            (r.get("owner_name_records") or "")
                            + " "
                            + (r.get("full_name") or "")
                        ).upper()
                        if (
                            "ESTATE OF" in addr_upper
                            or "ESTATE OF" in owner_upper
                            or "DECEASED" in owner_upper
                        ):
                            continue
                        r["__table__"] = table
                        out.append(r)
                    if len(rows) < PAGE:
                        break
                    page += 1
                except Exception as e:
                    self.logger.warning(
                        f"candidate query on {table} page {page}: {e}"
                    )
                    break
        self.logger.info(
            f"middle_tn_skiptrace: {len(out)} focus-county candidates "
            f"(cap {max_per_run})"
        )
        return out[:max_per_run]


def run() -> dict:
    # Default cap raised to 700 (from 300) on 2026-05-09. After the
    # one-shot MTN promotion flush we have ~631 MTN leads in
    # live+staging missing phones; a single run after BatchData credit
    # top-up should sweep the full backlog. Daily cron caps re-tighten
    # naturally as backlog drains.
    if not os.environ.get("FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN"):
        os.environ["FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN"] = "700"
    bot = MiddleTnSkipTraceBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
