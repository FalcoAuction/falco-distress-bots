"""Middle-TN-focused BatchData skip-trace.

Wraps BatchDataSkipTraceBot but constrains candidate selection to the
Middle TN core counties — Davidson, Williamson, Sumner, Rutherford,
Wilson — plus stretch counties Maury, Montgomery.

Goal (model-proof phase): get a phone on every focus lead so the
downstream Twilio Lookup + dial-probe can run on a clean superset.

Run via:
  python -m src.bots.middle_tn_skiptrace_bot

Env knobs:
  FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN  (default 300 — enough for
                                          all 281 missing-phone focus
                                          leads, with margin)
  FALCO_BATCHDATA_SKIPTRACE_SAMPLE       (=1 to dry-run without writes)
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

from .batchdata_skip_trace_bot import BatchDataSkipTraceBot


CORE_COUNTIES = {"davidson", "williamson", "sumner", "rutherford", "wilson"}
STRETCH_COUNTIES = {"maury", "montgomery"}
FOCUS_COUNTIES = CORE_COUNTIES | STRETCH_COUNTIES


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
        """Custom candidate query — filter to focus counties at the DB level.

        The county column has inconsistent casing/suffixes ('Davidson',
        'Davidson County', 'davidson') so we have to over-pull and
        normalize in Python — but constraining to no-phone + has-name +
        has-address up front cuts the wire data ~95%.
        """
        out: List[Dict[str, Any]] = []
        PAGE = 1000
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            page = 0
            while True:
                try:
                    q = (
                        client.table(table)
                        .select(
                            "id, property_address, owner_name_records, "
                            "full_name, county, priority_score, "
                            "phone_metadata, distress_type"
                        )
                        .is_("phone", "null")
                        .not_.is_("owner_name_records", "null")
                        .not_.is_("property_address", "null")
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
    # Default to a higher per-run cap than the parent (300 vs 50) since
    # we have 281 missing-phone focus leads.
    if not os.environ.get("FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN"):
        os.environ["FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN"] = "300"
    bot = MiddleTnSkipTraceBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
