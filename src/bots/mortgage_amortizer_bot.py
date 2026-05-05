"""Mortgage amortizer — replaces "original principal = current balance"
with actual amortized balance for every defensible-mortgage lead.

The math: a loan with original principal P, rate r (monthly), term n
months, and elapsed e months has remaining balance:
  B = P × [(1+r)^n - (1+r)^e] / [(1+r)^n - 1]

Inputs (in priority order):
  1. mortgage_signal.amount + mortgage_signal.match_year + interest_rate
     + loan_term  (HMDA-anchored — uses exact rate + term)
  2. rod_lookup.original_principal + document_date  (ROD-verified — uses
     TN-average 30Y rate for the origination year)
  3. nashville_ledger extracted.original_principal + structured.trust_date
     (notice-extracted — uses TN-average rate)

Writes:
  phone_metadata.mortgage_balance_amortized = {
    original_principal, rate_pct, term_years, years_elapsed,
    current_balance, equity_estimate (vs property_value),
    method ('hmda' | 'tn_avg_rate'), confidence
  }
  Promotes current_balance to mortgage_balance column when confidence
  >= 0.65.

Run via:
  python -m src.bots.mortgage_amortizer_bot
"""
from __future__ import annotations

import os
import re
import sys
import traceback as tb
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from ._provenance import record_field


# TN-average 30Y fixed mortgage rates by origination year (Freddie Mac PMMS)
TN_AVG_30Y_RATES = {
    2007: 6.34, 2008: 6.04, 2009: 5.04, 2010: 4.69, 2011: 4.45,
    2012: 3.66, 2013: 3.98, 2014: 4.17, 2015: 3.85, 2016: 3.65,
    2017: 3.99, 2018: 4.54, 2019: 3.94, 2020: 3.11, 2021: 2.96,
    2022: 5.34, 2023: 6.81, 2024: 6.74, 2025: 6.85, 2026: 6.50,
}
DEFAULT_RATE = 6.50
DEFAULT_TERM_YEARS = 30


def amortize(
    original_principal: float,
    annual_rate_pct: float,
    term_years: int,
    years_elapsed: float,
) -> float:
    """Standard fixed-rate amortization. Returns remaining principal."""
    if original_principal <= 0:
        return 0.0
    if years_elapsed <= 0:
        return original_principal
    if years_elapsed >= term_years:
        return 0.0
    n = term_years * 12
    paid = min(int(years_elapsed * 12), n)
    r = (annual_rate_pct / 100.0) / 12.0
    if r == 0:
        return original_principal * (1 - paid / n)
    factor_n = (1 + r) ** n
    factor_k = (1 + r) ** paid
    monthly_payment = original_principal * r * factor_n / (factor_n - 1)
    remaining = original_principal * factor_k - monthly_payment * (factor_k - 1) / r
    return max(0.0, remaining)


def _to_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None


def _years_elapsed(origination_iso: str) -> Optional[float]:
    """Parse origination date and return years elapsed to today."""
    if not origination_iso:
        return None
    try:
        if len(origination_iso) >= 10:
            dt = datetime.fromisoformat(origination_iso[:10]).date()
        else:
            dt = datetime.strptime(origination_iso, "%Y").date()
        return (date.today() - dt).days / 365.25
    except Exception:
        return None


def _resolve_origination(pm: Dict[str, Any]) -> Dict[str, Any]:
    """Return the best available (principal, year, rate, term, source)
    triple from the lead's phone_metadata. Priority: HMDA > ROD > NL."""
    out: Dict[str, Any] = {}

    # Priority 1: HMDA (has rate + term)
    sig = pm.get("mortgage_signal") or {}
    if isinstance(sig, dict) and sig.get("source") == "hmda_match":
        principal = _to_float(sig.get("amount"))
        match_year = sig.get("match_year")
        rate = _to_float(sig.get("interest_rate"))
        term_str = sig.get("loan_term")
        term = _to_float(term_str)
        if principal and match_year:
            try:
                year_int = int(match_year)
            except (ValueError, TypeError):
                year_int = None
            if year_int:
                out = {
                    "original_principal": principal,
                    "origination_year": year_int,
                    "rate_pct": rate or TN_AVG_30Y_RATES.get(year_int, DEFAULT_RATE),
                    "term_years": int(term / 12) if term and term > 60 else DEFAULT_TERM_YEARS,
                    "method": "hmda" if rate else "hmda+tn_avg_rate",
                    "source": "hmda_match",
                    "confidence": sig.get("confidence", 0.65),
                }
                return out

    # Priority 2: ROD (has principal + doc date, no rate)
    rod = pm.get("rod_lookup") or {}
    if isinstance(rod, dict) and rod.get("original_principal") and rod.get("document_date"):
        principal = _to_float(rod["original_principal"])
        try:
            doc_date = rod["document_date"][:10]
            year_int = int(doc_date[:4])
        except Exception:
            year_int = None
        if principal and year_int:
            return {
                "original_principal": principal,
                "origination_year": year_int,
                "rate_pct": rod.get("rate_pct") or TN_AVG_30Y_RATES.get(year_int, DEFAULT_RATE),
                "term_years": DEFAULT_TERM_YEARS,
                "method": "rod+tn_avg_rate",
                "source": "ustitlesearch_rod",
                "confidence": 0.85,
            }

    # Priority 3: nashville_ledger extracted
    if isinstance(sig, dict) and sig.get("source") == "nashville_ledger_extracted":
        principal = _to_float(sig.get("amount"))
        # trust_date from raw_payload.structured (need to fetch from lead)
        # We'll handle this in the bot's loop by passing raw_payload.
        if principal:
            out = {
                "original_principal": principal,
                "method": "nashville_ledger+tn_avg_rate",
                "source": "nashville_ledger_extracted",
                "confidence": 0.85,
            }
            return out

    return out


class MortgageAmortizerBot(BotBase):
    name = "mortgage_amortizer"
    description = (
        "Amortizes original principal forward to today's current balance "
        "using exact HMDA rate (when available) or TN-average rate. "
        "Replaces 'mortgage_balance = original principal' on every "
        "defensible-mortgage lead."
    )
    throttle_seconds = 0
    expected_min_yield = 0
    max_leads_per_run = 1000

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        client = _supabase()
        if client is None:
            return self._fail(started, "no_supabase_client")

        attempted = 0
        amortized = 0
        skipped_no_origination = 0
        promoted = 0
        sample = os.environ.get("FALCO_AMORTIZER_SAMPLE") == "1"

        for table in ("homeowner_requests_staging", "homeowner_requests"):
            page = 0
            while True:
                try:
                    r = (
                        client.table(table)
                        .select(
                            "id, county, phone_metadata, mortgage_balance, "
                            "property_value, raw_payload, owner_name_records"
                        )
                        .range(page * 1000, (page + 1) * 1000 - 1)
                        .execute()
                    )
                    rows = getattr(r, "data", None) or []
                    if not rows:
                        break
                    for lead in rows:
                        pm = lead.get("phone_metadata") or {}
                        if not isinstance(pm, dict):
                            continue
                        # Only process defensible-source leads
                        is_rod = bool(pm.get("rod_lookup"))
                        sig = pm.get("mortgage_signal") or {}
                        is_hmda_anchored = (
                            isinstance(sig, dict)
                            and sig.get("source") == "hmda_match"
                            and (sig.get("sale_anchored") or sig.get("year_anchored"))
                        )
                        is_nl = (
                            isinstance(sig, dict)
                            and sig.get("source") == "nashville_ledger_extracted"
                        )
                        if not (is_rod or is_hmda_anchored or is_nl):
                            continue

                        attempted += 1
                        origination = _resolve_origination(pm)

                        # Special case: NL needs trust_date from raw_payload
                        if origination.get("source") == "nashville_ledger_extracted":
                            raw = lead.get("raw_payload") or {}
                            structured = raw.get("structured") or {} if isinstance(raw, dict) else {}
                            td = structured.get("trust_date")
                            if td:
                                m = re.search(r"/(\d{4})", str(td))
                                if m:
                                    yr = int(m.group(1))
                                    origination["origination_year"] = yr
                                    origination["rate_pct"] = TN_AVG_30Y_RATES.get(yr, DEFAULT_RATE)
                                    origination["term_years"] = DEFAULT_TERM_YEARS

                        if not origination.get("origination_year") or not origination.get("original_principal"):
                            skipped_no_origination += 1
                            continue

                        years_elapsed = (
                            date.today().year - origination["origination_year"]
                            + 0.5  # mid-year estimate
                        )
                        if years_elapsed < 0:
                            years_elapsed = 0
                        current_balance = amortize(
                            origination["original_principal"],
                            origination["rate_pct"],
                            origination["term_years"],
                            years_elapsed,
                        )

                        avm = _to_float(lead.get("property_value"))
                        equity = (avm - current_balance) if avm else None

                        amort_record = {
                            "original_principal": origination["original_principal"],
                            "rate_pct": origination["rate_pct"],
                            "term_years": origination["term_years"],
                            "origination_year": origination["origination_year"],
                            "years_elapsed": round(years_elapsed, 2),
                            "current_balance": round(current_balance, 2),
                            "equity_estimate": round(equity, 2) if equity is not None else None,
                            "method": origination["method"],
                            "source": origination["source"],
                            "confidence": origination.get("confidence", 0.65),
                            "computed_at": datetime.now(timezone.utc).isoformat(),
                        }
                        amortized += 1

                        if sample:
                            self.logger.info(
                                f"  SAMPLE id={lead['id'][:8]} "
                                f"orig=${origination['original_principal']:,.0f} "
                                f"({origination['origination_year']}, {origination['rate_pct']:.2f}%, "
                                f"{origination['term_years']}yr, {years_elapsed:.1f}yr elapsed) "
                                f"-> ${current_balance:,.0f} "
                                f"equity=${equity:,.0f}" if equity else f"avm=?"
                            )
                            continue

                        # Write back
                        pm["mortgage_balance_amortized"] = amort_record
                        update: Dict[str, Any] = {"phone_metadata": pm}
                        # Promote amortized current_balance to mortgage_balance
                        # for high-confidence leads
                        if amort_record["confidence"] >= 0.65:
                            update["mortgage_balance"] = int(round(current_balance))
                            promoted += 1
                        try:
                            client.table(table).update(update).eq("id", lead["id"]).execute()
                        except Exception as e:
                            self.logger.warning(f"  update failed id={lead['id']}: {e}")
                            continue
                        if (
                            update.get("mortgage_balance")
                            and table == "homeowner_requests"
                        ):
                            try:
                                record_field(
                                    client, lead["id"], "mortgage_balance",
                                    int(round(current_balance)),
                                    "amortized:" + origination["source"],
                                    confidence=amort_record["confidence"],
                                    metadata={
                                        "method": origination["method"],
                                        "rate_pct": origination["rate_pct"],
                                        "years_elapsed": years_elapsed,
                                    },
                                )
                            except Exception:
                                pass

                    if len(rows) < 1000:
                        break
                    page += 1
                except Exception as e:
                    self.logger.warning(f"page query error {table}: {e}")
                    break

        self.logger.info(
            f"attempted={attempted} amortized={amortized} "
            f"promoted={promoted} skipped={skipped_no_origination}"
        )
        finished = datetime.now(timezone.utc)
        self._report_health(
            status="ok", started_at=started, finished_at=finished,
            fetched_count=attempted, parsed_count=amortized,
            staged_count=promoted, duplicate_count=0,
        )
        return {
            "name": self.name, "status": "ok",
            "attempted": attempted, "amortized": amortized,
            "promoted": promoted, "skipped": skipped_no_origination,
            "fetched": attempted, "staged": promoted, "duplicates": 0,
        }

    def _fail(self, started, msg: str) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status="failed", started_at=started, finished_at=finished,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
            error_message=msg,
        )
        return {"name": self.name, "status": "failed", "error": msg,
                "fetched": 0, "staged": 0, "duplicates": 0}


def run() -> dict:
    bot = MortgageAmortizerBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
