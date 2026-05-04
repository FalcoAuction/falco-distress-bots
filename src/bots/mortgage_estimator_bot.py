"""
Mortgage estimator — ATTOM-equivalent fallback for properties without
direct ROD data.

This enricher takes whatever signals we already have (last sale price,
last sale date from TPAD/Inigo/PADCTN; AVM from any assessor) and
produces a current-balance estimate using standard amortization at
prevailing TN mortgage rates. This is exactly what ATTOM does
internally for the ~30% of records where they don't have a direct
ROD feed — they estimate. We do it explicitly with a confidence flag.

Inputs (any combination):
  - last_sale_price (purchase price; primary signal)
  - last_sale_date (years_elapsed since purchase)
  - appraised value / AVM (secondary signal for LTV sanity check)
  - distress_type (foreclosure-stage leads use the published delinquent
    amount instead — this estimator only fills when no foreclosure
    notice is available)
  - county (rate lookup if needed; we use a flat TN average)

Assumptions (all conservative, documented, swappable):
  - LTV at purchase: 80% (FHA loans go higher but most TN purchases
    are ~80% conventional)
  - Term: 30 years fixed
  - Rate: average TN 30-year rate at time of purchase. We hard-code
    a year-by-year table compiled from Federal Reserve H.15 averages
    (publicly-published rates). Updated occasionally.
  - No early-prepay assumption (debtors usually pay on schedule
    until they can't)

Output written to:
  - mortgage_balance_estimate (numeric, dollars)
  - phone_metadata.mortgage_estimate (JSON blob with breakdown +
    confidence so the dialer can show the math)

Confidence heuristic:
  - 0.7 if last_sale_date within 7 years (recent; LTV assumption
    holds best, refinances less likely)
  - 0.5 if 7-15 years (refinance possible but not certain)
  - 0.3 if 15+ years (probably refinanced; estimate is shaky)
  - 0.0 if no last_sale_price available (don't write)

Distress type: N/A (utility enricher).
"""

from __future__ import annotations

import json
import sys
import traceback as tb
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._provenance import record_field


# Average annual TN 30-year fixed mortgage rates (Freddie Mac PMMS
# / Federal Reserve H.15). Used to compute amortization; older years
# matter for old purchases. Kept as a constant so changes are explicit.
TN_AVG_30Y_RATES = {
    1995: 7.93, 1996: 7.81, 1997: 7.60, 1998: 6.94, 1999: 7.44,
    2000: 8.05, 2001: 6.97, 2002: 6.54, 2003: 5.83, 2004: 5.84,
    2005: 5.87, 2006: 6.41, 2007: 6.34, 2008: 6.03, 2009: 5.04,
    2010: 4.69, 2011: 4.45, 2012: 3.66, 2013: 3.98, 2014: 4.17,
    2015: 3.85, 2016: 3.65, 2017: 3.99, 2018: 4.54, 2019: 3.94,
    2020: 3.11, 2021: 2.96, 2022: 5.34, 2023: 6.81, 2024: 6.74,
    2025: 6.85, 2026: 6.50,
}

# Default fallback for years not in the table (e.g., very old)
DEFAULT_RATE = 6.50

# Standard assumptions
DEFAULT_LTV = 0.80
DEFAULT_TERM_YEARS = 30


def amortized_balance(
    original_principal: float,
    annual_rate_pct: float,
    term_years: int,
    elapsed_months: int,
) -> float:
    """Standard mortgage amortization. Returns remaining principal balance
    after `elapsed_months` payments on a fixed-rate fully-amortizing loan.
    """
    if original_principal <= 0 or elapsed_months <= 0:
        return original_principal
    if elapsed_months >= term_years * 12:
        return 0.0  # paid off

    r = (annual_rate_pct / 100.0) / 12.0  # monthly rate
    n = term_years * 12  # total payments

    if r == 0:  # edge case: 0% interest
        return original_principal * (1 - elapsed_months / n)

    # Monthly payment: P * r * (1+r)^n / ((1+r)^n - 1)
    factor = (1 + r) ** n
    monthly_payment = original_principal * r * factor / (factor - 1)

    # Remaining balance after k payments:
    # B = P * (1+r)^k - M * ((1+r)^k - 1) / r
    factor_k = (1 + r) ** elapsed_months
    remaining = (
        original_principal * factor_k
        - monthly_payment * (factor_k - 1) / r
    )
    return max(0.0, remaining)


def estimate_current_balance(
    last_sale_price: Optional[float],
    last_sale_date_iso: Optional[str],
    avm: Optional[float] = None,
    year_built: Optional[int] = None,
    ltv: float = DEFAULT_LTV,
    term_years: int = DEFAULT_TERM_YEARS,
    today: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    """Returns dict with estimate + breakdown + confidence.

    Three estimation paths in order of precision:
      1. SALE-BASED: have last_sale_price + last_sale_date → amortize
         from purchase. Confidence 0.7 (recent) → 0.1 (very old).
      2. AVM+AGE FALLBACK: have AVM + year_built (or county records of
         age) → assume sale at year_built (or 7 years ago for
         post-2010 builds, accounting for refi cycles). Confidence
         0.4 (fresh build) → 0.2 (old build).
      3. AVM-ONLY FALLBACK: have only AVM → apply TN-statewide
         homeowner-equity median (~58% per Federal Reserve Survey of
         Consumer Finances). Confidence 0.2.
      4. None of the above → return None.
    """
    today = today or date.today()

    # ── Path 1: sale-based (most precise) ──────────────────────────────
    if last_sale_price and last_sale_price > 0 and last_sale_date_iso:
        try:
            sale_date = datetime.strptime(last_sale_date_iso[:10], "%Y-%m-%d").date()
            sale_year = sale_date.year
            years_elapsed = (today - sale_date).days / 365.25
            elapsed_months = (today.year - sale_date.year) * 12 + (today.month - sale_date.month)
            return _build_estimate(
                last_sale_price, sale_year, years_elapsed,
                elapsed_months, avm, ltv, term_years,
                source="sale_based",
            )
        except (ValueError, TypeError):
            pass

    # ── Path 2: AVM + year_built fallback ──────────────────────────────
    if avm and avm > 0 and year_built and year_built > 1900:
        # Assume the homeowner refinanced at most ~7 years ago for newer
        # builds, OR purchased at year_built for older builds.
        # For pre-2018 builds, assume current loan age = min(year_built
        # difference, 7 years) — most homeowners have refi'd at least
        # once when rates dropped 2020-2021.
        years_elapsed = today.year - year_built
        if year_built <= 2018 and years_elapsed > 7:
            # Assume last refi 7 years ago at then-prevailing rate
            assumed_purchase_year = today.year - 7
            years_elapsed = 7
        else:
            assumed_purchase_year = year_built
        elapsed_months = years_elapsed * 12
        # Original principal estimate: assume 80% of CURRENT AVM (conservative
        # — actual purchase price was likely lower but this approximates
        # post-refi cash-out scenarios)
        return _build_estimate(
            avm, assumed_purchase_year, years_elapsed,
            elapsed_months, avm, ltv, term_years,
            source="avm_plus_age",
        )

    # ── Path 3: AVM-only — TN-median equity assumption ─────────────────
    if avm and avm > 0:
        # Federal Reserve Survey of Consumer Finances 2022: median TN
        # homeowner equity ~58% of home value. Assume current_balance
        # = AVM × 0.42 (42% mortgage remaining).
        TN_MEDIAN_EQUITY_PCT = 0.58
        current_balance = avm * (1.0 - TN_MEDIAN_EQUITY_PCT)
        equity = avm * TN_MEDIAN_EQUITY_PCT
        return {
            "estimated_current_balance": round(current_balance, 2),
            "original_principal_estimated": None,
            "estimated_equity": round(equity, 2),
            "assumed_ltv": None,
            "assumed_term_years": None,
            "assumed_rate_pct": None,
            "purchase_year": None,
            "purchase_price": None,
            "years_elapsed": None,
            "elapsed_months": None,
            "confidence": 0.2,
            "source": "avm_only_tn_median",
            "note": (
                "Estimate uses TN-statewide median homeowner equity (58% per "
                "Fed Reserve SCF 2022). Confidence intentionally low; reach "
                "for sale-based or AVM+age estimates when those signals "
                "become available."
            ),
        }

    return None


def _build_estimate(
    purchase_price: float,
    purchase_year: int,
    years_elapsed: float,
    elapsed_months: int,
    avm: Optional[float],
    ltv: float,
    term_years: int,
    source: str,
) -> Dict[str, Any]:
    """Shared amortization + equity logic for sale-based + avm+age paths."""
    # Pick rate based on purchase year
    rate = TN_AVG_30Y_RATES.get(purchase_year, DEFAULT_RATE) if purchase_year else DEFAULT_RATE

    # Original principal estimate
    original_principal = purchase_price * ltv

    # Current balance via amortization
    current_balance = amortized_balance(original_principal, rate, term_years, elapsed_months)

    # Equity = AVM - current_balance (when AVM available)
    equity = None
    if avm and avm > 0:
        equity = avm - current_balance

    # Confidence based on age + data quality (sale-based) or fallback path
    if source == "avm_plus_age":
        # AVM+age path is less precise; cap confidence at 0.4
        confidence = 0.4 if years_elapsed < 7 else 0.3 if years_elapsed < 15 else 0.2
    else:  # sale_based
        if years_elapsed < 7:
            confidence = 0.7
        elif years_elapsed < 15:
            confidence = 0.5
        elif years_elapsed < 30:
            confidence = 0.3
        else:
            confidence = 0.1  # very old, almost certainly refinanced

    return {
        "estimated_current_balance": round(current_balance, 2),
        "original_principal_estimated": round(original_principal, 2),
        "estimated_equity": round(equity, 2) if equity is not None else None,
        "assumed_ltv": ltv,
        "assumed_term_years": term_years,
        "assumed_rate_pct": rate,
        "purchase_year": purchase_year,
        "purchase_price": purchase_price,
        "years_elapsed": round(years_elapsed, 2),
        "elapsed_months": elapsed_months,
        "confidence": confidence,
        "source": source,
        "note": (
            "Estimate based on standard amortization assumptions (80% LTV, 30y fixed, "
            "TN avg rate at purchase year). Refinances + HELOCs not modeled."
        ),
    }


class MortgageEstimatorBot(BotBase):
    name = "mortgage_estimator"
    description = "ATTOM-equivalent mortgage current-balance estimator using TPAD last-sale data + amortization"
    throttle_seconds = 0.0
    expected_min_yield = 1

    max_leads_per_run = 5000

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
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "estimated": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        estimated = 0
        skipped = 0
        no_signals = 0
        error_message: Optional[str] = None

        try:
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                rows = self._candidates(client, table)
                self.logger.info(f"{table}: {len(rows)} candidates")

                for row in rows[:self.max_leads_per_run]:
                    sale_price, sale_date, avm, year_built = self._extract_signals(row)
                    # Allow AVM-only / AVM+age fallbacks — only bail when we
                    # have literally nothing (no sale, no AVM).
                    if not (sale_price and sale_date) and not avm:
                        no_signals += 1
                        continue

                    estimate = estimate_current_balance(
                        sale_price, sale_date, avm, year_built
                    )
                    if estimate is None:
                        no_signals += 1
                        continue

                    # Skip if we already have a higher-confidence value
                    # (foreclosure notices give actual delinquent amounts;
                    # don't overwrite those with an estimate).
                    if row.get("mortgage_balance"):
                        existing_meta = row.get("phone_metadata") or {}
                        if isinstance(existing_meta, dict):
                            existing_est = existing_meta.get("mortgage_estimate") or {}
                            if existing_est.get("confidence", 0) >= estimate["confidence"]:
                                skipped += 1
                                continue
                        # Real mortgage_balance present (from a foreclosure
                        # notice), no existing estimate. Don't pollute the
                        # lead view with a low-confidence avm_only_tn_median
                        # guess that disagrees with the real ROD figure by
                        # 80%+. Audit found 54 leads with this pollution
                        # (Curtis Ward: real $75K mortgage vs $674K guess).
                        if estimate.get("source") == "avm_only_tn_median":
                            skipped += 1
                            continue

                    update: Dict[str, Any] = {}
                    # Only fill mortgage_balance if it's currently null —
                    # don't override foreclosure-notice-derived values.
                    # Column is INTEGER, so round before write.
                    if not row.get("mortgage_balance"):
                        update["mortgage_balance"] = int(round(estimate["estimated_current_balance"]))

                    existing_meta = row.get("phone_metadata") or {}
                    if not isinstance(existing_meta, dict):
                        existing_meta = {}
                    existing_meta["mortgage_estimate"] = estimate
                    update["phone_metadata"] = existing_meta

                    try:
                        client.table(table).update(update).eq("id", row["id"]).execute()
                        estimated += 1
                        # Record provenance for live table
                        if table == "homeowner_requests":
                            record_field(
                                client, row["id"], "mortgage_balance",
                                int(round(estimate["estimated_current_balance"])),
                                "mortgage_estimator",
                                confidence=estimate["confidence"],
                                metadata={
                                    "purchase_year": estimate["purchase_year"],
                                    "ltv": estimate["assumed_ltv"],
                                    "rate_pct": estimate["assumed_rate_pct"],
                                },
                            )
                    except Exception as e:
                        self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif estimated == 0 and skipped == 0 and no_signals == 0:
            status = "zero_yield"
        elif estimated == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=estimated + skipped + no_signals,
            parsed_count=estimated + skipped,
            staged_count=estimated, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(f"estimated={estimated} skipped={skipped} no_signals={no_signals}")
        return {
            "name": self.name, "status": status,
            "estimated": estimated, "skipped": skipped, "no_signals": no_signals,
            "error": error_message,
            "staged": estimated, "duplicates": skipped,
            "fetched": estimated + skipped + no_signals,
        }

    def _candidates(self, client, table: str) -> List[Dict[str, Any]]:
        # PostgREST caps any .limit() at 1000 silently. Paginate so the
        # full corpus gets covered (Hamilton-tax-delinquent alone has
        # 2000+ rows in staging).
        out = []
        PAGE_SIZE = 1000
        MAX_PAGES = 10
        for page in range(MAX_PAGES):
            try:
                q = (
                    client.table(table)
                    .select("id, mortgage_balance, property_value, raw_payload, phone_metadata")
                    .order("id")
                    .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                out.extend(rows)
                if len(rows) < PAGE_SIZE:
                    break
            except Exception as e:
                self.logger.warning(f"candidate query on {table} page {page} failed: {e}")
                break
        return out

    @staticmethod
    def _extract_signals(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[str], Optional[float], Optional[int]]:
        """Pull last_sale_price + last_sale_date + AVM + year_built from any
        of the assessor blobs we've stored in raw_payload."""
        raw = row.get("raw_payload") or {}
        if not isinstance(raw, dict):
            return (None, None, None, None)

        sale_price = sale_date = avm = year_built = None

        # Williamson Inigo
        w = raw.get("williamson_inigo") or {}
        if isinstance(w, dict):
            if not sale_price and w.get("last_price"):
                sale_price = float(w["last_price"]) if w["last_price"] else None
            if not sale_date and w.get("last_transfer_date"):
                sale_date = w["last_transfer_date"]
            if not avm and w.get("appraised"):
                avm = float(w["appraised"])
            if not year_built and w.get("year_built"):
                try:
                    year_built = int(w["year_built"])
                except (ValueError, TypeError):
                    pass

        # PADCTN (Davidson)
        p = raw.get("padctn") or {}
        if isinstance(p, dict):
            if not avm and p.get("appraised"):
                avm = float(p["appraised"])

        # TPAD enricher
        t = raw.get("tpad") or {}
        if isinstance(t, dict):
            if not sale_price and t.get("last_sale_price"):
                sale_price = float(str(t["last_sale_price"]).replace(",", ""))
            if not sale_date and (t.get("last_sale_date") or t.get("last_sale")):
                sale_date = t.get("last_sale_date") or t.get("last_sale")
            if not avm and t.get("appraised_value"):
                try:
                    avm = float(str(t["appraised_value"]).replace(",", ""))
                except (ValueError, TypeError):
                    pass
            if not year_built and t.get("year_built"):
                try:
                    year_built = int(t["year_built"])
                except (ValueError, TypeError):
                    pass

        # Hamilton assessor (Chattanooga CSV)
        h = raw.get("hamilton_assessor") or {}
        if isinstance(h, dict):
            if not sale_price and h.get("last_sale_price"):
                try:
                    sale_price = float(h["last_sale_price"])
                except (ValueError, TypeError):
                    pass
            if not sale_date and h.get("last_sale_date"):
                sale_date = h["last_sale_date"]
            if not avm and h.get("appraised"):
                try:
                    avm = float(h["appraised"])
                except (ValueError, TypeError):
                    pass
            if not year_built and h.get("year_built"):
                try:
                    year_built = int(h["year_built"])
                except (ValueError, TypeError):
                    pass

        # Shelby ArcGIS
        sh = raw.get("shelby_arcgis") or {}
        if isinstance(sh, dict):
            if not sale_price and sh.get("last_sale_price"):
                try:
                    sale_price = float(sh["last_sale_price"])
                except (ValueError, TypeError):
                    pass
            if not sale_date and sh.get("last_sale_date"):
                sale_date = sh["last_sale_date"]
            if not avm and sh.get("appraised"):
                try:
                    avm = float(sh["appraised"])
                except (ValueError, TypeError):
                    pass
            if not year_built and sh.get("year_built"):
                try:
                    year_built = int(sh["year_built"])
                except (ValueError, TypeError):
                    pass

        # Rutherford ArcGIS
        rc = raw.get("rutherford_arcgis") or {}
        if isinstance(rc, dict):
            if not sale_price and rc.get("last_sale_price"):
                try:
                    sale_price = float(rc["last_sale_price"])
                except (ValueError, TypeError):
                    pass
            if not sale_date and rc.get("last_sale_date"):
                sale_date = rc["last_sale_date"]
            if not avm and rc.get("appraised"):
                try:
                    avm = float(rc["appraised"])
                except (ValueError, TypeError):
                    pass
            if not year_built and rc.get("year_built"):
                try:
                    year_built = int(rc["year_built"])
                except (ValueError, TypeError):
                    pass

        # Fallback: row-level property_value
        if not avm and row.get("property_value"):
            try:
                avm = float(row["property_value"])
            except (ValueError, TypeError):
                pass

        return (sale_price, sale_date, avm, year_built)


def run() -> dict:
    bot = MortgageEstimatorBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # CLI test: estimate price + date
        # Usage: python mortgage_estimator_bot.py 350000 2018-06-15 [475000]
        price = float(sys.argv[1])
        date_str = sys.argv[2] if len(sys.argv) > 2 else None
        avm = float(sys.argv[3]) if len(sys.argv) > 3 else None
        result = estimate_current_balance(price, date_str, avm)
        print(json.dumps(result, indent=2))
    else:
        print(run())
