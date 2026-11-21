"""
Shelby County stand-in value for pre-foreclosure leads, from the recorded loan.

The Shelby Assessor is off for us (it 403s the honest bot user agent), so
Shelby pre-foreclosures with an address and owner sat off the ranked list
for want of a value. Until the Assessor's certified roll arrives from the
records request, each lead gets an estimate from the free Register of Deeds
index:

    value = original loan / LTV_ASSUMED * (Memphis HPI now / HPI at the loan)

  - original loan: the deed of trust's "Mort $" on the register (free, no
    login; the same pages shelby_substitute_trustee reads). Memphis Daily
    News notices carry the deed of trust instrument number; substitute
    trustee leads already carry the loan.
  - LTV_ASSUMED 0.95 is an unverified screening assumption. Refinances,
    junior loans and property condition can produce errors in either direction.
  - FHFA all-transactions index for the Memphis MSA (_memphis_hpi.py).

Written as property_value with property_value_source
"shelby_loan_hpi_estimate" (the CRM shows it as Estimated) and the inputs in
raw_payload.value_estimate. Never overwrites a value from another source.

Register access: Crawl-delay 30, so 1 request per 30s and at most
max_fetch_per_run deed lookups per run; leads that already carry the loan
need no request.

Distress type: N/A (enricher only).
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from ._memphis_hpi import MEMPHIS_HPI
from ._shelby_register import detail_url, parse_detail
from .shelby_substitute_trustee_bot import HOME, LOAN_DOC

LTV_ASSUMED = 0.95
# Above this the deed of trust is a blanket or commercial loan over many
# parcels (one Fosterwood Dr notice carried a $20.65M loan): no estimate.
MAX_LOAN = 2_000_000
SOURCE = "shelby_loan_hpi_estimate"
TYPES = ("PRE_FORECLOSURE", "TRUSTEE_NOTICE", "SUBSTITUTION_OF_TRUSTEE")
LATEST_Q = max(MEMPHIS_HPI)


def quarter_of(iso_day: str) -> Optional[str]:
    try:
        d = date.fromisoformat(iso_day[:10])
    except (TypeError, ValueError):
        return None
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def estimate(loan: float, loan_date: str) -> Optional[Dict[str, Any]]:
    """Stand-in value from the original loan, or None if it can't be indexed."""
    q = quarter_of(loan_date)
    if not loan or loan <= 0 or loan > MAX_LOAN or not q:
        return None
    if q not in MEMPHIS_HPI:
        if q > LATEST_Q:
            q = LATEST_Q          # loan newer than the index: no growth applied
        else:
            return None           # before 1995: too old to index
    growth = MEMPHIS_HPI[LATEST_Q] / MEMPHIS_HPI[q]
    value = round(loan / LTV_ASSUMED * growth, -3)
    return {"value": int(value), "method": "original loan / assumed LTV x Memphis FHFA HPI growth",
            "original_loan": loan, "loan_date": loan_date[:10], "ltv_assumed": LTV_ASSUMED,
            "hpi_from": q, "hpi_to": LATEST_Q, "growth": round(growth, 3)}


def dot_ref(raw: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Deed of trust instrument + year from a Memphis Daily News notice."""
    st = raw.get("structured") if isinstance(raw.get("structured"), dict) else {}
    inst = re.sub(r"\D", "", str(st.get("instrument_no") or ""))
    if len(inst) != 8:
        return None
    # Shelby instrument numbers start with the recording year (21100977 -> 2021).
    return {"instrument": inst, "year": f"20{inst[:2]}"}


class ShelbyLoanValueBot(BotBase):
    name = "shelby_loan_value"
    description = "Shelby stand-in value for pre-foreclosures: recorded loan x Memphis HPI"
    throttle_seconds = 30.0     # register Crawl-delay
    expected_min_yield = 0
    frozen_after_runs = 10_000
    max_fetch_per_run = 15      # ~8 minutes, inside the runner's per-bot cap

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase", "staged": 0, "duplicates": 0, "fetched": 0}
        valued = fetched = skipped = 0
        error_message: Optional[str] = None
        try:
            rows = self._candidates(client)
            session_ready = False
            for r in rows:
                raw = self._raw_payload(client, r["id"])
                if raw is None:
                    continue
                loan = (raw.get("loan") or {}) if isinstance(raw.get("loan"), dict) else {}
                amount, loan_date, extra = loan.get("original_amount"), loan.get("loan_date"), {}
                if not amount:
                    ref = dot_ref(raw)
                    if not ref or fetched >= self.max_fetch_per_run:
                        skipped += 1
                        continue
                    if not session_ready:
                        self.fetch(HOME)  # session cookie
                        session_ready = True
                    fetched += 1
                    res = self.fetch(detail_url(ref["instrument"], ref["year"]))
                    if res is None or res.status_code != 200:
                        continue  # try again next run
                    dot = parse_detail(res.text)
                    if not LOAN_DOC.search(dot.get("doc_type") or "") or not dot.get("mortgage_amount"):
                        raw["value_estimate"] = {"status": "no_loan_on_record", "instrument": ref["instrument"],
                                                 "checked_at": started.isoformat()}
                        self._write(client, r["id"], {"raw_payload": raw})
                        continue
                    amount = dot["mortgage_amount"]
                    loan_date = dot.get("instrument_date") or dot.get("file_date")
                    extra = {"instrument": ref["instrument"], "parcel_id": dot.get("parcel_id")}
                est = estimate(float(amount), loan_date or "")
                if not est:
                    raw["value_estimate"] = {"status": "not_indexable", "original_loan": amount,
                                             "loan_date": loan_date, "checked_at": started.isoformat()}
                    self._write(client, r["id"], {"raw_payload": raw})
                    continue
                raw["value_estimate"] = {**est, **extra, "computed_at": started.isoformat()}
                if self._write(client, r["id"], {"property_value": est["value"], "property_value_source": SOURCE,
                                                 "raw_payload": raw}):
                    valued += 1
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error_message else ("ok" if valued else "zero_yield")
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=fetched, parsed_count=valued, staged_count=valued,
                            duplicate_count=skipped, error_message=error_message)
        self.logger.info(f"valued={valued} register_fetches={fetched} deferred={skipped}")
        return {"name": self.name, "status": status, "valued": valued, "fetched": fetched,
                "deferred": skipped, "error": error_message, "staged": valued, "duplicates": skipped}

    def _candidates(self, client) -> List[Dict[str, Any]]:
        """Shelby pre-foreclosures with an address, no value, not yet tried;
        soonest sale first, then no-date leads."""
        try:
            return (client.table("homeowner_requests_staging").select("id")
                    .ilike("county", "shelby%").in_("distress_type", list(TYPES))
                    .is_("property_value", "null").not_.is_("property_address", "null")
                    .is_("raw_payload->value_estimate", "null")
                    .or_(f"trustee_sale_date.gte.{date.today().isoformat()},trustee_sale_date.is.null")
                    .order("trustee_sale_date", desc=False, nullsfirst=False)
                    .limit(500).execute().data) or []
        except Exception as e:  # noqa: BLE001
            self.logger.warning(f"candidates query failed: {e}")
            return []

    @staticmethod
    def _raw_payload(client, row_id: str) -> Optional[Dict[str, Any]]:
        rows = (client.table("homeowner_requests_staging").select("raw_payload").eq("id", row_id)
                .limit(1).execute().data) or []
        raw = rows[0].get("raw_payload") if rows else None
        if raw is None:
            return {}
        return dict(raw) if isinstance(raw, dict) else None

    def _write(self, client, row_id: str, update: Dict[str, Any]) -> bool:
        try:
            # Never overwrite a value another source set since the read.
            q = client.table("homeowner_requests_staging").update(update).eq("id", row_id)
            if "property_value" in update:
                q = q.is_("property_value", "null")
            q.execute()
            return True
        except Exception as e:  # noqa: BLE001
            self.logger.warning(f"  write failed id={row_id}: {e}")
            return False


def run() -> dict:
    return ShelbyLoanValueBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
