"""Open-lien stack from recorded instruments.

Input is what a county Register of Deeds index shows for one owner and
property: deeds of trust, credit lines, releases, assignments, substitute
trustee appointments, judgments and other liens, each with a recording date,
book/page or instrument number and, for loans, the original amount.

Output separates fact from estimate:
  fact      what is recorded: lender, original amount, date, book/page, and
            whether a release references the instrument
  estimate  today's balance, as a range. The low end assumes on-schedule
            payments (standard 30-year amortization at the Tennessee average
            rate for the origination year). The high end adds missed payments
            and fees for loans in default, because a distressed owner's real
            payoff runs above the schedule. A credit line is recorded at its
            limit, so its range runs from 0 to the limit.

Rules for deciding a lien is open (no vendor publishes theirs; these are ours):
  released         a release, satisfaction or partial release cites the
                   instrument by book/page or instrument number
  confirmed_open   a substitute trustee appointment or foreclosure notice cites
                   it: open and in default
  likely_paid      a later full-size deed of trust (a refinance) replaced it
                   and no release recorded yet, it is past its term, or a
                   judgment is past its 10-year life
  open             none of the above

Tennessee requires a release when a loan is paid, but the penalty only applies
after the borrower demands one in writing, so unreleased paid loans are
common. That is why likely_paid exists and why equity is reported as a range.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

# Freddie Mac PMMS / H.15 annual averages, used as the Tennessee 30-year
# rate for the origination year. The mortgage estimator and amortizer bots
# import this table so all three agree.
TN_AVG_30Y_RATES = {
    1995: 7.93, 1996: 7.81, 1997: 7.60, 1998: 6.94, 1999: 7.44,
    2000: 8.05, 2001: 6.97, 2002: 6.54, 2003: 5.83, 2004: 5.84,
    2005: 5.87, 2006: 6.41, 2007: 6.34, 2008: 6.03, 2009: 5.04,
    2010: 4.69, 2011: 4.45, 2012: 3.66, 2013: 3.98, 2014: 4.17,
    2015: 3.85, 2016: 3.65, 2017: 3.99, 2018: 4.54, 2019: 3.94,
    2020: 3.11, 2021: 2.96, 2022: 5.34, 2023: 6.81, 2024: 6.74,
    2025: 6.85, 2026: 6.50,
}
DEFAULT_RATE = 6.50

LOAN_TYPES = {"DOT", "HELOC"}
LIEN_TYPES = LOAN_TYPES | {"JUDGMENT", "LIEN"}
RELEASE_TYPES = {"RELEASE", "PARTIAL_RELEASE"}
DEFAULT_TYPES = {"SUB_TRUSTEE", "FORECLOSURE_NOTICE"}
TRANSFER_TYPES = {"DEED"}

TERM_YEARS = 30
JUDGMENT_LIFE_YEARS = 10          # T.C.A. 28-3-110: judgments run 10 years
REFI_WINDOW_DAYS = 120            # a new loan this soon after another usually paid it off
REFI_MIN_RATIO = 0.80             # ...when it is at least 80% of the old amount
DEFAULT_ARREARS_MONTHS = 12       # assumed missed payments for a loan in default
DEFAULT_FEE_RATE = 0.05           # assumed fees, costs and attorney charges on default


def amortize(original_principal: float, annual_rate_pct: float, term_years: int, years_elapsed: float) -> float:
    """Remaining principal on a fixed-rate loan paid on schedule."""
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
    payment = original_principal * r * factor_n / (factor_n - 1)
    return max(0.0, original_principal * factor_k - payment * (factor_k - 1) / r)


def monthly_payment(original_principal: float, annual_rate_pct: float, term_years: int = TERM_YEARS) -> float:
    n = term_years * 12
    r = (annual_rate_pct / 100.0) / 12.0
    if original_principal <= 0:
        return 0.0
    if r == 0:
        return original_principal / n
    f = (1 + r) ** n
    return original_principal * r * f / (f - 1)


def ref_key(book: Any = None, page: Any = None, instrument: Any = None) -> List[str]:
    """Every key an instrument can be cited by: 'B123P45' and 'I20260001234'."""
    keys = []
    b = str(book or "").strip().lstrip("0")
    p = str(page or "").strip().lstrip("0")
    if b and p:
        keys.append(f"B{b}P{p}")
    i = "".join(ch for ch in str(instrument or "") if ch.isalnum()).upper().lstrip("0")
    if i:
        keys.append(f"I{i}")
    return keys


@dataclass
class Instrument:
    """One recorded document. `refs` holds the book/page or instrument
    numbers this document cites (a release cites the loan it releases)."""
    type: str
    recorded: date
    amount: Optional[float] = None
    lender: Optional[str] = None
    book: Optional[str] = None
    page: Optional[str] = None
    instrument: Optional[str] = None
    refs: List[str] = field(default_factory=list)
    mers: bool = False
    arm_length: bool = True        # deeds only: False for quitclaims between family, etc.

    @property
    def keys(self) -> List[str]:
        return ref_key(self.book, self.page, self.instrument)

    def cites(self, other: "Instrument") -> bool:
        mine = set()
        for r in self.refs:
            mine.update(_normalize_ref(r))
        return bool(mine & set(other.keys))


def _normalize_ref(r: str) -> List[str]:
    """Accept 'B123P45', '123/45', 'Book 123 Page 45' or a bare instrument number."""
    s = str(r or "").upper().strip()
    m = (re.fullmatch(r"B0*(\d+)P0*(\d+)", s)
         or re.search(r"\b0*(\d+)\s*/\s*0*(\d+)\b", s)
         or re.search(r"\b(?:BOOK|BK)\.?\s*0*(\d+)\D+?(?:PAGE|PG)\.?\s*0*(\d+)", s))
    if m:
        return [f"B{m.group(1)}P{m.group(2)}"]
    digits = "".join(ch for ch in s if ch.isalnum()).lstrip("0")
    return [f"I{digits}"] if digits else []


def _years_between(a: date, b: date) -> float:
    return max(0.0, (b - a).days / 365.25)


def rate_for(year: int) -> float:
    return TN_AVG_30Y_RATES.get(year, DEFAULT_RATE)


def lien_stack(instruments: Iterable[Instrument], as_of: date) -> Dict[str, Any]:
    """Classify every lien recorded since the owner took title."""
    docs = sorted(instruments, key=lambda d: d.recorded)
    transfers = [d for d in docs if d.type in TRANSFER_TYPES and d.arm_length]
    acquired = transfers[-1].recorded if transfers else None
    releases = [d for d in docs if d.type in RELEASE_TYPES]
    defaults = [d for d in docs if d.type in DEFAULT_TYPES]

    liens = [d for d in docs if d.type in LIEN_TYPES]
    if acquired:
        # Loans before the owner bought were paid at that closing. Judgments
        # attach to the person, so they count whenever they were recorded,
        # as long as they are still within their 10-year life.
        liens = [d for d in liens if d.recorded >= acquired or d.type == "JUDGMENT"]

    out: List[Dict[str, Any]] = []
    for lien in liens:
        flags: List[str] = []
        status = "open"
        if any(r.cites(lien) and r.recorded >= lien.recorded for r in releases):
            status = "released"
        elif any(d.cites(lien) for d in defaults):
            status = "confirmed_open"
        else:
            if lien.type == "JUDGMENT" and _years_between(lien.recorded, as_of) > JUDGMENT_LIFE_YEARS:
                status = "likely_paid"
                flags.append("judgment_past_10_years")
            elif lien.type in LOAN_TYPES and _years_between(lien.recorded, as_of) >= TERM_YEARS:
                status = "likely_paid"
                flags.append("past_term")
            elif lien.type == "DOT" and lien.amount:
                # A later full-size first loan is a refinance that paid this
                # one off; the release often lags or never records. A loan
                # recorded within REFI_WINDOW_DAYS is a second lien taken at
                # the same purchase, not a refinance.
                for later in docs:
                    if (later.type == "DOT" and later is not lien and later.amount
                            and (later.recorded - lien.recorded).days > REFI_WINDOW_DAYS
                            and later.amount >= REFI_MIN_RATIO * lien.amount):
                        status = "likely_paid"
                        flags.append("replaced_by_later_loan")
                        break
        if lien.mers:
            flags.append("mers_lender_may_differ")
        if lien.type == "HELOC":
            flags.append("credit_line_recorded_at_limit")

        low, high, basis = _balance_range(lien, status, as_of)
        out.append({
            "type": lien.type,
            "recorded": lien.recorded.isoformat(),
            "lender": lien.lender,
            "original_amount": lien.amount,
            "book": lien.book, "page": lien.page, "instrument": lien.instrument,
            "status": status,
            "flags": flags,
            "balance_low": low,
            "balance_high": high,
            "basis": basis,
        })
    return {
        "as_of": as_of.isoformat(),
        "acquired": acquired.isoformat() if acquired else None,
        "liens": out,
        "counts": {s: sum(1 for l in out if l["status"] == s)
                   for s in ("open", "confirmed_open", "likely_paid", "released")},
    }


def _balance_range(lien: Instrument, status: str, as_of: date):
    if status == "released":
        return 0, 0, "released"
    amount = float(lien.amount or 0)
    if not amount:
        return None, None, "amount_not_recorded"
    if lien.type == "HELOC":
        return 0, round(amount), "credit_line_limit"
    if lien.type in ("JUDGMENT", "LIEN"):
        hi = round(amount)
        return (0 if status == "likely_paid" else hi), hi, "recorded_amount"
    rate = rate_for(lien.recorded.year)
    sched = amortize(amount, rate, TERM_YEARS, _years_between(lien.recorded, as_of))
    if status == "likely_paid":
        return 0, round(sched), "likely_paid_schedule_if_not"
    if status == "confirmed_open":
        hi = sched + DEFAULT_ARREARS_MONTHS * monthly_payment(amount, rate) + DEFAULT_FEE_RATE * sched
        return round(sched), round(hi), f"schedule_{rate}pct_plus_default_costs"
    return round(sched), round(sched), f"schedule_{rate}pct"


def equity_range(value: Optional[float], stack: Dict[str, Any]) -> Dict[str, Any]:
    """Equity low counts every lien that might still be owed at its high end;
    equity high counts only open liens at their low end."""
    if not value:
        return {"low": None, "high": None, "unknown_amounts": 0}
    owed_high = 0.0
    owed_low = 0.0
    unknown = 0
    for l in stack["liens"]:
        if l["status"] == "released":
            continue
        if l["balance_high"] is None:
            unknown += 1
            continue
        owed_high += l["balance_high"]
        if l["status"] in ("open", "confirmed_open"):
            owed_low += l["balance_low"] or 0
    return {"low": round(value - owed_high), "high": round(value - owed_low), "unknown_amounts": unknown}
