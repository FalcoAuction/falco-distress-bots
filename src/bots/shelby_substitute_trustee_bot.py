"""
Shelby County Appointments of Substitute Trustee, from the Register of
Deeds' free public index (https://search.register.shelby.tn.us).

In Tennessee a lender records an Appointment of Substitute Trustee (code
APPT) before it advertises a non-judicial foreclosure sale, so this is the
earliest public foreclosure signal: weeks before the notice runs in the
paper. Shelby records about five a business day.

Per filing:
  1. the search results row gives the instrument, recording date, the
     lender and borrower(s) (grantors) and the new trustee (grantee);
  2. the APPT detail page cross-references the deed of trust;
  3. the deed of trust's detail page gives the property address, parcel,
     original loan amount and date, and the borrowers on the loan.
The loan is amortized forward at the year's average 30-year rate for a
rough payoff. A loan in default is usually owed more than that (missed
payments, late and legal fees), so it is stored as an estimate.

Access rules: the index is free with no login and no CAPTCHA, and the
terms carry only an "as is" disclaimer. robots.txt disallows /imgView.php
(document images), which this never requests. The register's main site
asks for Crawl-delay: 30 and rate-limits faster clients, so every request
here waits 30 seconds. Already-staged filings are skipped before any
detail request, so a normal day is a dozen requests.

Distress type: SUBSTITUTION_OF_TRUSTEE (the CRM labels it Pre-Foreclosure
(SOT); the auto promoter applies the foreclosure mortgage gate).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set

from ._base import BotBase, LeadPayload, _supabase
from ._shelby_register import (BASE, address_of, deed_of_trust_ref, detail_url, parse_detail, parse_results,
                               split_parties, trustees, years_since)
from .ustitlesearch_rod_bot import DEFAULT_RATE, TN_AVG_30Y_RATES, _amortize

SEARCH = BASE + "/p2.php"
HOME = BASE + "/index.php"
CODE = "APPT"


def build_lead(bot: "ShelbySubstituteTrusteeBot", row: Dict[str, Any], appt: Optional[Dict[str, Any]],
               dot: Optional[Dict[str, Any]], today: Optional[date] = None) -> Optional[LeadPayload]:
    parties = split_parties(row["grantors"], (dot or {}).get("grantors") or None)
    if not parties["borrowers"]:
        return None
    lender = parties["lenders"][0] if parties["lenders"] else None
    loan: Dict[str, Any] = {}
    balance = None
    if dot and dot.get("mortgage_amount"):
        loan = {"original_amount": dot["mortgage_amount"], "loan_date": dot.get("instrument_date") or dot.get("file_date")}
        yrs = years_since(loan["loan_date"], today)
        if yrs is not None:
            rate = TN_AVG_30Y_RATES.get(int(loan["loan_date"][:4]), DEFAULT_RATE)
            balance = round(_amortize(float(dot["mortgage_amount"]), rate, yrs), -2)
            loan.update({"rate_assumed_pct": rate, "amortized_balance": balance,
                         "method": "scheduled amortization; a defaulted loan's payoff is usually higher"})
    address = address_of(dot) if dot else None
    sub_trustees = trustees(row["grantees"])
    note = f"Substitute trustee appointed {row['recorded'] or '?'} (Shelby inst. {row['instrument']})"
    if lender:
        note += f" by {lender}"
    if sub_trustees:
        note += f"; trustee {', '.join(sub_trustees)}"
    if not address:
        note += ". No address yet: deed of trust not linked or has no street."
    return LeadPayload(
        bot_source=bot.name,
        pipeline_lead_key=bot.make_lead_key(bot.name, f"shelby|{row['instrument']}"),
        distress_type="SUBSTITUTION_OF_TRUSTEE",
        county="Shelby",
        property_address=address,
        full_name=parties["borrowers"][0],
        owner_name_records="; ".join(parties["borrowers"]),
        mortgage_balance=balance,
        source_url=row.get("detail_url"),
        admin_notes=note,
        raw_payload={
            "source": "shelby_register", "state": "TN", "instrument": row["instrument"], "code": row["code"],
            "recorded": row["recorded"], "appointment_date": (appt or {}).get("instrument_date"),
            "lenders": parties["lenders"], "borrowers": parties["borrowers"], "party_basis": parties["basis"],
            "substitute_trustees": sub_trustees, "deed_of_trust": (appt and deed_of_trust_ref(appt)) or None,
            "parcel_id": (dot or {}).get("parcel_id"), "subdivision": (dot or {}).get("subdivision"),
            "loan": loan or None,
        },
    )


class ShelbySubstituteTrusteeBot(BotBase):
    name = "shelby_substitute_trustee"
    description = "Shelby County Appointments of Substitute Trustee (earliest foreclosure signal), free register index"
    # The register asks for Crawl-delay: 30 and 429s faster clients.
    throttle_seconds = 30.0
    expected_min_yield = 0      # weekends and holidays record nothing
    frozen_after_runs = 10_000

    days_back = 7               # first run and catch-up window
    max_new_per_run = 15        # 2 detail requests each at 30s: under 15 minutes

    def scrape(self) -> List[LeadPayload]:
        self.fetch(HOME)  # session cookie
        end = date.today()
        start = end - timedelta(days=self.days_back)
        res = self.fetch(SEARCH, method="POST", data={
            "itype1": CODE, "itype2": "", "startdate": start.strftime("%m/%d/%Y"),
            "enddate": end.strftime("%m/%d/%Y"), "searchtype": "ITYPE"})
        if res is None or res.status_code != 200:
            self.logger.warning(f"search failed ({getattr(res, 'status_code', 'no response')})")
            return []
        rows = [r for r in parse_results(res.text) if r["code"] == CODE]
        self.logger.info(f"{len(rows)} APPT filing(s) {start} to {end}")
        known = self._known({self.make_lead_key(self.name, f"shelby|{r['instrument']}") for r in rows})
        fresh = [r for r in rows if self.make_lead_key(self.name, f"shelby|{r['instrument']}") not in known]
        fresh.sort(key=lambda r: r["recorded"] or "", reverse=True)
        if len(fresh) > self.max_new_per_run:
            self.logger.info(f"{len(fresh)} new; reading the newest {self.max_new_per_run}, the rest next run")
        leads: List[LeadPayload] = []
        for row in fresh[: self.max_new_per_run]:
            appt = self._detail(row["detail_url"]) if row.get("detail_url") else None
            ref = deed_of_trust_ref(appt) if appt else None
            dot = self._detail(detail_url(ref["instrument"], ref["year"])) if ref else None
            if dot and "TRUST" not in (dot.get("doc_type") or "").upper():
                dot = None  # cross reference points at something other than a deed of trust
            lead = build_lead(self, row, appt, dot)
            if lead:
                leads.append(lead)
        self.logger.info(f"staging {len(leads)} lead(s), {sum(1 for l in leads if l.property_address)} with an address")
        return leads

    def _detail(self, url: str) -> Optional[Dict[str, Any]]:
        res = self.fetch(url)
        if res is None or res.status_code != 200:
            return None
        return parse_detail(res.text)

    def _known(self, keys: Set[str]) -> Set[str]:
        """Lead keys already in staging, so their detail pages are not re-read."""
        client = _supabase()
        if client is None or not keys:
            return set()
        try:
            res = (client.table("homeowner_requests_staging").select("pipeline_lead_key")
                   .eq("bot_source", self.name).in_("pipeline_lead_key", sorted(keys)).execute())
            return {r["pipeline_lead_key"] for r in (getattr(res, "data", None) or [])}
        except Exception as e:  # noqa: BLE001 - a failed lookup only costs extra requests
            self.logger.warning(f"known-key lookup failed: {e}")
            return set()


def run() -> dict:
    return ShelbySubstituteTrusteeBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
