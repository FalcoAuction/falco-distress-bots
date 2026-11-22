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
detail request, and reads at most 6 new filings per run (about 7 minutes).

Distress type: SUBSTITUTION_OF_TRUSTEE (the CRM labels it Pre-Foreclosure
(SOT); the auto promoter applies the foreclosure mortgage gate).
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

from requests.adapters import HTTPAdapter

from ._base import BotBase, LeadPayload, _supabase
from ._shelby_register import (BASE, address_of, deed_of_trust_ref, detail_url, parse_detail, parse_results,
                               split_parties, trustees, years_since)
from .ustitlesearch_rod_bot import DEFAULT_RATE, TN_AVG_30Y_RATES, _amortize

SEARCH = BASE + "/p2.php"
HOME = BASE + "/index.php"
CODE = "APPT"
# The register files most deeds of trust under doc type "MORTGAGE"; checking
# for "TRUST" alone threw away the page with the address and loan.
LOAN_DOC = re.compile(r"^\s*(?:MORTGAGE|(?:CORRECTED )?DEED OF TRUST)\s*$", re.I)


def build_lead(bot: "ShelbySubstituteTrusteeBot", row: Dict[str, Any], appt: Optional[Dict[str, Any]],
               dot: Optional[Dict[str, Any]], today: Optional[date] = None) -> Optional[LeadPayload]:
    parties = split_parties(row["grantors"], (dot or {}).get("grantors") or None)
    borrower_found = bool(parties["borrowers"])
    if not borrower_found:
        # Every grantor read as a lender. Stage it anyway: an unstaged filing is
        # re-read on every run and would eat the per-run budget forever.
        if not row["grantors"]:
            return None
        parties = {**parties, "basis": "unidentified"}
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
    if not borrower_found:
        note += ". Borrower not identified; check the filing."
    if not address:
        note += ". No address yet: deed of trust not linked or has no street."
    return LeadPayload(
        bot_source=bot.name,
        pipeline_lead_key=bot.make_lead_key(bot.name, f"shelby|{row['instrument']}"),
        distress_type="SUBSTITUTION_OF_TRUSTEE",
        county="Shelby",
        property_address=address,
        full_name=parties["borrowers"][0] if borrower_found else None,
        owner_name_records="; ".join(parties["borrowers"]) or None,
        mortgage_balance=balance,
        source_url=row.get("detail_url"),
        admin_notes=note,
        raw_payload={
            "source": "shelby_register", "state": "TN", "instrument": row["instrument"], "code": row["code"],
            "recorded": row["recorded"], "appointment_date": (appt or {}).get("instrument_date"),
            "lenders": parties["lenders"], "borrowers": parties["borrowers"], "party_basis": parties["basis"],
            "substitute_trustees": sub_trustees, "deed_of_trust": (appt and deed_of_trust_ref(appt)) or None,
            # Preserve the index evidence even when a legacy reference cannot
            # be resolved by the numeric deed-of-trust lookup.
            **({"appointment_cross_references": appt["xrefs"]} if appt and appt.get("xrefs") else {}),
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

    days_back = 21              # allow catch-up across multiple scheduled weeks
    recent_slots = 2            # keep early signals moving during a backlog
    # 2 detail requests each at 30s, plus the search: 6 filings is ~7 minutes,
    # inside the runner's 10-minute per-bot cap (leads are written only when
    # scrape() returns, so a timed-out run stages nothing). Two runs a day read
    # 12, against ~5 new filings a business day.
    max_new_per_run = 6
    # Staged filings whose deed of trust was never read (an old run, a
    # failed request): re-read a few per run, newest first, at 1 request each.
    max_backfill_per_run = 3

    def __init__(self):
        super().__init__()
        # Transport retries would run inside fetch(), bypassing its crawl delay.
        self.session.mount(BASE + "/", HTTPAdapter(max_retries=0))

    def scrape(self) -> List[LeadPayload]:
        home = self.fetch(HOME, allow_redirects=False)  # session cookie
        if home is None or home.status_code != 200:
            return []
        end = date.today()
        start = end - timedelta(days=self.days_back)
        res = self.fetch(SEARCH, method="POST", allow_redirects=False, data={
            "itype1": CODE, "itype2": "", "startdate": start.strftime("%m/%d/%Y"),
            "enddate": end.strftime("%m/%d/%Y"), "searchtype": "ITYPE"})
        if res is None or res.status_code != 200:
            self.logger.warning(f"search failed ({getattr(res, 'status_code', 'no response')})")
            return []
        rows = {r["instrument"]: r for r in parse_results(res.text)
                if r["code"] == CODE and start.isoformat() <= (r["recorded"] or "") <= end.isoformat()}
        self.logger.info(f"{len(rows)} APPT filing(s) {start} to {end}")
        keyed = {self.make_lead_key(self.name, f"shelby|{inst}"): row for inst, row in rows.items()}
        known = self._known(set(keyed))
        fresh = sorted((row for key, row in keyed.items() if key not in known),
                       key=lambda row: (row["recorded"], row["instrument"]))
        recent_start = (end - timedelta(days=7)).isoformat()
        recent = [row for row in reversed(fresh) if row["recorded"] >= recent_start][:self.recent_slots]
        priority_ids = {row["instrument"] for row in recent}
        selected = (recent + [row for row in fresh if row["instrument"] not in priority_ids])[:self.max_new_per_run]
        leads: List[LeadPayload] = []
        for row in selected:
            appt = self._detail(row["detail_url"]) if row.get("detail_url") else None
            if row.get("detail_url") and appt is None:
                continue  # Retry the appointment before marking this instrument as collected.
            ref = deed_of_trust_ref(appt) if appt else None
            dot = self._detail(detail_url(ref["instrument"], ref["year"])) if ref else None
            if dot and not LOAN_DOC.search(dot.get("doc_type") or ""):
                dot = None  # cross reference points at something other than a deed of trust
            lead = build_lead(self, row, appt, dot)
            if lead:
                leads.append(lead)
        self.logger.info(f"staging {len(leads)} lead(s), {sum(1 for l in leads if l.property_address)} with an address")
        used = self.retry_deferred_appointment() if self.max_backfill_per_run >= 2 else 0
        self.backfill(self.max_backfill_per_run - used)
        return leads

    def _deferred_rows(self, client):
        return (client.table("homeowner_requests_staging")
                .select("id,raw_payload,property_address").eq("bot_source", self.name)
                .eq("staging_status", "pending")
                .eq("raw_payload->recovery->>source", "saved_register_APPT_index")
                .eq("raw_payload->recovery->>present_owner_check", "pending_independent_evidence")
                .not_.is_("raw_payload->recovery->>detail_issue", "null")
                .is_("raw_payload->recovery->>detail_checked_at", "null")
                .order("raw_payload->recovery->>last_detail_attempt", nullsfirst=True)
                .order("id").limit(1).execute().data)

    def _verified_detail(self, instrument, year):
        res = self.fetch(detail_url(instrument, year), allow_redirects=False)
        if res is None or res.status_code != 200:
            return None
        if not re.search(r"Information For Instrument #\s*" + re.escape(instrument) + r"\s*<", res.text, re.I):
            return None
        doc = parse_detail(res.text)
        return doc if doc.get("doc_type") else None

    def retry_deferred_appointment(self):
        """One pending index-only recovery per run, using at most two detail requests."""
        client = _supabase()
        if client is None:
            return 0
        used = 0
        try:
            rows = self._deferred_rows(client)
            if not rows:
                return 0
            row = rows[0]
            original = row["raw_payload"]
            raw = dict(original)
            recovery = dict(raw["recovery"])
            raw["recovery"] = recovery
            now = datetime.now(timezone.utc).isoformat()
            recovery["last_detail_attempt"] = now
            used = 1
            appt = self._verified_detail(raw["instrument"], raw["recorded"][:4])
            patch = {"raw_payload": raw}
            if appt and appt["doc_type"].upper().startswith("APPOINTMENT OF SUBSTITUTE TR"):
                ref = deed_of_trust_ref(appt)
                dot = None
                if ref:
                    used += 1
                    dot = self._verified_detail(ref["instrument"], ref["year"])
                if appt.get("xrefs") and not raw.get("appointment_cross_references"):
                    raw["appointment_cross_references"] = appt["xrefs"]
                if not raw.get("appointment_date"):
                    raw["appointment_date"] = appt.get("instrument_date")
                if not ref or dot:
                    recovery["detail_checked_at"] = now
                    recovery["detail_outcome"] = "no_linked_loan"
                if dot and LOAN_DOC.fullmatch(dot["doc_type"]):
                    address = address_of(dot)
                    parcel = dot.get("parcel_id")
                    recovery.update(detail_outcome="linked_loan", loan_reference=ref,
                                    loan_grantors=dot["grantors"], register_address=address, register_parcel=parcel)
                    # Keep a disagreement in source evidence for review. Never combine
                    # a new parcel with a different address or loan from an earlier read.
                    agrees = (raw.get("deed_of_trust") in (None, ref)
                              and raw.get("parcel_id") in (None, parcel)
                              and row["property_address"] in (None, address))
                    if agrees:
                        raw["deed_of_trust"] = ref
                        if parcel:
                            raw["parcel_id"] = parcel
                        if row["property_address"] is None and address:
                            patch["property_address"] = address
                        raw["dot_checked_at"] = raw.get("dot_checked_at") or now[:10]
            update = (client.table("homeowner_requests_staging").update(patch)
                      .eq("id", row["id"]).eq("bot_source", self.name).eq("staging_status", "pending")
                      .filter("raw_payload", "eq", json.dumps(original, separators=(",", ":"))))
            address = row["property_address"]
            update = update.is_("property_address", "null") if address is None else update.eq("property_address", address)
            update.execute()
        except Exception as exc:
            self.logger.warning("Deferred appointment retry failed (%s)", type(exc).__name__)
        return used

    def backfill(self, limit: int) -> int:
        """Read the deed of trust for staged filings that have its reference
        but no address, and fill address, borrowers, loan and parcel."""
        client = _supabase()
        if client is None or limit <= 0:
            return 0
        try:
            rows = (client.table("homeowner_requests_staging")
                    .select("id,source_url,raw_payload").eq("bot_source", self.name)
                    .is_("property_address", "null").not_.is_("raw_payload->deed_of_trust", "null")
                    .is_("raw_payload->dot_checked_at", "null")
                    .order("staged_at", desc=True).limit(limit).execute().data) or []
        except Exception as e:  # noqa: BLE001
            self.logger.warning(f"backfill query failed: {e}")
            return 0
        filled = 0
        for r in rows:
            raw = r.get("raw_payload") if isinstance(r.get("raw_payload"), dict) else None
            ref = (raw or {}).get("deed_of_trust") or {}
            if not raw or not ref.get("instrument"):
                continue
            dot = self._detail(detail_url(ref["instrument"], ref.get("year") or ""))
            if dot is None:
                continue  # request failed: try again next run
            update: Dict[str, Any] = {}
            if LOAN_DOC.search(dot.get("doc_type") or ""):
                row = {"instrument": raw.get("instrument"), "code": raw.get("code") or CODE,
                       "grantors": (raw.get("lenders") or []) + (raw.get("borrowers") or []),
                       "grantees": raw.get("substitute_trustees") or [], "recorded": raw.get("recorded"),
                       "detail_url": r.get("source_url")}
                lead = build_lead(self, row, {"instrument_date": raw.get("appointment_date")}, dot)
                if lead:
                    d = lead.as_db_row("")
                    for k in ("property_address", "full_name", "owner_name_records", "mortgage_balance", "admin_notes"):
                        if d.get(k) is not None:
                            update[k] = d[k]
                    raw = {**raw, **{k: v for k, v in (lead.raw_payload or {}).items() if v is not None and k != "deed_of_trust"}}
            raw["dot_checked_at"] = date.today().isoformat()
            update["raw_payload"] = raw
            try:
                client.table("homeowner_requests_staging").update(update).eq("id", r["id"]).execute()
                filled += 1 if update.get("property_address") else 0
            except Exception as e:  # noqa: BLE001
                self.logger.warning(f"backfill write failed id={r['id']}: {e}")
        self.logger.info(f"backfill: {len(rows)} re-read, {filled} now have an address")
        return filled

    def _detail(self, url: str) -> Optional[Dict[str, Any]]:
        res = self.fetch(url, allow_redirects=False)
        if res is None or res.status_code != 200:
            return None
        return parse_detail(res.text)

    def _known(self, keys: Set[str]) -> Set[str]:
        """Lead keys already in staging, so their detail pages are not re-read."""
        client = _supabase()
        if client is None or not keys:
            return set()
        ordered = sorted(keys)
        known = set()
        # Bound the URL length when a busy three-week index contains hundreds of filings.
        for offset in range(0, len(ordered), 100):
            res = (client.table("homeowner_requests_staging").select("pipeline_lead_key")
                   .eq("bot_source", self.name).in_("pipeline_lead_key", ordered[offset:offset + 100]).execute())
            known.update(r["pipeline_lead_key"] for r in res.data or [])
        return known


def run() -> dict:
    return ShelbySubstituteTrusteeBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
