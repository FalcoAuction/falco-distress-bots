"""Resume free Shelby mortgage/parcel histories without claiming title clearance.

Index searches are parcel-specific. Only explicit full releases linked to a
loan affect its status. Sales are evidence, not current appraisals. Every
request is paced at 30 seconds; progress survives a per-run request budget.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone, timedelta
from collections import Counter
from urllib.parse import urlparse

from ._base import BotBase, _supabase
from ._shelby_register import BASE, detail_url, parse_detail, parse_results, address_of
from .shelby_loan_value_bot import dot_ref
from .free_county_completion_bot import apply
from ._free_data import tracked_run
from ..enrichment.lien_stack import Instrument, lien_stack


def record_type(doc):
    text = str(doc.get("doc_type") or doc.get("code") or "").upper()
    if "PARTIAL" in text or text in {"PR", "PRL"}:
        return "PARTIAL_RELEASE"
    if text in {"REL", "RL", "SAT", "RELEASE"} or "RELEASE" in text or "SATISFACTION" in text:
        return "RELEASE"
    if text in {"TD", "DT", "MTG"} or "MORTGAGE" in text or "DEED OF TRUST" in text:
        return "DOT"
    return "OTHER"


def evidence_stack(documents):
    instruments = []
    for key, doc in documents.items():
        kind = record_type(doc)
        day = doc.get("file_date") or doc.get("recorded")
        if kind not in {"DOT", "RELEASE"} or not day:
            continue
        # Detail has to establish release references; search results alone do not.
        if kind == "RELEASE" and not doc.get("detail_read"):
            continue
        try:
            instruments.append(Instrument(kind, date.fromisoformat(day),
                amount=doc.get("mortgage_amount"), instrument=key,
                lender="; ".join(doc.get("grantees") or []),
                refs=[r["instrument"] for r in doc.get("xrefs", []) if r.get("instrument")]))
        except (ValueError, TypeError):
            continue
    stack = lien_stack(instruments, date.today())
    # A later loan is not proof of refinancing/payoff; retain the uncertainty.
    for lien in stack["liens"]:
        if lien["status"] == "likely_paid":
            lien["status"] = "unresolved"
        if lien["status"] == "open":
            lien["status"] = "no_release_found_in_retrieved_records"
        if lien["status"] != "released":
            # Recorded principal does not establish today's debt. Keep it
            # without an assumed rate/term balance masquerading as a range.
            lien["balance_low"] = lien["balance_high"] = None
            lien["basis"] = "recorded_principal_only_payoff_unknown"
    stack["counts"] = dict(Counter(l["status"] for l in stack["liens"]))
    stack["coverage"] = "partial_public_index"
    stack["payoff_verified"] = False
    stack["equity"] = None
    return stack


class ShelbyFreeChainBot(BotBase):
    name = "shelby_free_chain"
    description = "Free Shelby parcel and mortgage history; explicit incomplete evidence"
    throttle_seconds = 30

    def __init__(self, write=True, max_fetches=12):
        super().__init__()
        self.write, self.max_fetches, self.fetched = write, max_fetches, 0

    def scrape(self):
        return []

    def read(self, url, **kwargs):
        # Only the observed public index endpoints, never document images.
        parsed = urlparse(url)
        if parsed.netloc != "search.register.shelby.tn.us" or parsed.path not in {"/p2.php", "/pdetail.php"}:
            raise ValueError("unexpected register URL")
        if self.fetched >= self.max_fetches:
            return None
        self.fetched += 1
        response = self.fetch(url, **kwargs)
        if response is None or response.status_code != 200:
            raise RuntimeError("register request failed; retry on next run")
        return response.text

    def enrich(self, row):
        raw = dict(row.get("raw_payload") or {})
        prior = raw.get("shelby_chain") or {}
        try:
            recent = datetime.fromisoformat(prior.get("updated_at", "")) > datetime.now(timezone.utc) - timedelta(days=7)
        except (ValueError, TypeError):
            recent = False
        chain = {**prior, "documents": dict(prior.get("documents") or {})}
        docs = chain["documents"]
        ref = raw.get("deed_of_trust") or dot_ref(raw)
        if not isinstance(ref, dict):
            ref = None
        parcel = chain.get("parcel_id") or raw.get("parcel_id") or (raw.get("value_estimate") or {}).get("parcel_id")
        evidence_key = [parcel, ref]
        if recent and prior.get("finished"):
            return {}
        if recent and prior.get("status") in {"needs_instrument_or_parcel", "needs_parcel_review"} and prior.get("evidence_key") == evidence_key:
            return {}
        chain["evidence_key"] = evidence_key
        if prior.get("finished") and not recent:
            chain.pop("parcel_search", None)
        if ref and ref.get("instrument") and ref["instrument"] not in docs:
            html = self.read(detail_url(ref["instrument"], ref.get("year") or ""))
            if html is None:
                return {}
            doc = parse_detail(html)
            docs[ref["instrument"]] = {**doc, "detail_read": bool(doc.get("doc_type")),
                                      "unreadable_detail": not bool(doc.get("doc_type")),
                                      "source_url": detail_url(ref["instrument"], ref.get("year") or "")}
            parcel = parcel or doc.get("parcel_id")
        chain["parcel_id"] = parcel
        if parcel and "parcel_search" not in chain:
            html = self.read(BASE + "/p2.php", method="POST", data={
                "parcelid": parcel, "subname": "", "lot": "", "searchtype": "PID",
                "startDate": "01/01/1995", "endDate": date.today().strftime("%m/%d/%Y")})
            if html is not None:
                results = parse_results(html)
                # An empty result is not proof of clear title (or a working parser).
                chain["parcel_search"] = {"rows": len(results), "since": "1995-01-01",
                    "truncated": len(results) >= 1000, "checked_at": datetime.now(timezone.utc).isoformat()}
                for doc in results:
                    docs.setdefault(doc["instrument"], {**doc, "source_url": doc.get("detail_url")})
        for key, doc in list(docs.items()):
            if record_type(doc) not in {"DOT", "RELEASE", "PARTIAL_RELEASE"} or doc.get("detail_read") or doc.get("unreadable_detail"):
                continue
            url = doc.get("detail_url") or doc.get("source_url")
            if not url:
                continue
            html = self.read(url)
            if html is None:
                break
            detail = parse_detail(html)
            if not detail.get("doc_type"):
                docs[key] = {**doc, "unreadable_detail": True}
            else:
                docs[key] = {**doc, **detail, "detail_read": True}
        pending = any(record_type(d) in {"DOT", "RELEASE", "PARTIAL_RELEASE"} and not d.get("detail_read") and not d.get("unreadable_detail")
                      for d in docs.values())
        chain["finished"] = bool(chain.get("parcel_search")) and not pending
        chain["status"] = ("retrieved_partial_history" if chain["finished"] else
                           "needs_parcel_review" if ref and not parcel and docs else
                           "pending" if parcel or ref else "needs_instrument_or_parcel")
        chain["sales"] = [{"instrument": k, "recorded": d.get("recorded"), "amount": d["transfer_amount"],
                           "code": d.get("code"), "source_url": d.get("source_url")}
                          for k, d in docs.items() if d.get("transfer_amount")]
        chain["updated_at"] = datetime.now(timezone.utc).isoformat()
        raw["shelby_chain"] = chain
        raw["lien_check"] = {**(raw.get("lien_check") or {}), "register_system": "shelby_register_free",
            "status": "partial_review_required" if chain["finished"] else chain["status"],
            "stack": evidence_stack(docs), "equity": None,
            "limitations": ["Not a title search or verified payoff", "Other liens and missing releases may exist",
                            "Parcel index begins 1995; older and unindexed documents require review"],
            "researched_at": chain["updated_at"]}
        patch = {"raw_payload": raw}
        # Only linked deed evidence may fill a missing address, never a name-only search.
        if ref and ref.get("instrument") in docs:
            doc = docs[ref["instrument"]]
            if record_type(doc) == "DOT":
                if not row.get("property_address") and address_of(doc):
                    patch["property_address"] = address_of(doc)
                # Mortgage grantors are historical borrowers, not proof of
                # current ownership. Keep them on the document only.
        return patch

    def run(self):
        return tracked_run(self.name, self._run, self.write)

    def _run(self):
        client = _supabase()
        if client is None:
            return {"status": "no_supabase"}
        stats = Counter()
        after = "00000000-0000-0000-0000-000000000000"
        candidates = []
        while True:
            rows = (client.table("homeowner_requests_staging").select("id,raw_payload,phone_metadata,property_address,owner_name_records")
                .ilike("county", "shelby%").gt("id", after).order("id").limit(100)
                .or_(f"trustee_sale_date.gte.{date.today().isoformat()},trustee_sale_date.is.null").execute().data) or []
            if not rows:
                break
            candidates.extend(rows)
            after = rows[-1]["id"]
        # The ranked lien queue gets the finite request budget first.
        candidates.sort(key=lambda r: ((r.get("raw_payload") or {}).get("lien_check") or {}).get("status") != "pending_free_lookup")
        for row in candidates:
            if self.fetched >= self.max_fetches:
                break
            stats["scanned"] += 1
            try:
                patch = self.enrich(row)
                if patch:
                    stats["changed"] += 1
                    if self.write:
                        stats["written"] += apply(client, "homeowner_requests_staging", row, patch)
            except RuntimeError as exc:
                stats["request_errors"] += 1
                # Do not hammer a blocked or changed source.
                return {"name": self.name, "status": "retry_needed", "error": str(exc), "fetched": self.fetched, **stats}
        return {"name": self.name, "status": "ok", "fetched": self.fetched,
                "staged": stats["written"], **stats}


def run():
    return ShelbyFreeChainBot().run()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--write", action="store_true")
    p.add_argument("--max-fetches", type=int, default=12)
    args = p.parse_args()
    print(ShelbyFreeChainBot(write=args.write, max_fetches=args.max_fetches).run())
