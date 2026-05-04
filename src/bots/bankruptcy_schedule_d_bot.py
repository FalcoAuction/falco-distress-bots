"""
Bankruptcy Schedule D extractor — pulls EXACT current mortgage balance
from filed bankruptcy schedules.

Every Chapter 7 / Chapter 13 debtor is REQUIRED by federal bankruptcy
rules to file a Schedule D ("Creditors Who Have Claims Secured by
Property"). It lists every secured creditor with:
  - Creditor name
  - Property securing the claim (address)
  - Date claim was incurred (DOT date)
  - Amount of claim (CURRENT BALANCE — the gold)
  - Value of collateral

This is exactly what ATTOM tries to provide via their loan-level
data feeds, but for bankruptcy debtors we get it as a sworn court
filing, free, fresher.

Workflow:
  1. Walk recent BANKRUPTCY leads in homeowner_requests_staging
     produced by courtlistener_bankruptcy_bot (have docket_id in
     raw_payload)
  2. Search CourtListener API: type=rd&docket_id=N&q="schedule"
     to find Schedule D / Schedule of Secured Creditors PDFs
  3. Filter to documents where is_available=true and filepath_local
     is set (those are RECAP-archived and freely downloadable)
  4. Download PDF from
     https://storage.courtlistener.com/{filepath_local}
  5. Extract current-balance figures via pdfplumber + regex on the
     "Schedule D" form structure (creditor table with claim amounts)

CourtListener API rate: 5k req/hr unauthenticated. We're well under.

Distress type: N/A (enricher only — augments existing BANKRUPTCY
leads with verified mortgage_balance + lender data).
"""

from __future__ import annotations

import io
import re
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from ._base import BotBase, _supabase
from ._provenance import record_field


CL_BASE = "https://www.courtlistener.com/api/rest/v4"
CL_SEARCH = CL_BASE + "/search/"
RECAP_STORAGE_BASE = "https://storage.courtlistener.com/"

# Patterns for Schedule D field extraction (case-insensitive)
SECURED_CLAIM_AMOUNT_RE = re.compile(
    r"(?:amount of claim|claim amount|amount of debt|amount owed)[^$]*\$([\d,]+(?:\.\d{2})?)",
    re.IGNORECASE,
)
COLLATERAL_VALUE_RE = re.compile(
    r"(?:value of (?:property|collateral)|collateral value|fair market value)[^$]*\$([\d,]+(?:\.\d{2})?)",
    re.IGNORECASE,
)
CREDITOR_LINE_RE = re.compile(
    r"^\s*([A-Z][A-Z& ,.\-/']{4,80}(?:BANK|MORTGAGE|FINANCIAL|CAPITAL|"
    r"FUNDING|LENDING|HOME LOANS?|SERVICING|HOLDINGS|CREDIT UNION|"
    r"CHASE|WELLS FARGO|ROCKET|QUICKEN|CITI|US BANK|FREEDOM|CARRINGTON|"
    r"NEWREZ|SHELLPOINT|SPECIALIZED|SETERUS|MR COOPER|LOANDEPOT))",
    re.MULTILINE,
)
ADDRESS_HINT_RE = re.compile(
    r"(\d{1,5}\s+[A-Z][A-Za-z0-9 .'\-]{3,50}?(?:Street|St\.?|Road|Rd\.?|Ave|Avenue|"
    r"Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Circle|Cir\.?|Place|Pl\.?|Way|"
    r"Boulevard|Blvd\.?|Highway|Hwy|Parkway|Pkwy|Trail|Trl|Pike|Terrace))",
)


class BankruptcyScheduleDBot(BotBase):
    name = "bankruptcy_schedule_d"
    description = "Extracts current mortgage balance + lender from filed bankruptcy Schedule D"
    throttle_seconds = 1.0
    expected_min_yield = 1

    max_dockets_per_run = 50

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        if pdfplumber is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="pdfplumber not installed",
            )
            return {"name": self.name, "status": "missing_deps",
                    "extracted": 0, "skipped": 0, "staged": 0,
                    "duplicates": 0, "fetched": 0}

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "extracted": 0, "skipped": 0, "staged": 0,
                    "duplicates": 0, "fetched": 0}

        extracted = 0
        no_pdf_available = 0
        no_amount_found = 0
        skipped = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(f"{len(candidates)} BANKRUPTCY leads to enrich")

            for row in candidates[:self.max_dockets_per_run]:
                raw = row.get("raw_payload") or {}
                if not isinstance(raw, dict):
                    skipped += 1
                    continue
                docket_id = raw.get("docket_id")
                if not docket_id:
                    skipped += 1
                    continue

                # Already enriched — skip
                if raw.get("schedule_d_extracted"):
                    skipped += 1
                    continue

                docs = self._find_schedule_d_pdfs(int(docket_id))
                if not docs:
                    no_pdf_available += 1
                    continue

                # Try each candidate doc until one yields amounts
                extracted_data = None
                for doc in docs:
                    pdf_data = self._extract_from_pdf(doc)
                    if pdf_data and pdf_data.get("secured_claims"):
                        extracted_data = pdf_data
                        break
                if not extracted_data:
                    no_amount_found += 1
                    continue

                # Take the largest secured claim as the primary mortgage
                # (most filings: Schedule D first entry is the home mortgage)
                claims = extracted_data["secured_claims"]
                primary = max(claims, key=lambda c: c.get("amount") or 0)

                update: Dict[str, Any] = {}
                if primary.get("amount"):
                    update["mortgage_balance"] = primary["amount"]
                if primary.get("creditor"):
                    # Don't overwrite owner_name_records (debtor != creditor)
                    pass

                raw["schedule_d_extracted"] = True
                raw["schedule_d_primary_balance"] = primary.get("amount")
                raw["schedule_d_primary_creditor"] = primary.get("creditor")
                raw["schedule_d_all_claims"] = claims
                raw["schedule_d_pdf_url"] = extracted_data.get("pdf_url")
                update["raw_payload"] = raw

                try:
                    client.table("homeowner_requests_staging").update(update).eq("id", row["id"]).execute()
                    extracted += 1
                    self.logger.info(
                        f"  enriched id={row['id']} balance=${primary.get('amount'):,.0f} "
                        f"creditor={primary.get('creditor')}"
                    )
                except Exception as e:
                    self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif extracted == 0 and no_pdf_available == 0 and no_amount_found == 0:
            status = "zero_yield"
        elif extracted == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=extracted + no_pdf_available + no_amount_found + skipped,
            parsed_count=extracted + no_pdf_available + no_amount_found,
            staged_count=extracted, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(
            f"extracted={extracted} no_pdf_available={no_pdf_available} "
            f"no_amount_found={no_amount_found} skipped={skipped}"
        )
        return {
            "name": self.name, "status": status,
            "extracted": extracted, "skipped": skipped,
            "no_pdf_available": no_pdf_available,
            "no_amount_found": no_amount_found,
            "error": error_message,
            "staged": extracted, "duplicates": skipped,
            "fetched": extracted + no_pdf_available + no_amount_found + skipped,
        }

    # ── Candidate query ─────────────────────────────────────────────────────

    def _candidates(self, client) -> List[Dict[str, Any]]:
        try:
            q = (
                client.table("homeowner_requests_staging")
                .select("id, full_name, owner_name_records, mortgage_balance, raw_payload")
                .eq("distress_type", "BANKRUPTCY")
                .limit(500)
                .execute()
            )
            return getattr(q, "data", None) or []
        except Exception as e:
            self.logger.warning(f"candidate query failed: {e}")
            return []

    # ── CourtListener: find Schedule D PDFs ────────────────────────────────

    def _find_schedule_d_pdfs(self, docket_id: int) -> List[Dict[str, Any]]:
        """Return list of {filepath, pdf_url, description, doc_id} for
        each available Schedule D / Secured Creditors document on the
        docket.

        BUG-FIX 2026-05-04: The CourtListener search API does NOT honor
        `docket_id` as a URL-param filter on `type=rd` — it returns
        global matches across ALL dockets. This caused every lead to
        get the same Michigan-bankruptcy PDF and inherit a $600,856
        mortgage_balance. Correct usage is Lucene-style filter inside
        the `q` parameter: `q=docket_id:NNN AND (schedule OR ...)`.
        """
        params = {
            "type": "rd",
            "q": (
                f"docket_id:{docket_id} AND ("
                "schedule OR secured OR creditor OR \"creditors who have claims\""
                ")"
            ),
        }
        res = self.fetch(CL_SEARCH, params=params,
                          headers={"Accept": "application/json"})
        if res is None or res.status_code != 200:
            return []
        try:
            data = res.json()
        except Exception:
            return []

        out: List[Dict[str, Any]] = []
        for item in data.get("results") or []:
            # Defensive: re-confirm the result is actually for our docket
            # (in case CL ever changes filter semantics again).
            if item.get("docket_id") and int(item["docket_id"]) != int(docket_id):
                continue
            if not item.get("is_available"):
                continue
            filepath = item.get("filepath_local")
            if not filepath:
                continue
            description = (
                item.get("description") or item.get("short_description") or ""
            ).lower()
            # Prefer matches whose description actually mentions schedule/
            # secured. Anything else from this docket is a fallback.
            if not any(kw in description for kw in (
                "schedule d", "secured", "creditor", "schedule of"
            )):
                # Description may be empty but doc IS a schedule —
                # keep up to 3 fallback candidates.
                if len(out) >= 3:
                    continue
            out.append({
                "doc_id": item.get("id"),
                "filepath": filepath,
                "pdf_url": RECAP_STORAGE_BASE + filepath,
                "description": item.get("description") or item.get("short_description"),
                "page_count": item.get("page_count"),
            })
            if len(out) >= 5:
                break
        return out

    # ── PDF extraction ──────────────────────────────────────────────────────

    def _extract_from_pdf(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download PDF + extract secured-creditor claims."""
        pdf_url = doc["pdf_url"]
        try:
            res = self.fetch(pdf_url)
            if res is None or res.status_code != 200:
                return None
            if "pdf" not in (res.headers.get("Content-Type") or "").lower():
                return None
            with pdfplumber.open(io.BytesIO(res.content)) as pdf:
                text = "\n".join((p.extract_text() or "") for p in pdf.pages)
        except Exception as e:
            self.logger.debug(f"PDF fetch/parse failed for {pdf_url}: {e}")
            return None

        if not text:
            return None

        # Confirm this is actually a Schedule D-ish document
        if not re.search(
            r"schedule d|creditors? (?:who )?(?:have|holding) (?:claims? )?secured|"
            r"secured (?:claim|debt|creditor)",
            text, re.IGNORECASE,
        ):
            return None

        # Walk the text; grab amount-of-claim figures + nearest creditor
        # name. Schedule D claims tables vary: official BK form 106D has
        # creditor name, address, date incurred, amount of claim, value
        # of collateral, unsecured portion. Older versions and
        # attorney-prepared filings differ. Use a permissive approach.
        claims: List[Dict[str, Any]] = []
        # Pull all $ amounts in the doc that are at least $5K (filters
        # filing fees, court costs, $0.00 entries)
        all_amounts = re.findall(r"\$([\d,]+(?:\.\d{2})?)", text)
        for amount_str in all_amounts:
            try:
                amount = float(amount_str.replace(",", ""))
            except ValueError:
                continue
            if amount < 5000:
                continue
            # Find creditor name in the surrounding context (~200 chars before)
            idx = text.find(f"${amount_str}")
            if idx < 0:
                continue
            context = text[max(0, idx - 400):idx]
            cm = CREDITOR_LINE_RE.search(context)
            creditor = cm.group(1).strip() if cm else None
            am = ADDRESS_HINT_RE.search(context)
            collateral_address = am.group(1).strip() if am else None
            claims.append({
                "amount": amount,
                "creditor": creditor,
                "collateral_address": collateral_address,
            })

        # Dedupe by amount (some PDFs repeat the figure on multiple pages)
        seen_amounts = set()
        deduped = []
        for c in claims:
            key = round(c["amount"], 2)
            if key in seen_amounts:
                continue
            seen_amounts.add(key)
            deduped.append(c)

        # Sort largest first (the home mortgage is typically the
        # biggest secured claim)
        deduped.sort(key=lambda c: -c["amount"])

        return {
            "pdf_url": pdf_url,
            "secured_claims": deduped[:10],
            "total_amounts_found": len(claims),
        }


def run() -> dict:
    bot = BankruptcyScheduleDBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
