"""Re-extract principal + lender from existing nashville_ledger leads
using improved regex patterns. Free + idempotent.

For each existing nashville_ledger lead:
  1. Reconstruct the tnledger.com source URL from raw_payload.structured.tdn_no
  2. Re-fetch the notice HTML
  3. Run improved parsers (lender + principal + DOT recording)
  4. Save body to raw_payload.body (so future iterations are free)
  5. Update raw_payload.extracted with new fields
  6. If principal newly extracted: write mortgage_balance + provenance
     with source='nashville_ledger_extracted'
  7. If lender newly extracted: write phone_metadata.mortgage_signal.lender

Run via:
  python -m src.bots.nashville_ledger_reextract_bot

Env:
  FALCO_LEDGER_REEXTRACT_FOCUS_ONLY  (=1 to only re-process Middle TN focus)
  FALCO_LEDGER_REEXTRACT_SAMPLE       (=1 for dry-run, no DB writes)
  FALCO_LEDGER_REEXTRACT_MAX          (default 200)
"""
from __future__ import annotations

import os
import re
import sys
import time
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from ._base import BotBase, _supabase
from ._provenance import record_field
from .nashville_ledger_bot import (
    LENDER_PATTERNS, PRINCIPAL_PATTERNS, DOT_BOOK_RE,
)


CORE_COUNTIES = {"davidson", "williamson", "sumner", "rutherford", "wilson"}
STRETCH_COUNTIES = {"maury", "montgomery"}
FOCUS_COUNTIES = CORE_COUNTIES | STRETCH_COUNTIES

DETAIL_URL = "https://www.tnledger.com/Search/Details/ViewNotice.aspx"


def _normalize_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


def _build_url_from_raw(raw: Dict[str, Any]) -> Optional[str]:
    structured = raw.get("structured") or {}
    if not isinstance(structured, dict):
        return None
    tdn = structured.get("tdn_no")
    pub = raw.get("publication_date")
    if not tdn or not pub:
        return None
    try:
        dt = datetime.fromisoformat(pub)
    except Exception:
        return None
    return f"{DETAIL_URL}?id={tdn}&date={dt.month}/{dt.day}/{dt.year}"


# Strings that mean we matched boilerplate, not a lender entity
LENDER_REJECT_PREFIXES = (
    "the association",            # HOA assessments boilerplate
    "in accordance",              # "in accordance with the terms..."
    "has demanded",
    "pursuant to",
    "attorney's fees",
    "association for",
    "the association for",
    "the deed of trust",
)


def _sanitize_lender(raw: str) -> Optional[str]:
    """Reject obvious boilerplate captures, normalize whitespace."""
    if not raw:
        return None
    name = " ".join(raw.split())
    low = name.lower()
    for bad in LENDER_REJECT_PREFIXES:
        if low.startswith(bad):
            return None
    # Lender names are typically 1-7 words. >8 words = grabbed a sentence.
    if len(name.split()) > 8:
        return None
    # Must contain at least one uppercase letter at the start of a word
    # (real entity names aren't all-lowercase phrases)
    if not re.search(r"\b[A-Z]", name):
        return None
    # Strip trailing punctuation
    name = name.rstrip(".,;:")
    if len(name) < 3:
        return None
    return name


def _parse_notice(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style"]):
        t.decompose()
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    principal = None
    for pat in PRINCIPAL_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                principal = float(m.group(1).replace(",", ""))
                break
            except Exception:
                continue

    lender = None
    for pat in LENDER_PATTERNS:
        m = pat.search(text)
        if m:
            candidate = _sanitize_lender(m.group(1))
            if candidate:
                lender = candidate
                break

    dot_recording = None
    m = DOT_BOOK_RE.search(text)
    if m:
        dot_recording = f"{m.group(1)}-{m.group(2)}"

    return {
        "principal": principal,
        "lender": lender,
        "dot_recording": dot_recording,
        "body": text,
    }


class NashvilleLedgerReextractBot(BotBase):
    name = "nashville_ledger_reextract"
    description = (
        "Re-fetch existing nashville_ledger notices and re-run improved "
        "lender + principal extractors. Saves body for free future iteration."
    )
    throttle_seconds = 0.6
    expected_min_yield = 0
    max_leads_per_run = 200

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

        focus_only = os.environ.get("FALCO_LEDGER_REEXTRACT_FOCUS_ONLY") == "1"
        sample = os.environ.get("FALCO_LEDGER_REEXTRACT_SAMPLE") == "1"
        max_per_run = int(
            os.environ.get("FALCO_LEDGER_REEXTRACT_MAX", self.max_leads_per_run)
        )

        candidates = self._candidates(client, focus_only, max_per_run)
        self.logger.info(
            f"{len(candidates)} nashville_ledger leads to re-extract "
            f"(focus_only={focus_only}, sample={sample})"
        )

        sess = requests.Session()
        sess.headers.update({"User-Agent": "Mozilla/5.0"})

        attempted = 0
        new_principal = 0
        new_lender = 0
        new_dot_recording = 0
        url_missing = 0
        fetch_errors = 0

        try:
            for lead in candidates:
                if attempted >= max_per_run:
                    break
                raw = lead.get("raw_payload") or {}
                if not isinstance(raw, dict):
                    continue
                url = _build_url_from_raw(raw)
                if not url:
                    url_missing += 1
                    continue

                attempted += 1
                try:
                    r = sess.get(url, timeout=15)
                    if r.status_code != 200:
                        fetch_errors += 1
                        continue
                except Exception as e:
                    self.logger.warning(
                        f"  fetch failed id={lead['id']}: {e}"
                    )
                    fetch_errors += 1
                    continue

                parsed = _parse_notice(r.text)
                existing_extracted = raw.get("extracted") or {}
                if not isinstance(existing_extracted, dict):
                    existing_extracted = {}

                got_new_principal = (
                    parsed["principal"] is not None
                    and existing_extracted.get("original_principal") is None
                )
                got_new_lender = (
                    parsed["lender"] is not None
                    and existing_extracted.get("lender") is None
                )
                got_new_dot = (
                    parsed["dot_recording"] is not None
                    and existing_extracted.get("dot_recording") is None
                )

                if got_new_principal:
                    new_principal += 1
                if got_new_lender:
                    new_lender += 1
                if got_new_dot:
                    new_dot_recording += 1

                if sample:
                    if got_new_principal or got_new_lender:
                        self.logger.info(
                            f"  SAMPLE id={lead['id'][:8]} "
                            f"county={lead.get('county')} | "
                            f"principal={parsed['principal']} "
                            f"lender={parsed['lender']!r}"
                        )
                    time.sleep(self.throttle_seconds)
                    continue

                # Update extracted block — keep existing values, fill new
                merged_extracted = dict(existing_extracted)
                if parsed["lender"]:
                    merged_extracted["lender"] = parsed["lender"]
                if parsed["principal"] is not None:
                    merged_extracted["original_principal"] = parsed["principal"]
                if parsed["dot_recording"]:
                    merged_extracted["dot_recording"] = parsed["dot_recording"]

                new_raw = dict(raw)
                new_raw["extracted"] = merged_extracted
                new_raw["body"] = parsed["body"]  # store for future iteration
                new_raw["last_reextract_at"] = datetime.now(timezone.utc).isoformat()

                update: Dict[str, Any] = {"raw_payload": new_raw}

                # Promote new principal to mortgage_balance
                if got_new_principal:
                    update["mortgage_balance"] = int(parsed["principal"])

                # Add lender to phone_metadata.mortgage_signal so it shows
                # on the dialer math sheet
                if got_new_lender:
                    pm = lead.get("phone_metadata") or {}
                    if not isinstance(pm, dict):
                        pm = {}
                    sig = pm.get("mortgage_signal") or {}
                    if not isinstance(sig, dict):
                        sig = {}
                    sig["lender"] = parsed["lender"]
                    sig["source"] = "nashville_ledger_extracted"
                    sig["confidence"] = 0.85
                    pm["mortgage_signal"] = sig
                    update["phone_metadata"] = pm

                table = lead["__table__"]
                try:
                    client.table(table).update(update).eq("id", lead["id"]).execute()
                except Exception as e:
                    self.logger.warning(f"  update failed id={lead['id']}: {e}")
                    continue

                # Provenance for the new principal (only homeowner_requests
                # has provenance writes per existing convention)
                if got_new_principal and table == "homeowner_requests":
                    try:
                        record_field(
                            client, lead["id"], "mortgage_balance",
                            int(parsed["principal"]),
                            "nashville_ledger_extracted",
                            confidence=0.85,
                            metadata={
                                "lender": parsed["lender"],
                                "dot_recording": parsed["dot_recording"],
                            },
                        )
                    except Exception as e:
                        self.logger.warning(
                            f"  provenance write failed id={lead['id']}: {e}"
                        )

                time.sleep(self.throttle_seconds)

        except Exception as e:
            err = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
            return self._wrap(
                started, attempted, new_principal, new_lender,
                new_dot_recording, url_missing, fetch_errors,
                status="failed", error=err,
            )

        self.logger.info(
            f"attempted={attempted} new_principal={new_principal} "
            f"new_lender={new_lender} new_dot_recording={new_dot_recording} "
            f"url_missing={url_missing} fetch_errors={fetch_errors}"
        )
        return self._wrap(
            started, attempted, new_principal, new_lender,
            new_dot_recording, url_missing, fetch_errors,
        )

    def _candidates(self, client, focus_only: bool, max_per_run: int) -> List[Dict[str, Any]]:
        out = []
        for table, src_field in (
            ("homeowner_requests_staging", "bot_source"),
            ("homeowner_requests", "source"),
        ):
            page = 0
            while True:
                try:
                    r = (
                        client.table(table)
                        .select(
                            "id, county, raw_payload, owner_name_records, "
                            "mortgage_balance, phone_metadata"
                        )
                        .eq(src_field, "nashville_ledger")
                        .range(page * 1000, (page + 1) * 1000 - 1)
                        .execute()
                    )
                    rows = getattr(r, "data", None) or []
                    if not rows:
                        break
                    for row in rows:
                        if focus_only and _normalize_county(row.get("county")) not in FOCUS_COUNTIES:
                            continue
                        row["__table__"] = table
                        out.append(row)
                    if len(rows) < 1000:
                        break
                    page += 1
                except Exception as e:
                    self.logger.warning(
                        f"candidate query on {table}: {e}"
                    )
                    break
        return out[:max_per_run]

    def _fail(self, started, msg: str) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status="failed", started_at=started, finished_at=finished,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
            error_message=msg,
        )
        return {"name": self.name, "status": "failed", "error": msg,
                "extracted": 0, "staged": 0, "duplicates": 0, "fetched": 0}

    def _wrap(self, started, attempted, new_principal, new_lender,
                new_dot_recording, url_missing, fetch_errors,
                status: str = "ok", error: Optional[str] = None) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=attempted,
            parsed_count=new_principal + new_lender + new_dot_recording,
            staged_count=new_principal + new_lender,
            duplicate_count=0,
            error_message=error,
        )
        return {
            "name": self.name, "status": status,
            "attempted": attempted,
            "new_principal": new_principal,
            "new_lender": new_lender,
            "new_dot_recording": new_dot_recording,
            "url_missing": url_missing,
            "fetch_errors": fetch_errors,
            "error": error,
            "staged": new_principal + new_lender,
            "duplicates": 0,
            "fetched": attempted,
        }


def run() -> dict:
    bot = NashvilleLedgerReextractBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
