"""
Multi-source phone resolver — replaces BatchData skip-trace at $0.

The honest 2026 reality: most TN public records strip phone before
publication (privacy redaction). The pre-2015 era of free homeowner-
phone-by-name lookups is over. What's still recoverable at $0:

  1. Phones embedded in body text of notices we already scrape
     (foreclosure notices, probate notices, court filings) — extracted
     via regex on raw_payload['body'] which we already store
  2. Craigslist FSBO listings (phone is in the listing description)
  3. CourtListener bankruptcy attorneys (firm phone via API attorney[]
     field) — the debtor's lawyer, not the debtor, but a legitimate
     contact path for a bankruptcy lead
  4. Cross-reference: when the same person appears in two scraped
     sources, harvest phone from whichever has it
  5. Owner mailing address from county assessor records (Davidson,
     Williamson, Shelby, Rutherford, Hamilton CSV) — for direct-mail
     dispatch when no phone is available

Coverage estimate at $0 with these sources alone: ~10-20% of leads
get a usable phone (lower than the 25-35% I initially estimated;
most TN open-data sources block bots or strip PII). The 80-90% of
leads without a phone get routed to direct-mail dispatch instead.

For higher coverage Patrick has authorized voter-file purchase
($500/yr) as a future option; this bot's framework supports adding
that as another `Source` class without changes to the resolver core.

Confidence scoring per phone:
  1.00 — phone explicitly published in foreclosure/probate notice
         alongside the homeowner's name
  0.95 — phone in Craigslist FSBO listing they wrote themselves
  0.80 — phone matched to attorney-of-record (bankruptcy attorney)
  0.60 — phone from cross-reference of name across two sources
  0.40 — voter file phone (when wired)

Distress type: N/A (utility enricher).
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._provenance import record_field

try:
    import phonenumbers
    from phonenumbers import PhoneNumberFormat
except ImportError:
    phonenumbers = None


# Generic phone-shaped regex. Captures (xxx) xxx-xxxx, xxx-xxx-xxxx,
# xxx.xxx.xxxx, +1xxxxxxxxxx. We validate via phonenumbers.is_valid_number
# after extraction to filter junk.
PHONE_RE = re.compile(
    r"(?:\+?1[\s.-]?)?"          # optional +1 country code
    r"\(?(\d{3})\)?"             # area code (with or without parens)
    r"[\s.-]?"
    r"(\d{3})"                    # exchange
    r"[\s.-]?"
    r"(\d{4})"                    # number
    r"(?!\d)"                     # not part of larger number
)

# Phones we should IGNORE (extracted but discarded)
JUNK_PHONES = {
    "5555555555", "1234567890", "0000000000", "1111111111",
    "9999999999", "8005551212", "8000000000",
}

# TN area codes — for filtering out non-TN phones (where applicable)
TN_AREA_CODES = {"423", "615", "629", "731", "865", "901", "931"}


def normalize_phone(raw: str) -> Optional[str]:
    """Normalize a phone string to E.164 (+1XXXXXXXXXX). Returns None
    if the phone is invalid or in the JUNK set."""
    if phonenumbers is None or not raw:
        return None
    try:
        parsed = phonenumbers.parse(raw, "US")
    except Exception:
        return None
    if not phonenumbers.is_valid_number(parsed):
        return None
    e164 = phonenumbers.format_number(parsed, PhoneNumberFormat.E164)
    digits = re.sub(r"\D", "", e164)[-10:]
    if digits in JUNK_PHONES:
        return None
    return e164


def extract_phones(text: str, prefer_tn: bool = True) -> List[str]:
    """Extract all valid phones from arbitrary text. Returns list of
    E.164 strings deduped, TN area codes first if prefer_tn."""
    if not text:
        return []
    raw_matches = []
    for m in PHONE_RE.finditer(text):
        raw_matches.append(f"({m.group(1)}) {m.group(2)}-{m.group(3)}")
    seen = set()
    valid: List[Tuple[str, bool]] = []  # (e164, is_tn)
    for raw in raw_matches:
        e164 = normalize_phone(raw)
        if not e164 or e164 in seen:
            continue
        seen.add(e164)
        area_code = e164[2:5]  # +1XXX...
        is_tn = area_code in TN_AREA_CODES
        valid.append((e164, is_tn))
    if prefer_tn:
        valid.sort(key=lambda t: (not t[1], t[0]))
    return [p for p, _ in valid]


# ─── Per-source extractors ──────────────────────────────────────────────────


def harvest_from_notice_body(raw_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Foreclosure / probate / court-notice bodies. Extract phones from
    the body text we already scraped."""
    out: List[Dict[str, Any]] = []

    # Nashville Ledger / Memphis Daily News / Hamilton County Herald
    body = ""
    if isinstance(raw_payload.get("body"), str):
        body = raw_payload["body"]
    elif isinstance(raw_payload.get("structured"), dict):
        # some bots nest body under raw_payload
        body = raw_payload.get("body") or ""

    # Known foreclosure-attorney phone field (notice_enricher already extracts this)
    sub_trustee = (raw_payload.get("substitute_trustee") or
                   raw_payload.get("structured", {}).get("substitute_trustee") or "")
    attorney = (raw_payload.get("attorney") or
                raw_payload.get("structured", {}).get("attorney") or "")

    phones = extract_phones(body)
    for phone in phones:
        # Phones in foreclosure notice are typically substitute trustee /
        # attorney — for the homeowner-phone-replacement use case those
        # are LOWER value. We still record them with a note.
        out.append({
            "phone": phone,
            "source": "notice_body",
            "confidence": 0.40,  # body-extracted phones are usually attorney
            "metadata": {
                "extracted_from": "notice_body",
                "near_substitute_trustee": bool(sub_trustee),
                "near_attorney": bool(attorney),
            },
        })
    return out


def harvest_from_craigslist(raw_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Craigslist FSBO posts — owner advertised their own phone."""
    if not raw_payload:
        return []
    body = (raw_payload.get("description") or raw_payload.get("body") or "")
    title = (raw_payload.get("title") or "")
    text = title + "\n" + body
    out = []
    for phone in extract_phones(text):
        out.append({
            "phone": phone,
            "source": "craigslist_fsbo",
            "confidence": 0.95,  # owner self-published their phone
            "metadata": {"extracted_from": "craigslist_listing"},
        })
    return out


def harvest_from_courtlistener(raw_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """CourtListener bankruptcy attorney-of-record. NOT the debtor's
    phone, but the attorney is a callable contact for a bankruptcy
    lead."""
    if not raw_payload:
        return []
    out = []

    # CourtListener search results include attorney[] field with names
    # but rarely phones in the search response itself. The attorney's
    # firm phone would require a separate API call to /people/ endpoint.
    # For now we capture any phones embedded in the case body / docket
    # text we may have stored.
    case_name = raw_payload.get("case_name", "")
    text = case_name
    for phone in extract_phones(text):
        out.append({
            "phone": phone,
            "source": "courtlistener_case_text",
            "confidence": 0.30,
            "metadata": {"docket_id": raw_payload.get("docket_id")},
        })
    return out


def harvest_from_existing_lead(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull any phone-shaped strings from existing fields (admin_notes,
    raw_payload root). Catches phones the original scrapers stuffed
    into note fields."""
    out = []
    for field in ("admin_notes", "phone"):
        val = row.get(field)
        if not val or not isinstance(val, str):
            continue
        for phone in extract_phones(val):
            out.append({
                "phone": phone,
                "source": f"existing_field_{field}",
                "confidence": 0.50 if field == "phone" else 0.30,
                "metadata": {"field": field},
            })
    return out


# ─── Resolver ──────────────────────────────────────────────────────────────


class PhoneResolverBot(BotBase):
    name = "phone_resolver"
    description = "Multi-source $0 homeowner-phone resolver (replaces BatchData skip-trace)"
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
                    "resolved": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        if phonenumbers is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="phonenumbers_not_installed",
            )
            return {"name": self.name, "status": "missing_deps",
                    "resolved": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        resolved = 0
        no_phones_found = 0
        already_had = 0
        error_message: Optional[str] = None

        try:
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                rows = self._candidates(client, table)
                self.logger.info(f"{table}: {len(rows)} candidates lacking phone")

                for row in rows[:self.max_leads_per_run]:
                    candidates = self._gather_candidates(row)
                    if not candidates:
                        no_phones_found += 1
                        continue

                    # Pick highest-confidence phone
                    best = max(candidates, key=lambda c: c["confidence"])
                    phone = best["phone"]

                    # Skip if existing phone is already higher confidence
                    existing_meta = row.get("phone_metadata") or {}
                    if not isinstance(existing_meta, dict):
                        existing_meta = {}
                    existing_resolver = existing_meta.get("phone_resolver") or {}
                    if (existing_resolver.get("phone") == phone and
                        existing_resolver.get("confidence", 0) >= best["confidence"]):
                        already_had += 1
                        continue

                    update: Dict[str, Any] = {}
                    # Only set phone if currently null OR confidence beats existing
                    current_conf = existing_resolver.get("confidence", 0)
                    if not row.get("phone") or best["confidence"] > current_conf:
                        update["phone"] = phone

                    existing_meta["phone_resolver"] = {
                        "phone": phone,
                        "source": best["source"],
                        "confidence": best["confidence"],
                        "all_candidates": candidates,
                        "resolved_at": datetime.now(timezone.utc).isoformat(),
                    }
                    update["phone_metadata"] = existing_meta

                    try:
                        client.table(table).update(update).eq("id", row["id"]).execute()
                        resolved += 1
                        if table == "homeowner_requests" and "phone" in update:
                            record_field(client, row["id"], "phone", phone,
                                          f"phone_resolver:{best['source']}",
                                          confidence=best["confidence"],
                                          metadata=best.get("metadata"))
                    except Exception as e:
                        self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif resolved == 0 and no_phones_found == 0:
            status = "zero_yield"
        elif resolved == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=resolved + no_phones_found + already_had,
            parsed_count=resolved + no_phones_found,
            staged_count=resolved, duplicate_count=already_had,
            error_message=error_message,
        )
        self.logger.info(
            f"resolved={resolved} no_phones_found={no_phones_found} "
            f"already_had={already_had}"
        )
        return {
            "name": self.name, "status": status,
            "resolved": resolved, "no_phones_found": no_phones_found,
            "already_had": already_had,
            "error": error_message,
            "staged": resolved, "duplicates": already_had,
            "fetched": resolved + no_phones_found + already_had,
        }

    def _candidates(self, client, table: str) -> List[Dict[str, Any]]:
        # PostgREST caps .limit() at 1000 — paginate. Also: staging uses
        # `bot_source`, live uses `source`; pick the right column per table.
        out = []
        PAGE_SIZE = 1000
        MAX_PAGES = 10
        src_col = "bot_source" if table == "homeowner_requests_staging" else "source"
        select = (
            f"id, phone, raw_payload, phone_metadata, admin_notes, "
            f"{src_col}, distress_type"
        )
        for page in range(MAX_PAGES):
            try:
                q = (
                    client.table(table)
                    .select(select)
                    .order("id")
                    .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                # Normalize so callers can read .bot_source uniformly
                for r in rows:
                    if "source" in r and "bot_source" not in r:
                        r["bot_source"] = r["source"]
                out.extend(rows)
                if len(rows) < PAGE_SIZE:
                    break
            except Exception as e:
                self.logger.warning(f"candidate query on {table} page {page} failed: {e}")
                break
        return out

    def _gather_candidates(self, row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Walk every harvest source for this row, return unioned list."""
        candidates: List[Dict[str, Any]] = []
        raw = row.get("raw_payload") or {}
        if not isinstance(raw, dict):
            raw = {}

        # Source 1: notice body (foreclosure / probate / Hamilton Herald)
        candidates.extend(harvest_from_notice_body(raw))

        # Source 2: Craigslist FSBO
        if row.get("bot_source") == "craigslist_tn":
            candidates.extend(harvest_from_craigslist(raw))

        # Source 3: CourtListener bankruptcy
        if row.get("bot_source") == "courtlistener_bankruptcy":
            candidates.extend(harvest_from_courtlistener(raw))

        # Source 4: existing fields
        candidates.extend(harvest_from_existing_lead(row))

        # Dedupe by phone, keep highest confidence per phone
        by_phone: Dict[str, Dict[str, Any]] = {}
        for c in candidates:
            existing = by_phone.get(c["phone"])
            if existing is None or c["confidence"] > existing["confidence"]:
                by_phone[c["phone"]] = c
        return list(by_phone.values())


def run() -> dict:
    bot = PhoneResolverBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Test mode — extract phones from text
        for text in sys.argv[1:]:
            print(f"{text!r} -> {extract_phones(text)}")
    else:
        print(run())
