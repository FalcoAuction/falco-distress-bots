"""EnformionGO skip-trace — waterfall layer behind BatchData.

Targets the leads BatchData couldn't finish:
  1. no_phones misses (BatchData returned nothing)
  2. address-match-only primaries (owner name unverified — Enformion's
     top-match either cross-verifies the number or replaces it)

Never re-spends on leads BatchData already name-verified.

Billing note: EnformionGO charges on successful match only, so misses
are free — safe to point at the hard tail.

Cross-verification: when Enformion's top phone equals the existing
BatchData primary, we don't overwrite — we mark
phone_metadata.batchdata_skip_trace.cross_verified = true. Two
independent data sources agreeing on a number is the strongest
signal we can get without dialing.

Env:
  FALCO_ENFORMION_AP_NAME       (access profile name)
  FALCO_ENFORMION_AP_PASSWORD   (access profile password)
  FALCO_MAX_ENFORMION_PER_RUN   (default 100)
  FALCO_ENFORMION_SAMPLE        (=1 dry-run, no writes)

Run:
  python -m src.bots.enformion_skip_trace_bot

Distress type: N/A (utility skip-tracer).
"""
from __future__ import annotations

import os
import re
import traceback as tb
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase

try:
    import requests
except ImportError:
    requests = None


ENFORMION_URL = "https://devapi.enformion.com/Contact/Enrich"
DEFAULT_MAX_PER_RUN = 100
REQUEST_TIMEOUT = 20

FOCUS_COUNTIES = {
    "davidson", "williamson", "sumner", "rutherford", "wilson",
    "maury", "montgomery", "cheatham", "robertson", "dickson",
}
FORECLOSURE_DISTRESS = {
    "PRE_FORECLOSURE", "PREFORECLOSURE", "TRUSTEE_NOTICE",
    "LIS_PENDENS", "SOT", "SUBSTITUTION_OF_TRUSTEE",
    "NOD", "NOTICE_OF_DEFAULT", "FORECLOSURE",
}

BUSINESS_RE = re.compile(
    r"\b(LLC|L\.L\.C|INC|CORP|TRUST|HOLDINGS|PROPERTIES|COMPANY|GROUP|"
    r"PARTNERS|REALTY|INVESTMENT|LP|LLP|FOUNDATION|CHURCH|ESTATES|"
    r"VENTURES|CONSTRUCTION)\b",
    re.I,
)


def _normalize_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


def _normalize_phone(raw: Any) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None


def _split_name(owner: str) -> Dict[str, str]:
    """Best-effort first/last from an owner-records string.

    Handles "First Last", "Last, First", and multi-owner strings
    ("A; B" — takes the first person).
    """
    primary = re.split(r"[;&]| and ", owner or "", flags=re.I)[0].strip()
    if "," in primary:
        last, _, first = primary.partition(",")
        return {"FirstName": first.strip().split(" ")[0], "LastName": last.strip()}
    tokens = [t for t in primary.split() if t]
    if len(tokens) >= 2:
        return {"FirstName": tokens[0], "LastName": tokens[-1]}
    return {}


def _split_address(address: str) -> Dict[str, str]:
    raw = (address or "").strip()
    if not raw:
        return {}
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    line1 = parts[0] if parts else raw
    line2 = ", ".join(parts[1:]) if len(parts) > 1 else ""
    return {"addressLine1": line1, "addressLine2": line2}


class EnformionSkipTraceBot(BotBase):
    name = "enformion_skip_trace"
    description = "EnformionGO contact-enrich waterfall behind BatchData"
    throttle_seconds = 0.4
    expected_min_yield = 0  # waterfall may legitimately have nothing to do

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        ap_name = os.environ.get("FALCO_ENFORMION_AP_NAME", "").strip()
        ap_password = os.environ.get("FALCO_ENFORMION_AP_PASSWORD", "").strip()
        if requests is None or not ap_name or not ap_password:
            msg = "requests_not_installed" if requests is None else "FALCO_ENFORMION_AP_NAME/_PASSWORD not set"
            self._report_health(
                status="failed" if requests is None else "zero_yield",
                started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message=msg,
            )
            return {"name": self.name, "status": "no_creds", "error": msg,
                    "staged": 0, "duplicates": 0, "fetched": 0}

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "staged": 0, "duplicates": 0, "fetched": 0}

        sample_mode = os.environ.get("FALCO_ENFORMION_SAMPLE", "").strip() == "1"
        try:
            max_per_run = int(os.environ.get("FALCO_MAX_ENFORMION_PER_RUN") or DEFAULT_MAX_PER_RUN)
        except (TypeError, ValueError):
            max_per_run = DEFAULT_MAX_PER_RUN

        attempted = 0
        new_phones = 0
        cross_verified = 0
        replaced = 0
        no_match = 0
        api_errors = 0
        consecutive_errors = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client, max_per_run)
            self.logger.info(
                f"{len(candidates)} waterfall candidates "
                f"(cap={max_per_run}, sample={sample_mode})"
            )
            for row in candidates:
                if attempted >= max_per_run:
                    break
                attempted += 1

                owner = row.get("owner_name_records") or row.get("full_name") or ""
                name_parts = _split_name(owner)
                addr_parts = _split_address(row.get("property_address") or "")
                if not addr_parts or not name_parts:
                    continue

                try:
                    person = self._enrich(ap_name, ap_password, name_parts, addr_parts)
                    consecutive_errors = 0
                except Exception as e:
                    api_errors += 1
                    consecutive_errors += 1
                    self.logger.warning(f"  API error id={row['id']}: {e}")
                    if consecutive_errors >= 3 and (new_phones + cross_verified) == 0:
                        error_message = (
                            f"aborted: {consecutive_errors} consecutive API errors, "
                            f"0 successes — account-level failure. Last: {str(e)[:200]}"
                        )
                        self.logger.error(error_message)
                        break
                    continue

                phones = self._extract_phones(person)
                if not phones:
                    no_match += 1
                    continue

                existing_digits = _normalize_phone(row.get("phone"))
                top = phones[0]

                if sample_mode:
                    self.logger.info(
                        f"  SAMPLE · {owner[:30]} · existing={existing_digits} "
                        f"· enformion_top={top} · all={phones[:5]}"
                    )
                    new_phones += 1
                    continue

                if existing_digits and existing_digits in phones:
                    self._mark_cross_verified(client, row, existing_digits, phones)
                    cross_verified += 1
                elif existing_digits:
                    # Disagreement: keep BatchData primary, add Enformion's
                    # numbers as alternates + record for the dialer to judge.
                    self._add_alternates(client, row, phones)
                    replaced += 1
                else:
                    self._write_primary(client, row, phones)
                    new_phones += 1

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message is None and api_errors > 0 and api_errors == attempted:
            error_message = f"all {attempted} attempts API-errored"
        if error_message:
            status = "failed"
        elif attempted == 0:
            status = "zero_yield"
        else:
            status = "ok"

        produced = new_phones + cross_verified + replaced
        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=attempted, parsed_count=produced + no_match,
            staged_count=produced, duplicate_count=0,
            error_message=error_message,
        )
        self.logger.info(
            f"attempted={attempted} new_phones={new_phones} "
            f"cross_verified={cross_verified} disagreements={replaced} "
            f"no_match={no_match} api_errors={api_errors}"
        )
        return {
            "name": self.name, "status": status,
            "attempted": attempted, "new_phones": new_phones,
            "cross_verified": cross_verified, "disagreements": replaced,
            "no_match": no_match, "api_errors": api_errors,
            "error": error_message,
            "staged": produced, "duplicates": 0, "fetched": attempted,
        }

    # ── Candidates: BatchData's misses + unverified primaries ────────────
    def _candidates(self, client, max_per_run: int) -> List[Dict[str, Any]]:
        # Default ON: if the primary skip-trace is down, this bot must not
        # sit idle behind a gate that the primary is responsible for opening.
        allow_primary = os.environ.get("FALCO_ENFORMION_PRIMARY", "1").strip() != "0"
        today_iso = date.today().isoformat()
        out: List[Dict[str, Any]] = []
        PAGE = 1000
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            page = 0
            while True:
                try:
                    q = (
                        client.table(table)
                        .select(
                            "id, property_address, owner_name_records, full_name, "
                            "county, distress_type, trustee_sale_date, phone, "
                            "alternate_phones, phone_metadata"
                        )
                        .in_("distress_type", list(FORECLOSURE_DISTRESS))
                        .not_.is_("owner_name_records", "null")
                        .not_.is_("property_address", "null")
                        .gte("trustee_sale_date", today_iso)
                        .order("trustee_sale_date")
                        .range(page * PAGE, (page + 1) * PAGE - 1)
                        .execute()
                    )
                    rows = getattr(q, "data", None) or []
                    if not rows:
                        break
                    for r in rows:
                        if _normalize_county(r.get("county")) not in FOCUS_COUNTIES:
                            continue
                        owner = (r.get("owner_name_records") or "") + " " + (r.get("full_name") or "")
                        if BUSINESS_RE.search(owner) or "ESTATE OF" in owner.upper():
                            continue
                        pm = r.get("phone_metadata") or {}
                        if not isinstance(pm, dict):
                            pm = {}
                        if pm.get("enformion_skip_trace"):
                            continue  # already waterfalled
                        st = pm.get("batchdata_skip_trace") or {}
                        has_phone = bool(_normalize_phone(r.get("phone")))
                        batchdata_tried = bool(st)
                        name_verified = st.get("primary_match_mode") == "owner_name_verified"
                        cross = st.get("cross_verified")
                        # Waterfall targets:
                        #   a) BatchData tried, found nothing
                        #   b) has a phone but match is unverified + not yet cross-checked
                        #   c) BatchData never ran on this row at all. Without
                        #      this, a dead BatchData account (403/no credit)
                        #      deadlocks the whole pipeline: BatchData never
                        #      marks rows as tried, so this waterfall sees no
                        #      candidates and enrichment stops entirely. When
                        #      the primary is down, Enformion steps up instead
                        #      of idling. Set FALCO_ENFORMION_PRIMARY=0 to
                        #      restrict this bot to strict waterfall mode.
                        if (batchdata_tried and not has_phone) or (
                            has_phone and batchdata_tried and not name_verified and not cross
                        ) or (
                            allow_primary and not batchdata_tried and not has_phone
                        ):
                            r["__table__"] = table
                            out.append(r)
                    if len(rows) < PAGE or len(out) >= max_per_run * 2:
                        break
                    page += 1
                except Exception as e:
                    self.logger.warning(f"candidate query {table} p{page}: {e}")
                    break
            if len(out) >= max_per_run * 2:
                break
        return out[:max_per_run]

    # ── API ───────────────────────────────────────────────────────────────
    def _enrich(
        self, ap_name: str, ap_password: str,
        name_parts: Dict[str, str], addr_parts: Dict[str, str],
    ) -> Dict[str, Any]:
        url = os.environ.get("FALCO_ENFORMION_URL", ENFORMION_URL)
        resp = requests.post(
            url,
            headers={
                "galaxy-ap-name": ap_name,
                "galaxy-ap-password": ap_password,
                "galaxy-search-type": "DevAPIContactEnrich",
                "Content-Type": "application/json",
            },
            json={
                "FirstName": name_parts.get("FirstName", ""),
                "LastName": name_parts.get("LastName", ""),
                "Address": addr_parts,
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        # Response shape: person object with phones list. Tolerate both
        # {"person": {...}} and top-level shapes across API versions.
        return data.get("person") or data

    @staticmethod
    def _extract_phones(person: Dict[str, Any]) -> List[str]:
        raw = person.get("phones") or person.get("phoneNumbers") or []
        out: List[str] = []
        seen = set()
        for p in raw:
            num = _normalize_phone(
                p.get("number") if isinstance(p, dict) else p
            )
            if num and num not in seen:
                seen.add(num)
                out.append(num)
        return out

    # ── Writes ────────────────────────────────────────────────────────────
    @staticmethod
    def _meta(row: Dict[str, Any]) -> Dict[str, Any]:
        pm = row.get("phone_metadata") or {}
        return pm if isinstance(pm, dict) else {}

    def _mark_cross_verified(self, client, row, digits: str, phones: List[str]) -> None:
        pm = self._meta(row)
        st = pm.get("batchdata_skip_trace") or {}
        st["cross_verified"] = True
        st["cross_verified_by"] = "enformion"
        pm["batchdata_skip_trace"] = st
        pm["enformion_skip_trace"] = {
            "agrees_with_primary": True,
            "phones": phones[:5],
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            client.table(row["__table__"]).update({"phone_metadata": pm}).eq("id", row["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  cross-verify write failed id={row['id']}: {e}")

    def _add_alternates(self, client, row, phones: List[str]) -> None:
        pm = self._meta(row)
        pm["enformion_skip_trace"] = {
            "agrees_with_primary": False,
            "phones": phones[:5],
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        existing = row.get("alternate_phones")
        alts = [a for a in existing if isinstance(a, str)] if isinstance(existing, list) else []
        for p in phones:
            if p not in alts and p != _normalize_phone(row.get("phone")):
                alts.append(p)
        try:
            client.table(row["__table__"]).update(
                {"phone_metadata": pm, "alternate_phones": alts[:6]}
            ).eq("id", row["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  alternates write failed id={row['id']}: {e}")

    def _write_primary(self, client, row, phones: List[str]) -> None:
        pm = self._meta(row)
        pm["enformion_skip_trace"] = {
            "primary_phone": phones[0],
            "phones": phones[:5],
            "source": "enformion_contact_enrich",
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        update: Dict[str, Any] = {
            "phone": phones[0],
            "phone_metadata": pm,
        }
        if len(phones) > 1:
            update["alternate_phones"] = phones[1:5]
        try:
            client.table(row["__table__"]).update(update).eq("id", row["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  primary write failed id={row['id']}: {e}")


def run() -> dict:
    bot = EnformionSkipTraceBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
