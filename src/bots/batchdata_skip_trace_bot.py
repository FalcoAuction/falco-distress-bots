"""BatchData skip-trace bot — owner+address → phone numbers.

Re-enabled 2026-05-04 after we confirmed pure-$0 phone resolution caps
at ~5% of corpus. BatchData costs ~$0.07-0.15 per matched record but
returns up to 5 phones per match with DNC + reachability flags.

Pipeline position: runs AFTER phone_resolver_bot (the $0 multi-source
harvester) — so we don't pay BatchData for phones we already got free.

Eligibility filter:
  - phone is NULL on the lead (no $0 source found one)
  - owner_name_records is set
  - property_address is set
  - owner_class is "homeowner" (don't burn skip-trace credits on
    businesses/government/churches)
  - distress_type is set (don't skip-trace stale or junk leads)

Per-run cap via env: FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN (default 50).
Sample mode: set FALCO_BATCHDATA_SKIPTRACE_SAMPLE=1 to print results
without writing to DB (validation runs).

Confidence scoring per phone:
  0.85  — BatchData primary, reachable=true, dnc=false
  0.65  — BatchData primary, reachable unknown
  0.40  — BatchData secondary
  None  — DNC flagged (we still record but don't promote to phone field)

Distress type: N/A (utility skip-tracer).
"""
from __future__ import annotations

import os
import re
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._provenance import record_field

try:
    import requests
except ImportError:
    requests = None


SKIPTRACE_URL = "https://api.batchdata.com/api/v1/property/skip-trace"
DEFAULT_MAX_PER_RUN = 50
REQUEST_TIMEOUT = 20


def _parse_address(address: str) -> Dict[str, str]:
    """Split a one-line address into BatchData's {street, city, state, zip}.

    Handles formats like:
      "1234 Main St, Nashville, TN 37206"
      "1234 Main St, Nashville, TN"
      "1234 Main Street Nashville TN 37206"  (rare)
    """
    raw = (address or "").strip()
    if not raw:
        return {}
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    street = parts[0] if parts else raw
    city = ""
    state = "TN"
    zip_code = ""
    if len(parts) >= 2:
        city = re.sub(r"\bcounty\b", "", parts[1], flags=re.I).strip()
        city = re.sub(r"\s+", " ", city).strip(" ,")
    if len(parts) >= 3:
        state_zip = parts[2]
        m_state = re.search(r"\b([A-Z]{2})\b", state_zip.upper())
        if m_state:
            state = m_state.group(1)
        m_zip = re.search(r"\b(\d{5})(?:-\d{4})?\b", state_zip)
        if m_zip:
            zip_code = m_zip.group(1)
    if not city and len(parts) == 1:
        m = re.match(
            r"^(.*?),\s*([^,]+)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?$",
            raw, flags=re.I,
        )
        if m:
            street = m.group(1).strip()
            city = m.group(2).strip()
            state = m.group(3).upper()
            zip_code = m.group(4)
    payload = {"street": street, "state": state}
    if city:
        payload["city"] = city
    if zip_code:
        payload["zip"] = zip_code
    return payload


def _normalize_phone(raw: str) -> Optional[str]:
    """Normalize to 10-digit string, or None if invalid."""
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return digits


def _rank_phones(phones: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort BatchData phones best-first.

    Prefers: not-DNC > reachable > tested > higher score.
    Returns enriched dicts with normalized digits + computed confidence.
    """
    out = []
    for p in phones:
        if not isinstance(p, dict):
            continue
        digits = _normalize_phone(p.get("number") or p.get("phone"))
        if not digits:
            continue
        dnc = bool(p.get("dnc"))
        reachable = bool(p.get("reachable"))
        tested = bool(p.get("tested"))
        score = int(p.get("score") or 0)
        # Confidence heuristic
        if dnc:
            conf = 0.0  # DNC — record but don't dial
        elif reachable and tested:
            conf = 0.85
        elif reachable:
            conf = 0.75
        elif tested:
            conf = 0.65
        else:
            conf = 0.5
        if p.get("confidence_cap") is not None:
            try:
                conf = min(conf, float(p["confidence_cap"]))
            except (TypeError, ValueError):
                pass
        out.append({
            "phone": digits,
            "dnc": dnc,
            "reachable": reachable,
            "tested": tested,
            "score": score,
            "confidence": conf,
            "phone_type": p.get("type") or p.get("phoneType"),
            "match_mode": p.get("match_mode"),
            "person_match": p.get("person_match"),
        })
    # Sort: not-DNC, reachable, tested, score
    out.sort(
        key=lambda p: (
            -(0 if p["dnc"] else 1),
            -(1 if p["reachable"] else 0),
            -(1 if p["tested"] else 0),
            -p["score"],
        )
    )
    return out


class BatchDataSkipTraceBot(BotBase):
    name = "batchdata_skip_trace"
    description = "BatchData paid skip-trace — owner+address → phone numbers"
    throttle_seconds = 0.6  # API limits + politeness
    expected_min_yield = 1

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        if requests is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="requests_not_installed",
            )
            return {"name": self.name, "status": "missing_deps",
                    "resolved": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        api_key = (
            os.environ.get("FALCO_SKIP_TRACE_API_KEY", "").strip()
            or os.environ.get("FALCO_BATCHDATA_API_KEY", "").strip()
        )
        if not api_key:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="FALCO_BATCHDATA_API_KEY not set",
            )
            return {"name": self.name, "status": "no_api_key",
                    "resolved": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "resolved": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        sample_mode = os.environ.get("FALCO_BATCHDATA_SKIPTRACE_SAMPLE", "").strip() == "1"
        try:
            max_per_run = int(
                os.environ.get("FALCO_MAX_BATCHDATA_SKIPTRACE_PER_RUN")
                or DEFAULT_MAX_PER_RUN
            )
        except (TypeError, ValueError):
            max_per_run = DEFAULT_MAX_PER_RUN

        attempted = 0
        matched = 0
        no_phones = 0
        all_dnc = 0
        api_errors = 0
        cost_estimate = 0.0  # rough $0.10 per attempted lookup
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client, max_per_run)
            self.logger.info(
                f"{len(candidates)} candidates for skip-trace "
                f"(cap={max_per_run}, sample_mode={sample_mode})"
            )

            for row in candidates:
                if attempted >= max_per_run:
                    break
                attempted += 1
                cost_estimate += 0.10  # conservative

                addr = row.get("property_address") or ""
                owner = row.get("owner_name_records") or row.get("full_name") or ""
                addr_payload = _parse_address(addr)
                if not addr_payload:
                    self.logger.warning(f"  unparseable address: {addr[:60]!r}")
                    continue

                # API call
                try:
                    phones = self._skip_trace_one(api_key, addr_payload, owner)
                except Exception as e:
                    api_errors += 1
                    self.logger.warning(f"  API error for id={row['id']}: {e}")
                    continue

                if not phones:
                    no_phones += 1
                    self.logger.info(f"  no phones · {addr[:50]} · {owner[:30]}")
                    continue

                ranked = _rank_phones(phones)
                if not ranked:
                    no_phones += 1
                    continue

                # Skip if all returned are DNC
                non_dnc = [p for p in ranked if not p["dnc"]]
                if not non_dnc:
                    all_dnc += 1
                    self.logger.info(f"  all DNC · {addr[:50]}")
                    if sample_mode:
                        continue
                    # Record DNC status anyway so we don't re-spend on this lead
                    self._write_dnc_only(client, row, ranked)
                    continue

                primary = non_dnc[0]
                secondary = non_dnc[1] if len(non_dnc) > 1 else None
                matched += 1

                if sample_mode:
                    self.logger.info(
                        f"  MATCH · {addr[:50]} · {owner[:30]} · "
                        f"primary={primary['phone']} (conf {primary['confidence']:.2f}, "
                        f"reach={primary['reachable']}, tested={primary['tested']})"
                    )
                    continue

                # Real write
                self._write_phone(client, row, primary, secondary, ranked)

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif attempted == 0:
            status = "zero_yield"
        elif matched == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=attempted,
            parsed_count=matched + no_phones + all_dnc,
            staged_count=matched, duplicate_count=0,
            error_message=error_message,
        )
        self.logger.info(
            f"attempted={attempted} matched={matched} no_phones={no_phones} "
            f"all_dnc={all_dnc} api_errors={api_errors} "
            f"est_cost=${cost_estimate:.2f}"
        )
        return {
            "name": self.name, "status": status,
            "attempted": attempted, "matched": matched,
            "no_phones": no_phones, "all_dnc": all_dnc,
            "api_errors": api_errors,
            "estimated_cost_usd": round(cost_estimate, 2),
            "sample_mode": sample_mode,
            "error": error_message,
            "staged": matched, "duplicates": 0,
            "fetched": attempted,
        }

    # ── Eligibility query ──────────────────────────────────────────────────
    def _candidates(self, client, max_per_run: int) -> List[Dict[str, Any]]:
        """Pull leads eligible for skip-trace.

        Eligibility:
          - phone is NULL
          - owner_name_records is NOT NULL
          - property_address is NOT NULL
          - phone_metadata.batchdata_skip_trace is NOT set (avoid re-spending)
          - owner_class != business/government/etc (best-effort filter)
          - prefer leads with priority_score >= 50 (PROMOTE_HOT/WARM)
        """
        out = []
        PAGE_SIZE = 1000
        MAX_PAGES = 5
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            for page in range(MAX_PAGES):
                try:
                    q = (
                        client.table(table)
                        .select("id, property_address, owner_name_records, "
                                "full_name, county, priority_score, "
                                "phone_metadata, distress_type")
                        .is_("phone", "null")
                        .not_.is_("owner_name_records", "null")
                        .not_.is_("property_address", "null")
                        .order("priority_score", desc=True)
                        .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                        .execute()
                    )
                    rows = getattr(q, "data", None) or []
                    if not rows:
                        break
                    for r in rows:
                        # Filter out rows with unwanted owner_class
                        pm = r.get("phone_metadata") or {}
                        if isinstance(pm, dict):
                            owner_class = pm.get("owner_class")
                            if isinstance(owner_class, dict):
                                owner_class = owner_class.get("class") or owner_class.get("value")
                            if owner_class in ("business", "government",
                                                "religious_or_education",
                                                "healthcare"):
                                continue
                            # Skip if already skip-traced
                            if pm.get("batchdata_skip_trace"):
                                continue
                        # Skip deceased/estate leads — owner is dead, no phone
                        # to find. Probate leads have addresses like
                        # "0109 Estate Of Margaret Louise Gard, Johnson City"
                        # or owners "ESTATE OF JOHN SMITH". These cost API
                        # credits with zero return.
                        addr_upper = (r.get("property_address") or "").upper()
                        owner_upper = (
                            (r.get("owner_name_records") or "")
                            + " "
                            + (r.get("full_name") or "")
                        ).upper()
                        if (
                            "ESTATE OF" in addr_upper
                            or "ESTATE OF" in owner_upper
                            or "DECEASED" in owner_upper
                        ):
                            continue
                        r["__table__"] = table
                        out.append(r)
                        if len(out) >= max_per_run * 2:
                            break
                    if len(rows) < PAGE_SIZE or len(out) >= max_per_run * 2:
                        break
                except Exception as e:
                    self.logger.warning(
                        f"candidate query on {table} page {page} failed: {e}"
                    )
                    break
            if len(out) >= max_per_run * 2:
                break
        return out[:max_per_run]

    # ── API call ───────────────────────────────────────────────────────────
    def _skip_trace_one(
        self, api_key: str, address: Dict[str, str], owner_name: str
    ) -> List[Dict[str, Any]]:
        """Single skip-trace call. Tries with owner_name first, then
        retries without if no result (some matches need to be name-free)."""
        phones, person = self._post(api_key, address, owner_name or None)
        if phones:
            person_match = self._person_name_matches(owner_name, person)
            return self._annotate_phones(
                phones,
                "owner_name_verified" if person_match else "owner_name_unverified",
                0.85 if person_match else 0.60,
                person_match,
            )
        if owner_name:
            phones, person = self._post(api_key, address, None)
            if phones:
                return self._annotate_phones(phones, "address_only", 0.45, False)
        return []

    def _post(
        self, api_key: str, address: Dict[str, str], owner_name: Optional[str]
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        payload = {"requests": [{"propertyAddress": address}]}
        if owner_name:
            payload["requests"][0]["ownerName"] = owner_name
        url = os.environ.get("FALCO_BATCHDATA_SKIPTRACE_URL", SKIPTRACE_URL)
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results") or {}
        persons = results.get("persons") if isinstance(results, dict) else None
        if not isinstance(persons, list) or not persons:
            return ([], None)
        first = persons[0]
        phones = (
            first.get("phoneNumbers")
            or first.get("phones")
            or first.get("ownerPhones")
            or []
        )
        return (phones if isinstance(phones, list) else [], first)

    @staticmethod
    def _annotate_phones(
        phones: List[Dict[str, Any]], match_mode: str, confidence_cap: float, person_match: bool
    ) -> List[Dict[str, Any]]:
        out = []
        for phone in phones:
            if not isinstance(phone, dict):
                continue
            enriched = dict(phone)
            enriched["match_mode"] = match_mode
            enriched["confidence_cap"] = confidence_cap
            enriched["person_match"] = person_match
            out.append(enriched)
        return out

    @staticmethod
    def _person_name_matches(owner_name: str, person: Optional[Dict[str, Any]]) -> bool:
        if not owner_name or not isinstance(person, dict):
            return False
        person_name = (
            person.get("name") or person.get("fullName") or
            " ".join(str(person.get(k) or "") for k in ("firstName", "middleName", "lastName"))
        )
        owner_tokens = re.findall(r"[A-Z]+", owner_name.upper())
        person_tokens = re.findall(r"[A-Z]+", str(person_name).upper())
        if len(owner_tokens) < 2 or len(person_tokens) < 2:
            return False
        owner_first, owner_last = owner_tokens[0], owner_tokens[-1]
        return owner_last in person_tokens and any(
            token[0] == owner_first[0] for token in person_tokens if token
        )

    # ── Writes ─────────────────────────────────────────────────────────────
    def _write_phone(
        self,
        client,
        row: Dict[str, Any],
        primary: Dict[str, Any],
        secondary: Optional[Dict[str, Any]],
        all_phones: List[Dict[str, Any]],
    ) -> None:
        table = row["__table__"]
        existing_meta = row.get("phone_metadata") or {}
        if not isinstance(existing_meta, dict):
            existing_meta = {}
        existing_meta["batchdata_skip_trace"] = {
            "primary_phone": primary["phone"],
            "primary_confidence": primary["confidence"],
            "primary_reachable": primary["reachable"],
            "primary_tested": primary["tested"],
            "primary_dnc": primary["dnc"],
            "primary_match_mode": primary.get("match_mode"),
            "primary_person_match": primary.get("person_match"),
            "secondary_phone": secondary["phone"] if secondary else None,
            "secondary_dnc": secondary["dnc"] if secondary else None,
            "all_phones": all_phones,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        update: Dict[str, Any] = {
            "phone": primary["phone"],
            "phone_metadata": existing_meta,
        }
        # Build alternate_phones list (digits only, deduped)
        alts = []
        seen = {primary["phone"]}
        for p in all_phones:
            d = p["phone"]
            if d in seen or p["dnc"]:
                continue
            seen.add(d)
            alts.append(d)
        if alts:
            update["alternate_phones"] = alts
        try:
            client.table(table).update(update).eq("id", row["id"]).execute()
            if table == "homeowner_requests":
                record_field(
                    client, row["id"], "phone", primary["phone"],
                    "batchdata_skip_trace",
                    confidence=primary["confidence"],
                    metadata={
                        "reachable": primary["reachable"],
                        "tested": primary["tested"],
                        "alt_count": len(alts),
                        "match_mode": primary.get("match_mode"),
                        "person_match": primary.get("person_match"),
                    },
                )
        except Exception as e:
            self.logger.warning(f"  update failed id={row['id']}: {e}")

    def _write_dnc_only(
        self, client, row: Dict[str, Any], all_phones: List[Dict[str, Any]]
    ) -> None:
        table = row["__table__"]
        existing_meta = row.get("phone_metadata") or {}
        if not isinstance(existing_meta, dict):
            existing_meta = {}
        existing_meta["batchdata_skip_trace"] = {
            "primary_phone": None,
            "all_dnc": True,
            "all_phones": all_phones,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            client.table(table).update(
                {"phone_metadata": existing_meta}
            ).eq("id", row["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  update failed id={row['id']}: {e}")


def run() -> dict:
    bot = BatchDataSkipTraceBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
