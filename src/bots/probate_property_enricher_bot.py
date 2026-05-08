"""
Probate → property cross-reference enricher.

`tn_probate_bot` produces PROBATE leads with decedent name + county
but NO property address — probate Notice-to-Creditors notices don't
contain the decedent's real-estate holdings. The address has to be
cross-referenced from the county assessor.

This enricher walks PROBATE leads in homeowner_requests_staging that
lack property_address, then for Davidson decedents queries PADCTN
(portal.padctn.org) by owner name. If exactly one match comes back,
the lead gets the property address + appraised value + parcel ID.

For probate leads from non-Davidson counties (Cheatham, Robertson,
Williamson, Sumner, etc), the equivalent owner-name lookup against
TPAD is left as future work — needs a per-county mapping from
probate-court county to TPAD jurisdiction code, which the existing
tpad_enricher already has but is keyed by lead.county which probate
leads do populate, so the wire-up is straightforward when we want
it. v1 keeps Davidson-only to validate the pattern.

Strict matching: we only commit an enrichment when PADCTN returns
EXACTLY ONE result. Multiple matches mean the decedent name is too
common (e.g. "Smith, John") and we'd risk pinning the wrong house.
The lead stays unenriched in that case — better to lose a lead than
poison the dataset with a wrong address.

Distress type: N/A (enricher only)
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from ._base import BotBase, _supabase
from .davidson_assessor_bot import (
    PADCTN_HOME, PADCTN_QUICKSEARCH, PADCTN_SEARCH,
    ACCOUNT_RE, APPRAISED_RE, LAND_SIZE_RE, LAND_USE_RE, PARCEL_RE,
)


def _decedent_to_owner_query(decedent: str) -> Optional[str]:
    """Convert 'Pamela Hobbs Wood' → 'Wood, Pamela'.

    PADCTN expects 'LASTNAME, FIRSTNAME'. Drop middle names — the
    assessor records often store them inconsistently and matching
    on (last, first) gives the best recall.
    """
    if not decedent:
        return None
    parts = [p for p in decedent.strip().split() if p]
    if len(parts) < 2:
        return None
    # Filter out common name suffixes
    suffixes = {"jr", "sr", "ii", "iii", "iv", "phd", "md", "esq"}
    while parts and parts[-1].lower().rstrip(".,") in suffixes:
        parts.pop()
    if len(parts) < 2:
        return None
    last = parts[-1]
    first = parts[0]
    return f"{last}, {first}"


class ProbatePropertyEnricherBot(BotBase):
    name = "probate_property_enricher"
    description = "Cross-reference probate decedents to Davidson County properties via PADCTN owner search"
    throttle_seconds = 1.5
    expected_min_yield = 1

    max_leads_per_run = 100

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
                    "enriched": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        # Establish PADCTN session
        self.fetch(PADCTN_HOME)
        self.fetch(PADCTN_QUICKSEARCH)

        enriched = 0
        ambiguous = 0    # multi-match — skipped to avoid wrong-house pinning
        not_found = 0
        skipped = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(
                f"{len(candidates)} probate leads in Davidson lacking property_address"
            )

            for row in candidates[:self.max_leads_per_run]:
                decedent = row.get("full_name") or row.get("owner_name_records") or ""
                query = _decedent_to_owner_query(decedent)
                if not query:
                    skipped += 1
                    continue
                results = self._lookup_owner(query)
                if results is None:
                    not_found += 1
                    continue
                if len(results) == 0:
                    not_found += 1
                    continue
                if len(results) > 1:
                    ambiguous += 1
                    continue

                hit = results[0]
                update: Dict[str, Any] = {}
                if hit.get("property_address") and not row.get("property_address"):
                    update["property_address"] = hit["property_address"]
                # Authoritative — override any prior HMDA-anchored phantom.
                if hit.get("appraised"):
                    update["property_value"] = int(round(float(hit["appraised"])))
                    update["property_value_source"] = "probate_assessor"

                # Merge into raw_payload for audit
                existing_raw = row.get("raw_payload") or {}
                if not isinstance(existing_raw, dict):
                    existing_raw = {}
                existing_raw["padctn_owner_match"] = {
                    "query": query,
                    "account_id": hit.get("account_id"),
                    "parcel": hit.get("parcel"),
                    "appraised": hit.get("appraised"),
                    "land_use": hit.get("land_use"),
                    "owner": hit.get("owner"),
                    "property_address": hit.get("property_address"),
                }
                update["raw_payload"] = existing_raw

                if not update:
                    skipped += 1
                    continue

                try:
                    client.table("homeowner_requests_staging").update(update).eq("id", row["id"]).execute()
                    enriched += 1
                except Exception as e:
                    self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif enriched == 0 and not_found == 0 and ambiguous == 0:
            status = "zero_yield"
        elif enriched == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=enriched + not_found + ambiguous + skipped,
            parsed_count=enriched + not_found + ambiguous,
            staged_count=enriched, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(
            f"enriched={enriched} ambiguous={ambiguous} "
            f"not_found={not_found} skipped={skipped}"
        )
        return {
            "name": self.name, "status": status,
            "enriched": enriched, "ambiguous": ambiguous,
            "not_found": not_found, "skipped": skipped,
            "error": error_message,
            "staged": enriched, "duplicates": skipped,
            "fetched": enriched + not_found + ambiguous + skipped,
        }

    # ── Internal ────────────────────────────────────────────────────────────

    def _candidates(self, client) -> List[Dict[str, Any]]:
        try:
            q = (
                client.table("homeowner_requests_staging")
                .select("id, full_name, owner_name_records, county, property_address, property_value, raw_payload")
                .eq("distress_type", "PROBATE")
                .eq("county", "davidson")
                .is_("property_address", "null")
                .limit(500)
                .execute()
            )
            return getattr(q, "data", None) or []
        except Exception as e:
            self.logger.warning(f"candidate query failed: {e}")
            return []

    def _lookup_owner(self, query: str) -> Optional[List[Dict[str, Any]]]:
        res = self.fetch(
            PADCTN_SEARCH,
            method="POST",
            data={
                "RealEstate": "true",
                "SelectedSearch": "1",   # 1 = Owner
                "StreetNumber": "",
                "SingleSearchCriteria": query,
                "AlterCriteria": "False",
            },
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        if res is None or res.status_code != 200:
            return None
        return self._parse_results(res.text)

    @staticmethod
    def _parse_results(html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("td.dxdvItem")
        out: List[Dict[str, Any]] = []
        for item in items:
            text = item.get_text("\n", strip=True)
            rec: Dict[str, Any] = {}
            m = ACCOUNT_RE.search(str(item))
            if m:
                rec["account_id"] = int(m.group(1))
            m = APPRAISED_RE.search(text)
            if m:
                rec["appraised"] = float(m.group(1).replace(",", ""))
            m = LAND_USE_RE.search(text)
            if m:
                rec["land_use"] = m.group(1).strip()
            m = PARCEL_RE.search(text)
            if m:
                rec["parcel"] = m.group(1).strip()

            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
            for i, ln in enumerate(lines):
                if PARCEL_RE.match(ln):
                    if i + 1 < len(lines):
                        rec["owner"] = lines[i + 1]
                    addr_lines = []
                    for j in range(i + 2, min(i + 5, len(lines))):
                        if APPRAISED_RE.match(lines[j]) or LAND_SIZE_RE.match(lines[j]):
                            break
                        addr_lines.append(lines[j])
                    if addr_lines:
                        # First line is street; rest is city/zip
                        rec["property_address"] = (
                            addr_lines[0] +
                            (", " + ", ".join(addr_lines[1:]) if len(addr_lines) > 1 else "")
                        )
                    break
            if rec.get("account_id"):
                out.append(rec)
        return out


def run() -> dict:
    bot = ProbatePropertyEnricherBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        bot = ProbatePropertyEnricherBot()
        bot.fetch(PADCTN_HOME)
        bot.fetch(PADCTN_QUICKSEARCH)
        for name in sys.argv[1:]:
            q = _decedent_to_owner_query(name)
            print(f"{name} -> query={q}")
            if q:
                results = bot._lookup_owner(q)
                print(f"  -> {len(results) if results else 0} results")
                if results:
                    for r in results[:3]:
                        print(f"    {r}")
    else:
        print(run())
