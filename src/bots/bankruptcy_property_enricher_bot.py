"""
Bankruptcy → property cross-reference enricher.

`courtlistener_bankruptcy_bot` produces BANKRUPTCY leads with debtor
name + court_id + chapter + filed date but NO property address —
CourtListener's search API doesn't expose schedule attachments
(those require deeper PACER pulls). Address has to be cross-
referenced from the county assessor.

This enricher walks BANKRUPTCY leads in homeowner_requests_staging
that lack property_address and looks up debtor name via:
  - Davidson PADCTN owner search (for tnmb cases probably in Davidson)
  - Williamson Inigo owner search (for tnmb cases probably in Williamson)

Strict matching policy: only commit an enrichment when the assessor
returns EXACTLY ONE match in EXACTLY ONE county. Multi-match (common
name) or multi-county (debtor owns property in both Davidson and
Williamson) is bucketed as `ambiguous` and the lead stays
unenriched. Better to lose a lead than poison the dataset.

For non-Davidson/Williamson bankruptcy leads (the other 36 counties
in tnmb plus all of tneb + tnwb), TPAD-based owner-name lookup is
left as future work — tpad_enricher already supports that mode but
needs a proper jurisdiction-code sweep to know WHICH county to query.

Distress type: N/A (enricher only)
"""

from __future__ import annotations

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
from .williamson_assessor_bot import (
    INIGO_HOME, INIGO_SEARCH, INIGO_DETAIL, CSRF_FORM_RE,
)
from .tpad_enricher_bot import (
    TPAD_BASE, TPAD_SEARCH, COUNTY_CODES, _build_session as _tpad_session,
    search_by_owner as tpad_search_by_owner,
)
from .probate_property_enricher_bot import _decedent_to_owner_query


# Court IDs that map plausibly to Davidson + Williamson
DAVIDSON_WILLIAMSON_COURTS = ("tnmb",)

# Middle TN counties covered by TPAD (excludes Davidson/Williamson which
# have their own bots, and the other EXTERNAL TPAD counties). These are
# the counties tnmb federal bankruptcy court has jurisdiction over.
TNMB_TPAD_COUNTIES = (
    "wilson", "sumner", "cheatham", "robertson", "maury", "dickson",
    "cannon", "coffee", "franklin", "lincoln", "marshall", "moore",
    "trousdale", "smith", "macon", "clay", "dekalb", "jackson",
    "putnam", "pickett", "overton", "fentress", "houston", "stewart",
    "humphreys", "perry", "lawrence", "wayne", "lewis",
    "grundy", "sequatchie", "vanburen", "white", "cumberland",
)


class BankruptcyPropertyEnricherBot(BotBase):
    name = "bankruptcy_property_enricher"
    description = "Cross-reference bankruptcy debtors to Davidson/Williamson properties via assessor owner-search"
    throttle_seconds = 1.5
    expected_min_yield = 1

    max_leads_per_run = 100

    def __init__(self):
        super().__init__()
        self._williamson_csrf: Optional[str] = None
        self._tpad_session = None

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

        # Init all three assessor sessions
        self.fetch(PADCTN_HOME)
        self.fetch(PADCTN_QUICKSEARCH)
        self._init_williamson()
        self._tpad_session = _tpad_session()

        enriched = 0
        ambiguous = 0
        not_found = 0
        skipped = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(
                f"{len(candidates)} BANKRUPTCY (tnmb) leads lacking property_address"
            )

            for row in candidates[:self.max_leads_per_run]:
                debtor = row.get("full_name") or row.get("owner_name_records") or ""
                query = _decedent_to_owner_query(debtor)  # same LASTNAME, FIRST format
                if not query:
                    skipped += 1
                    continue

                # Try Davidson first
                davidson_hits = self._lookup_padctn(query) or []
                # Try Williamson second
                williamson_hits = self._lookup_williamson(query) or []
                # Try TPAD-covered Middle TN counties third
                tpad_hits = self._lookup_tpad_middle_tn(query) or []

                # Combine + classify
                hits = (
                    [("davidson", h) for h in davidson_hits]
                    + [("williamson", h) for h in williamson_hits]
                    + tpad_hits  # already (county, hit) tuples
                )
                if len(hits) == 0:
                    not_found += 1
                    continue
                if len(hits) > 1:
                    ambiguous += 1
                    continue

                county, hit = hits[0]
                update: Dict[str, Any] = {}
                if hit.get("property_address") and not row.get("property_address"):
                    update["property_address"] = hit["property_address"]
                if hit.get("appraised") and not row.get("property_value"):
                    update["property_value"] = hit["appraised"]
                update["county"] = county

                existing_raw = row.get("raw_payload") or {}
                if not isinstance(existing_raw, dict):
                    existing_raw = {}
                existing_raw["assessor_owner_match"] = {
                    "matched_county": county,
                    "query": query,
                    "appraised": hit.get("appraised"),
                    "owner": hit.get("owner"),
                    "parcel": hit.get("parcel") or hit.get("parcel_id"),
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

    # ── Candidate query ─────────────────────────────────────────────────────

    def _candidates(self, client) -> List[Dict[str, Any]]:
        try:
            q = (
                client.table("homeowner_requests_staging")
                .select("id, full_name, owner_name_records, county, property_address, property_value, raw_payload")
                .eq("distress_type", "BANKRUPTCY")
                .is_("property_address", "null")
                .limit(500)
                .execute()
            )
            rows = getattr(q, "data", None) or []
            # Filter to Middle TN court (tnmb) — that's where Davidson +
            # Williamson live. Eastern + Western will need separate enrichers.
            out = []
            for r in rows:
                court_id = ((r.get("raw_payload") or {}).get("court_id") or "")
                if court_id in DAVIDSON_WILLIAMSON_COURTS:
                    out.append(r)
            return out
        except Exception as e:
            self.logger.warning(f"candidate query failed: {e}")
            return []

    # ── PADCTN (Davidson) lookup ────────────────────────────────────────────

    def _lookup_padctn(self, query: str) -> Optional[List[Dict[str, Any]]]:
        res = self.fetch(
            PADCTN_SEARCH,
            method="POST",
            data={
                "RealEstate": "true",
                "SelectedSearch": "1",
                "StreetNumber": "",
                "SingleSearchCriteria": query,
                "AlterCriteria": "False",
            },
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        if res is None or res.status_code != 200:
            return None
        return self._parse_padctn(res.text)

    @staticmethod
    def _parse_padctn(html: str) -> List[Dict[str, Any]]:
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
                        rec["property_address"] = (
                            addr_lines[0] +
                            (", " + ", ".join(addr_lines[1:]) if len(addr_lines) > 1 else "")
                        )
                    break
            if rec.get("account_id"):
                out.append(rec)
        return out

    # ── TPAD Middle-TN sweep ────────────────────────────────────────────────

    def _lookup_tpad_middle_tn(self, query: str) -> List[tuple]:
        """Walk every TPAD-covered Middle TN county for the debtor name.
        Returns list of (county_name, hit_dict) tuples. Uses the LASTNAME,
        FIRST format the other lookups use; TPAD owner search accepts that.
        """
        if not self._tpad_session:
            return []
        results: List[tuple] = []
        for county in TNMB_TPAD_COUNTIES:
            jur = COUNTY_CODES.get(county)
            if not jur:
                continue
            try:
                hits = tpad_search_by_owner(self._tpad_session, jur, query)
            except Exception as e:
                self.logger.warning(f"  TPAD {county} ({jur}) search failed: {e}")
                continue
            for h in hits or []:
                results.append((
                    county,
                    {
                        "owner": h.get("owner"),
                        "parcel": h.get("parcel") or h.get("parcelNumber"),
                        "property_address": h.get("address") or h.get("propertyAddress"),
                        "appraised": None,  # TPAD search doesn't include appraisal — would need detail fetch
                        "tpad_jur": jur,
                        "tpad_raw": h,
                    },
                ))
        return results

    # ── Williamson Inigo lookup ─────────────────────────────────────────────

    def _init_williamson(self) -> bool:
        res = self.fetch(INIGO_HOME)
        if res is None or res.status_code != 200:
            return False
        import re
        m = CSRF_FORM_RE.search(res.text)
        if not m:
            return False
        self._williamson_csrf = m.group(1)
        return True

    def _lookup_williamson(self, query: str) -> Optional[List[Dict[str, Any]]]:
        if not self._williamson_csrf:
            return None
        res = self.fetch(INIGO_SEARCH, params={
            "csrf_token": self._williamson_csrf,
            "owner_name": query,
            "property_address": "",
            "parcel": "",
            "map_number": "",
            "lot": "",
            "subdivision": "",
            "city": "",
            "sales_date_start": "",
            "sales_date_end": "",
        })
        if res is None or res.status_code != 200:
            return None
        try:
            data = res.json()
        except Exception:
            return None
        items = data.get("data") or []
        if not items:
            return []
        # Filter to active parcels first (Williamson exposes Status='A')
        active = [it for it in items if it.get("Status") == "A"]
        chosen_pool = active or items
        out: List[Dict[str, Any]] = []
        for it in chosen_pool:
            out.append({
                "lrsn": it.get("DT_RowId") or it.get("lrsn"),
                "parcel_id": it.get("Parcel ID"),
                "owner": it.get("Owner"),
                "property_address": (it.get("Property Address") or "") +
                                    ((", " + it.get("Property City")) if it.get("Property City") else ""),
                "last_price": it.get("Last Price"),
                # Williamson appraised value lives on the detail page; we'd
                # have to fetch it separately. For dedup-counting purposes
                # the count of items is enough to detect ambiguity.
                "appraised": None,
            })
        return out


def run() -> dict:
    bot = BankruptcyPropertyEnricherBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        bot = BankruptcyPropertyEnricherBot()
        bot.fetch(PADCTN_HOME)
        bot.fetch(PADCTN_QUICKSEARCH)
        bot._init_williamson()
        for name in sys.argv[1:]:
            q = _decedent_to_owner_query(name)
            print(f"{name} -> query={q}")
            d = bot._lookup_padctn(q) or [] if q else []
            w = bot._lookup_williamson(q) or [] if q else []
            print(f"  Davidson: {len(d)} matches, Williamson: {len(w)} matches")
            for rec in d[:2]:
                print(f"    DAV: {rec.get('owner')} | {rec.get('property_address')} | ${rec.get('appraised')}")
            for rec in w[:2]:
                print(f"    WIL: {rec.get('owner')} | {rec.get('property_address')}")
    else:
        print(run())
