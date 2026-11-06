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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from ._base import BotBase, _supabase
from . import _owner_lookup as owner_lookup
from . import _court_notices as court_notices
from ._asset_profile import asset_profile
from .tpad_enricher_bot import (
    COUNTY_CODES as TPAD_COUNTY_CODES,
    EXTERNAL_COUNTIES as TPAD_EXTERNAL_COUNTIES,
    _norm_county as _tpad_norm_county,
    _build_session as _tpad_session,
    search_by_owner as _tpad_search_by_owner,
    fetch_detail as _tpad_fetch_detail,
)
from .davidson_assessor_bot import (
    PADCTN_HOME, PADCTN_QUICKSEARCH, PADCTN_SEARCH,
    ACCOUNT_RE, APPRAISED_RE, LAND_SIZE_RE, LAND_USE_RE, PARCEL_RE,
)


def _decedent_from_payload(raw: Any) -> str:
    """Older tn_public_notice probate rows often left full_name empty even
    though the excerpt reads "Estate of NAME, Deceased". Recover it from the
    structured court notice or the excerpt; empty string when absent."""
    if not isinstance(raw, dict):
        return ""
    notice = raw.get("court_notice")
    if isinstance(notice, dict) and notice.get("decedent"):
        return str(notice["decedent"])
    excerpt = raw.get("excerpt") or ""
    if not excerpt:
        return ""
    found = court_notices.extract_probate([excerpt]).get("decedent") or ""
    # Excerpts are truncated; a "name" that runs into notice boilerplate is junk.
    if len(found.split()) > 5 or re.search(r"notice|letters|hereby|court", found, re.I):
        return ""
    return found


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


def _decedent_to_tpad_query(decedent: str) -> Optional[str]:
    """TPAD wants "LASTNAME FIRSTNAME" and prefix-matches it.

    Unlike PADCTN it does not accept a comma: "Crowder, Patricia"
    returns nothing while "Crowder" returns nine. Owner values come back
    like "CROWDER PHILLIP ETUX RITA CROWDER", so last-then-first with a
    space is the shape that matches.
    """
    q = _decedent_to_owner_query(decedent)
    return q.replace(",", "") if q else None


class ProbatePropertyEnricherBot(BotBase):
    name = "probate_property_enricher"
    description = "Cross-reference probate decedents to TN properties via PADCTN (Davidson) + statewide TPAD owner search"
    throttle_seconds = 1.5
    expected_min_yield = 1

    max_leads_per_run = 100
    def __init__(self):
        super().__init__()
        # Shelby's Assessor server needs TLS legacy renegotiation; scope the
        # adapter to that host only.
        self.session.mount(owner_lookup.SHELBY_BASE, owner_lookup.LegacyTLSAdapter())

    # A miss (not found, ambiguous, unsupported county) is retried after this
    # many days: estates get recorded and assessor rolls update, but not daily.
    retry_after_days = 30

    # Lazily built so a Davidson-only run never opens a TPAD session.
    _tpad_sess = None

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
        unsupported = 0   # county has no owner-name lookup wired (Knox, Shelby, ...)
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(
                f"{len(candidates)} probate leads lacking property_address"
            )

            for row in candidates[:self.max_leads_per_run]:
                decedent = row.get("full_name") or row.get("owner_name_records") or _decedent_from_payload(row.get("raw_payload"))
                query = _decedent_to_owner_query(decedent)
                if not query:
                    skipped += 1
                    self._park(client, row, "no_query")
                    continue
                # Route by county: Davidson has its own assessor portal,
                # the other 85 TPAD counties go through the statewide
                # system, and the remaining externals have no owner-name
                # lookup wired yet (Knox and Shelby are the big two).
                county_key = _tpad_norm_county(row.get("county") or "")
                hit: Optional[Dict[str, Any]] = None

                if county_key == "davidson":
                    results = self._lookup_owner(query)
                    if results is None:
                        not_found += 1   # portal error: retry next run, do not park
                        continue
                    # PADCTN searches last+first only. Every record goes through
                    # the shared matcher: "TATE, NORMA D." is not "Norma Jean
                    # Tate", and two parcels owned by the same person are one
                    # estate (previously any second record parked as ambiguous).
                    cands = [owner_lookup.Candidate(
                        owner=r.get("owner") or "", address=r.get("property_address"), parcel=r.get("parcel"),
                        value=r.get("appraised"), acres=r.get("acres"), property_class=r.get("land_use"),
                        extra={"account_id": r.get("account_id")}) for r in results]
                    result = owner_lookup.match_candidates(decedent, cands, query)
                    status = result["status"]
                    if status != "matched":
                        if status == "ambiguous":
                            ambiguous += 1
                        else:
                            not_found += 1
                        self._park(client, row, "name_conflict" if status == "not_found" and results else status,
                                   detail={"query": query, "owners": [c.owner for c in cands][:10]})
                        continue
                    primary = result["primary"] or {}
                    hit = {
                        "property_address": primary.get("address"),
                        "appraised": primary.get("value"),
                        "parcel": primary.get("parcel"),
                        "owner": primary.get("owner"),
                        "land_use": primary.get("class"),
                        "acres": primary.get("acres"),
                        "account_id": (primary.get("extra") or {}).get("account_id"),
                        "owner_match": {**result, "county": "davidson"},
                    }
                elif county_key and county_key not in TPAD_EXTERNAL_COUNTIES:
                    tq = _decedent_to_tpad_query(decedent)
                    hit = self._tpad_lookup(county_key, tq) if tq else None
                    if hit is None:
                        not_found += 1
                        self._park(client, row, "not_found")
                        continue
                    if not owner_lookup.owner_agrees(decedent, hit.get("owner")):
                        not_found += 1
                        self._park(client, row, "name_conflict", detail={"query": tq, "owner": hit.get("owner")})
                        continue
                elif county_key in owner_lookup.supported_counties():
                    # Montgomery, Rutherford, Williamson (and Shelby when enabled): not in TPAD,
                    # searched by owner name on each county's own system.
                    result = owner_lookup.lookup(self.fetch, county_key, decedent)
                    status = result["status"]
                    if status == "source_error":
                        # The county system was down or changed; do not park,
                        # so the next run retries.
                        not_found += 1
                        continue
                    if status != "matched":
                        if status == "ambiguous":
                            ambiguous += 1
                        else:
                            not_found += 1
                        self._park(client, row, status, detail={k: result.get(k) for k in ("query", "owner_keys")})
                        continue
                    primary = result["primary"] or {}
                    address = primary.get("address")
                    # Montgomery and Shelby give the street only; name the
                    # county rather than guess a city.
                    if address and "," not in address:
                        address = f"{address}, {county_key.title()} County, TN"
                    hit = {
                        "property_address": address,
                        "appraised": primary.get("value"),
                        "parcel": primary.get("parcel"),
                        "owner": ", ".join(result.get("owner_keys") or []) or None,
                        "land_use": primary.get("class"),
                        "owner_match": {**result, "county": county_key},
                    }
                else:
                    unsupported += 1
                    self._park(client, row, "unsupported_county")
                    continue
                update: Dict[str, Any] = {}
                if hit.get("property_address") and not row.get("property_address"):
                    update["property_address"] = hit["property_address"]
                # Authoritative — override any prior HMDA-anchored phantom.
                # The source goes in property_value_source (staging column
                # added 2026-11-05) and raw_payload['value_source'].
                value_source = None
                if hit.get("appraised"):
                    update["property_value"] = int(round(float(hit["appraised"])))
                    value_source = "probate_assessor"

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
                    "acres": hit.get("acres"),
                    "owner": hit.get("owner"),
                    "property_address": hit.get("property_address"),
                }
                if hit.get("owner_match"):
                    existing_raw["owner_match"] = hit["owner_match"]
                    value_source = f"{county_key}_assessor_owner_match" if value_source else None
                if value_source:
                    existing_raw["value_source"] = value_source
                    update["property_value_source"] = value_source
                profile = asset_profile(existing_raw)
                if profile:
                    existing_raw["asset_profile"] = profile
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
            fetched_count=enriched + not_found + ambiguous + skipped + unsupported,
            parsed_count=enriched + not_found + ambiguous,
            staged_count=enriched, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(
            f"enriched={enriched} ambiguous={ambiguous} "
            f"not_found={not_found} skipped={skipped} unsupported={unsupported}"
        )
        return {
            "name": self.name, "status": status,
            "enriched": enriched, "ambiguous": ambiguous,
            "not_found": not_found, "skipped": skipped,
            "unsupported": unsupported,
            "error": error_message,
            "staged": enriched, "duplicates": skipped,
            "fetched": enriched + not_found + ambiguous + skipped + unsupported,
        }

    # ── Internal ────────────────────────────────────────────────────────────

    def _park(self, client, row: Dict[str, Any], result: str, detail: Optional[Dict[str, Any]] = None) -> None:
        """Record an unsuccessful match so _candidates skips the row until
        retry_after_days pass. Best-effort: a failed write only means the row
        is tried again next run."""
        raw = row.get("raw_payload") or {}
        if not isinstance(raw, dict):
            raw = {}
        raw["probate_match_attempt"] = {"at": datetime.now(timezone.utc).isoformat(), "result": result, **(detail or {})}
        try:
            client.table("homeowner_requests_staging").update({"raw_payload": raw}).eq("id", row["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  park failed id={row['id']}: {e}")

    def _tpad_lookup(self, county_key: str, query: str) -> Optional[Dict[str, Any]]:
        """Owner-name lookup against the TN Comptroller's statewide TPAD.

        Same strict rule as the Davidson path: commit only on an exact
        single match, because a common decedent name would otherwise pin
        the wrong house onto the lead. Returns a dict shaped like the
        PADCTN parser output so the commit block stays shared.
        """
        jur = TPAD_COUNTY_CODES.get(county_key)
        if not jur:
            return None
        if self._tpad_sess is None:
            self._tpad_sess = _tpad_session()
        try:
            results = _tpad_search_by_owner(self._tpad_sess, jur, query)
        except Exception as e:
            self.logger.warning(f"  tpad search failed {county_key}: {e}")
            return None
        if not results or len(results) != 1:
            return None
        hit = results[0]
        addr = (hit.get("propertyAddress") or "").strip()
        if not addr:
            return None
        out: Dict[str, Any] = {
            "property_address": addr,
            "owner": hit.get("owner"),
            "parcel": hit.get("parcelId"),
            "account_id": hit.get("parcelKey"),
        }
        # Appraised value lives on the detail page, not the search result.
        try:
            detail = _tpad_fetch_detail(
                self._tpad_sess,
                hit.get("parcelId") or "",
                jur,
                hit.get("parcelKey") or "",
            )
            val = detail.get("appraised_value") or detail.get("appraised_value_alt")
            if val:
                out["appraised"] = val
        except Exception:
            pass
        return out

    def _candidates(self, client) -> List[Dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.retry_after_days)).isoformat()
        try:
            q = (
                client.table("homeowner_requests_staging")
                .select("id, full_name, owner_name_records, county, property_address, property_value, raw_payload")
                .eq("distress_type", "PROBATE")
                # Davidson-only was a v1 guard while the PADCTN pattern was
                # being validated. It is why Knox (822 leads) and Shelby (599)
                # have zero addresses: a probate notice names a decedent, not
                # a property, so without a lookup the lead is just a name.
                # TPAD covers 86 of 95 counties off the same owner-name key.
                .is_("property_address", "null")
                # Skip rows already tried within the retry window, newest
                # notices first. Unordered and with no memory of misses, the
                # bot re-walked the same 2,000 of ~8,400 rows every run and
                # rarely reached a new estate.
                .or_(
                    "raw_payload->probate_match_attempt.is.null,"
                    f"raw_payload->probate_match_attempt->>at.lt.{cutoff}"
                )
                .order("staged_at", desc=True)
                .limit(2000)
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
            m = LAND_SIZE_RE.search(text)
            if m:
                rec["acres"] = float(m.group(1))
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
