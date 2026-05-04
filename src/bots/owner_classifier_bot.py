"""
Owner classifier — flags business-entity owners vs natural-person
homeowners across the lead corpus.

The pilot's economics depend on Chris reaching actual homeowners
(distressed, motivated, can sign on their own behalf). LLC/Inc/Trust/
Government-owned properties are harder leads — owner is anonymous,
contact requires lawyers/registered agents, motivation is unclear.

This enricher walks every staged + live lead, classifies the owner
name string, and writes the result to `phone_metadata` (which already
holds an arbitrary JSONB blob keyed off the lead) so the dialer + UI
can sort by owner_class without DB schema changes.

Heuristic signals:
  - Suffix tokens: LLC, LLP, INC, CORP, CO, COMPANY, LP, LIMITED, PLC
  - Standalone words: PARTNERSHIP, TRUST, ASSOCIATION, FUND, HOLDINGS,
    GROUP, ENTERPRISES, PROPERTIES, INVESTMENTS, DEVELOPMENT, REALTY
  - Government markers: CITY OF, COUNTY OF, STATE OF, DEPT OF, US,
    USA, GOVERNMENT, MUNICIPALITY, AUTHORITY, DISTRICT
  - Religious/educational: CHURCH, SYNAGOGUE, MOSQUE, TEMPLE, MINISTRY,
    SCHOOL, UNIVERSITY, COLLEGE, FOUNDATION
  - Healthcare: HOSPITAL, MEDICAL CENTER, CLINIC

Anything NOT matching is classified `homeowner` (natural person).
Conservative stance: when in doubt, classify as `homeowner` so we
don't accidentally exclude a real lead.

Distress type: N/A (enricher only, doesn't change distress_type).
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase


# Patterns are ALL UPPERCASE — we uppercase the input for matching.
BUSINESS_SUFFIX_TOKENS = (
    " LLC", " L.L.C.", " LLP", " L.L.P.", " INC", " INC.", " CORP", " CORP.",
    " CO ", " CO.", " COMPANY", " LP ", " L.P.", " LIMITED", " LTD",
    " PLC", " PARTNERSHIP", " GENERAL PARTNERSHIP", " GP ", " GP.",
    " PA ", " PA.", " PLLC", " P.L.L.C.",
)
BUSINESS_WORDS = (
    "TRUST ", "ASSOCIATION", "FUND ", "HOLDINGS", "ENTERPRISES",
    "PROPERTIES", "INVESTMENTS", "DEVELOPMENT", "REALTY",
    "MANAGEMENT", "CAPITAL", "VENTURES", "GROUP",
    # Trade businesses (audit found 30+ of these leaking through)
    "PAINTING", "CONTRACTING", "CONSTRUCTION", "REMODELING",
    "EXCAVATING", "EXCAVATION", "IRRIGATION", "LANDSCAPING",
    "LAWN CARE", "TREE SERVICE", "PRESSURE WASHING",
    "TOWING", "AUTO REPAIR", "AUTO BODY", "TIRE",
    "PLUMBING", "ELECTRIC", "ROOFING", "FLOORING", "HVAC",
    # Personal / retail services
    "CLEANING", "JANITORIAL", "DETAILING", "CAR WASH", "CARWASH",
    "GRAPHICS", "PRINTING", "DESIGN",
    "SALON", "BARBER", "BARBERSHOP", "SPA",
    "GYM", "FITNESS", "TRAINING", "ACADEMY",
    "BOUTIQUE", "STUDIO",
    # Healthcare-adjacent
    "CHIROPRACTIC", "DENTAL", "DENTISTRY", "ORTHODONTICS",
    "VETERINARY", "ANIMAL HOSPITAL", "PHARMACY",
    # Food
    "GRILL", "RESTAURANT", "CAFE", "DINER", "BURGER", "PIZZA",
    "BAKERY", "BARBECUE", "BBQ", "TACOS", "TACO",
    # Generic services
    "SERVICES", "SVCS", "SVC ", "SOLUTIONS", "CONSULTING",
    "LOGISTICS", "DISTRIBUTION", "DELIVERY",
    # Industry
    "MILLS", "HEMP", "FARMS", "AGRICULTURAL", "INDUSTRIES",
    "MANUFACTURING", "ENGINEERING", "EQUIPMENT", "SUPPLY",
)
# Suffix-style trade tokens that are too short for the BUSINESS_WORDS
# whole-word match. Treated as suffixes (require trailing/leading
# space or end-of-string).
BUSINESS_TRADE_SUFFIXES = (
    " PRO ", " PRO.",  # SUBSURFACEPRO etc.
    " AUTO ", " AUTO.",
)
GOV_MARKERS = (
    "CITY OF ", "COUNTY OF ", "STATE OF ", "DEPT OF ", "DEPARTMENT OF ",
    " GOVERNMENT", "MUNICIPAL", " AUTHORITY", " DISTRICT", " COUNCIL",
    "U.S.", "USA ", "UNITED STATES",
    "METRO ", "METROPOLITAN ",
    " HOUSING AUTH",
)
RELIGIOUS_EDU_MARKERS = (
    " CHURCH", " SYNAGOGUE", " MOSQUE", " TEMPLE", " MINISTRY", " MINISTRIES",
    " SCHOOL", " UNIVERSITY", " COLLEGE", " ACADEMY", " FOUNDATION",
    " SOCIETY", " INSTITUTE",
)
HEALTHCARE_MARKERS = (
    " HOSPITAL", " MEDICAL CENTER", " CLINIC", " HEALTHCARE", " HEALTH CARE",
)


def classify_owner(owner: Optional[str]) -> Tuple[str, Optional[str]]:
    """Return (owner_class, evidence_marker).

    owner_class is one of: 'homeowner', 'business', 'government',
    'religious_or_education', 'healthcare', 'unknown'.
    evidence_marker is the substring that triggered the non-homeowner
    classification (None for homeowner / unknown).
    """
    if not owner or not isinstance(owner, str):
        return ("unknown", None)
    s = owner.strip()
    if not s:
        return ("unknown", None)

    upper = " " + s.upper() + " "

    # Government has the strongest signal — check first.
    for marker in GOV_MARKERS:
        if marker in upper:
            return ("government", marker.strip())

    for marker in HEALTHCARE_MARKERS:
        if marker in upper:
            return ("healthcare", marker.strip())

    for marker in RELIGIOUS_EDU_MARKERS:
        if marker in upper:
            return ("religious_or_education", marker.strip())

    for marker in BUSINESS_SUFFIX_TOKENS:
        if marker in upper:
            return ("business", marker.strip())

    for marker in BUSINESS_TRADE_SUFFIXES:
        if marker in upper:
            return ("business", marker.strip())

    # Glued-on suffixes: SUBSURFACEPRO, AUTOPRO, ELECTRICPRO etc.
    # Match a single all-caps word of 8+ chars ending in PRO/SVCS/SVC.
    for suffix in ("PRO", "SVCS", "SVC"):
        # word boundary, then 5+ letters, then suffix, then word boundary
        if re.search(rf"\b[A-Z]{{5,}}{suffix}\b", upper):
            return ("business", suffix)

    # BUSINESS_WORDS are stricter — match as whole words to avoid
    # false positives (e.g., "TRUST" in "TRUSTON FAMILY")
    for marker in BUSINESS_WORDS:
        if re.search(rf"\b{re.escape(marker.strip())}\b", upper):
            return ("business", marker.strip())

    return ("homeowner", None)


class OwnerClassifierBot(BotBase):
    name = "owner_classifier"
    description = "Tag every lead with owner_class (homeowner vs business/gov/religious/healthcare) for dialer sort"
    throttle_seconds = 0.0  # local-only, no fetches
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
                    "enriched": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        classified = 0
        skipped = 0
        per_class: Dict[str, int] = {}
        error_message: Optional[str] = None

        try:
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                rows = self._candidates(client, table)
                self.logger.info(f"{table}: {len(rows)} unclassified leads")

                for row in rows[:self.max_leads_per_run]:
                    owner = (row.get("owner_name_records") or row.get("full_name") or "")
                    klass, marker = classify_owner(owner)
                    per_class[klass] = per_class.get(klass, 0) + 1

                    existing_meta = row.get("phone_metadata") or {}
                    if not isinstance(existing_meta, dict):
                        existing_meta = {}
                    # Skip if already classified the same way (idempotent)
                    if existing_meta.get("owner_class") == klass:
                        skipped += 1
                        continue
                    existing_meta["owner_class"] = klass
                    if marker:
                        existing_meta["owner_class_evidence"] = marker
                    elif "owner_class_evidence" in existing_meta:
                        del existing_meta["owner_class_evidence"]

                    try:
                        client.table(table).update({
                            "phone_metadata": existing_meta,
                        }).eq("id", row["id"]).execute()
                        classified += 1
                    except Exception as e:
                        self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif classified == 0 and skipped == 0:
            status = "zero_yield"
        elif classified == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=classified + skipped,
            parsed_count=classified + skipped,
            staged_count=classified, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(f"classified={classified} skipped={skipped} per_class={per_class}")
        return {
            "name": self.name, "status": status,
            "classified": classified, "skipped": skipped,
            "per_class": per_class,
            "error": error_message,
            "staged": classified, "duplicates": skipped,
            "fetched": classified + skipped,
        }

    def _candidates(self, client, table: str) -> List[Dict[str, Any]]:
        # PostgREST caps .limit() at 1000 silently — paginate to ensure
        # the full corpus gets classified. Audit found owner_classifier
        # only re-tagged 1085 of 2900+ rows in a single pass because of
        # this cap, leaving ~1900 rows with stale classifications.
        rows: List[Dict[str, Any]] = []
        PAGE_SIZE = 1000
        MAX_PAGES = 10
        for filter_kind in ("has_owner", "has_full_name"):
            for page in range(MAX_PAGES):
                try:
                    q = (client.table(table)
                            .select("id, full_name, owner_name_records, phone_metadata")
                            .order("id")
                            .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1))
                    if filter_kind == "has_owner":
                        q = q.not_.is_("owner_name_records", "null")
                    else:
                        q = q.is_("owner_name_records", "null").not_.is_("full_name", "null")
                    r = q.execute()
                    page_rows = getattr(r, "data", None) or []
                    if not page_rows:
                        break
                    rows.extend(page_rows)
                    if len(page_rows) < PAGE_SIZE:
                        break
                except Exception as e:
                    self.logger.warning(
                        f"candidate query on {table} page {page} ({filter_kind}) failed: {e}"
                    )
                    break
        return rows


def run() -> dict:
    bot = OwnerClassifierBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for name in sys.argv[1:]:
            print(f"{name!r} -> {classify_owner(name)}")
    else:
        print(run())
