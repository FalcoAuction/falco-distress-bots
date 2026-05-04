"""
Skip-trace enricher — extracts contact + absentee-owner signals from
data we already scraped, with no external paid lookups.

The traditional skip-trace stack (BatchData $30/mo, ATTOM, BeenVerified)
sells you the homeowner's contact info derived from voter rolls,
utility records, court filings, telecom databases. Three of those four
sources we already have via free public data:

  1. Court filings — bankruptcy debtors and probate decedents have
     filed names + sometimes addresses
  2. ROD/assessor records — property + MAILING address of every
     parcel owner (we now scrape this for Davidson + Williamson +
     Hamilton + Shelby + Rutherford)
  3. Foreclosure notices — borrower name + property address +
     "Other Interested Parties" addresses

What we DON'T have free is reliable phone-by-name lookup. But we
have a different angle the paid services don't emphasize: the OWNER
MAILING ADDRESS we've been scraping is itself a goldmine for
homeowner identification.

This enricher computes:
  1. is_absentee_owner — owner mailing differs from property
     address. Strong distress + flip-likelihood signal — absentee
     owners are 3-5x more likely to sell at discount.
  2. is_out_of_state_owner — even stronger signal. The Memphis-FL
     example we saw is a textbook flip-target (owner won't drive
     down to maintain it, eventual fire-sale).
  3. owner_mailing_normalized — clean address for downstream
     manual lookups.
  4. distance_owner_to_property — rough miles estimate via zip code
     centroid (no geocoder needed; embedded TN-zip lookup table for
     the major counties we cover).

This DOESN'T replace BatchData phone-by-name lookup — that remains
the one paid line. But it surfaces the highest-priority leads
(absentee, out-of-state) so Chris's BatchData budget hits the
homeowners we MOST want phones for.

Distress type: N/A (utility enricher).
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._provenance import record_field


# Approximate latitude/longitude for major TN cities (zip-prefix based)
# — used to compute owner-to-property distance when both have addresses.
TN_ZIP_CENTROIDS = {
    # Davidson (Nashville area)
    "370": (36.16, -86.78), "371": (36.16, -86.78), "372": (36.16, -86.78),
    "381": (36.16, -86.78),
    # Williamson (Franklin)
    "37027": (35.97, -86.83), "37064": (35.92, -86.87), "37067": (35.93, -86.85),
    "37069": (35.96, -86.82), "37174": (35.75, -86.92), "37179": (35.86, -86.98),
    # Rutherford (Murfreesboro)
    "37127": (35.81, -86.40), "37128": (35.84, -86.43), "37129": (35.87, -86.44),
    "37130": (35.85, -86.39), "37132": (35.85, -86.36), "37167": (35.96, -86.51),
    "37086": (36.01, -86.58),
    # Hamilton (Chattanooga)
    "374": (35.05, -85.31),
    # Knox (Knoxville)
    "379": (35.96, -83.92),
    # Shelby (Memphis)
    "380": (35.15, -90.05), "381": (35.15, -90.05),
    # Sumner / Wilson / Cheatham etc.
    "37075": (36.39, -86.46), "37087": (36.21, -86.30),
    # Generic state catch-all (centroid of TN)
    "*": (35.86, -86.66),
}


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles."""
    from math import asin, cos, radians, sin, sqrt
    R = 3958.8  # earth radius in miles
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return R * c


def lookup_zip_centroid(zip_code: str) -> Optional[Tuple[float, float]]:
    """Return (lat, lon) for a TN zip prefix; None if non-TN."""
    if not zip_code:
        return None
    z = re.sub(r"[^\d]", "", zip_code)[:5]
    if not z:
        return None
    # Try exact 5-digit first, then 3-digit prefix
    if z in TN_ZIP_CENTROIDS:
        return TN_ZIP_CENTROIDS[z]
    if z[:3] in TN_ZIP_CENTROIDS:
        return TN_ZIP_CENTROIDS[z[:3]]
    return None


def is_in_tennessee(state_or_zip: Optional[str], mailing_text: Optional[str] = None) -> bool:
    """Quick check whether a mailing address is in TN."""
    if state_or_zip:
        s = state_or_zip.strip().upper()
        if s == "TN" or s == "TENNESSEE":
            return True
        # 5-digit zip
        digits = re.sub(r"[^\d]", "", s)
        if digits and digits[:3] in {"370", "371", "372", "374", "377", "378", "379", "380", "381", "382", "383", "384", "385"}:
            return True
    if mailing_text:
        if re.search(r"\bTN\s*\d{5}\b", mailing_text.upper()):
            return True
    return False


def normalize_address_compare(addr: str) -> str:
    """Normalize for owner-vs-property comparison."""
    if not addr:
        return ""
    s = addr.upper()
    s = re.sub(r"[,.]", " ", s)
    s = re.sub(r"\b(STREET|ST|ROAD|RD|AVENUE|AVE|DRIVE|DR|LANE|LN|"
                r"BOULEVARD|BLVD|COURT|CT|CIRCLE|CIR|PLACE|PL|"
                r"WAY|HIGHWAY|HWY|PARKWAY|PKWY|TRAIL|TRL|TER|TERRACE|PIKE)\b\.?", "ST", s)
    s = re.sub(r"\b(NORTH|N|SOUTH|S|EAST|E|WEST|W)\b\.?", "", s)
    s = re.sub(r"\s+TN\s+\d{5}.*$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


class SkipTraceEnricherBot(BotBase):
    name = "skip_trace_enricher"
    description = "Compute absentee-owner + out-of-state + distance signals from scraped owner_mailing data"
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
                    "tagged": 0, "skipped": 0,
                    "staged": 0, "duplicates": 0, "fetched": 0}

        tagged = 0
        absentee = 0
        out_of_state = 0
        skipped = 0
        error_message: Optional[str] = None

        try:
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                rows = self._candidates(client, table)
                self.logger.info(f"{table}: {len(rows)} candidates")

                for row in rows[:self.max_leads_per_run]:
                    skip_meta = self._compute(row)
                    if skip_meta is None:
                        skipped += 1
                        continue

                    existing_meta = row.get("phone_metadata") or {}
                    if not isinstance(existing_meta, dict):
                        existing_meta = {}
                    if existing_meta.get("skip_trace") == skip_meta:
                        skipped += 1
                        continue
                    existing_meta["skip_trace"] = skip_meta

                    try:
                        client.table(table).update({
                            "phone_metadata": existing_meta,
                        }).eq("id", row["id"]).execute()
                        tagged += 1
                        if skip_meta.get("is_absentee_owner"):
                            absentee += 1
                        if skip_meta.get("is_out_of_state_owner"):
                            out_of_state += 1
                    except Exception as e:
                        self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif tagged == 0 and skipped == 0:
            status = "zero_yield"
        elif tagged == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=tagged + skipped,
            parsed_count=tagged + skipped,
            staged_count=tagged, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(
            f"tagged={tagged} absentee={absentee} out_of_state={out_of_state} skipped={skipped}"
        )
        return {
            "name": self.name, "status": status,
            "tagged": tagged, "absentee": absentee, "out_of_state": out_of_state,
            "skipped": skipped,
            "error": error_message,
            "staged": tagged, "duplicates": skipped,
            "fetched": tagged + skipped,
        }

    def _candidates(self, client, table: str) -> List[Dict[str, Any]]:
        # PostgREST caps .limit() at 1000 — paginate so the full corpus
        # gets skip-traced.
        out = []
        PAGE_SIZE = 1000
        MAX_PAGES = 10
        for page in range(MAX_PAGES):
            try:
                q = (
                    client.table(table)
                    .select("id, property_address, raw_payload, phone_metadata")
                    .not_.is_("property_address", "null")
                    .order("id")
                    .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                out.extend(rows)
                if len(rows) < PAGE_SIZE:
                    break
            except Exception as e:
                self.logger.warning(f"candidate query on {table} page {page} failed: {e}")
                break
        return out

    def _compute(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract owner mailing from any scraped assessor blob and compute
        absentee + out-of-state + distance flags."""
        prop_addr = row.get("property_address") or ""
        raw = row.get("raw_payload") or {}
        if not isinstance(raw, dict):
            return None

        # Find mailing address from any assessor source we've scraped
        mailing = None
        mailing_state = None
        mailing_zip = None
        source_used = None

        # Rutherford
        rc = raw.get("rutherford_arcgis") or {}
        if isinstance(rc, dict) and rc.get("owner_mailing"):
            mailing = rc.get("owner_mailing")
            mailing_state = rc.get("owner_state")
            mailing_zip = rc.get("owner_zip")
            source_used = "rutherford_arcgis"

        # Shelby
        sc = raw.get("shelby_arcgis") or {}
        if not mailing and isinstance(sc, dict) and sc.get("owner_mailing"):
            mailing = sc.get("owner_mailing")
            mailing_state = sc.get("owner_state")
            mailing_zip = sc.get("owner_zip")
            source_used = "shelby_arcgis"

        # Williamson
        w = raw.get("williamson_inigo") or {}
        if not mailing and isinstance(w, dict) and w.get("owner_address"):
            mailing = w.get("owner_address")
            source_used = "williamson_inigo"

        # PADCTN
        p = raw.get("padctn") or {}
        if not mailing and isinstance(p, dict) and p.get("mailing_address"):
            mailing = p.get("mailing_address")
            source_used = "padctn"

        # TPAD enricher
        t = raw.get("tpad") or {}
        if not mailing and isinstance(t, dict) and t.get("mailing_address"):
            mailing = t.get("mailing_address")
            source_used = "tpad_enricher"

        # Hamilton CSV
        h = raw.get("mailing_address")
        if not mailing and h:
            mailing = h
            source_used = "hamilton_tax_delinquent"

        if not mailing:
            return None

        # Normalize for comparison
        prop_norm = normalize_address_compare(prop_addr)
        mail_norm = normalize_address_compare(mailing)

        # Same property as mailing? (owner-occupied)
        is_same = (prop_norm and mail_norm and prop_norm in mail_norm) or (mail_norm and prop_norm and mail_norm in prop_norm)
        is_absentee = not is_same

        # In TN?
        in_tn = is_in_tennessee(mailing_state or mailing_zip, mailing)

        # Distance estimate
        distance_miles = None
        try:
            prop_zip = self._extract_zip(prop_addr)
            mail_zip = mailing_zip or self._extract_zip(mailing)
            if prop_zip and mail_zip:
                p_centroid = lookup_zip_centroid(prop_zip)
                m_centroid = lookup_zip_centroid(mail_zip)
                if p_centroid and m_centroid:
                    distance_miles = round(haversine_miles(*p_centroid, *m_centroid), 1)
        except Exception:
            pass

        out: Dict[str, Any] = {
            "is_absentee_owner": is_absentee,
            "is_out_of_state_owner": is_absentee and not in_tn,
            "owner_mailing": mailing,
            "owner_mailing_state": mailing_state,
            "owner_mailing_zip": mailing_zip,
            "distance_owner_to_property_miles": distance_miles,
            "source": source_used,
        }
        # Distress score boost — out-of-state absentee owners flip more often
        score = 0
        if is_absentee:
            score += 1
        if not in_tn and is_absentee:
            score += 2
        if distance_miles and distance_miles > 200:
            score += 1
        if distance_miles and distance_miles > 1000:
            score += 1
        out["absentee_distress_score"] = score
        return out

    @staticmethod
    def _extract_zip(text: str) -> Optional[str]:
        if not text:
            return None
        m = re.search(r"\b(\d{5})(?:-\d{4})?\b", text)
        return m.group(1) if m else None


def run() -> dict:
    bot = SkipTraceEnricherBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # CLI test: pass property_address and mailing_address
        prop = sys.argv[1]
        mail = sys.argv[2] if len(sys.argv) > 2 else None
        from urllib.parse import urlparse
        bot = SkipTraceEnricherBot()
        fake_row = {
            "property_address": prop,
            "raw_payload": {
                "shelby_arcgis": {
                    "owner_mailing": mail,
                    "owner_state": "FL" if mail and "FL" in mail.upper() else "TN",
                    "owner_zip": re.search(r"\d{5}", mail or "").group(0) if mail and re.search(r"\d{5}", mail) else None,
                },
            } if mail else {},
        }
        print(bot._compute(fake_row))
    else:
        print(run())
