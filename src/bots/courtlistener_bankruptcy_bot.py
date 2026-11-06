"""
CourtListener TN bankruptcy filings scraper.

CourtListener (Free Law Project) maintains a free, public mirror of
PACER bankruptcy data via the RECAP archive. Their REST API exposes
filings WITHOUT authentication for read-only access — 5k req/hour
unauthenticated, 5k req/hour with a free token. We don't need a token
for the volume we'll consume.

API base: https://www.courtlistener.com/api/rest/v4/search/?type=r&...

TN bankruptcy court IDs:
  tnmb = M.D. Tennessee (Nashville) — Davidson, Cheatham, Robertson,
         Wilson, Sumner, Williamson, Rutherford, Maury, Dickson, etc.
  tneb = E.D. Tennessee (Knoxville/Chattanooga) — Knox, Hamilton,
         Sullivan, Bradley, etc.
  tnwb = W.D. Tennessee (Memphis) — Shelby, Tipton, Fayette, etc.

Why this matters:
  - Chapter 13 = "wage earner's plan", commonly used to STOP a
    foreclosure (automatic stay). These homeowners are deeply
    distressed and have a court-supervised payment plan.
  - Chapter 7 = liquidation; trustee may sell real estate.
  - Filing dates within last 30-90 days = freshest possible distress
    signal — better than ATTOM's foreclosure data which is weeks late.

Property address NOT in the API response — has to be cross-referenced
via county assessor lookup by debtor name (the existing davidson_/
williamson_assessor_bot enrichers already support owner-name search).

Distress type: BANKRUPTCY
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload


CL_BASE = "https://www.courtlistener.com/api/rest/v4"
CL_SEARCH = CL_BASE + "/search/"
# Per-case bankruptcy details, including the trustee (trustee_str).
CL_BANKRUPTCY_INFO = CL_BASE + "/bankruptcy-information/"
# CourtListener stopped serving unauthenticated API requests (401 on search
# from late October 2026; the bot reported zero_yield for nine days before
# anyone noticed). A free account token restores it:
# https://www.courtlistener.com/help/api/rest/#your-authorization-token
TOKEN_ENV = "FALCO_COURTLISTENER_TOKEN"


class CourtListenerAuthError(RuntimeError):
    """Raised so the run is recorded as failed (and alerts), not zero_yield."""

TN_BANKRUPTCY_COURTS = (
    ("tnmb", "Middle TN (Nashville)"),
    ("tneb", "Eastern TN (Knoxville/Chattanooga)"),
    ("tnwb", "Western TN (Memphis)"),
)

# Chapter codes we care about — homeowner distress
TARGET_CHAPTERS = ("7", "13")


class CourtListenerBankruptcyBot(BotBase):
    name = "courtlistener_bankruptcy"
    description = "TN bankruptcy filings (Chapters 7 + 13) via CourtListener RECAP archive"
    throttle_seconds = 0.8
    expected_min_yield = 10  # TN has hundreds of filings/week across 3 courts

    # Walk last N days of filings per court
    days_to_scan = 30
    # Cap per court per run to be polite + manage volume
    max_per_court = 200
    # Chapter 7 trustee lookups per run (one request each).
    max_trustee_lookups = 150

    def _headers(self) -> Dict[str, str]:
        # Without an explicit Accept: application/json the API serves HTML.
        headers = {"Accept": "application/json"}
        token = (os.environ.get(TOKEN_ENV) or "").strip()
        if token:
            headers["Authorization"] = f"Token {token}"
        return headers

    def _check_auth(self, res) -> None:
        if res is not None and res.status_code in (401, 403):
            hint = "set " + TOKEN_ENV if not os.environ.get(TOKEN_ENV) else TOKEN_ENV + " was rejected"
            raise CourtListenerAuthError(f"CourtListener returned {res.status_code}: an API token is required ({hint})")

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        cutoff = date.today() - timedelta(days=self.days_to_scan)

        lookups = 0
        for court_id, court_label in TN_BANKRUPTCY_COURTS:
            self.logger.info(f"fetching {court_id} ({court_label})")
            results = self._fetch_court(court_id, cutoff)
            self.logger.info(f"  {court_id}: {len(results)} filings within {self.days_to_scan} days")
            for hit in results[: self.max_per_court]:
                # Chapter 7 trustees hire auctioneers to liquidate estates
                # with court approval; the trustee, not the debtor, is the
                # repeat B2B contact for that lane.
                trustee = None
                if str(hit.get("chapter") or "").strip() == "7" and hit.get("docket_id") \
                        and lookups < self.max_trustee_lookups:
                    trustee = self._fetch_trustee(hit["docket_id"])
                    lookups += 1
                lead = self._build_lead(hit, court_id, court_label, trustee=trustee)
                if lead is not None:
                    leads.append(lead)
        return leads

    def _fetch_trustee(self, docket_id: Any) -> Optional[str]:
        res = self.fetch(CL_BANKRUPTCY_INFO, params={"docket": docket_id}, headers=self._headers())
        self._check_auth(res)
        if res is None or res.status_code != 200:
            return None
        try:
            results = (res.json() or {}).get("results") or []
        except Exception:
            return None
        for item in results:
            name = (item.get("trustee_str") or "").strip()
            if name:
                return name
        return None

    # ── Fetch ────────────────────────────────────────────────────────────────

    def _fetch_court(self, court_id: str, cutoff: date) -> List[Dict[str, Any]]:
        """Walk paginated search results for a given court, stopping when we
        cross the cutoff date or hit max_per_court."""
        url = CL_SEARCH
        params = {
            "type": "r",                              # RECAP/PACER results
            "court": court_id,
            "order_by": "dateFiled desc",
            "filed_after": cutoff.isoformat(),        # YYYY-MM-DD
        }
        json_headers = self._headers()
        out: List[Dict[str, Any]] = []
        next_url: Optional[str] = None

        for page_idx in range(5):  # cap pagination depth
            if next_url:
                res = self.fetch(next_url, headers=json_headers)
            else:
                res = self.fetch(url, params=params, headers=json_headers)
            self._check_auth(res)
            if res is None or res.status_code != 200:
                break
            try:
                data = res.json()
            except Exception:
                break

            items = data.get("results") or []
            for it in items:
                # Filter to chapters we care about
                ch = str(it.get("chapter") or "").strip()
                if ch and ch not in TARGET_CHAPTERS:
                    continue
                # Cutoff date filter (defensive — API filter should already
                # handle this, but cursor pagination occasionally returns
                # earlier records)
                df = it.get("dateFiled") or ""
                if df and df < cutoff.isoformat():
                    return out
                out.append(it)
                if len(out) >= self.max_per_court:
                    return out

            next_url = data.get("next")
            if not next_url:
                break
        return out

    # ── Lead construction ────────────────────────────────────────────────────

    def _build_lead(self, hit: Dict[str, Any], court_id: str,
                     court_label: str, trustee: Optional[str] = None) -> Optional[LeadPayload]:
        case_name = (hit.get("caseName") or "").strip()
        if not case_name:
            return None
        chapter = str(hit.get("chapter") or "").strip() or "?"
        date_filed = hit.get("dateFiled") or ""
        docket = (hit.get("docketNumber") or "").strip()
        docket_id = hit.get("docket_id")
        pacer_case_id = hit.get("pacer_case_id")
        assigned_judge = (hit.get("assignedTo") or "").strip()
        parties = hit.get("party") or []

        # Take the first party name as the debtor — typically the title-cased
        # case name and party[0] are the same person for individual filings.
        # In joint petitions party[0] + party[1] are the spouses.
        debtor = parties[0] if parties else case_name
        co_debtor = parties[1] if len(parties) > 1 else None

        # CL gives an absolute_url for the docket page; we expose this for
        # operators to inspect the actual schedules later.
        docket_abs_url = (hit.get("docket_absolute_url") or "").strip()
        source_url = (
            f"https://www.courtlistener.com{docket_abs_url}" if docket_abs_url else None
        )

        admin_parts = [
            f"Ch.{chapter}",
            f"court={court_id}",
            f"docket={docket}" if docket else "",
            f"filed={date_filed}",
            f"judge={assigned_judge}" if assigned_judge else "",
            f"trustee={trustee}" if trustee else "",
        ]
        if co_debtor and co_debtor != debtor:
            admin_parts.append(f"co-debtor={co_debtor}")
        admin_notes = " · ".join(p for p in admin_parts if p)

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(
                self.name, f"{court_id}-{docket_id or pacer_case_id or docket}"
            ),
            full_name=debtor,
            owner_name_records=debtor,
            distress_type="BANKRUPTCY",
            admin_notes=admin_notes,
            source_url=source_url,
            raw_payload={
                "court_id": court_id,
                "court_label": court_label,
                "chapter": chapter,
                "date_filed": date_filed,
                "docket_number": docket,
                "docket_id": docket_id,
                "pacer_case_id": pacer_case_id,
                "assigned_judge": assigned_judge,
                "parties": parties,
                "case_name": case_name,
                "courtlistener_docket_url": source_url,
                "trustee": trustee,
            },
        )


def run() -> dict:
    bot = CourtListenerBankruptcyBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
