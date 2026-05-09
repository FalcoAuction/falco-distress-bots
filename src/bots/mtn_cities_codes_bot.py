"""
Middle Tennessee secondary cities — code-enforcement scraper.

Six target cities were investigated; only Mt. Juliet exposes a public,
no-auth, no-JS data feed for active code-enforcement issues. The rest
require auth, only ship a citizen-reporter UI, or do not publish
case-level data online at all. Findings:

  Murfreesboro (Rutherford Co.) — SKIP
      Building & Codes runs the City Court for citations; no public
      case search portal. ArcGIS Online has a "Blight Problems" Web
      Map (item id f27231f1de2948d0a2fe601d52ac5caa) backed by a
      public CitizenProblems_blight FeatureService. As of probe date
      it has 1 record total — citizen-reporter app, not a real case
      database. Not worth scraping.

  Franklin (Williamson Co.) — SKIP
      Property Maintenance Codes are handled inside Building &
      Neighborhood Services. Site is Cloudflare-fronted (HTTP 403
      to non-browser UAs). No public case-search portal advertised.
      No franklintn-energovweb.tylerhost.net deployment found.

  Brentwood (Williamson Co.) — SKIP (auth required)
      OneStop "OnLama" portal at brentwood.onlama.com. /AdvancedSearch.aspx
      redirects to Login.aspx?ReturnURL=%2fAdvancedSearch.aspx — case
      lookup is not anonymous despite city FAQ claiming otherwise.

  Hendersonville (Sumner Co.) — SKIP
      Tyler EnerGov SelfService at hendersonvilletn-energovweb.tylerhost.net.
      Page loads (200) but is a SPA; the /api/energov/search/search
      endpoint returns HTTP 500 to anonymous clients across every
      SearchModule value (0-8). SeeClickFix slug `hendersonville` is
      tied to Public Works (flooding, street signs) not Codes — only
      ~2 entries, no code-enforcement request types. No code-case
      data is publicly addressable.

  Mt. Juliet (Wilson Co.) — SCRAPED
      Code Enforcement Division publishes its intake on the
      SeeClickFix v2 public API:
        https://seeclickfix.com/api/v2/issues?place_url=mount-juliet
      ~80 pages of issues; request_types include "Property
      maintenance issue" (39286), "Zoning violation" (39287), "Debris
      or trash on private property" (39282). No auth, no JS rendering.

  Lebanon (Wilson Co.) — SKIP
      Codes Department directs complaints through an email form.
      OnBase guest login (GUEST / OnBase4321~) is for City Documents,
      not a code-enforcement case database. No public ArcGIS feature
      service for code cases. No SeeClickFix presence.

Distress type for the one ingested city: CODE_VIOLATION
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload
from ._cv_filter import is_auctionable_cv


# ─────────────────────────── Mt. Juliet config ──────────────────────────────

MTJULIET_SCF_PLACE = "mount-juliet"
MTJULIET_SCF_BASE = "https://seeclickfix.com/api/v2/issues"
MTJULIET_HTML_BASE = "https://seeclickfix.com/issues/{id}"

# SeeClickFix request_type titles that map to actual code-enforcement
# work. Filters out Public Works noise (street signs, dead animals,
# trees) so we only stage things that could become deals.
MTJULIET_CV_REQUEST_TYPES = {
    "Property maintenance issue",
    "Zoning violation",
    "Debris or trash on private property",
    "Dilapidated building",
    "Dilapidated structure",
    "Abandoned vehicle on private property",
    "Code violation",
    "Code enforcement",
}

# "Open" + "Acknowledged" mean staff has the case but it isn't closed.
# Closed ("Closed", "Archived") cases are not actionable for us.
ACTIVE_SCF_STATUSES = {"Open", "Acknowledged"}


# ───────────────────────────── Bot class ────────────────────────────────────


class MtnCitiesCodesBot(BotBase):
    """Middle TN secondary cities — code-enforcement leads.

    Currently only Mt. Juliet is wired (others are auth/JS-locked or
    don't publish online). Adding a new city is a matter of writing
    one more `_scrape_<city>()` and appending its results to `leads`
    in `scrape()`.
    """

    name = "mtn_cities_codes"
    description = "Middle TN secondary cities — open code-enforcement cases (Mt. Juliet only as of 2026-05)"
    throttle_seconds = 1.5
    expected_min_yield = 5  # SeeClickFix flow is small per city; alert if dry

    # ── entry point ────────────────────────────────────────────────────────

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        leads.extend(self._scrape_mtjuliet())
        # Future: Murfreesboro / Franklin / Brentwood / Hendersonville /
        # Lebanon — see module docstring for blockers.
        self.logger.info(f"total leads built: {len(leads)}")
        return leads

    # ── Mt. Juliet (SeeClickFix) ───────────────────────────────────────────

    def _scrape_mtjuliet(self) -> List[LeadPayload]:
        """Pull active code-enforcement issues from Mt. Juliet's SeeClickFix
        feed. SeeClickFix returns paginated JSON; we walk pages until empty
        or we hit a hard page cap."""
        leads: List[LeadPayload] = []
        page = 1
        max_pages = 40       # safety cap (~1000 issues)
        per_page = 50

        while page <= max_pages:
            params = {
                "place_url": MTJULIET_SCF_PLACE,
                "per_page": str(per_page),
                "page": str(page),
                "status": "Open,Acknowledged",
                "sort": "created_at",
                "sort_direction": "DESC",
            }
            res = self.fetch(MTJULIET_SCF_BASE, params=params, timeout=30)
            if res is None or res.status_code != 200:
                self.logger.error(
                    f"mt-juliet page {page} fetch failed: "
                    f"{res.status_code if res else 'none'}"
                )
                break

            try:
                data = res.json()
            except Exception as e:
                self.logger.error(f"mt-juliet page {page} JSON parse failed: {e}")
                break

            issues = data.get("issues") or []
            self.logger.info(f"mt-juliet page {page}: {len(issues)} issues")
            if not issues:
                break

            for issue in issues:
                lead = self._build_mtjuliet_lead(issue)
                if lead is not None:
                    leads.append(lead)

            # Stop when SeeClickFix says no next page
            pagination = (data.get("metadata") or {}).get("pagination") or {}
            if not pagination.get("next_page"):
                break
            page += 1

        self.logger.info(f"mt-juliet leads built: {len(leads)}")
        return leads

    def _build_mtjuliet_lead(self, issue: Dict[str, Any]) -> Optional[LeadPayload]:
        # Status gate — SeeClickFix sometimes returns stale Closed even
        # when status filter is set, so re-check defensively.
        status = (issue.get("status") or "").strip()
        if status not in ACTIVE_SCF_STATUSES:
            return None

        # Request-type gate — keep only code-enforcement-flavored types.
        # Public Works categories (street signs, potholes, dead animal)
        # are never our deal.
        rt = issue.get("request_type") or {}
        rt_title = (rt.get("title") or "").strip()
        if rt_title not in MTJULIET_CV_REQUEST_TYPES:
            return None

        # Confirm the issue actually belongs to Mt. Juliet's org. SeeClickFix
        # `place_url` matching is loose; the request_type's organization
        # field is the truth.
        rt_org = (rt.get("organization") or "").lower()
        if "juliet" not in rt_org:
            return None

        issue_id = issue.get("id")
        if issue_id is None:
            return None
        case_num = f"MJ-SCF-{issue_id}"

        address = self._normalize_address(str(issue.get("address") or ""))
        if not address:
            return None

        # Reuse the shared CV filter on whatever description text we
        # have. SeeClickFix doesn't have a structured violation-code
        # field, so we feed it `summary + description + request type`
        # and let the filter cite-by-cite split.
        violation_blob = " | ".join(
            p for p in [
                rt_title,
                str(issue.get("summary") or "").strip(),
                str(issue.get("description") or "").strip(),
            ] if p
        )
        keep, reason = is_auctionable_cv(violation_blob)
        if not keep:
            return None

        date_iso = self._iso_date(issue.get("created_at"))
        notes_parts = [f"case {case_num}"]
        if rt_title:
            notes_parts.append(f"type: {rt_title}")
        summary = (issue.get("summary") or "").strip()
        if summary:
            notes_parts.append(f"summary: {summary[:160]}")
        if date_iso:
            notes_parts.append(f"received: {date_iso}")
        notes_parts.append(f"status: {status}")
        notes_parts.append(f"filter: {reason}")

        source_url = (
            issue.get("html_url")
            or MTJULIET_HTML_BASE.format(id=issue_id)
        )

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, case_num),
            property_address=address,
            county="Wilson County",
            owner_name_records=None,  # SeeClickFix doesn't expose owner of record
            distress_type="CODE_VIOLATION",
            admin_notes=" · ".join(notes_parts),
            source_url=source_url,
            raw_payload={
                "city": "Mt. Juliet",
                "platform": "seeclickfix",
                "issue": issue,
            },
        )

    # ── helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_address(addr: str) -> Optional[str]:
        """SeeClickFix addresses look like
            '1003 Windrush Rd Mt Juliet, TN, 37122, USA'
            '1011 Carriage Trl Mount Juliet, Tennessee, 37122'
        Normalize trailing ', USA', collapse whitespace, force a final
        ', TN <ZIP>' tail for the enrichment matcher."""
        if not addr:
            return None
        a = addr.strip()
        a = re.sub(r",\s*USA\s*$", "", a, flags=re.I)
        a = re.sub(r",\s*Tennessee", ", TN", a, flags=re.I)
        a = re.sub(r"\s+", " ", a).strip(" ,")
        return a or None

    @staticmethod
    def _iso_date(value: Any) -> Optional[str]:
        """SeeClickFix returns ISO 8601 with offset, e.g.
            '2026-05-07T16:53:55-04:00'
        We only want the YYYY-MM-DD."""
        if not value:
            return None
        s = str(value)
        try:
            # Python 3.11+ accepts the offset directly.
            return datetime.fromisoformat(s).date().isoformat()
        except ValueError:
            return s[:10] if len(s) >= 10 else None


def run() -> dict:
    bot = MtnCitiesCodesBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
