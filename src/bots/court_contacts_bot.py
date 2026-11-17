"""
Callable business contacts for court-driven asset leads (bankruptcies and
federal vessel arrests), written to raw_payload.b2b_contacts:

  trustee   Chapter 7 panel trustee's phone and e-mail from the U.S. Trustee
            Program's public list (free). The trustee decides whether an
            auctioneer sells the estate's boat, plane or equipment.
  counsel   Attorneys of record with phone and e-mail from CourtListener's
            v4 docket API (free account token in FALCO_COURTLISTENER_TOKEN).
            Without the token this half skips cleanly. The Florida and
            Tennessee bar directories disallow automated lookups in
            robots.txt, so they are not used.

These are business lines (trustees, law firms), not consumer phones, so no
skip-trace or do-not-call scrub is involved.

Distress type: N/A (enricher).
"""

from __future__ import annotations

import os
import sys
import traceback as tb
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from ._ust_trustees import URL as UST_URL
from ._ust_trustees import index, lookup, parse

CL_BASE = "https://www.courtlistener.com/api/rest/v4"
TOKEN_ENV = "FALCO_COURTLISTENER_TOKEN"
SOURCES = ("court_rss", "courtlistener_bankruptcy")
COUNSEL_RECHECK_DAYS = 3        # counsel shows up on RECAP after the case is filed
COUNSEL_GIVE_UP_DAYS = 30
_COLS = ("id, distress_type, bot_source, court_id:raw_payload->>court_id, case_number:raw_payload->>case_number, "
         "docket_number:raw_payload->>docket_number, docket_id:raw_payload->>docket_id, "
         "trustee:raw_payload->>trustee, b2b:raw_payload->b2b_contacts")


def cl_docket_number(case_number: Optional[str]) -> Optional[str]:
    """'1:26-cv-22409' / '26-25183' -> the docket number CourtListener stores."""
    return (case_number or "").strip() or None


def counsel_from(attorneys: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for a in attorneys:
        raw = a.get("contact_raw") or ""
        firm = next((ln.strip() for ln in raw.splitlines()[1:] if ln.strip()), None) if raw else None
        out.append({"name": a.get("name"), "firm": firm, "phone": a.get("phone") or None,
                    "email": a.get("email") or None})
    return [c for c in out if c["name"] and (c["phone"] or c["email"])]


class CourtContactsBot(BotBase):
    name = "court_contacts"
    description = "Trustee (USTP list) and counsel (CourtListener) phones for bankruptcy and vessel-arrest leads"
    throttle_seconds = 1.0
    expected_min_yield = 0
    frozen_after_runs = 10_000

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase", "staged": 0, "duplicates": 0, "fetched": 0}
        scanned = updated = 0
        error = None
        try:
            res = self.fetch(UST_URL, timeout=60)
            trustees = index(parse(res.text)) if res is not None and res.status_code == 200 else {}
            self.logger.info(f"USTP list: {sum(len(v) for v in trustees.values())} chapter 7 trustees (FL+TN)")
            token = (os.environ.get(TOKEN_ENV) or "").strip()
            if not token:
                self.logger.info(f"{TOKEN_ENV} not set: counsel lookup skipped (trustees only)")
            for lead in self._leads(client):
                scanned += 1
                upd = self._contacts(lead, trustees, token)
                if upd is None:
                    continue
                raw = self._raw_payload(client, lead["id"])
                if raw is None:
                    continue
                raw["b2b_contacts"] = upd
                client.table("homeowner_requests_staging").update({"raw_payload": raw}).eq("id", lead["id"]).execute()
                updated += 1
        except Exception as e:
            error = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error else "ok" if updated else ("all_dupes" if scanned else "zero_yield")
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=scanned, staged_count=updated,
                            duplicate_count=scanned - updated, error_message=error)
        self.logger.info(f"scanned={scanned} updated={updated}")
        return {"name": self.name, "status": status, "scanned": scanned, "updated": updated, "error": error,
                "staged": updated, "duplicates": scanned - updated, "fetched": scanned}

    def _contacts(self, lead: Dict[str, Any], trustees, token: str) -> Optional[Dict[str, Any]]:
        """New b2b_contacts dict, or None when nothing changed."""
        prev = lead.get("b2b") if isinstance(lead.get("b2b"), dict) else {}
        cur = dict(prev)
        court = (lead.get("court_id") or "").lower()
        if not cur.get("trustee") and lead.get("trustee"):
            t = lookup(trustees, lead["trustee"], court or None)
            if t:
                cur["trustee"] = {"name": t["name"], "phone": t["phone"], "email": t["email"], "source": "ustp_panel"}
        if token and not cur.get("counsel") and self._counsel_due(prev):
            counsel = self._counsel(court, lead, token)
            cur["counsel_checked"] = datetime.now(timezone.utc).isoformat()
            cur.setdefault("counsel_first_checked", cur["counsel_checked"])
            if counsel:
                cur["counsel"] = counsel
                cur["counsel_source"] = "courtlistener"
        return cur if cur != prev else None

    @staticmethod
    def _counsel_due(prev: Dict[str, Any]) -> bool:
        checked = prev.get("counsel_checked")
        if not checked:
            return True
        age = datetime.now(timezone.utc) - datetime.fromisoformat(checked)
        first = prev.get("counsel_first_checked") or checked
        if datetime.now(timezone.utc) - datetime.fromisoformat(first) > timedelta(days=COUNSEL_GIVE_UP_DAYS):
            return False
        return age > timedelta(days=COUNSEL_RECHECK_DAYS)

    def _counsel(self, court: str, lead: Dict[str, Any], token: str) -> List[Dict[str, Any]]:
        headers = {"Accept": "application/json", "Authorization": f"Token {token}"}
        docket_id = lead.get("docket_id")
        if not docket_id:
            num = cl_docket_number(lead.get("case_number") or lead.get("docket_number"))
            if not court or not num:
                return []
            res = self.fetch(f"{CL_BASE}/dockets/", params={"court": court, "docket_number": num}, headers=headers)
            if res is None or res.status_code != 200:
                return []
            hits = res.json().get("results") or []
            docket_id = hits[0].get("id") if hits else None
            if not docket_id:
                return []
        res = self.fetch(f"{CL_BASE}/attorneys/", params={"docket": docket_id}, headers=headers)
        if res is None or res.status_code != 200:
            return []
        return counsel_from(res.json().get("results") or [])

    # ── Supabase I/O ────────────────────────────────────────────────────

    def _leads(self, client):
        last_id = None
        while True:
            q = (client.table("homeowner_requests_staging").select(_COLS).in_("bot_source", list(SOURCES))
                 .in_("distress_type", ["BANKRUPTCY", "VESSEL_ARREST"]).order("id").limit(1000))
            if last_id is not None:
                q = q.gt("id", last_id)
            rows = getattr(q.execute(), "data", None) or []
            yield from rows
            if len(rows) < 1000:
                return
            last_id = rows[-1]["id"]

    @staticmethod
    def _raw_payload(client, row_id: str) -> Optional[Dict[str, Any]]:
        q = client.table("homeowner_requests_staging").select("raw_payload").eq("id", row_id).limit(1).execute()
        rows = getattr(q, "data", None) or []
        raw = rows[0].get("raw_payload") if rows else None
        return dict(raw) if isinstance(raw, dict) else ({} if raw is None else None)


def run() -> dict:
    return CourtContactsBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
