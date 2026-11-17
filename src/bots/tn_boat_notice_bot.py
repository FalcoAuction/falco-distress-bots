"""
Tennessee marina-lien and abandoned-vessel notices from tnpublicnotice.com
(the Tennessee Press Association's free statewide index). Free; works off
search-result excerpts only (the full notice is behind a reCAPTCHA and is
never fetched).

Why: a Tennessee marina must advertise a lien sale in a newspaper for two
consecutive weeks before selling a boat (Tenn. Code Ann. 66-19-212). Those
notices name the owner, the TN registration, the hull ID and the marina,
which makes this the owner-direct boat lane. A marina running a multi-boat
lien auction is staged too: the marina itself needs an auctioneer.

Search mechanics: keyword search with NO popular-search category and the
form's real date-range fields (rbLastNumDays + txtLastNumDays). Passing a
category, or a number in dateRange, silently drops the keyword filter.

Leads are keyed on the boat (registration, else hull ID), so the second
week's publication of the same notice is a duplicate, not a new lead.
Parsing and the high-value filter live in `_boat_notice.py`; the
personal-property lane grades them (lane vessel_lien).

Distress type: MARINA_LIEN
"""

from __future__ import annotations

import os
import re
import sys
from typing import Dict, List

from ._base import LeadPayload
from ._boat_notice import grade, parse
from .tn_public_notice_bot import TNPN_BASE, TnPublicNoticeBot

KEYWORDS = ("vessel", "hull", "marina", "boat", "houseboat", "pontoon", "watercraft", "yacht")
P = "ctl00$ContentPlaceHolder1$as1$"


class TnBoatNoticeBot(TnPublicNoticeBot):
    name = "tn_boat_notice"
    description = "TN marina-lien and abandoned-vessel notices (owner, registration, hull ID, marina)"
    throttle_seconds = 1.5
    expected_min_yield = 0
    frozen_after_runs = 10_000
    rows_per_page = 50
    max_pages_per_keyword = 6
    date_range_days = int(os.getenv("FALCO_BOAT_NOTICE_DAYS", "45"))

    def scrape(self) -> List[LeadPayload]:
        rows: Dict[str, Dict] = {}
        for kw in KEYWORDS:
            for r in self._keyword_rows(kw):
                rows.setdefault(r["notice_id"], r)
        leads: Dict[str, LeadPayload] = {}
        parsed = kept = 0
        for r in sorted(rows.values(), key=lambda x: x["notice_id"]):
            v = parse(r.get("excerpt") or "")
            if not v:
                continue
            parsed += 1
            g = grade(v)
            if not g:
                continue
            lead = self._lead(r, v, g)
            if lead.pipeline_lead_key not in leads:
                leads[lead.pipeline_lead_key] = lead
                kept += 1
        self.logger.info(f"{len(rows)} notices, {parsed} boat notices, {kept} high-value boats staged")
        return list(leads.values())

    def _keyword_rows(self, kw: str) -> List[Dict]:
        sess = self._init_search_session()
        if sess is None:
            return []
        url, state, cookies = sess
        data = {
            "__EVENTTARGET": P + "btnGo1", "__EVENTARGUMENT": "",
            "__VIEWSTATE": state.get("__VIEWSTATE", ""),
            "__VIEWSTATEGENERATOR": state.get("__VIEWSTATEGENERATOR", ""),
            "__EVENTVALIDATION": state.get("__EVENTVALIDATION", ""),
            P + "ddlPopularSearches": "0", P + "txtSearch": kw, P + "rdoType": "0", P + "txtExclude": "",
            P + "dateRange": "rbLastNumDays", P + "txtLastNumDays": str(self.date_range_days),
            P + "txtLastNumWeeks": "52", P + "txtLastNumMonths": "12", P + "txtSSID": "0",
            P + "hdnField": "", P + "hdnLastScrollPos": "0", P + "hdnCountyScrollPosition": "-1",
            P + "hdnCityScrollPosition": "-1", P + "hdnPubScrollPosition": "-1",
        }
        res = self.fetch(url, method="POST", data=data, cookies=cookies)
        if res is None or res.status_code != 200:
            self.logger.warning(f'"{kw}": search failed')
            return []
        page = self._set_per_page(url, self._extract_form_state(res.text), cookies, res.text)
        out: List[Dict] = []
        for _ in range(self.max_pages_per_keyword):
            batch = self._extract_rows(page["html"])
            out += batch
            if len(batch) < self.rows_per_page:
                break
            page = self._next_page(url, page, cookies)
            if page is None:
                break
        self.logger.info(f'"{kw}": {len(out)} notices')
        return out

    def _lead(self, r: Dict, v: Dict, g: str) -> LeadPayload:
        ident = v.get("registration") or v.get("hin")
        if ident:
            key_id = f"boat|{ident}"
        elif v["kind"] == "marina_auction":
            # One auction runs in several weekly papers: key on marina + month.
            month = re.search(r"([A-Z][a-z]+)\s+\d{1,2},\s+(\d{4})", r.get("pub_date") or "")
            key_id = f"auction|{(v.get('marina') or '').upper()}|{' '.join(month.groups()) if month else r['notice_id']}"
        else:
            key_id = f"notice|{r['notice_id']}"
        county = (r.get("county") or "").strip() or None
        boat = " ".join(str(x) for x in (v.get("year") if v.get("year") and str(v.get("year")) not in
                                         (v.get("description") or "") else None, v.get("description")) if x) or "vessel"
        if v["kind"] == "marina_auction":
            note = f"{v.get('marina')} is holding a marina lien auction (TN notice {r['notice_id']}, {r.get('pub_date')})"
            who = v.get("marina")
        else:
            note = (f"{boat} ({v.get('registration') or v.get('hin') or 'unregistered'}) held by "
                    f"{v.get('marina') or 'a recovery party'}" + (f"; owner {v['owner']}" if v.get("owner") else ""))
            who = v.get("owner")
        return LeadPayload(
            bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, key_id),
            distress_type="MARINA_LIEN", county=county.title() if county else None,
            full_name=who, owner_name_records=v.get("owner"),
            source_url=f"{TNPN_BASE}/Details.aspx?ID={r['notice_id']}", admin_notes=note,
            raw_payload={"source": "tn_public_notice", "state": "TN", "county": (county or "").lower() or None,
                         "notice_id": r["notice_id"], "newspaper": r.get("paper"),
                         "publication_date": r.get("pub_date"), "excerpt": r.get("excerpt"),
                         "vessel": {**v, "grade": g}},
        )


def run() -> dict:
    return TnBoatNoticeBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
