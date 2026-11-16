"""
Florida code enforcement from free open-data APIs (no login, no CAPTCHA):

  Miami-Dade County (ArcGIS, unincorporated + county building code)
    Open_Building_Violations   CASE_TYPE 'Unsafe Structure' and any case the
                               county has billed under the unsafe-structures
                               process (USBILLDATE), plus cases with a
                               recorded lien (BuildingViolationLien)
  City of Orlando (Socrata k6e8-nw6w)
    open Housing (minimum housing) and Abatement cases on single-family,
    condo and small multifamily parcels

Picky on purpose: expired permits, signs, lots, tenant-level complaints in
apartment complexes and anything older than LOOKBACK_DAYS (except
unsafe structures with a lien, which stay actionable) are dropped. The
fl_roll_enricher attaches owner, value and mailing from the DOR roll using
the folio / parcel id carried here.

Distress type: CODE_VIOLATION
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload

MDC = "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services"
ORLANDO = "https://data.cityoforlando.net/resource/k6e8-nw6w.json"
LOOKBACK_DAYS = 365
ORLANDO_KEEP_USE = ("single family", "condominium", "townhouse", "duplex", "triplex", "quadruplex",
                    "multi-family class i", "mobile home", "vacant residential")
ORLANDO_VALUE_MAX = 3_000_000   # above this it is a complex, not an owner-occupant


def _ms_date(ms: Optional[int]) -> Optional[str]:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).date().isoformat() if ms else None


class FlCodeEnforcementBot(BotBase):
    name = "fl_code_enforcement"
    description = "Miami-Dade unsafe structures + liens, Orlando housing/abatement cases (free open data)"
    throttle_seconds = 0.5
    expected_min_yield = 0
    frozen_after_runs = 10_000

    def scrape(self) -> List[LeadPayload]:
        leads: Dict[str, LeadPayload] = {}
        for lead in self._miami_dade() + self._orlando():
            leads.setdefault(lead.pipeline_lead_key, lead)
        self.logger.info(f"staging {len(leads)} lead(s)")
        return list(leads.values())

    # ── Miami-Dade ──────────────────────────────────────────────────────

    def _arcgis(self, layer: str, where: str, fields: str) -> List[Dict[str, Any]]:
        out, offset = [], 0
        while True:
            res = self.fetch(f"{MDC}/{layer}/FeatureServer/0/query", timeout=90, params={
                "where": where, "outFields": fields, "f": "json", "resultOffset": offset,
                "resultRecordCount": 2000, "orderByFields": "OBJECTID" if layer != "BuildingViolationLien" else "ObjectId"})
            if res is None or res.status_code != 200:
                self.logger.warning(f"{layer}: HTTP {getattr(res, 'status_code', 'none')}")
                return out
            feats = res.json().get("features", [])
            out += [f["attributes"] for f in feats]
            if len(feats) < 2000:
                return out
            offset += len(feats)

    def _miami_dade(self) -> List[LeadPayload]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        liens = {r["CASE_NUM"].strip(): r for r in self._arcgis("BuildingViolationLien", "1=1",
                                                                  "CASE_NUM,LIEN_BOOK,LIEN_PAGE,LIEN_RECDT")
                 if r.get("CASE_NUM")}
        rows = self._arcgis("Open_Building_Violations",
                            "CASE_TYPE = 'Unsafe Structure' OR USBILLDATE IS NOT NULL",
                            "CASE_NUM,CASE_TYPE,FOLIO,PROP_ADDR,USBILLDATE,VIOL_NAME,OPEN_DATE,PROTECTED")
        out = []
        for r in rows:
            if (r.get("PROTECTED") or "").upper() == "Y":
                continue   # s. 119.071(4)(d) protected owner: never stage
            case = (r.get("CASE_NUM") or "").strip()
            lien = liens.get(case)
            opened = _ms_date(r.get("OPEN_DATE"))
            if not lien and (not opened or opened < cutoff):
                continue
            addr = (r.get("PROP_ADDR") or "").strip()
            if not addr or not case:
                continue
            raw = {"source": "mdc_building_violations", "state": "FL", "county": "miami-dade",
                   "case_number": case, "case_type": r.get("CASE_TYPE"), "parcel_id": (r.get("FOLIO") or "").strip(),
                   "opened": opened, "unsafe_billed": _ms_date(r.get("USBILLDATE")),
                   "violator": (r.get("VIOL_NAME") or "").strip() or None}
            if lien:
                raw["lien"] = {"book": lien.get("LIEN_BOOK"), "page": lien.get("LIEN_PAGE"),
                               "recorded": _ms_date(lien.get("LIEN_RECDT"))}
            out.append(LeadPayload(
                bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, f"mdc|{case}"),
                distress_type="CODE_VIOLATION", county="Miami-Dade", property_address=f"{addr}, Miami-Dade County, FL",
                full_name=raw["violator"], owner_name_records=raw["violator"],
                source_url="https://gis-mdc.opendata.arcgis.com/", raw_payload=raw,
                admin_notes=(f"Miami-Dade {r.get('CASE_TYPE')} case {case} opened {opened or '?'}"
                             + (f"; unsafe-structure billing {raw['unsafe_billed']}" if raw["unsafe_billed"] else "")
                             + (f"; lien recorded {raw['lien']['recorded']} (book {raw['lien']['book']} "
                                f"page {raw['lien']['page']})" if lien else "")),
            ))
        self.logger.info(f"Miami-Dade: {len(rows)} unsafe/billed open cases, {len(out)} kept")
        return out

    # ── Orlando ─────────────────────────────────────────────────────────

    def _orlando(self) -> List[LeadPayload]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT00:00:00")
        res = self.fetch(ORLANDO, timeout=90, params={
            "$where": f"caseinfostatus != 'Closed' AND case_type in ('Housing','Abatement') AND casedt > '{cutoff}'",
            "$order": "casedt DESC", "$limit": 5000})
        if res is None or res.status_code != 200:
            self.logger.warning(f"Orlando: HTTP {getattr(res, 'status_code', 'none')}")
            return []
        rows = res.json()
        out = []
        for r in rows:
            use = (r.get("pa_dordesc") or "").lower()
            value = float(r.get("parassdvalue") or 0)
            if not any(use.startswith(u) for u in ORLANDO_KEEP_USE) or value > ORLANDO_VALUE_MAX:
                continue
            addr = " ".join((r.get("derived_address") or "").split())
            if not addr or "NO ADDRESS" in addr.upper():
                continue
            case = r.get("apno")
            out.append(LeadPayload(
                bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, f"orl|{case}"),
                distress_type="CODE_VIOLATION", county="Orange",
                property_address=addr.replace(" ORLANDO FL", ", Orlando, FL"),
                property_value=value or None, source_url="https://data.cityoforlando.net/d/k6e8-nw6w",
                raw_payload={"source": "orlando_code_enforcement", "state": "FL", "county": "orange",
                             "case_number": case, "case_type": r.get("case_type"), "case_name": r.get("casename"),
                             "status": r.get("code"), "opened": (r.get("casedt") or "")[:10],
                             "parcel_id": r.get("parcel_id"), "use": r.get("pa_dordesc"),
                             "unit": (r.get("location_notes") or "").strip() or None},
                admin_notes=f"Orlando {r.get('case_type')} case {case} opened {(r.get('casedt') or '')[:10]}: "
                            f"{r.get('casename')} ({r.get('code')})",
            ))
        self.logger.info(f"Orlando: {len(rows)} open housing/abatement cases, {len(out)} kept")
        return out


def run() -> dict:
    return FlCodeEnforcementBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
