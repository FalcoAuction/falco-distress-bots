"""
Florida business distress from Sunbiz daily files, joined to the DOR roll.
Free: the state's public SFTP plus the DOR parcel/equipment roll.

  BUSINESS_CLOSURE  voluntary or administrative dissolution, or withdrawal
                    of a foreign entity, filed today, by a business the DOR
                    NAP roll shows holding $100K+ of equipment in a FALCO
                    Florida county. Lane: business_liquidation.
  TAX_LIEN          federal tax lien filed with the Department of State:
                    - business debtors with $100K+ of equipment (seizure
                      lane: the IRS can levy it), or
                    - any debtor whose address is a parcel worth
                      PARCEL_MIN+ in a FALCO county (owner-direct real
                      estate lead).

Everything that does not join to the roll is dropped in memory: a
dissolution or lien alone says nothing about assets. Daily files only
cover what was filed that day, so this runs every pipeline pass; lead
keys are document numbers, so re-reads are duplicates.

Distress types: BUSINESS_CLOSURE, TAX_LIEN
"""

from __future__ import annotations

import os
import re
import sys
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._aircraft_match import clean, entity_key
from ._base import BotBase, LeadPayload
from ._fl_roll import DATA_DIR, open_roll, parcel_tag, tpp_tag
from ._personal_property import FL_EQUIPMENT_MIN
from ._sunbiz import (CORP_COLS, QUARTERLY_COR, corp_row, debtor_person_name, list_dir, parse_cor_line,
                      parse_debtors, parse_events, parse_liens, public_login, remote_mtime, sftp)

PARCEL_MIN = 150_000
EVENT_DIR = "cor/events"
FLR_DIRS = ("FLR/DEBTORS", "FLR/FILINGS")
MAX_FILES = 3   # newest daily files per folder


class FlSunbizBot(BotBase):
    name = "fl_sunbiz"
    description = "FL business dissolutions and federal tax liens (Sunbiz daily files) joined to the DOR roll"
    throttle_seconds = 0.0
    expected_min_yield = 0
    frozen_after_runs = 10_000

    def scrape(self) -> List[LeadPayload]:
        login = public_login(self.session)
        if not login:
            self.logger.warning("Sunbiz public login not found on the data-downloads page")
            return []
        roll = open_roll(self.session, self.logger, datetime.now(timezone.utc).year, refresh=False)
        if roll is None:
            self.logger.warning("FL roll cache empty (fl_roll_refresh has not run); Sunbiz join skipped")
            return []
        roll.db.execute(f"CREATE TABLE IF NOT EXISTS corp ({', '.join(CORP_COLS)}, PRIMARY KEY (doc_number))")
        leads: Dict[str, LeadPayload] = {}
        for lead in self._closures(login, roll) + self._liens(login, roll):
            leads.setdefault(lead.pipeline_lead_key, lead)
        self.logger.info(f"staging {len(leads)} lead(s)")
        return list(leads.values())

    def _corp_index(self, login: str, roll) -> None:
        """Doc number -> name / officer, kept only for entities the NAP roll
        shows holding $100K+ of equipment in a FALCO county (a few thousand
        rows out of ~4M). Rebuilt when the state posts a new quarterly file."""
        db = roll.db
        db.execute(f"CREATE TABLE IF NOT EXISTS corp ({', '.join(CORP_COLS)}, PRIMARY KEY (doc_number))")
        mtime = remote_mtime(login, QUARTERLY_COR)
        seen = db.execute("SELECT modified FROM source WHERE kind = 'COR' AND co_no = 0").fetchone()
        have = db.execute("SELECT count(*) FROM corp").fetchone()[0]
        if mtime is None or (seen and seen[0] == mtime and have):
            return
        wanted = set()
        for (name,) in db.execute("SELECT own_name FROM tpp WHERE jv_total >= ?", (FL_EQUIPMENT_MIN,)):
            wanted.add(entity_key(name) or clean(name))
        path = os.path.join(DATA_DIR, "cordata.zip")
        self.logger.info(f"Sunbiz quarterly corporate file changed ({mtime}); indexing {len(wanted):,} equipment owners")
        if sftp(login, QUARTERLY_COR, timeout=3600, out_file=path) is None or not os.path.exists(path):
            self.logger.warning("Sunbiz quarterly download failed; closures need the index")
            return
        rows = []
        try:
            with zipfile.ZipFile(path) as zf:
                for member in zf.namelist():
                    with zf.open(member) as fh:
                        for bline in fh:
                            line = bline.decode("latin-1")
                            name = line[12:204].strip()
                            if (entity_key(name) or clean(name)) in wanted:
                                rows.append(corp_row(parse_cor_line(line)))
        finally:
            os.remove(path)
        db.execute("DELETE FROM corp")
        db.executemany(f"INSERT OR REPLACE INTO corp VALUES ({','.join('?' * len(CORP_COLS))})", rows)
        db.execute("INSERT OR REPLACE INTO source VALUES ('COR', 0, 'cordata.zip', ?)", (mtime,))
        db.commit()
        self.logger.info(f"Sunbiz index: {len(rows):,} equipment-holding entities")

    def _newest(self, login: str, folder: str, suffix: str) -> List[str]:
        names = [n for n in list_dir(login, folder) if re.fullmatch(rf"\d{{8}}{suffix}\.txt", n)]
        return names[-MAX_FILES:]

    def _closures(self, login: str, roll) -> List[LeadPayload]:
        out = []
        for fname in self._newest(login, EVENT_DIR, "ce"):
            raw = sftp(login, f"{EVENT_DIR}/{fname}")
            if raw is None:
                continue
            events = parse_events(raw.decode("latin-1"))
            kept = 0
            for ev in events:
                corp = roll.db.execute("SELECT * FROM corp WHERE doc_number = ?", (ev["doc_number"],)).fetchone()
                if corp is None:
                    continue   # not an equipment holder in a FALCO county (index keeps only those)
                ev["name"] = ev["name"] or corp["name"]
                ev["officer"], ev["officer_address"] = corp["officer"], corp["officer_address"]
                if not ev["address1"]:
                    ev["address1"], ev["city"], ev["state"], ev["zip"] = corp["address"], "", "", ""
                equipment = [tpp_tag(t) for t in roll.equipment_of(ev["name"])]
                total = sum(t["equipment_value"] or 0 for t in equipment)
                if total < FL_EQUIPMENT_MIN:
                    continue
                top = max(equipment, key=lambda t: t["equipment_value"] or 0)
                kept += 1
                out.append(LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=self.make_lead_key(self.name, f"ev|{ev['doc_number']}|{ev['code']}"),
                    distress_type="BUSINESS_CLOSURE", full_name=ev["name"], owner_name_records=ev["name"],
                    county=top["county"].title(), property_address=top["situs"] + ", FL" if top["situs"] else None,
                    source_url="https://search.sunbiz.org/Inquiry/CorporationSearch/ByDocumentNumber",
                    admin_notes=f"{ev['description'].title()} filed {ev['filed']}: {ev['name']} "
                                f"(doc {ev['doc_number']}); ${total:,.0f} business equipment on the FL roll",
                    raw_payload={"source": "sunbiz_events", "state": "FL", "county": top["county"],
                                 "doc_number": ev["doc_number"], "filed": ev["filed"],
                                 "sunbiz_event": {"code": ev["code"], "description": ev["description"],
                                                  "filed": ev["filed"], "effective": ev["effective"]},
                                 "principal_address": ", ".join(x for x in (ev["address1"], ev["city"],
                                                                            ev["state"], ev["zip"]) if x),
                                 "officer": ev.get("officer"), "officer_address": ev.get("officer_address"),
                                 "fl_tpp": equipment[:3]},
                ))
            self.logger.info(f"{fname}: {len(events)} closures, {kept} with $100K+ equipment")
        return out

    def _liens(self, login: str, roll) -> List[LeadPayload]:
        debtor_files = self._newest(login, "FLR/DEBTORS", "flrd")
        out = []
        for dname in debtor_files:
            day = dname[:8]
            debt = sftp(login, f"FLR/DEBTORS/{dname}")
            fil = sftp(login, f"FLR/FILINGS/{day}flrf.txt")
            if debt is None:
                continue
            liens = parse_liens(fil.decode("latin-1")) if fil else {}
            debtors = parse_debtors(debt.decode("latin-1"))
            kept = 0
            for d in debtors:
                lead = self._lien_lead(d, liens.get(d["doc_number"], {}), roll)
                if lead:
                    out.append(lead)
                    kept += 1
            self.logger.info(f"{dname}: {len(debtors)} debtors, {kept} joined to the roll")
        return out

    def _lien_lead(self, d: Dict[str, Any], lien: Dict[str, Any], roll) -> Optional[LeadPayload]:
        addr = ", ".join(x for x in (d["address1"], d["city"], f"{d['state']} {d['zip']}".strip()) if x)
        name = d["name"] if d["entity"] else debtor_person_name(d["name"])
        equipment = [tpp_tag(t) for t in roll.equipment_of(name)] if d["entity"] else []
        total = sum(t["equipment_value"] or 0 for t in equipment)
        parcel = roll.parcel_at(None, addr) if d["state"] == "FL" else None
        ptag = parcel_tag(parcel) if parcel else None
        if ptag and (ptag["just_value"] or 0) < PARCEL_MIN:
            ptag = None
        if total < FL_EQUIPMENT_MIN and ptag is None:
            return None
        county = (ptag or (equipment[0] if equipment else {})).get("county")
        filed = lien.get("filed")
        raw = {"source": "sunbiz_flr", "state": "FL", "county": county, "doc_number": d["doc_number"],
               "filed": filed, "debtor": d["name"], "debtor_address": addr, "entity": d["entity"]}
        if equipment:
            raw["fl_tpp"] = equipment[:3]
        if ptag:
            raw["fl_parcel"] = {**ptag, "match": "lien_debtor_address"}
        return LeadPayload(
            bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, f"flr|{d['doc_number']}|{d['name']}"),
            distress_type="TAX_LIEN", full_name=d["name"], owner_name_records=(ptag or {}).get("owner") or d["name"],
            county=county.title() if county else None,
            property_address=(ptag["situs"] + ", FL") if ptag else addr,
            property_value=ptag["just_value"] if ptag else None,
            source_url="https://search.sunbiz.org/Inquiry/FederalTaxLienSearch",
            admin_notes=(f"Federal tax lien {d['doc_number']} filed {filed or '?'} against {d['name']}"
                         + (f"; ${total:,.0f} business equipment" if total >= FL_EQUIPMENT_MIN else "")
                         + (f"; owns {ptag['situs']} (${ptag['just_value']:,.0f}"
                            + (", homestead" if ptag["homestead"] else "") + ")" if ptag else "")),
            raw_payload=raw,
        )


def run() -> dict:
    return FlSunbizBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
