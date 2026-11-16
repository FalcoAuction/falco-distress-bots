"""
Match leads to ATF federal firearms licensees (FFL). Pure; no network, no DB.

Source: ATF's monthly state listing, e.g.
  https://www.atf.gov/sites/default/files/ffl/1026-ffl-list-tennessee.csv
(MMYY prefix). ATF's CDN refuses scripted downloads, so the files are saved
by hand into a folder and this reads every CSV in it. Keeping old months
matters: a licensee who dies drops off the next listing, so a decedent is
usually only found in a list from before their death.

Columns: LIC_REGN, LIC_DIST, LIC_CNTY, LIC_TYPE, LIC_XPRDTE, LIC_SEQN,
LICENSE_NAME ("LAST, FIRST M" for individuals), BUSINESS_NAME,
PREMISE_STREET, PREMISE_CITY, PREMISE_STATE, PREMISE_ZIP_CODE, MAIL_*, VOICE_PHONE.
The public state file has no type 03 (curio and relic collectors).

What a match means:
  decedent / owner (person)  licensed in their own name at their own address:
                             a home-based dealer or maker with inventory
  business (entity)          the debtor or closing business is a gun dealer
"""

from __future__ import annotations

import csv
import io
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._aircraft_match import entity_key, parse_addr, person_keys, street_key, units_of

TYPE_LABEL = {
    "01": "dealer/gunsmith", "02": "pawnbroker", "03": "curio and relic collector",
    "06": "ammunition maker", "07": "firearms manufacturer", "08": "importer",
    "09": "destructive device dealer", "10": "destructive device maker", "11": "destructive device importer",
}
# A business name on more licenses than this is a chain: its stores are not
# one distressed dealer's inventory.
MAX_LICENSES_PER_BUSINESS = 5


def _row_dicts(text_lines: Iterable[str]) -> Iterable[Dict[str, str]]:
    rd = csv.DictReader(text_lines)
    for r in rd:
        yield {(k or "").strip().upper(): (v or "").strip() for k, v in r.items()}


def load_rows(paths: Iterable[str]) -> List[Dict[str, str]]:
    """Every row of every CSV, tagged with the file it came from (the month)."""
    out = []
    for p in paths:
        with open(p, encoding="utf-8-sig", errors="ignore", newline="") as f:
            for r in _row_dicts(f):
                if r.get("LICENSE_NAME"):
                    r["_SOURCE"] = os.path.basename(p)
                    out.append(r)
    return out


def rows_from_text(text: str, source: str = "inline") -> List[Dict[str, str]]:
    out = []
    for r in _row_dicts(io.StringIO(text)):
        if r.get("LICENSE_NAME"):
            r["_SOURCE"] = source
            out.append(r)
    return out


def list_files(folder: str) -> List[str]:
    if not folder or not os.path.isdir(folder):
        return []
    return sorted(os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".csv"))


def _person_key(license_name: str) -> Optional[Tuple[str, str, str]]:
    keys = person_keys(license_name)   # 'LAST, FIRST M' -> (LAST, FIRST, M)
    return keys[0] if keys else None


class FflIndex:
    def __init__(self, rows: List[Dict[str, str]]):
        self.by_person: Dict[Tuple[str, str], List[Tuple[Dict[str, str], str]]] = defaultdict(list)
        self.by_entity: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        self.by_addr: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        self.count = 0
        seen = set()
        for r in rows:
            ident = (r.get("LIC_REGN"), r.get("LIC_DIST"), r.get("LIC_CNTY"), r.get("LIC_TYPE"), r.get("LIC_SEQN"))
            if ident in seen:
                continue   # same license in several months: keep the first (oldest file sorts first)
            seen.add(ident)
            self.count += 1
            lic, biz = r.get("LICENSE_NAME", ""), r.get("BUSINESS_NAME", "")
            for nm in (lic, biz):
                ek = entity_key(nm)
                if ek:
                    self.by_entity[ek].append(r)
            pk = _person_key(lic)
            if pk:
                self.by_person[pk[:2]].append((r, pk[2]))
            k = street_key(r.get("PREMISE_STREET"), (r.get("PREMISE_ZIP_CODE") or "")[:5] or None)
            if k:
                self.by_addr[k].append(r)

    def match(self, lead: Dict[str, Any]) -> List[Dict[str, Any]]:
        """lead: distress_type, full_name, owner_name_records, property_address, and flattened raw
        fields subject, owner_name_1, case_name, decedent (court_notice), warn_company."""
        if (lead.get("distress_type") or "").upper() == "VESSEL_ARREST":
            return []   # names are the plaintiff, not an owner
        hits: Dict[Tuple, Tuple[Dict[str, str], str, str]] = {}
        names = [lead.get(k) for k in ("decedent", "full_name", "owner_name_records", "subject",
                                       "owner_name_1", "case_name", "warn_company")]
        names = [n for n in names if n]
        addr = lead.get("property_address")
        st, _, zip5 = parse_addr(addr)
        akey = street_key(st, zip5)
        lead_units = units_of(st)

        def key(r):
            return (r.get("LIC_REGN"), r.get("LIC_DIST"), r.get("LIC_CNTY"), r.get("LIC_TYPE"), r.get("LIC_SEQN"))

        for nm in names:
            ek = entity_key(nm)
            if ek:
                held = self.by_entity.get(ek, [])
                if len(held) > MAX_LICENSES_PER_BUSINESS:
                    continue   # a chain (e.g. FirstCash pawn shops), not a local dealer to liquidate
                for r in held:
                    hits.setdefault(key(r), (r, "business_name", "high"))
                continue
            for last, first, mi in person_keys(nm):
                for r, fmi in self.by_person.get((last, first), []):
                    if mi and fmi and mi != fmi:
                        continue
                    premise = street_key(r.get("PREMISE_STREET"), (r.get("PREMISE_ZIP_CODE") or "")[:5])
                    if akey and premise == akey and units_of(r.get("PREMISE_STREET")) == lead_units:
                        hits[key(r)] = (r, "name_and_address", "high")
                    elif mi and fmi and zip5 and (r.get("PREMISE_ZIP_CODE") or "")[:5] == zip5:
                        hits.setdefault(key(r), (r, "name_middle_zip", "medium"))
        return [self._tag(r, how, conf) for r, how, conf in hits.values()]

    @staticmethod
    def _tag(r: Dict[str, str], how: str, conf: str) -> Dict[str, Any]:
        t = r.get("LIC_TYPE", "")
        return {
            "license_type": t,
            "license_label": TYPE_LABEL.get(t, "licensee"),
            "license_name": r.get("LICENSE_NAME"),
            "business_name": r.get("BUSINESS_NAME") or None,
            "premise": re.sub(r"\s+", " ", f'{r.get("PREMISE_STREET", "")}, {r.get("PREMISE_CITY", "")} '
                                            f'{r.get("PREMISE_STATE", "")} {r.get("PREMISE_ZIP_CODE", "")[:5]}').strip(),
            "listing": r.get("_SOURCE"),
            "match_type": how,
            "confidence": conf,
        }
