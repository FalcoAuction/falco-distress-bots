"""Closed United Methodist churches in Tennessee, from the Tennessee-Western
Kentucky Conference's published closure resolutions.

Each resolution names the church, its street address and county, and vests
the property in the Conference Trustees with authority to sell it. One
seller (the Trustees) for every building, which makes this a relationship
lane: the leads are the buildings, the contact is the Conference.

Source PDFs are listed in CLOSURE_PDFS (the conference posts one set a year
before its June session; add the new URL when it is published). When the
conference reorganized its site in 2026 it removed the 2023 and 2025 files
(its server answers 403 for missing uploads); those come from the Internet
Archive's public copies instead. Counties the resolutions leave out are
filled from the city (Comptroller index when present, else CITY_COUNTY).

Distress type: CHURCH_CLOSURE. Tennessee addresses only.

    python -m src.bots.church_closure_bot
"""

from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload

CLOSURE_PDFS = [
    # 2026: the live file refuses automated requests; saved in _data/church_closures_2026.json.
    "https://twkumc.org/wp-content/uploads/2026/06/2026-Charge-Line-Changes-and-Closures-1.pdf",
    # Removed from twkumc.org in 2026; Internet Archive copies of the originals.
    "https://web.archive.org/web/20250704074646id_/https://twkumc.org/wp-content/uploads/2025/05/"
    "TWK-Resolutions-for-Church-Closure-2025-revised-5.28.2025.pdf",
    "https://web.archive.org/web/20250121212301id_/https://twkumc.org/wp-content/uploads/2023/06/"
    "TWK-Resolutions-for-Church-Closure-2023-revised-6.9.2023-.docx.pdf",
]
SELLER = "Tennessee-Western Kentucky Conference Trustees"
# Years whose PDF the conference's server will not serve to this bot: the
# closures parsed from the public file, saved by hand (src/bots/_data).
DATA_DIR = Path(__file__).resolve().parent / "_data"
# Cities the resolutions name without a county (Middle TN and nearby).
CITY_COUNTY = {
    "nashville": "davidson", "madison": "davidson", "antioch": "davidson", "hermitage": "davidson",
    "murfreesboro": "rutherford", "smyrna": "rutherford", "franklin": "williamson", "brentwood": "williamson",
    "clarksville": "montgomery", "cookeville": "putnam", "lebanon": "wilson", "mt juliet": "wilson",
    "gordonsville": "smith", "elmwood": "smith", "woodbury": "cannon", "pelham": "grundy",
    "estill springs": "franklin", "winchester": "franklin", "spring hill": "maury", "columbia": "maury",
    "byrdstown": "pickett", "mason": "tipton", "millington": "shelby", "mckenzie": "carroll",
}

_RESOLUTION_RE = re.compile(
    r"known\s+as\s+(?P<church>[^,]{3,90}?(?:Church|Chapel|Charge)),\s+located\s+at,?\s+(?P<loc>.{4,160}?),?\s+"
    r"in\s+the\s+(?P<district>[A-Za-z .]{3,40}?District)", re.I | re.S)
_COUNTY_RE = re.compile(r"(?:in\s+)?([A-Za-z .]{3,25}?)\s+County\b", re.I)
_CLOSE_DATE_RE = re.compile(r"final\s+service\s+of\s+worship\s+was(?:/will\s+be)?\s+held\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.I)


def closures(text: str) -> List[Dict[str, Any]]:
    """Every church closure in a resolutions document, Tennessee and Kentucky."""
    t = re.sub(r"\s+", " ", text or "")
    out: List[Dict[str, Any]] = []
    starts = [m.start() for m in re.finditer(r"Resolution to (?:Close|Discontinue)", t, re.I)] + [len(t)]
    for a, b in zip(starts, starts[1:]):
        block = t[a:b]
        m = _RESOLUTION_RE.search(block)
        if not m:
            continue
        loc = m.group("loc")
        state = "KY" if re.search(r"\bKentucky\b", loc, re.I) else "TN"
        cm = _COUNTY_RE.search(loc)
        county = re.sub(r"^in\s+", "", cm.group(1).strip().split(",")[-1].strip().lower()) if cm else None
        addr = re.split(r",?\s*(?:in\s+)?[A-Za-z .]{3,25}?\s+County\b", loc, flags=re.I)[0]
        addr = re.sub(r",?\s*(?:Tennessee|Kentucky)\b,?", ",", addr, flags=re.I)
        addr = re.sub(r"\s*,\s*,", ",", re.sub(r"\s+", " ", addr)).strip(" ,.")
        d = _CLOSE_DATE_RE.search(block)
        out.append({
            "church": m.group("church").strip(),
            "address": addr,
            "county": county,
            "state": state,
            "district": m.group("district").strip(),
            "final_service": d.group(1) if d else None,
            "action": "discontinue" if "discontinue" in block[:40].lower() else "close",
        })
    seen, uniq = set(), []
    for c in out:
        k = (c["church"].lower(), c["address"].lower())
        if k not in seen:
            seen.add(k)
            uniq.append(c)
    return uniq


def _city_index() -> Dict[str, str]:
    """Postal city -> county from the Comptroller index when it is built."""
    try:
        from .land_lane_bot import DB_PATH
        from ._comptroller import ParcelIndex
        if DB_PATH.exists():
            return {k.lower(): v.lower() for k, v in ParcelIndex(str(DB_PATH)).city_county.items() if k}
    except Exception:
        pass
    return {}


def county_for(address: str, index: Dict[str, str]) -> Optional[str]:
    """County from the city in an address like '625 East 20th Street, Cookeville'."""
    for part in reversed([p.strip().lower() for p in (address or "").split(",") if p.strip()]):
        city = re.sub(r"\b(?:tn|tenn?e+ssee)\b\.?|\d{5}", "", part).strip(" .")
        if city in CITY_COUNTY:
            return CITY_COUNTY[city]
        if city in index:
            return index[city]
    return None


class ChurchClosureBot(BotBase):
    name = "church_closure"
    description = "Closed United Methodist churches (TN), property vested in the Conference Trustees"
    throttle_seconds = 1.0
    expected_min_yield = 0

    def scrape(self) -> List[LeadPayload]:
        import pdfplumber
        city_index = _city_index()
        batches: List[tuple] = []
        for path in sorted(DATA_DIR.glob("church_closures_*.json")):
            doc = json.loads(path.read_text(encoding="utf-8"))
            batches.append((doc.get("source") or path.name, doc.get("closures") or []))
        for url in CLOSURE_PDFS:
            res = self.fetch(url, timeout=120)
            if res is None or res.status_code != 200 or not res.content.startswith(b"%PDF"):
                self.logger.warning(f"skipped {url} ({getattr(res, 'status_code', 'no response')})")
                continue
            with pdfplumber.open(io.BytesIO(res.content)) as pdf:
                pages = [(p.extract_text() or "", p.extract_text(x_tolerance=1) or "") for p in pdf.pages]
            # Some exports run words together at the default spacing; keep
            # whichever reading finds more closures.
            readings = [closures("\n".join(pg[i] for pg in pages)) for i in (0, 1)]
            batches.append((url, max(readings, key=len)))
        leads: Dict[str, LeadPayload] = {}
        for source, found in batches:
            self.logger.info(f"{source.rsplit('/', 1)[-1]}: {len(found)} closures")
            for c in found:
                if c.get("state") != "TN":
                    continue
                c["county"] = c.get("county") or county_for(c["address"], city_index)
                key = self.make_lead_key(self.name, f"{c['church']}|{c['address']}")
                leads.setdefault(key, LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=key,
                    property_address=f"{c['address']}, TN",
                    county=c["county"],
                    full_name=SELLER,
                    owner_name_records=SELLER,
                    distress_type="CHURCH_CLOSURE",
                    admin_notes=f"{c['church']} ({c['district']}) closed; property vested in the Conference "
                                f"Trustees, who may sell it. Final service {c.get('final_service') or 'n/a'}.",
                    source_url=source if source.startswith("http") else None,
                    raw_payload={"source": "twk_umc_closures", "state": "TN", "property_lane": "commercial",
                                 "church_closure": c, "seller": SELLER},
                ))
        return list(leads.values())


def run() -> dict:
    return ChurchClosureBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
