"""Davidson County tax-sale lists from the Chancery Court Clerk and Master,
as a parcel -> (sale date, amount due) map for stacking onto other leads.

Each monthly sale's list is posted as a PDF and reissued (_1 ... _7) as
owners pay; the last revision is what was still unpaid going into the
sale. After a sale the owner keeps a one-year redemption right
(TCA 67-5-2701), so a parcel sold within the last ~15 months is still a
motivated owner. robots.txt allows everything but /wp-admin/.

    parcels = load_tax_sale_parcels(cache_dir)   # {'12809000400': {...}, ...}

Map/parcel numbers use the same 11-character form as Metro's parcel layer
(STANPAR), e.g. 12809000400 or 043100B10200CO for condos.
"""
from __future__ import annotations

import io
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

BASE = "https://chanceryclerkandmaster.nashville.gov"
MEDIA = f"{BASE}/wp-json/wp/v2/media"
UA = {"User-Agent": "FALCO-Lead-Research/1.0 (+ops@falco.llc)"}
LOOKBACK_DAYS = 450                       # one-year redemption plus a quarter of slack

_REV_RE = re.compile(r"^(?P<sale>.+?Tax[-_ ]?Sale\d{4})_(?P<rev>\d+)(?:-\d+)?\.pdf$", re.I)
_PARCEL_RE = re.compile(r"\b(\d{5}[0-9A-Z]{6,7}(?:CO)?)\b")   # 11 digits, or e.g. 043100B10200CO
_AMOUNT_RE = re.compile(r"\$([\d,]+\.\d{2})")
_SALE_DATE_RE = re.compile(r"\bON\s+[A-Z]+,\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.I)


def list_media(session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    s = session or requests.Session()
    out: List[Dict[str, Any]] = []
    for term in ("Tax-Sale", "Tax_Sale"):
        page = 1
        while True:
            r = s.get(MEDIA, params={"search": term, "per_page": 100, "page": page}, headers=UA, timeout=60)
            if r.status_code != 200:
                break
            batch = r.json() or []
            out.extend(m for m in batch if m.get("mime_type") == "application/pdf")
            if len(batch) < 100:
                break
            page += 1
    seen, uniq = set(), []
    for m in out:
        if m.get("source_url") not in seen:
            seen.add(m.get("source_url"))
            uniq.append(m)
    return uniq


def sale_revisions(media: List[Dict[str, Any]], today: date,
                   lookback_days: int = LOOKBACK_DAYS) -> Dict[str, List[Tuple[Tuple[int, str], str]]]:
    """{sale key: [((revision, upload date), url), ...] oldest first} for sales
    uploaded within the lookback."""
    sales: Dict[str, List[Tuple[Tuple[int, str], str]]] = {}
    cutoff = (today - timedelta(days=lookback_days)).isoformat()
    for m in media:
        url = m.get("source_url") or ""
        if (m.get("date") or "")[:10] < cutoff:
            continue
        mt = _REV_RE.match(url.rsplit("/", 1)[-1])
        if not mt:
            continue
        # One sale is posted under several spellings ("January16th_Tax-Sale2023",
        # "January_Tax-Sale2023"; "August_7th" for "August_17th"): key on month + tax year.
        key = re.sub(r"\d+(?:st|nd|rd|th)|[^a-z0-9]", "", mt.group("sale").lower())
        sales.setdefault(key, []).append(((int(mt.group("rev")), m.get("date") or ""), url))
    return {k: sorted(v) for k, v in sales.items()}


def final_revisions(media: List[Dict[str, Any]], today: date, lookback_days: int = LOOKBACK_DAYS) -> List[str]:
    """Last revision URL of each sale uploaded within the lookback."""
    return sorted(v[-1][1] for v in sale_revisions(media, today, lookback_days).values())


def parse_list(text: str) -> Tuple[Optional[str], Dict[str, Optional[float]]]:
    """(sale date ISO, {parcel: amount due}). The amount sits on the line just
    before each parcel row in the extracted text, so take the nearest
    preceding dollar figure."""
    sale = None
    m = _SALE_DATE_RE.search(text)
    if m:
        try:
            sale = datetime.strptime(re.sub(r"\s+", " ", m.group(1)), "%B %d, %Y").date().isoformat()
        except ValueError:
            sale = None
    parcels: Dict[str, Optional[float]] = {}
    for pm in _PARCEL_RE.finditer(text):
        tok = pm.group(1)
        if re.fullmatch(r"\d{8}", tok) or (tok.isdigit() and len(tok) != 11):
            continue                      # instrument / deed numbers, not parcels
        window = text[max(0, pm.start() - 160):pm.start()]
        amts = _AMOUNT_RE.findall(window)
        parcels[tok] = float(amts[-1].replace(",", "")) if amts else None
    return sale, parcels


def load_tax_sale_parcels(cache_dir: str, today: Optional[date] = None,
                          session: Optional[requests.Session] = None) -> Dict[str, Dict[str, Any]]:
    import pdfplumber
    s = session or requests.Session()
    today = today or date.today()
    os.makedirs(cache_dir, exist_ok=True)

    def parsed(url: str) -> Optional[Tuple[Optional[str], Dict[str, Optional[float]]]]:
        name = url.rsplit("/", 1)[-1]
        path = os.path.join(cache_dir, name)
        if not os.path.exists(path):
            r = s.get(url, headers=UA, timeout=120)
            if r.status_code != 200 or not r.content.startswith(b"%PDF"):
                return None
            with open(path, "wb") as f:
                f.write(r.content)
        try:
            with pdfplumber.open(path) as pdf:
                return parse_list("\n".join((p.extract_text() or "") for p in pdf.pages))
        except Exception:
            return None

    out: Dict[str, Dict[str, Any]] = {}
    for key, revs in sale_revisions(list_media(s), today).items():
        sale_date: Optional[str] = None
        seen: Dict[str, Tuple[Optional[float], str]] = {}
        final: Dict[str, Optional[float]] = {}
        for i, (_, url) in enumerate(revs):
            res = parsed(url)
            if not res:
                continue
            sd, parcels = res
            sale_date = sale_date or sd
            for p, amt in parcels.items():
                seen[p] = (amt if amt is not None else (seen.get(p) or (None, ""))[0], url.rsplit("/", 1)[-1])
            if i == len(revs) - 1:
                final = parcels
        for p, (amt, name) in seen.items():
            status = "unpaid_at_sale" if p in final else "paid_before_sale"
            rec = {"sale_date": sale_date, "amount_due": final.get(p, amt), "status": status,
                   "list": name if status == "paid_before_sale" else revs[-1][1].rsplit("/", 1)[-1]}
            prev = out.get(p)
            if prev is None or (sale_date or "") > (prev.get("sale_date") or "") \
                    or (rec["status"] == "unpaid_at_sale" and prev["status"] != "unpaid_at_sale"):
                out[p] = rec
    return out
