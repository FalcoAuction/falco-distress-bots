"""
U.S. Trustee Program list of Chapter 7 panel trustees (public, free):
https://www.justice.gov/ust/list-chapter-7-panel-trustees

Each entry has the trustee's name, mailing address, e-mail and phone,
grouped by state and district. Chapter 7 trustees hire auctioneers to
liquidate estates, so the trustee is the business call on a bankruptcy
lead. Parsing is pure; tests need no network.
"""

from __future__ import annotations

import html
import re
from typing import Dict, List, Optional, Tuple

URL = "https://www.justice.gov/ust/list-chapter-7-panel-trustees"
STATES = {"FLORIDA": "fl", "TENNESSEE": "tn"}
DISTRICTS = {"NORTHERN": "n", "MIDDLE": "m", "SOUTHERN": "s", "EASTERN": "e", "WESTERN": "w"}
ENTRY_RE = re.compile(r"<dd>\s*<b>(?P<name>.*?)</b>(?P<body>.*?)</dd>", re.S)
SUFFIX = {"JR", "SR", "II", "III", "IV", "ESQ"}


def _text(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


def name_key(name: Optional[str]) -> Optional[Tuple[str, str]]:
    """('WEINER', 'ROBIN') from 'Weiner, Robin R' or 'Robin R. Weiner'."""
    n = re.sub(r"[^A-Za-z,\- ]", " ", name or "").upper()
    if "," in n:
        last, _, rest = n.partition(",")
        first = [t for t in rest.split() if t not in SUFFIX]
        lt = [t for t in last.split() if t not in SUFFIX]
        return (lt[-1], first[0]) if lt and first else None
    toks = [t for t in n.split() if t not in SUFFIX]
    return (toks[-1], toks[0]) if len(toks) >= 2 else None


def parse(page: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for sec in re.split(r"<h3>", page)[1:]:
        head = _text(sec.split("</h3>", 1)[0]).upper()
        st = next((code for word, code in STATES.items() if word in head), None)
        if st is None:
            continue
        dist = next((code for word, code in DISTRICTS.items() if f"{word} DISTRICT" in head), "")
        court = f"{st}{dist}b" if dist else None
        for m in ENTRY_RE.finditer(sec):
            name = _text(m.group("name"))
            body = _text(m.group("body"))
            phone = re.search(r"Phone:\s*(\(?\d{3}\)?\s*[\d\-]{7,9})", body)
            email = re.search(r"E-mail:\s*(\S+@\S+)", body)
            out.append({"name": name, "court": court, "state": st.upper(),
                        "phone": phone.group(1).strip() if phone else None,
                        "email": email.group(1).strip() if email else None,
                        "address": re.split(r"\s*E-mail:|\s*Phone:", body)[0].strip(" ,")})
    return out


def index(entries: List[Dict[str, str]]) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    idx: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for e in entries:
        k = name_key(e["name"])
        if k:
            idx.setdefault(k, []).append(e)
    return idx


def lookup(idx: Dict[Tuple[str, str], List[Dict[str, str]]], trustee: Optional[str],
           court: Optional[str] = None) -> Optional[Dict[str, str]]:
    hits = idx.get(name_key(trustee) or ("", ""), [])
    if court:
        same = [h for h in hits if h["court"] == court]
        hits = same or hits
    return hits[0] if len(hits) == 1 or (hits and court) else None
