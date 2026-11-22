"""Read a Tennessee trustee's sale notice from its first few hundred
characters, which is all the public notice search shows (the full text sits
behind a captcha, and FALCO does not get around captchas).

Tennessee foreclosure notices open the same way across trustee firms:

  "Notice Of Substitute Trustee's Sale Whereas, Ryan James Holder, unmarried
   man by Deed of Trust ... of record in Deed Book 6240, Page(s) 23-35,
   and/or as Instrument Number 1462312, in Register's Office of Sumner County"
  "WHEREAS, on May 4, 2022, ACME LLC, a Tennessee limited liability company
   ("Grantor"), executed a Deed of Trust ..."
  "... a Deed of Trust dated May 1, 2019, executed by John Doe and Jane Doe,
   conveying certain real property ..."

Newspapers file these under whatever category they like (Sumner and
Montgomery sales arrive as "Delinquent Taxes"), so the text decides the
distress type, not the category.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional

TRUSTEE_SALE_RE = re.compile(
    r"\b(?:substitute|successor)\s+trustee'?s?\s+sale|\btrustee'?s\s+sale\b|\bdeed\s+of\s+trust\b"
    r"|\btrustee'?s?\s+notice\s+of\s+sale\b|\bsale\s+at\s+public\s+auction\b",
    re.IGNORECASE)
GENERAL_TAX_RE = re.compile(
    r"taxes\s+(?:are|is)\s+now\s+due|taxes\s+(?:are|is)\s+due\s+and\s+payable|tax\s+relief|"
    r"property\s+tax\s+freeze|reappraisal|board\s+of\s+equalization",
    re.IGNORECASE)

_NAME_END = (r"(?=,?\s+(?:an?\s+)?(?:unmarried|married|single|widow|divorced|husband|wife|"
             r"tenants?\s+by|joint\s+tenants|individually|a\s+tennessee|an?\s+\w+\s+limited|an?\s+individual)\b"
             r"|\s*\(\s*(?:the\s+)?[\"“]?(?:grantor|borrower)|,?\s+by\s+deed\s+of\s+trust|,\s*executed|,?\s+con-?\s?veying"
             r"|,\s*(?:his|her|their)\s+heirs|,\s+to\s+[A-Z])")
WHEREAS_RE = re.compile(
    r"\bwhereas,?\s+(?:on\s+[A-Za-z]+\.?\s+\d{1,2},?\s+\d{4},?\s+)?(?:default\b.*?)?([A-Za-z][A-Za-z0-9.,'&\- ]{2,120}?)" + _NAME_END,
    re.IGNORECASE | re.DOTALL)
EXECUTED_BY_RE = re.compile(
    r"\bexecuted\s+(?:on\s+)?(?:[A-Za-z]+\.?\s+\d{1,2},?\s+\d{4},?\s+)?by\s+([A-Za-z][A-Za-z0-9.,'&\- ]{2,120}?)"
    + _NAME_END,
    re.IGNORECASE | re.DOTALL)
BOOK_PAGE_RE = re.compile(r"\bBook\s+(\d{1,6})\s*,?\s*Pages?(?:\s*\(s\))?\s+(\d{1,5})", re.IGNORECASE)
INSTRUMENT_RE = re.compile(r"\bInstrument\s+(?:Number|No\.?|#)\s*:?\s*([0-9][0-9\-]{3,})", re.IGNORECASE)
FROM_GRANTOR_RE = re.compile(
    r"\bdeed of trust from ([A-Za-z][A-Za-z .&'\-]{2,110}?)\s*\(.?grantors?\b", re.I)
CONVEYED_RE = re.compile(
    r"\bCounty,\s*Tennessee,\s*([A-Za-z][A-Za-z .&'\-]{2,110}?)\s+conveyed the\b", re.I)
ENTITY_RE = re.compile(r"\b(LLC|L\.L\.C|INC|CORP|CORPORATION|COMPANY|LP|LLP|TRUST|TRUSTEE|PARTNERSHIP|HOLDINGS|PROPERTIES)\b",
                       re.IGNORECASE)


def classify(text: str) -> Optional[str]:
    """'foreclosure' for a trustee's sale, 'general_tax' for a county-wide
    tax notice that names no property, else None (keep the category)."""
    t = text or ""
    if TRUSTEE_SALE_RE.search(t):
        return "foreclosure"
    if GENERAL_TAX_RE.search(t):
        return "general_tax"
    return None


def _clean(name: str) -> str:
    n = re.sub(r"\s+", " ", name)
    n = re.sub(r"([a-z])- ([a-z])", r"\1\2", n)                        # "Calde- ron" from a line break
    n = re.split(r"\s+(?:aka|a/k/a|f/k/a|fka)\s+", n, flags=re.IGNORECASE)[0]
    n = re.split(r"\s+on\s+behalf\s+of\s+", n, flags=re.IGNORECASE)[-1]  # the principal, not the signer
    n = re.sub(r"\s+(?:and|&)\s*$", "", n.strip(" ,.;"), flags=re.IGNORECASE)
    n = re.sub(r"^(?:the\s+)?(?:estate\s+of\s+)", "", n, flags=re.IGNORECASE)
    return n.strip(" ,.;")


_NOT_A_NAME = re.compile(
    r"\b(?:occurred|covenants|conditions|performance|default|deed|trust|dated|payment|obligations|"
    r"secured|recorded|record|register|notice|sale|whereas|instrument)\b", re.IGNORECASE)


def _plausible(name: str) -> bool:
    """A captured grantor that reads like a person or a company, not a
    stretch of the notice's legal wording."""
    if not name or len(name) < 4 or _NOT_A_NAME.search(name):
        return False
    if re.search(r"\d", name) and not ENTITY_RE.search(name):
        return False
    return len(name.split()) <= 14


def plausible_name(name: str) -> bool:
    return _plausible(_clean(name or ""))


def borrowers(text: str) -> List[str]:
    """Grantor names on the deed of trust, split on 'and' / '&'. Entities
    are kept whole."""
    t = re.sub(r"\s+", " ", text or "")
    raw = ""
    for rx in (WHEREAS_RE, EXECUTED_BY_RE, FROM_GRANTOR_RE, CONVEYED_RE):
        m = rx.search(t)
        if m and _plausible(_clean(m.group(1))):
            raw = _clean(m.group(1))
            break
    if not raw:
        return []
    if ENTITY_RE.search(raw):
        return [raw]
    parts = [_clean(p) for p in re.split(r"\s+and\s+|\s*&\s*", raw, flags=re.IGNORECASE)]
    return [p for p in parts if len(p.split()) >= 2]


def deed_ref(text: str) -> Dict[str, Optional[str]]:
    t = text or ""
    bp = BOOK_PAGE_RE.search(t)
    ins = INSTRUMENT_RE.search(t)
    return {"book": bp.group(1) if bp else None, "page": bp.group(2) if bp else None,
            "instrument": ins.group(1) if ins else None}


SALE_DATE_RE = re.compile(
    r"\b(?:sale\s+at\s+public\s+auction\s+will\s+be\s+(?:held(?:\s+on)?|on)|"
    r"will\s+(?:sell|offer\s+for\s+sale)\s+at\s+public\s+(?:auction|outcry)\s+on|"
    r"(?:foreclosure|trustee['’]s)\s+sale\s+(?:will\s+be\s+held\s+on|date\s*:))"
    r"\s+(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+)?"
    r"([A-Za-z]+\.?\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})\b",
    re.IGNORECASE)


def sale_date(text: str) -> Optional[str]:
    """Return a calendar-valid date only when explicit sale dates agree."""
    if re.search(r"\b(?:sale (?:is|has been|was) (?:hereby )?(?:cancelled|canceled|postponed|continued|withdrawn)"
                 r"|notice of (?:cancellation|postponement) of (?:the )?(?:trustee['’]s )?sale)\b", text or "", re.I):
        return None
    dates = set()
    for match in SALE_DATE_RE.finditer(text or ""):
        value = " ".join(match[1].replace(".", "").replace(",", "").split())
        for fmt in ("%B %d %Y", "%b %d %Y", "%m/%d/%Y"):
            try:
                dates.add(datetime.strptime(value, fmt).date().isoformat())
                break
            except ValueError:
                continue
        else:
            return None
    return dates.pop() if len(dates) == 1 else None


def is_entity(name: str) -> bool:
    return bool(ENTITY_RE.search(name or ""))


def property_address_text(excerpt: str) -> str:
    """Isolate the subject property when a notice also describes the auction venue."""
    label = re.search(r"\b(?:property address|street address of (?:the )?property|"
                      r"property (?:is )?(?:commonly known as|located at))\s*[:\-]?\s*(?=\d)", excerpt, re.I)
    if label:
        return excerpt[label.end():].upper()
    if re.search(r"\b(?:courthouse|court house|justice center|auction location|place of sale)\b", excerpt, re.I):
        return ""
    return excerpt
