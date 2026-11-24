"""Free official lookups that can name a person for a company owner.

Pure functions (no network, no database). cre_review_bot makes the request
and stores the result on the row under raw_payload.signer_lookup, so each
company is looked up once and rechecked rarely. _entity_signer reads the
stored result and decides the tier; nothing here can verify authority.

Hamilton County Clerk business license search
    http://tncountyclerk.com/hamilton/public/  (linked from the County
    Clerk's own business page as "Online Business Search by Name or
    Address"). Free, no login, no CAPTCHA, no terms gate. robots.txt
    carries no Disallow rule (read 2026-11-23); the bot reads it again on
    every run and skips the lookup if that changes. One request per
    company, REQUEST_GAP seconds apart, at most MAX_PER_RUN a run, sent
    with the shared agent in _base.py (FALCO-research/1.0 (+falco.llc)).
    A record prints the business name and address, start and close dates,
    status, and for many accounts an owner name and address.

Sources checked and NOT used are listed in the pull request that added this
module (other states' registries sit behind bot challenges; the Tennessee
Secretary of State search is off limits).
"""
from __future__ import annotations

import re
from datetime import date
from html import unescape
from typing import Any, Dict, List, Optional

from ._entity_signer import base_key, clean_owner_name, entity_key

CLERK_KEY = "hamilton_county_clerk"
CLERK_URL = "http://tncountyclerk.com/hamilton/public/index.php"
CLERK_ROBOTS = "http://tncountyclerk.com/robots.txt"
CLERK_LABEL = "Hamilton County Clerk business license search"
REQUEST_GAP = 6.0          # seconds between requests
MAX_PER_RUN = 15           # new lookups per run
RECHECK_DAYS = 180         # a stored answer is reused this long
MAX_RECORDS = 10
_FIELDS = ("account", "product", "baddress", "bcity", "bstate", "bzip", "bphone", "sdate", "edate", "owner",
           "oaddress", "ocity", "ostate", "ozip", "ophone")
_SUFFIX = re.compile(r"[\s,]+(?:L\.?L\.?C\.?|INC\.?|INCORPORATED|CORP\.?|CORPORATION|COMPANY|CO\.?|L\.?P\.?|LTD\.?|GP)\s*$", re.I)


def robots_allows(text: Any, path: str = "/hamilton/public/index.php") -> bool:
    """True when robots.txt has no rule for all agents that covers the
    path. Any text that cannot be read as robots rules counts as a no."""
    if not isinstance(text, str):
        return False
    group_all, in_group = False, False
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        field, value = (x.strip() for x in line.split(":", 1))
        field = field.lower()
        if field == "user-agent":
            in_group = value == "*" or value.lower().startswith("falco")
            group_all = group_all or in_group
        elif field == "disallow" and in_group and value:
            rule = re.escape(value).replace(r"\*", ".*")
            if re.match(rule, path):
                return False
    return True


def clerk_query(entity: Any) -> Optional[str]:
    """Search words for a company: its name without the entity type. The
    site matches a substring of the business name."""
    name = _SUFFIX.sub("", clean_owner_name(entity)).strip(" ,.")
    name = re.sub(r"\s*-\s*", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name if len(name) >= 3 else None


def clerk_form(query: str) -> Dict[str, str]:
    data = {k: "" for k in _FIELDS}
    data.update(bname=query, performSearch="1", orderby="BmBusName", ascdesc="asc")
    return data


def _cell(html: str) -> List[str]:
    parts = re.split(r"<br\s*/?>", html, flags=re.I)
    return [re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", p))).strip() for p in parts]


def _day(text: str) -> Optional[str]:
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", text.strip())
    if not m or m[1] == "0000":
        return None
    try:
        return date(int(m[1]), int(m[2]), int(m[3])).isoformat()
    except ValueError:
        return None


def parse_clerk(html: Any) -> Optional[List[Dict[str, Any]]]:
    """Every record on a result page, or None when the page is not a result
    page (an error page, a block page, a changed layout)."""
    text = str(html or "")
    if "Results Found" not in text and "No Records Found" not in text:
        return None
    if "Business Close Date" not in text:
        return None
    out: List[Dict[str, Any]] = []
    for row in re.findall(r"<tr[^>]*bgcolor[^>]*>(.*?)</tr>", text, re.S | re.I):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S | re.I)
        if len(cells) < 7:
            continue
        biz, owner = _cell(cells[0]), _cell(cells[6])
        if not biz or not biz[0]:
            continue
        flat = lambda c: " ".join(_cell(c)).strip()
        out.append({"business": biz[0], "business_address": ", ".join(x for x in biz[1:] if x),
                    "product": flat(cells[1]) or None, "start": _day(flat(cells[2])), "close": _day(flat(cells[3])),
                    "status": flat(cells[5]) or None, "owner": owner[0] or None,
                    "owner_address": ", ".join(x for x in owner[1:] if x) or None})
    return out


def clerk_result(entity: Any, query: str, html: Any, today: date) -> Optional[Dict[str, Any]]:
    """The value stored under signer_lookup[CLERK_KEY], or None when the
    page could not be read (nothing is stored and the old value stays).
    Only records whose name is this company's words are kept; whether the
    entity type also matches is decided when the card is built."""
    records = parse_clerk(html)
    if records is None:
        return None
    name = clean_owner_name(entity)
    mine = [r for r in records if base_key(r["business"]) == base_key(name)]
    mine.sort(key=lambda r: (r["business"], r["start"] or "", r["close"] or "", r["owner"] or ""))
    return {"entity": name, "entity_key": entity_key(name), "query": query, "checked_on": today.isoformat(),
            "label": CLERK_LABEL, "url": "http://tncountyclerk.com/hamilton/public/",
            "found": len(records), "records": mine[:MAX_RECORDS]}


def is_fresh(stored: Any, entity: Any, today: date) -> bool:
    """True when a stored lookup is for this same company and recent enough."""
    if not isinstance(stored, dict) or stored.get("entity_key") != entity_key(clean_owner_name(entity)):
        return False
    try:
        age = (today - date.fromisoformat(str(stored.get("checked_on"))[:10])).days
    except ValueError:
        return False
    return 0 <= age < RECHECK_DAYS
