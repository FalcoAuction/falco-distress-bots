"""Read Rubin Lublin's Tennessee foreclosure property listings (one page).

Used only to recheck Memphis Daily News notices that name Rubin Lublin as
substitute trustee (see _trustee_list_match.py). It stages nothing.

Access: robots.txt allows the page. The firm's listing index asks visitors
to agree to its terms of service (warranty and accuracy disclaimers; no
restriction on reading or use) before following the link to this page. That
agreement is Patrick's to give, so the read is off unless
RUBIN_LUBLIN_LIST_ENABLED=1 is set. One GET per run, no retries here.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

URL = "https://rlselaw.com/property-listing/tennessee-property-listings/"
HEADERS = ["Sale Date", "File #", "Property", "City", "Zip", "County", "Bid"]
ENABLE_ENV = "RUBIN_LUBLIN_LIST_ENABLED"


def enabled() -> bool:
    return os.environ.get(ENABLE_ENV, "").strip() == "1"


def parse(page_html: str, county: str = "Shelby") -> List[Dict[str, Any]]:
    """Rows for one county. Raises ValueError on anything but the expected
    table, so a changed or partial page can never read as 'not listed'."""
    page = BeautifulSoup(page_html, "html.parser")
    rows = page.select("table.property-listing tr")
    if not rows or [th.get_text(" ", strip=True) for th in rows[0].select("th")] != HEADERS:
        raise ValueError("listing table not found")
    out = []
    for tr in rows[1:]:
        cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(cells) != len(HEADERS):
            raise ValueError("unexpected listing row")
        when = re.match(r"(\d{1,2}/\d{1,2}/\d{4})\b", cells[0])
        if not when or not cells[2] or not re.fullmatch(r"\d{5}", cells[4]) or not cells[5]:
            raise ValueError("incomplete listing row")
        if cells[5].casefold() != county.casefold():
            continue
        out.append({"date": datetime.strptime(when.group(1), "%m/%d/%Y").date().isoformat(), "file": cells[1] or None,
                    "street": cells[2], "city": cells[3], "zip": cells[4]})
    if len(rows) < 2:
        raise ValueError("empty listing table")
    return out


def load(session, timeout: int = 20) -> Optional[List[Dict[str, Any]]]:
    """Today's Shelby rows, or None when the read is off. Raises on a failed
    or unexpected response; the caller treats that as 'not read'."""
    if not enabled():
        return None
    response = session.get(URL, timeout=timeout)
    if response.status_code != 200:
        raise ValueError(f"listing page returned HTTP {response.status_code}")
    return parse(response.text)
