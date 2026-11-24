"""
Which public feeds are read, which were stopped, and how a feed that is
stopped or failing is described downstream.

Rules this module encodes (audit of 2026-11-24, evidence files in
falco-workspace/reports/2026-11-24-feed-access-audit/):

  * Every request identifies itself with the one shared agent in _base.py.
    A site that refuses that agent has made an access decision: the source
    is unavailable, and no browser agent is substituted.
  * A path a site's robots.txt disallows is not fetched. Collection from it
    is recorded in STOPPED with the date it ended. Rows already staged keep
    their evidence and their original dates.
  * A feed that is stopped, refused or failing says only "not read since
    DATE". It never says a sale was withdrawn, a tract was removed or a sale
    was held. Those statements need a successful read that shows them.

Pure functions: no network, no database.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

AUDIT_DATE = "2026-11-24"

# source key -> why collection ended. The key is a bot_source, or
# "bot_source:part" when only one part of a bot's collection ended.
STOPPED: Dict[str, Dict[str, Any]] = {
    "nashville_ledger": {
        "label": "The Nashville Ledger foreclosure notices (tnledger.com)",
        "host": "www.tnledger.com",
        "stopped_on": AUDIT_DATE,
        "rule": "robots.txt read 2026-11-24: 'User-agent: *' 'Disallow: /'. Every path is closed to crawlers.",
        "replacement": None,
        "gap": "New Middle Tennessee foreclosure notices printed only in the Ledger. Sales by Mackie Wolf, "
               "Brock & Scott and Rubin Lublin still arrive from each firm's own sale list.",
    },
    "court_notices:nashville_ledger": {
        "label": "The Nashville Ledger court notices (tnledger.com)",
        "host": "www.tnledger.com",
        "stopped_on": AUDIT_DATE,
        "rule": "robots.txt read 2026-11-24: 'User-agent: *' 'Disallow: /'. Every path is closed to crawlers.",
        "replacement": "Cheatham tax sale tracts: the Cheatham County Trustee's posted notice (read directly). "
                       "Probate and court sale notices: none.",
        "gap": "New Davidson and Middle Tennessee probate, partition and court sale notices.",
    },
    "memphis_daily_news:detail": {
        "label": "Memphis Daily News notice detail pages (/Search/Details/ViewNotice.aspx)",
        "host": "www.memphisdailynews.com",
        "stopped_on": AUDIT_DATE,
        "rule": "robots.txt read 2026-11-24 allows /notices/ and disallows /Search/ (except /Search/Search.aspx).",
        "replacement": "The allowed daily /notices/ index: borrower, street, advertised sale date, first notice date.",
        "gap": "For notices first published after the stop date: substitute trustee, city and ZIP, county, "
               "instrument number and the notice text.",
    },
    "nashville_ledger_reextract": {
        "label": "The Nashville Ledger notice re-read (tnledger.com)",
        "host": "www.tnledger.com",
        "stopped_on": AUDIT_DATE,
        "rule": "robots.txt read 2026-11-24: 'User-agent: *' 'Disallow: /'. Every path is closed to crawlers.",
        "replacement": None,
        "gap": "Lender and original principal on Ledger rows staged without a notice body.",
    },
}


def stopped(source: Any, part: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """The stop record for a source (or one part of it), or None while it is read."""
    key = f"{source}:{part}" if part else str(source or "")
    return STOPPED.get(key)


def _naive_utc(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        stamp = value
    elif isinstance(value, date):
        stamp = datetime(value.year, value.month, value.day)
    else:
        try:
            stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return stamp if stamp.tzinfo is None else stamp.astimezone(timezone.utc).replace(tzinfo=None)


def read_state(source: Any, last_read: Any, now: Any, stale_days: int, part: Optional[str] = None) -> Dict[str, Any]:
    """Whether a source's evidence is current.

    last_read is the last successful read (a datetime, a date or an ISO
    string; None when there never was one). Returns

      status           "current"   read within stale_days
                       "stopped"   collection was ended on purpose
                       "not_read"  not read within stale_days (refused, down, changed)
      not_read_since   ISO day of the last successful read, None when current
                       or never read
      stopped_on       ISO day, when stopped

    Nothing here means withdrawn, removed, paid or sold.
    """
    read, at = _naive_utc(last_read), _naive_utc(now)
    since = read.date().isoformat() if read else None
    halt = stopped(source, part)
    if halt:
        return {"status": "stopped", "not_read_since": since, "stopped_on": halt["stopped_on"]}
    if read is None or at is None or (at - read).days >= stale_days:
        return {"status": "not_read", "not_read_since": since, "stopped_on": None}
    return {"status": "current", "not_read_since": None, "stopped_on": None}


def not_read_text(what: str, state: Dict[str, Any]) -> str:
    """One sentence for a card: "<what> has not been read since DATE."."""
    since = state.get("not_read_since")
    text = f"{what} has not been read since {since}" if since else f"{what} has never been read"
    if state.get("stopped_on"):
        text += f" (collection from that source stopped on {state['stopped_on']})"
    return text + "."
