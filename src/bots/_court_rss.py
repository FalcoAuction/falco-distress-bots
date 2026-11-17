"""
Parse federal court CM/ECF RSS feeds (free, public, no PACER account).

Each court publishes the last ~24 hours of docket entries at
  https://ecf.{court}.uscourts.gov/cgi-bin/rss_outside.pl
One <item> per docket entry:
  bankruptcy  title "26-25183 Tina Lucy Griffin"
              description "Type: bk Office: 0 Chapter: 13 Trustee: Weiner, Robin R [Chapter 13 Plan] (...)"
  district    title "1:26-cv-27369 Lost Cities, Inc. v. Get In Holdings, Inc. et al"
              description "[Order on Motion ...] (...)"
  link        https://ecf.{court}.uscourts.gov/cgi-bin/DktRpt.pl?{caseid}

Two things are pulled out:
  vessel arrests   civil cases whose caption names a vessel (M/Y, M/V, in rem,
                   official number...): a lender, marina or crew has arrested
                   a boat and it is headed for a Marshal's sale
  bankruptcies     debtor names, chapter and trustee, for owner overlays

Pure; no network. The bot wraps it.
"""

from __future__ import annotations

import email.utils
import html
import re
from typing import Any, Dict, List, Optional

_ITEM = re.compile(r"<item>(.*?)</item>", re.S)
_TAG = re.compile(r"<[^>]+>")


def _field(item: str, tag: str) -> str:
    m = re.search(rf"<{tag}>(.*?)</{tag}>", item, re.S)
    if not m:
        return ""
    v = m.group(1)
    v = re.sub(r"^<!\[CDATA\[(.*)\]\]>$", r"\1", v.strip(), flags=re.S)
    return html.unescape(v).strip()


def parse_feed(xml: str) -> List[Dict[str, Any]]:
    out = []
    for raw in _ITEM.findall(xml or ""):
        title = _field(raw, "title")
        desc = _field(raw, "description")
        m = re.match(r"\s*(\S+)\s+(.*)$", title)
        case_number = m.group(1).rstrip("-") if m else title
        caption = m.group(2).strip() if m else ""
        entry = re.search(r"\[([^\]]+)\]", desc)
        chapter = re.search(r"Chapter:\s*(\w+)", desc)
        trustee = re.search(r"Trustee:\s*(.*?)\s*\[", desc)
        pub = _field(raw, "pubDate")
        try:
            pub_iso = email.utils.parsedate_to_datetime(pub).isoformat() if pub else None
        except (TypeError, ValueError):
            pub_iso = None
        out.append({
            "case_number": case_number,
            "caption": caption,
            "entry_type": entry.group(1).strip() if entry else None,
            "chapter": chapter.group(1) if chapter else None,
            "trustee": (trustee.group(1).strip() or None) if trustee else None,
            "docket_url": _field(raw, "link") or None,
            "published": pub_iso,
        })
    return out


# ── Vessel arrests ─────────────────────────────────────────────────────────

_VESSEL_CUE = re.compile(r"\b(M/Y|M/V|S/V|F/V|M\.Y\.|M\.V\.|S\.V\.|YACHT|VESSEL|IN REM|OFFICIAL (?:NO|NUMBER)|"
                         r"O\.N\.|HULL (?:ID|IDENTIFICATION)|HIN)\b", re.I)
# Owners asking a court to cap their liability after an accident are not
# distress, and neither are criminal or habeas matters.
_NOT_ARREST = re.compile(r"LIMITATION OF LIABILITY|EXONERATION|\bIN RE\b|\bIN THE MATTER OF\b|COMPLAINT OF|PETITION OF",
                         re.I)
_YACHT_CUE = re.compile(r"\b(M/Y|M\.Y\.|S/V|S\.V\.|YACHT|SPORTFISH\w*|SAILBOAT|CATAMARAN)\b", re.I)
_LENDER = re.compile(r"\b(BANK|CREDIT UNION|FINANCIAL|FINANCE|CAPITAL|LENDING|ACCEPTANCE|MORTGAGE|LEASING|"
                     r"FUNDING|N\.A\.|FSB|TRUST COMPANY)\b", re.I)
_SERVICER = re.compile(r"\b(MARINA|BOATYARD|BOAT YARD|SHIPYARD|YACHT (?:SERVICES|CLUB)|DRY ?DOCK|STORAGE|MARINE "
                       r"(?:SERVICES|REPAIR))\b", re.I)
_VESSEL_NAME = re.compile(r"\b(?:M/Y|M/V|S/V|F/V|M\.Y\.|M\.V\.|S\.V\.)\s+[\"']?([A-Z0-9][A-Z0-9 '&.\-]{1,40}?)[\"']?"
                          r"(?=,|\s+(?:her|its|their|a\s|an\s|in rem|official|o\.n\.|hull|et al|\(|$))", re.I)


def is_civil(case_number: str) -> bool:
    return "-cv-" in (case_number or "").lower() or "-ad-" in (case_number or "").lower()


def vessel_case(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """A civil case whose caption names a vessel defendant, or None."""
    cap = item.get("caption") or ""
    if not is_civil(item.get("case_number", "")) or not _VESSEL_CUE.search(cap) or _NOT_ARREST.search(cap):
        return None
    plaintiff, _, defendants = cap.partition(" v. ")
    if not defendants:
        plaintiff, _, defendants = cap.partition(" vs. ")
    name = _VESSEL_NAME.search(defendants or cap)
    kind = "lender" if _LENDER.search(plaintiff) else "service_lien" if _SERVICER.search(plaintiff) else "other"
    return {
        "vessel": name.group(1).strip().rstrip(".,") if name else None,
        "plaintiff": plaintiff.strip() or None,
        "defendants": defendants.strip() or None,
        "claim_kind": kind,
        "yacht": bool(_YACHT_CUE.search(cap)),
    }


# ── Bankruptcy debtors ─────────────────────────────────────────────────────

def debtors(caption: str) -> List[str]:
    """'Jane Q Doe and John R Doe' -> ['Jane Q Doe', 'John R Doe']."""
    parts = re.split(r"\s+and\s+|\s*&\s*", caption or "", flags=re.I)
    return [p.strip(" ,") for p in parts if p.strip(" ,")]


# ── Bankruptcy events (TN) ─────────────────────────────────────────────────
#
# Later docket entries that put a debtor's house back in play. Labels differ
# by court; these patterns were checked against a day of tnmb/tneb/tnwb feeds
# (2026-11-16). The feed gives only the entry's name, never its outcome, so
# anything that might be a motion or an order whose result is unknown is
# staged as the weaker signal, never as the final one.

BK_DISMISSED = "BK_DISMISSED"                  # chapter 13 case dismissed: stay gone, foreclosure can resume
BK_DISMISSAL_MOTION = "BK_DISMISSAL_MOTION"    # trustee/creditor asked to dismiss, or an order whose result the feed hides
BK_STAY_RELIEF = "BK_STAY_RELIEF"              # order on a creditor's request to lift the automatic stay
BK_ABANDONED = "BK_ABANDONED"                  # trustee notice, motion or order to abandon property
BK_EVENT_TYPES = (BK_DISMISSED, BK_DISMISSAL_MOTION, BK_STAY_RELIEF, BK_ABANDONED)

# Entries that mention these words but undo or argue about the event.
_BK_NOISE = re.compile(r"adversary|set aside|vacat|withdraw|objection|response|reinstat|\bden(y|ied|ying)\b|reconsider",
                       re.I)
_BK_ORDER = re.compile(r"\border\b|bk order|\bdismissing\b|\(\s*dismissed\s+13\s*\)", re.I)
_BK_FINAL_DISMISSAL = re.compile(r"\bdismissing\b|\(\s*dismissed\s+13\s*\)|\bbk order\b", re.I)
_BK_MOTION = re.compile(r"\bmotion\b", re.I)
_STAY_RELIEF = re.compile(r"relief from (the )?(automatic )?stay", re.I)
_CO_DEBTOR = re.compile(r"co-?\s?debtor", re.I)


def bk_event(item: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Classify one bankruptcy docket entry, or None if it is not one of the
    events above. Returns {'type', 'status', 'entry'} where status is
    'order', 'motion', 'notice' or 'order_on_motion' (an order was entered on
    a motion, but the feed does not say whether it was granted)."""
    entry = (item.get("entry_type") or "").strip()
    if not entry or _BK_NOISE.search(entry):
        return None
    chapter = (item.get("chapter") or "").strip()

    if re.search(r"abandon", entry, re.I):
        status = "order" if _BK_ORDER.search(entry) and not _BK_MOTION.search(entry) else \
                 "order_on_motion" if _BK_ORDER.search(entry) else \
                 "notice" if re.search(r"\bnotice\b", entry, re.I) else "motion"
        return {"type": BK_ABANDONED, "status": status, "entry": entry}

    if _STAY_RELIEF.search(entry) and not _CO_DEBTOR.search(entry):
        # Only orders: a motion for relief is a request the court may refuse.
        if not _BK_ORDER.search(entry):
            return None
        status = "order_on_motion" if _BK_MOTION.search(entry) else "order"
        return {"type": BK_STAY_RELIEF, "status": status, "entry": entry}

    if re.search(r"dismiss", entry, re.I):
        if chapter != "13":
            return None
        if _BK_FINAL_DISMISSAL.search(entry) and not _BK_MOTION.search(entry):
            return {"type": BK_DISMISSED, "status": "order", "entry": entry}
        status = "order_on_motion" if _BK_ORDER.search(entry) else "motion"
        return {"type": BK_DISMISSAL_MOTION, "status": status, "entry": entry}
    return None
