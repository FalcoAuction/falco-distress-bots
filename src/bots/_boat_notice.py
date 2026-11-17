"""
Parse Tennessee newspaper notices about boats held for unpaid marina or
storage charges (Tenn. Code Ann. 66-19-212: two weeks of newspaper notice
plus certified mail before a marina lien sale) and abandoned-vessel
recoveries. Pure functions; tested without network.

Formats seen on tnpublicnotice.com (search-result excerpts, ~330 chars;
the full notice sits behind a reCAPTCHA and is never fetched):

  A  "NOTICE ATTENTION <owner> or (the) current owner of a(n) <desc> vessel
      with TN registration #: <reg> and hull ID#: <hin> that is stored at
      <marina> located at <address> ..."
  B  "<marina>, <address>, claims a lien for unpaid storage charges against
      a <year> <make> <dims> ... Vessel, TN Registration No. <reg>, ...
      HIN <hin>, owned by <owner>."
  C  "NOTICE OF PUBLIC AUCTION MARINA LIEN SALE ... <marina> will sell at
      public auction the vessels described below ..." (several boats; the
      excerpt carries only the marina)
  D  "PUBLIC NOTICE ABANDONED VESSEL ... VESSEL DESCRIPTION: <desc> HULL
      IDENTIFICATION/REGISTRATION: <hin>/<reg> ..."

Limitation-of-liability complaints, vehicle tow sales and trustee sales
that merely mention a hull or a boat ramp are rejected.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, Optional

REG_RE = re.compile(r"\bTN\s?(\d{3,4})\s?([A-Z]{2})\b")
HIN_RE = re.compile(r"(?:hull\s*(?:ID|identification)(?:\s*number)?\s*#?\s*:?\s*|\bHIN\s*#?\s*:?\s*)"
                    r"([A-Z]{2,3}[A-Z0-9]{8,11})\b", re.I)
HIN_SLASH_RE = re.compile(r"HULL IDENTIFICATION/REGISTRATION:\s*([A-Z0-9]{10,14})\s*/\s*(TN\s?\d{3,4}\s?[A-Z]{2})", re.I)
DIMS_RE = re.compile(r"(\d{2})\s*['’]\s*x\s*(\d{2,3})\s*['’]", re.I)
LENGTH_RE = re.compile(r"\b(\d{2})\s*(?:['’]|-?\s*(?:FOOT|FT)\b)", re.I)
YEAR_RE = re.compile(r"\b(19[5-9]\d|20[0-3]\d)\b")

ATTENTION_RE = re.compile(
    r"ATTENTION\s+(?P<owner>.+?)\s+or\s+(?:the\s+)?current\s+owner\s+of\s+an?\s+(?P<desc>.*?)\s*vessel\b"
    r".*?stored\s+at\s+(?P<marina>.+?)\s+(?:located\s+at|at)\s+(?P<addr>.+?)(?:\.\s|\s+You\s+have|$)",
    re.I | re.S)
CLAIMS_RE = re.compile(
    r"(?P<marina>[A-Z][\w&'.,\- ]{3,80}?),\s+(?P<addr>\d[^,]+,\s*[A-Za-z .]+,\s*TN\s*\d{5}),\s+claims\s+a\s+lien"
    r".*?against\s+an?\s+(?P<desc>.+?)(?:,|\s+TN\s+Registration)", re.I | re.S)
OWNED_BY_RE = re.compile(r"owned\s+by\s+(?P<owner>[A-Z][A-Za-z.'\- ]{3,60}?)\s*(?:\.\s+(?:Amount|The|If|Said|Sale|Notice|Any|You|This)\b|,|\s+and\s|\.?\s*$)")
LEAD_IN_RE = re.compile(r"^(?:.*?\bGIVEN\s+that\s+|(?:PUBLIC\s+)?NOTICE\s+)", re.I)
MARINA_SALE_RE = re.compile(
    r"(?:MARINA\s+LIEN\s+SALE|NOTICE\s+OF\s+PUBLIC\s+(?:SALE|AUCTION)).*?(?:that\s+)?(?P<marina>[A-Z][\w&'.\- /]{3,80}?"
    r"(?:Marina|Yacht\s+Club|Boat\s+Dock|Harbor|Resort)[\w&'.\- /]{0,40}?)(?:\s*\(|,|\s+will\s+sell)", re.I | re.S)
ABANDONED_RE = re.compile(r"ABANDONED\s+(?:VESSEL|WATERCRAFT)|abandoned\s+vessel\s+recovered", re.I)
DESCRIPTION_RE = re.compile(r"VESSEL\s+DESCRIPTION:\s*(?P<desc>.+?)\s+HULL", re.I | re.S)

REJECT_RE = re.compile(
    r"LIMITATION\s+OF\s+LIABILITY|EXONERATION|TRUSTEE['’]?S\s+(?:NOTICE\s+OF\s+)?SALE|DEED\s+OF\s+TRUST|"
    r"VEHICLES?\s+(?:TO\s+BE\s+AUCTIONED|WILL\s+BE)|BOAT\s+RAMP\s+CLOSED|aircraft", re.I)

HOUSEBOAT_MAKES = re.compile(
    r"\b(SUMERSET|STARDUST|LAKEVIEW|BLUEWATER|BLUE\s+WATER|THOROUGHBRED|FANTASY|SHARPE|GIBSON|"
    r"DESTINATION\s+YACHTS?|TWIN\s+ANCHORS|BURNS\s*CRAFT|HOLIDAY\s+MANSION|HARBOR\s*MASTER|MONTICELLO|"
    r"LAZY\s+DAYS|SKIPPERLINER|KINGS\s*CRAFT|JAMESTOWNER|NAUTALINE|BOATEL|AQUA\s+CHALET|"
    r"HOUSE\s*BOAT|HOUSEBOAT|CRUISE\s*A\s*HOME|DRIFTER|HORIZON\s+HOUSEBOAT)\b", re.I)
CRUISER_MAKES = re.compile(
    r"\b(SEA\s*RAY|CARVER|CRUISERS|CHRIS[\s\-]*CRAFT|SILVERTON|MERIDIAN|REGAL|FORMULA|FOUR\s+WINNS|TIARA|VIKING|"
    r"HATTERAS|BERTRAM|MARQUIS|COBALT|CHAPARRAL|MONTEREY|RINKER|MAINSHIP|TROJAN|MARINETTE|MAXUM|"
    r"CABIN\s+CRUISER|YACHT)\b", re.I)
PREMIUM_MAKES = re.compile(
    r"\b(MASTERCRAFT|MASTER\s+CRAFT|MALIBU|NAUTIQUE|TIGE|AXIS|CENTURION|SUPRA|MOOMBA|BENNINGTON|BARLETTA|"
    r"MANITOU|HARRIS|AVALON|PREMIER|SYLVAN)\b", re.I)


def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip(" .,;:")


def norm_reg(s: Optional[str]) -> Optional[str]:
    m = REG_RE.search((s or "").upper())
    return f"TN{m.group(1)}{m.group(2)}" if m else None


def hin_model_year(hin: Optional[str], today: Optional[date] = None) -> Optional[int]:
    """Modern (post-1984) 12-character HINs end in the 2-digit model year."""
    h = re.sub(r"[^A-Z0-9]", "", (hin or "").upper())
    if len(h) != 12 or not h[-2:].isdigit():
        return None
    yy = int(h[-2:])
    cur = (today or date.today()).year % 100
    return 2000 + yy if yy <= cur + 1 else 1900 + yy


def parse(excerpt: str) -> Optional[Dict[str, Any]]:
    text = clean(excerpt)
    if not text or REJECT_RE.search(text):
        return None
    out: Dict[str, Any] = {"owner": None, "description": None, "marina": None, "marina_address": None,
                           "registration": None, "hin": None, "kind": None}
    m = ATTENTION_RE.search(text)
    if m:
        out.update(kind="marina_lien_notice", owner=clean(m.group("owner")), description=clean(m.group("desc")),
                   marina=clean(m.group("marina")), marina_address=clean(m.group("addr")))
    else:
        m = CLAIMS_RE.search(text)
        if m:
            o = OWNED_BY_RE.search(text)
            out.update(kind="marina_lien_notice", marina=clean(m.group("marina")),
                       marina_address=clean(m.group("addr")), description=clean(m.group("desc")),
                       owner=clean(o.group("owner")) if o else None)
        elif ABANDONED_RE.search(text):
            d = DESCRIPTION_RE.search(text)
            out.update(kind="abandoned_vessel", description=clean(d.group("desc")) if d else None)
        else:
            m = MARINA_SALE_RE.search(text)
            if m and re.search(r"\bvessels?\b|\bboats?\b", text, re.I):
                out.update(kind="marina_auction", marina=clean(m.group("marina")))
            else:
                return None
    s = HIN_SLASH_RE.search(text)
    if s:
        out["hin"], out["registration"] = s.group(1).upper(), norm_reg(s.group(2))
    h = HIN_RE.search(text)
    out["hin"] = out["hin"] or (h.group(1).upper() if h else None)
    out["registration"] = out["registration"] or norm_reg(text)
    if out["kind"] in ("abandoned_vessel",) and not (out["hin"] or out["registration"]):
        return None   # unidentified jon boats: nothing to sell, no one to call
    desc = out["description"] or ""
    y = YEAR_RE.search(desc)
    out["year"] = int(y.group(1)) if y else hin_model_year(out["hin"])
    dims = DIMS_RE.search(text)
    length = LENGTH_RE.search(desc) or LENGTH_RE.search(text)
    out["length_ft"] = int(dims.group(2)) if dims else (int(length.group(1)) if length else None)
    out["width_ft"] = int(dims.group(1)) if dims else None
    if out["marina"]:
        out["marina"] = clean(LEAD_IN_RE.sub("", out["marina"])) or out["marina"]
    return out


def grade(v: Dict[str, Any]) -> Optional[str]:
    """Picky: houseboats and cruisers, or late-model premium boats. Old
    bass boats, jon boats and basic pontoons are not auction lots."""
    desc = " ".join(x for x in (v.get("description"), v.get("marina")) if x)
    year = v.get("year")
    length = v.get("length_ft") or 0
    if v.get("kind") == "marina_auction":
        return "B"   # a marina selling boats at auction is itself the client
    if HOUSEBOAT_MAKES.search(desc) or (v.get("width_ft") or 0) >= 12 or length >= 40:
        return "A" if year is None or year >= 1995 else "B"
    if CRUISER_MAKES.search(desc) or length >= 26:
        if year is None:
            return "B"
        return "A" if year >= 2010 else "B" if year >= 1990 else None
    if PREMIUM_MAKES.search(desc) and (year or 0) >= 2015:
        return "B"
    return None
