"""Sort the court notices newspapers publish under "Orders" and "Public
sales" into FALCO lanes, from the excerpt text.

  PARTITION        a suit to divide (and usually sell) property co-owned by
                   heirs or ex-spouses: the court orders a sale, and an
                   auction is the normal way to run it
  COURT_SALE       a sale already ordered: Clerk and Master, special master
                   or commissioner, receiver's sale, order of sale
  DIVORCE          service by publication in a divorce: one spouse cannot be
                   found, the marital home is usually divided or sold
  CONSERVATORSHIP  a court-supervised ward whose property may be sold
  PRE_FORECLOSURE  a lender suing in Chancery (judicial foreclosure)

Order matters: a partition sale ordered by the Clerk and Master is a
partition; a lender's suit that mentions a receiver is still a foreclosure.
Storage-unit and towed-vehicle lien sales, bid advertisements and other
auctioneers' sale ads return None (not a lead for FALCO).
"""

from __future__ import annotations

import re
from typing import List, Optional

PARTITION_RE = re.compile(r"\bpartition\b", re.I)
COURT_SALE_RE = re.compile(
    r"clerk\s*(?:and|&)\s*master'?s?\s+(?:sale|auction)|clerk\s*(?:and|&)\s*master\b.{0,80}\b(?:sell|sale|auction)"
    r"|special\s+(?:master|commissioner)|commissioner'?s\s+sale|receiver'?s?\s+(?:auction|sale)"
    r"|\border\s+of\s+sale\b|\bjudicial\s+sale\b|\bdecree\s+of\s+sale\b", re.I | re.S)
DIVORCE_RE = re.compile(r"\bdivorce\b|dissolution\s+of\s+(?:the\s+)?marriage", re.I)
CONSERVATOR_RE = re.compile(r"\bconservator(?:ship)?\b|\bguardianship\b", re.I)
LENDER_SUIT_RE = re.compile(
    r"\b(?:bank|mortgage|lending|loan|credit\s+union|financial|servicing|federal\s+savings|n\.?a\.?)\b[^.;]{0,60}"
    r"\bplaintiffs?\b", re.I)
COURT_RE = re.compile(r"\b(?:chancery|circuit|probate|general\s+sessions)\s+court\b|\bin\s+the\s+matter\s+of\b", re.I)
NOT_A_LEAD_RE = re.compile(
    r"self[\s-]*storage|storage\s+units?|\bunits?\s+will\s+be\s+sold|\bVIN\b|towing|wrecker|abandoned\s+vehicle"
    r"|advertisement\s+for\s+bids?|sealed\s+bids|request\s+for\s+proposals", re.I)

PARTIES_RE = re.compile(
    r"([A-Z][A-Za-z.,'\- ]{3,80}?),?\s+(?:Petitioners?|Plaintiffs?)\s*,?\s*(?:v\.?|vs\.?|versus)\s+"
    r"(?:Case\s+No\.?\s*[\w\-]+\s+)?([A-Za-z][A-Za-z0-9.,'\-;() ]{3,200}?)(?:,?\s+(?:Respondents?|Defendants?)\b|\s*\.\.\.|$)")
# Lenders name the occupants as "Tenants of 2203 Springlot Road, Clarksville, TN (if any)".
TENANTS_OF_RE = re.compile(r"\btenants\s+of\s+(\d{1,6}\s*[A-Za-z][A-Za-z0-9 .'\-]{2,60}?,\s*[A-Za-z .]{2,30}?,?\s*TN)\b", re.I)
# A sale already booked with an auction firm is another auctioneer's sale,
# not a listing FALCO can route.
BOOKED_AUCTION_RE = re.compile(r"(?:realty|real\s+estate)\s*(?:&|and)\s*auction|auction(?:eer)?s?,?\s+(?:inc|llc|co)\b|"
                               r"\bauctioneer\b|absolute\s+auction|online\s+only", re.I)


def kind(text: str) -> Optional[str]:
    t = " ".join((text or "").split())
    if not t or NOT_A_LEAD_RE.search(t) and not (COURT_RE.search(t) or COURT_SALE_RE.search(t)):
        return None
    if PARTITION_RE.search(t):
        return "PARTITION"
    if CONSERVATOR_RE.search(t):
        return "CONSERVATORSHIP"
    if DIVORCE_RE.search(t):
        return "DIVORCE"
    if LENDER_SUIT_RE.search(t) and COURT_RE.search(t):
        return "PRE_FORECLOSURE"
    if COURT_SALE_RE.search(t) and not BOOKED_AUCTION_RE.search(t):
        return "COURT_SALE"
    return None


def tenants_address(text: str) -> Optional[str]:
    """'Tenants of 2203Springlot Road, Clarksville, TN' -> '2203 Springlot Road, Clarksville, TN'."""
    m = TENANTS_OF_RE.search(" ".join((text or "").split()))
    if not m:
        return None
    a = re.sub(r"^(\d+)(?=[A-Za-z])", r"\1 ", m.group(1).strip())
    return a.title().replace(", Tn", ", TN")


def parties(text: str) -> List[str]:
    """People named as parties (plaintiff side first). Companies and
    'Unknown heirs' are left out."""
    t = " ".join((text or "").replace("<", " ").split())
    m = PARTIES_RE.search(t)
    if not m:
        return []
    out = []
    # The plaintiff side often starts with the court caption
    # ("IN THE CHANCERY COURT FOR SUMNER COUNTY, TENNESSEE JOHN A DOE").
    plaintiff = re.split(r"\bTENNESSEE\b|\bCOUNTY\b,?|\bCOURT\b", m.group(1), flags=re.I)[-1]
    for side in (plaintiff, m.group(2)):
        for name in re.split(r";|\band\b|&", side):
            n = name.strip(" ,.")
            if (len(n.split()) >= 2 and not re.search(r"\b(LLC|INC|BANK|CORP|N\.A|TRUST|UNKNOWN|HEIRS|TENANTS|SECRETARY|"
                                                      r"STATE OF|COUNTY|CITY OF|ESTATE)\b", n, re.I)):
                out.append(n)
    return out
