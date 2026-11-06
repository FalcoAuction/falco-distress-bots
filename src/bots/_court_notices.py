"""Parse court notices from The Daily News Publishing Co. papers.

The Nashville Ledger (tnledger.com) and Memphis Daily News
(memphisdailynews.com) share one platform. Each edition's Notices.aspx page
prints its court notices in full inside table#ContentPane_CourtGridView,
one <tr> per notice, the notice text as <p> paragraphs and the notice ID in
an OpenChildFT2('CL310527', ...) link. tnpublicnotice.com carries the same
probate notices but truncates them and puts the full text behind a CAPTCHA,
which this deliberately does not touch; these papers are the full-text path.

What a probate notice to creditors gives us that the excerpt never did: the
court and county, docket, decedent, date of death, the date letters issued,
who the personal representatives are (often with a home address), and the
estate's attorney (often with an office address). The attorney is the B2B
contact: the same few dozen attorneys handle most estates in a county.

Pure functions only. No network, no database. The bot wraps these.
"""

from __future__ import annotations

import html as _html
import re
from typing import Any, Dict, List, Optional

# ── Row extraction ─────────────────────────────────────────────────────────

# The Ledger uses id="ContentPane_CourtGridView"; the Memphis Daily News
# nests it as id="ctl00_ctl00_ContentPane_ContentPane_CourtGridView".
_GRID_RE = re.compile(r'id="[A-Za-z0-9_]*CourtGridView"(.*?)</table>', re.S | re.I)
_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_ID_RE = re.compile(r"OpenChildFT2\('([A-Za-z]+\d+)'")
_P_RE = re.compile(r"<p[^>]*>(.*?)</p>", re.S | re.I)
_TAG_RE = re.compile(r"<[^>]+>")


def _clean(fragment: str) -> str:
    text = _html.unescape(_TAG_RE.sub("", fragment))
    text = text.replace("\xa0", " ").replace("�", "'")
    return re.sub(r"\s+", " ", text).strip()


def paragraphs_from_html(fragment: str) -> List[str]:
    """Non-empty, whitespace-normalized <p> texts in document order."""
    return [t for t in (_clean(p) for p in _P_RE.findall(fragment)) if t]


def parse_court_rows(page_html: str) -> List[Dict[str, Any]]:
    """Every court notice on an index page: {'notice_id', 'lines'}.

    Returns [] when the page has no court grid (wrong date, layout change).
    The caller treats an empty result on a publication day as a format
    alarm, not as "no notices".
    """
    grid = _GRID_RE.search(page_html)
    if not grid:
        return []
    rows: List[Dict[str, Any]] = []
    for tr in _ROW_RE.findall(grid.group(1)):
        m = _ID_RE.search(tr)
        if not m:
            continue  # header row
        lines = paragraphs_from_html(tr)
        if lines:
            rows.append({"notice_id": m.group(1).upper(), "lines": lines})
    return rows


# ── Classification ─────────────────────────────────────────────────────────

KIND_PROBATE = "probate_notice_to_creditors"
KIND_COURT_SALE = "court_ordered_sale"
KIND_OTHER = "other"

_SALE_RE = re.compile(
    r"\bpartition\b|sale of (the )?real (estate|property)|sell (the )?real (estate|property)"
    r"|order of sale|petition to sell|judicial sale|clerk and master'?s sale|special master'?s sale"
    r"|receiver(ship)?\b",
    re.I,
)


def classify(lines: List[str]) -> str:
    text = " ".join(lines)
    upper = text.upper()
    if "NOTICE TO CREDITORS" in upper and ("DECEASED" in upper or "WHO DIED" in upper or "ESTATE" in upper):
        return KIND_PROBATE
    # Juvenile and dependency notices mention petitions constantly; never
    # treat them as asset sales.
    if "JUVENILE COURT" in upper or "CHILDREN'S SERVICES" in upper:
        return KIND_OTHER
    if _SALE_RE.search(text):
        # A substitute trustee's sale under a deed of trust is a foreclosure,
        # not a court-ordered sale, even though it says "sale of real estate".
        if "DEED OF TRUST" in upper and not _JUDICIAL_RE.search(text):
            return KIND_OTHER
        return KIND_COURT_SALE
    return KIND_OTHER


_JUDICIAL_RE = re.compile(
    r"\bpartition\b|order of sale|judicial sale|clerk and master|special (?:master|commissioner)|chancery court|petition to sell",
    re.I)


# ── Field extraction ───────────────────────────────────────────────────────

_COUNTY_RES = [
    re.compile(r"(?:probate|circuit|chancery|general sessions)\s+court\s+(?:for|of)\s+([A-Z][A-Za-z .']+?)\s+County", re.I),
    re.compile(r"\b([A-Z][A-Za-z.']+(?: [A-Z][A-Za-z.']+)?)\s+County,\s+Tennessee", re.I),
]
_DOCKET_RE = re.compile(r"(?:Probate\s+)?(?:Docket|Case|File)\s*(?:No\.?|Number|#)?\s*:?\s*([A-Z0-9][A-Z0-9\-]{2,24})", re.I)
_DECEDENT_RES = [
    re.compile(r"Estate of\s+(.+?),?\s+Deceased", re.I),
    re.compile(r"In Re(?: the Matter of)?:?\s+(?:the Estate of\s+)?(.+?)(?:,?\s+Deceased|$)", re.I),
    re.compile(r"in respect (?:of|to) (?:the estate of )?(.+?),?\s+(?:deceased,?\s+)?who died", re.I),
]
_DIED_RE = re.compile(r"who died(?: on)?\s+([A-Za-z]{3,9}\.? \d{1,2},? \d{4}|\d{1,2}/\d{1,2}/\d{2,4})", re.I)
_LETTERS_RE = re.compile(r"on the (\d{1,2})(?:st|nd|rd|th)? day of ([A-Za-z]+),?\s*(\d{4})?", re.I)
_LETTERS_TYPE_RE = re.compile(r"Letters\s+(?:of\s+)?(Testamentary|Administration|Authority)", re.I)
_ROLE_RE = re.compile(
    r"^(?:co[- ]?)?(executor|executrix|executors|administrator|administratrix|administrators|personal representative)",
    re.I,
)
_ROLE_TAIL_RE = re.compile(
    r"\s*,?\s*((?:co-?)?(?:executor|executrix|executors|administrator|administratrix|administrator\(s\)|administrators"
    r"|personal representative(?:\(s\))?)s?)\b.*$",
    re.I,
)
_ATTORNEY_INLINE_RE = re.compile(r"^(?:Estate\s+)?Attorney(?: for [A-Za-z ()]+)?\s*:\s*(.+)$", re.I)
_ATTORNEY_MARK_RE = re.compile(r"^(?:Estate\s+)?Attorney(?: for [A-Za-z ()]+)?\s*:?\s*$", re.I)
# "Attorney for Kirsten E. Feinberg, Executrix": names the client and her role;
# the attorney is the name on the line above.
_ATTORNEY_FOR_PERSON_RE = re.compile(
    r"^Attorney for (.+?),\s*((?:co-?)?(?:executor|executrix|administrator|administratrix|personal representative))\b", re.I)
# A docket line: "Case No.: MC CH CV PB-26-417", "Probate Docket No.: P-4289".
_DOCKET_LINE_RE = re.compile(r"^(?:Probate\s+)?(?:Docket|Case|File)\s*(?:No\.?|Number|#)?\s*:?\s*([A-Za-z0-9][A-Za-z0-9 \-]{2,40})$", re.I)
_ATTORNEY_AFTER_RE = re.compile(r"^Attorney for (?:the )?(?:Personal Representative|Estate|Executor|Administrator)", re.I)
# "Dickson, TN 37055" and, in Main Street Media papers, "Dickson TN 37055".
_CITY_ZIP_RE = re.compile(r"^[A-Za-z .']+?,?\s+(?:TN|Tennessee|[A-Z]{2})\.?,?\s+\d{5}(?:-\d{4})?$")
_STREET_RE = re.compile(r"^\d+[A-Za-z]?\s+\S+|^(?:P\.?\s?O\.?\s+Box|Suite|Ste\.?|Room)\b", re.I)
_STOP_RE = re.compile(r"clerk|deputy|^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.? \d", re.I)
_SIGNED_RE = re.compile(r"^This\s+(?:the\s+)?\d{1,2}(?:st|nd|rd|th)?\s+day of", re.I)


def _first(patterns, text: str) -> Optional[str]:
    for pattern in patterns:
        m = pattern.search(text)
        if m:
            return m.group(1).strip()
    return None


def _county(text: str) -> Optional[str]:
    raw = _first(_COUNTY_RES, text)
    if not raw:
        return None
    raw = re.sub(r"^(?:(?:the|of|for)\s+)+", "", raw.strip(), flags=re.I)
    return raw.lower()


def _name(value: str) -> str:
    value = re.sub(r"\s+", " ", value).strip(" ,;:")
    # Keep the period on "Jr." / "Sr." / an initial; drop sentence punctuation.
    if value.endswith(".") and not re.search(r"\b(?:Jr|Sr|[A-Z])\.$", value):
        value = value[:-1]
    value = re.sub(r",?\s+Esq\.?$", "", value, flags=re.I)
    # "Kennedy Jr., David Randall" -> "David Randall Kennedy Jr."
    m = re.match(r"^([A-Za-z'\-]+(?: (?:Jr|Sr|II|III|IV)\.?)?),\s+([A-Za-z .'\-]+)$", value)
    if m and " " not in m.group(1).split(" ")[0]:
        return f"{m.group(2).strip()} {m.group(1).strip()}"
    return value


def _is_address(line: str) -> bool:
    return bool(_STREET_RE.match(line) or _CITY_ZIP_RE.match(line))


def _signature_block(lines: List[str]) -> List[str]:
    """Lines after the 'This __ day of' signing line (or after the statutory
    twelve-month clause), before the clerk and publication dates."""
    start = None
    for i, line in enumerate(lines):
        if _SIGNED_RE.match(line):
            start = i + 1
    if start is None:
        for i, line in enumerate(lines):
            if "twelve (12) months" in line.lower():
                start = i + 1
    if start is None:
        return []
    block: List[str] = []
    for line in lines[start:]:
        if _STOP_RE.search(line) and not _ATTORNEY_MARK_RE.match(line):
            # Davidson signs as "JOSEPH P. DAY" / "Probate Court Clerk": the
            # name line before a clerk title is the clerk, not a signer.
            # Robertson prints the clerk's name on the title line itself
            # ("Jon T Graves, Clerk & Master"); then the line above is a signer.
            clerk_named_here = re.match(r"^[^,]+,\s*(?:deputy\s+)?clerk", line, re.I)
            if re.search(r"clerk", line, re.I) and block and not clerk_named_here and not _is_address(block[-1]) \
                    and not re.match(r"^attorney", block[-1], re.I) and not re.search(r",\s*attorney\b", block[-1], re.I):
                block.pop()
            break
        block.append(line)
    return block


def _parse_signers(block: List[str]) -> Dict[str, Any]:
    """Representatives (with roles and addresses) and attorney from the
    signature block. Handles three layouts seen in the wild:

      Shelby:   "Grace Ann Mabry Executor" / "Attorney for the Estate:" / "Name"
      Davidson: "Name" / street / city / "Personal Representative(s)" /
                "Last, First" / street / city / "Attorney for Personal Representative(s)"
      Others:   "A and" / "B" / "Co-Executors of the Estate ..." / "Attorney: Name" / street / city
    """
    reps: List[Dict[str, Any]] = []
    attorney: Dict[str, Any] = {"names": [], "address": []}
    pending_names: List[str] = []
    pending_address: List[str] = []
    mode = "reps"  # reps -> attorney (after a "Attorney:" marker)
    label_role: Optional[str] = None  # set by "Personal Representative:" style labels

    def role_name(word: str) -> str:
        word = word.lower()
        return "executor" if word.startswith(("exec", "co-exec", "coexec")) else \
            "administrator" if "admin" in word else "personal_representative"

    def flush_reps(role: Optional[str] = None) -> None:
        role = role or label_role or "personal_representative"
        names: List[str] = []
        for chunk in pending_names:
            # "James Hugh Atkins II and" / "Patrice Atkins Empson": the
            # conjunction can end a line as well as sit mid-line.
            chunk = re.sub(r"\s+(?:and|&)\s*$", "", chunk, flags=re.I)
            names.extend(n for n in re.split(r"\s+and\s+|\s*&\s*", chunk) if n.strip())
        for n in names:
            reps.append({"name": _name(n), "role": role, "address": list(pending_address)})
        pending_names.clear()
        pending_address.clear()

    for line in block:
        if mode == "attorney":
            if _is_address(line):
                attorney["address"].append(line)
            elif not _ATTORNEY_AFTER_RE.match(line):
                attorney["names"].append(_name(line))
            continue
        for_person = _ATTORNEY_FOR_PERSON_RE.match(line)
        if for_person:
            attorney["names"].extend(_name(n) for n in pending_names)
            attorney["address"].extend(pending_address)
            pending_names.clear()
            pending_address.clear()
            reps.append({"name": _name(for_person.group(1)), "role": role_name(for_person.group(2)), "address": []})
            continue
        # Robertson: "Rebecca A Bell-Houchin, Attorney" on one line.
        trailing = re.match(r"^(.+?),\s*Attorney(?:\s+for\b.*)?$", line, re.I)
        if trailing and not _ATTORNEY_FOR_PERSON_RE.match(line):
            if pending_names:
                flush_reps()
            attorney["names"].append(_name(trailing.group(1)))
            continue
        inline = _ATTORNEY_INLINE_RE.match(line)
        if inline and not _ATTORNEY_AFTER_RE.match(line):
            if pending_names:
                flush_reps()
            attorney["names"].append(_name(inline.group(1)))
            mode = "attorney"
            continue
        # Davidson prints "Attorney for Personal Representative(s)" with no
        # colon AFTER the attorney's name and address, as a label. With a
        # colon ("Attorney for the Estate:") it introduces the name below.
        if _ATTORNEY_AFTER_RE.match(line) and ":" not in line and pending_names:
            attorney["names"].extend(_name(n) for n in pending_names)
            attorney["address"].extend(pending_address)
            pending_names.clear()
            pending_address.clear()
            continue
        if _ATTORNEY_MARK_RE.match(line):
            if ":" not in line and pending_names and pending_address:
                # Main Street Media papers: "Brian Ragan" / street / city /
                # "Attorney". A bare label after a name and address names
                # that block, like Davidson's "Attorney for ..." label.
                attorney["names"].extend(_name(n) for n in pending_names)
                attorney["address"].extend(pending_address)
                pending_names.clear()
                pending_address.clear()
                continue
            if pending_names:
                flush_reps()
            mode = "attorney"
            continue
        if _ATTORNEY_AFTER_RE.match(line):
            # Davidson: the pending name/address IS the attorney.
            attorney["names"].extend(_name(n) for n in pending_names)
            attorney["address"].extend(pending_address)
            pending_names.clear()
            pending_address.clear()
            continue
        role_only = _ROLE_RE.match(line)
        if role_only:
            role = role_name(role_only.group(1))
            if line.rstrip().endswith(":") and not pending_names:
                label_role = role  # label precedes the names
                continue
            flush_reps(role)
            continue
        tail = _ROLE_TAIL_RE.search(line)
        if tail and tail.start() > 0:
            # "Grace Ann Mabry Executor" on one line.
            pending_names.append(line[: tail.start()])
            flush_reps(role_name(tail.group(1)))
            continue
        if _is_address(line):
            pending_address.append(line)
            continue
        if re.match(r"^of\s+|^the estate|^estate of\b|^deceased$", line, re.I):
            continue
        pending_names.append(line)
    if pending_names and mode == "reps":
        # Names with no role line: treat as representatives, never guess an attorney.
        flush_reps()
    attorney["names"] = [n for n in attorney["names"] if n and len(n) < 80]
    return {"representatives": reps, "attorney": attorney}


def extract_probate(lines: List[str]) -> Dict[str, Any]:
    """Structured fields from a probate notice to creditors. Missing fields
    are None or empty, never guessed."""
    text = " ".join(lines)
    letters = _LETTERS_RE.search(text)
    letters_date = None
    if letters:
        letters_date = f"{letters.group(2)} {letters.group(1)}" + (f", {letters.group(3)}" if letters.group(3) else "")
    decedent = _first(_DECEDENT_RES, text)
    if decedent:
        decedent = re.sub(r"\s*,\s*$", "", decedent)
        decedent = re.split(r"\s+(?:Notice is hereby|Docket)", decedent)[0].strip(" ,")
    docket = None
    for line in lines[:12]:
        m = _DOCKET_LINE_RE.match(line)
        if m:
            docket = re.sub(r"\s+", "", m.group(1)).strip("-").upper()
            break
    if docket is None:
        m = _DOCKET_RE.search(text)
        if m:
            docket = m.group(1).strip("-").upper()
    died = _DIED_RE.search(text)
    letters_type = _LETTERS_TYPE_RE.search(text)
    signers = _parse_signers(_signature_block(lines))
    return {
        "county": _county(text),
        "docket": docket,
        "decedent": decedent,
        "date_of_death": died.group(1) if died else None,
        "letters_date": letters_date,
        "letters_type": letters_type.group(1).lower() if letters_type else None,
        "representatives": signers["representatives"],
        "attorney": signers["attorney"],
    }


def extract_court_sale(lines: List[str]) -> Dict[str, Any]:
    """Minimal fields for a partition / court-ordered sale notice. These need
    a human read (or a later parser) for the property; keep the evidence."""
    text = " ".join(lines)
    m = _DOCKET_RE.search(text)
    return {
        "county": _county(text),
        "docket": m.group(1).strip("-").upper() if m else None,
        "headline": " ".join(lines[:3])[:300],
    }
