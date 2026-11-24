"""
County Register of Deeds index replies: one staging shape, small parsers.

Tennessee Registers answer a public records request for their recording
index of substitute trustee appointments and lis pendens with whatever
their software prints: a PDF listing, a spreadsheet, a CSV. Each layout
gets one small parser that turns the file into IndexEntry records. The
rest (party split, lead keys, the staging payload, the evidence block) is
shared, so the next county is usually one parser, or none when the county
uses a layout already handled.

    listing = parse_file("recording_listing", "reply.pdf")
    leads = build_leads(listing, county="Putnam", received="2026-11-23")

What a row carries:

  county, the book and page or instrument number, recording date,
  instrument type as printed, the parties split into borrower, lender and
  trustee with the basis for the split stated, the property reference when
  the index prints one, and an evidence block naming the office, the
  listing and the day it was received.

What a row never carries: a lender or trustee as the owner. An index lists
the lender and the borrowers of an appointment in one unlabeled column; a
name with an institution word goes to the lender side, and only what is
left is staged as borrower. The borrower is not a verified owner either:
preforeclosure_review checks the county roll before any card says so.

A lis pendens entry lists two groups of parties without saying who sued
whom. It is staged with owner_not_established and no owner fields.

Lead key: sha1("<county>_register_index|<county>|<reference>"), the same
key the first Putnam load used, so a repeat load inserts nothing.

No network. The PDF reader needs pdfplumber (already a dependency).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

SOURCE_SUFFIX = "_register_index"
APPOINTMENT = "SUBSTITUTION_OF_TRUSTEE"
LIS_PENDENS = "LIS_PENDENS"
PARTY_BASIS = "listing grantors, lender names split by keyword"

# All 95 Tennessee counties as the staging table spells them.
TN_COUNTIES = (
    "Anderson Bedford Benton Bledsoe Blount Bradley Campbell Cannon Carroll Carter Cheatham Chester Claiborne Clay "
    "Cocke Coffee Crockett Cumberland Davidson Decatur DeKalb Dickson Dyer Fayette Fentress Franklin Gibson Giles "
    "Grainger Greene Grundy Hamblen Hamilton Hancock Hardeman Hardin Hawkins Haywood Henderson Henry Hickman "
    "Houston Humphreys Jackson Jefferson Johnson Knox Lake Lauderdale Lawrence Lewis Lincoln Loudon McMinn McNairy "
    "Macon Madison Marion Marshall Maury Meigs Monroe Montgomery Moore Morgan Obion Overton Perry Pickett Polk "
    "Putnam Rhea Roane Robertson Rutherford Scott Sequatchie Sevier Shelby Smith Stewart Sullivan Sumner Tipton "
    "Trousdale Unicoi Union Van_Buren Warren Washington Wayne Weakley White Williamson Wilson"
).split()
TN_COUNTIES = tuple(name.replace("_", " ") for name in TN_COUNTIES)


def county_slug(county: str) -> str:
    """'Van Buren County' -> 'van_buren'."""
    text = re.sub(r"\s+county$", "", str(county or "").strip(), flags=re.I)
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def canonical_county(county: str) -> str:
    """The county as staged ('Putnam'). Raises ValueError for a name that is
    not a Tennessee county, so a typo cannot open a new source."""
    slug = county_slug(county).replace("_", "")
    for name in TN_COUNTIES:
        if county_slug(name).replace("_", "") == slug:
            return name
    raise ValueError(f"not a Tennessee county: {county!r}")


def source_name(county: str) -> str:
    return county_slug(canonical_county(county)) + SOURCE_SUFFIX


def register_sources() -> List[str]:
    """Every bot_source a Register index load can have written."""
    return [county_slug(name) + SOURCE_SUFFIX for name in TN_COUNTIES]


def lead_key(county: str, reference: str) -> str:
    slug = county_slug(canonical_county(county))
    return hashlib.sha1(f"{slug}{SOURCE_SUFFIX}|{slug}|{reference}".encode("utf-8")).hexdigest()


# ── The common shape ────────────────────────────────────────────────────────


@dataclass
class IndexEntry:
    """One recorded instrument as the Register's index prints it."""

    reference: str                       # "RB 1736-282" or an instrument number; part of the lead key
    recorded: str                        # ISO recording date
    doc_type: str                        # instrument type as printed
    first_parties: List[str] = field(default_factory=list)    # grantor column, or the parties listed first
    second_parties: List[str] = field(default_factory=list)   # grantee column, or the parties listed second
    instrument_date: Optional[str] = None
    code: Optional[str] = None           # the office's short type code, when printed
    property_ref: Optional[str] = None   # property as indexed, when the listing has one
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def kind(self) -> str:
        return classify(self.doc_type, self.code)


@dataclass
class Listing:
    """A parsed reply: its entries and how the office titled it."""

    entries: List[IndexEntry]
    title: str                           # e.g. "Recording Listing by Date and Time, 2026-10-01 thru 2026-11-20, printed ..."
    layout: str
    county: Optional[str] = None         # county printed on the listing, when it prints one
    skipped: List[str] = field(default_factory=list)   # lines the parser could not place


_APPOINTMENT = re.compile(r"SUB\w*\.?\s+(OF\s+)?TRUSTEE|APPOINT\w*\b.*TRUSTEE|SUCCESSOR\s+TRUSTEE|SUB\s+TRUSTEE", re.I)
_LIS_PENDENS = re.compile(r"LIS\s+PENDENS", re.I)
_APPOINTMENT_CODES = {"SUBTR", "APTR", "SOT", "AST", "APST", "SUBT"}
_LIS_PENDENS_CODES = {"LLP", "LISP", "LISPEN"}


def classify(doc_type: Optional[str], code: Optional[str] = None) -> str:
    """'appointment', 'lis_pendens' or 'other' (not staged)."""
    text = str(doc_type or "")
    short = str(code or "").upper()
    if _LIS_PENDENS.search(text) or short in _LIS_PENDENS_CODES:
        return "lis_pendens"
    if _APPOINTMENT.search(text) or short in _APPOINTMENT_CODES:
        return "appointment"
    return "other"


# Institution words. A grantor with one of these is never staged as a
# borrower. A company that really is the borrower lands on the lender side
# too; that loses a name, which is the safe direction.
_INSTITUTION = re.compile(
    r"\b(LLC|L L C|LP|L P|LLP|PLLC|INC|CORP|CORPORATION|COMPANY|CO|BANK|BANCORP|BANCSHARES|MORTGAGE|MTG|LENDING|"
    r"LENDER|LOANS?|FINANCIAL|FINANCE|FUNDING|SERVIC\w*|CREDIT UNION|FCU|NATIONAL ASSOCIATION|N A|NA|FSB|FEDERAL|"
    r"SAVINGS|CAPITAL|ACQUISITION|SECURITIZATION|SECURITIES|ASSET|ASSETS|TRUST|TRUSTEE|ASSOCIATION|ASSN|HOUSING|"
    r"AUTHORITY|AGENCY|DEPARTMENT|DEPT|FANNIE|FREDDIE|HUD|SECRETARY|VETERANS|USDA|UNITED STATES|MERS|"
    r"REGISTRATION SYSTEMS|HOLDINGS|PARTNERS|PARTNERSHIP|FUND|INVESTMENTS?|PROPERTIES|GROUP|"
    r"WELLS FARGO|JPMORGAN|TRUIST|PENNYMAC|NATIONSTAR|USAA|SHELLPOINT|NEWREZ|CITIBANK|CITIMORTGAGE|DEUTSCHE|THDA|"
    r"OCWEN|MR COOPER|FIRST HORIZON)\b"
)


def is_institution(name: Any) -> bool:
    text = re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper())).strip()
    if not text or re.search(r"\d", text):
        return True                      # a person on an index has no digits
    return bool(_INSTITUTION.search(text)) or len(text.split()) < 2


def split_parties(entry: IndexEntry) -> Dict[str, Any]:
    """Borrowers, lenders and trustees of an appointment, with the basis.

    The grantor column lists the lender and the borrowers together. Names
    with an institution word, names that repeat a grantee and one-word
    names go to the lender side. The grantee column is the trustee.
    """
    trustees = [n for n in entry.second_parties if n]
    taken = {re.sub(r"\W+", " ", t.upper()).strip() for t in trustees}
    lenders = [n for n in entry.first_parties
               if is_institution(n) or re.sub(r"\W+", " ", n.upper()).strip() in taken]
    borrowers = [n for n in entry.first_parties if n not in lenders]
    # Some offices print the lender in the grantee column as well ("CREDIT
    # HUMAN FEDERAL CREDIT UNION" on both sides of one appointment). A bank or
    # credit union, or a grantee that repeats a grantor-side lender, is the
    # lender. It is never staged as the substitute trustee.
    lender_names = {_party_stem(n) for n in lenders}
    repeated = [t for t in trustees if _DEPOSIT_INSTITUTION.search(_party_stem(t)) or _party_stem(t) in lender_names]
    notes: List[str] = []
    if repeated:
        trustees = [t for t in trustees if t not in repeated]
        lenders = lenders + [t for t in repeated if _party_stem(t) not in lender_names]
        notes.append("The listing prints " + ", ".join(repeated) + " in the grantee column. It is the lender, so it is "
                     "not staged as substitute trustee" + ("; the listing names no other grantee" if not trustees else ""))
    return {"borrowers": borrowers, "lenders": lenders, "trustees": trustees, "notes": notes,
            "basis": PARTY_BASIS + ("; lender printed in the grantee column moved to the lender side" if repeated else "")}


_DEPOSIT_INSTITUTION = re.compile(r"\b(BANK|BANCORP|BANCSHARES|CREDIT UNION|FCU|FSB|SAVINGS|NATIONAL ASSOCIATION)\b")


def _party_stem(name: Any) -> str:
    """A party name for comparison: upper case, no punctuation, no trailing
    AKA / FKA / DBA left over from a name that ran onto a second line."""
    text = re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper())).strip()
    return re.sub(r"(?:\s+(?:AKA|FKA|DBA|NKA|A K A|F K A|D B A))+$", "", text)


# ── Staging payload ─────────────────────────────────────────────────────────


def evidence_block(county: str, title: str, received: str, sender: Optional[str] = None) -> Dict[str, str]:
    office = f"{canonical_county(county)} County Register of Deeds"
    datetime.strptime(received, "%Y-%m-%d")          # a wrong date fails here, not in the CRM
    return {"kind": "records_request_reply", "office": office, "listing": title, "received": received,
            "from": sender or f"{office} (email)"}


def street_of(property_ref: Optional[str]) -> Optional[str]:
    """The street from a property reference, only when it opens with a house
    number: '120 HELEN PLACE; Phase: IV; Lot: 81' -> '120 Helen Place'."""
    first = str(property_ref or "").split(";")[0].strip()
    return first.title() if re.match(r"^\d+[A-Za-z]?\s+\S+", first) else None


def build_lead(entry: IndexEntry, county: str, evidence: Dict[str, str], layout: str) -> Optional[Dict[str, Any]]:
    """The staging fields for one entry (LeadPayload keyword arguments), or
    None for an instrument type this lane does not stage."""
    name = canonical_county(county)
    slug = county_slug(name)
    base = {"bot_source": slug + SOURCE_SUFFIX, "pipeline_lead_key": lead_key(name, entry.reference), "county": name}
    raw = {"source": f"{slug}_register_listing", "state": "TN", "book_page": entry.reference, "code": entry.code,
           "doc_type": entry.doc_type, "recorded": entry.recorded, "layout": layout, "evidence": evidence}
    street = street_of(entry.property_ref)
    address = f"{street}, {name} County, TN" if street else None
    if entry.kind == "appointment":
        parties = split_parties(entry)
        borrowers, lenders, trustees = parties["borrowers"], parties["lenders"], parties["trustees"]
        note = (f"Substitute trustee appointed, recorded {entry.recorded} ({name} {entry.reference})"
                + (f" by {lenders[0]}" if lenders else "") + (f"; trustee {', '.join(trustees)}" if trustees else "")
                + ("; no substitute trustee printed (the grantee column repeats the lender)"
                   if parties["notes"] and not trustees else "")
                + (f". Property as indexed: {entry.property_ref}." if entry.property_ref
                   else ". Index only: no address or deed of trust on the listing."))
        raw.update({"appointment_date": entry.instrument_date, "lenders": lenders, "borrowers": borrowers,
                    "substitute_trustees": trustees, "party_basis": parties["basis"]})
        if parties["notes"]:
            # The columns exactly as printed stay on the record beside the reading of them.
            raw.update({"party_notes": parties["notes"], "grantors_as_printed": list(entry.first_parties),
                        "grantees_as_printed": list(entry.second_parties)})
        if entry.property_ref:
            raw["property_as_indexed"] = entry.property_ref
        return {**base, "distress_type": APPOINTMENT, "property_address": address,
                "full_name": borrowers[0] if borrowers else None,
                "owner_name_records": "; ".join(borrowers) or None, "admin_notes": note, "raw_payload": raw}
    if entry.kind == "lis_pendens":
        note = (f"Lis pendens recorded {entry.recorded} ({name} {entry.reference}). "
                f"Listed first: {', '.join(entry.first_parties)}. "
                f"Listed second: {', '.join(entry.second_parties) or 'none'}. "
                "The listing does not say who sued whom or why. "
                f"Property as indexed: {entry.property_ref}.")
        raw.update({"instrument_date": entry.instrument_date, "first_parties": list(entry.first_parties),
                    "second_parties": list(entry.second_parties), "property_as_indexed": entry.property_ref,
                    "owner_not_established": True})
        return {**base, "distress_type": LIS_PENDENS, "property_address": address, "admin_notes": note,
                "raw_payload": raw}
    return None


def build_leads(listing: Listing, county: str, received: str, sender: Optional[str] = None) -> List[Dict[str, Any]]:
    """Staging fields for every appointment and lis pendens on a listing.
    Raises ValueError when the listing prints a different county."""
    name = canonical_county(county)
    if listing.county and county_slug(listing.county) != county_slug(name):
        raise ValueError(f"the listing is headed {listing.county!r}, not {name} County")
    evidence = evidence_block(name, listing.title, received, sender)
    leads, seen = [], set()
    for entry in listing.entries:
        lead = build_lead(entry, name, evidence, listing.layout)
        if lead and lead["pipeline_lead_key"] not in seen:
            seen.add(lead["pipeline_lead_key"])
            leads.append(lead)
    return leads


# ── Layout: "Recording Listing by Date and Time" (PDF) ──────────────────────
#
# One head row per instrument: book type, book-page, recording date and
# time, type code, instrument date, pages, consideration, instrument type.
# Under it, two name columns headed GRANTOR NAMES AND ADDRESS and GRANTEE
# NAMES AND ADDRESS, each name followed by a one-letter marker in its own
# column, and an optional "Property>" line. First seen from Putnam County.
#
# The parser works on positioned text runs, because the two name columns
# sit side by side and only the x position says which one a name is in.

Run = Dict[str, Any]                     # page, top, x0, x1, text

_DATE = re.compile(r"^\d{1,2}/\s?\d{1,2}/\d{4}$")
_TIME = re.compile(r"^\d{1,2}:\d{2}\s?[ap]m?$", re.I)
_REFERENCE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)?$")
_MARKER = re.compile(r"^[A-Z]{1,2}$")
_COLUMN_SLACK = 2.5


def _iso(mdy: str) -> str:
    return datetime.strptime(re.sub(r"\s", "", mdy), "%m/%d/%Y").date().isoformat()


def read_pdf_runs(path: str) -> List[Run]:
    """Positioned text runs of a PDF, in the order the file draws them."""
    import pdfplumber

    runs: List[Run] = []
    with pdfplumber.open(path) as pdf:
        for number, page in enumerate(pdf.pages, start=1):
            for word in page.extract_words(keep_blank_chars=True, use_text_flow=True, x_tolerance=1.5):
                text = re.sub(r"\s+", " ", word["text"]).strip()
                if text:
                    runs.append({"page": number, "top": float(word["top"]), "x0": float(word["x0"]),
                                 "x1": float(word["x1"]), "text": text})
    return runs


def _rows(runs: Sequence[Run]) -> List[List[Run]]:
    """Runs grouped into printed rows, top to bottom, each row left to right."""
    rows: List[List[Run]] = []
    for run in sorted(runs, key=lambda r: (r["page"], r["top"], r["x0"])):
        if rows and rows[-1][0]["page"] == run["page"] and abs(rows[-1][0]["top"] - run["top"]) <= 2.5:
            rows[-1].append(run)
        else:
            rows.append([run])
    return [sorted(row, key=lambda r: r["x0"]) for row in rows]


def _head(row: List[Run]) -> Optional[Dict[str, Any]]:
    """The fields of an instrument head row, or None for any other row."""
    texts = [r["text"] for r in row]
    dates = [t for t in texts if _DATE.match(t)]
    if len(dates) != 2 or len(texts) < 6:
        return None
    if not re.fullmatch(r"[A-Z]{1,4}", texts[0]) or not _REFERENCE.match(texts[1]) or not re.search(r"\d", texts[1]):
        return None
    rest = [t for t in texts[2:] if not _DATE.match(t) and not _TIME.match(t) and not t.startswith("$")]
    codes = [t for t in rest if re.fullmatch(r"[A-Z][A-Z0-9]{1,7}", t)]
    doc_types = [t for t in rest if not re.fullmatch(r"\d+", t) and t not in codes[:1]]
    if not doc_types:
        return None
    return {"reference": f"{texts[0]} {texts[1]}", "recorded": _iso(dates[0]), "instrument_date": _iso(dates[1]),
            "code": codes[0] if codes else None, "doc_type": doc_types[-1],
            "time": next((t for t in texts if _TIME.match(t)), None),
            "pages": next((int(t) for t in rest if re.fullmatch(r"\d+", t)), None)}


def parse_recording_listing(runs: Sequence[Run]) -> Listing:
    rows = _rows(runs)
    title, county, printed, span = "Recording Listing", None, None, None
    # Markers ("O") print in their own column. A long name can run into it
    # ("CREDIT UNION AKAO"), so learn where the column ends first.
    marker_ends = {round(r["x1"], 1) for row in rows for r in row if _MARKER.match(r["text"]) and len(r["text"]) == 1}
    entries: List[IndexEntry] = []
    skipped: List[str] = []
    current: Optional[IndexEntry] = None
    grantor_x = grantee_x = None
    in_header = True
    page = None
    for row in rows:
        if row[0]["page"] != page:
            page, in_header = row[0]["page"], True
        texts = [r["text"] for r in row]
        joined = " ".join(texts)
        if in_header:
            for text in texts:
                if text.lower().startswith("recording listing"):
                    title = text
                m = re.match(r"^(.+?) County$", text)
                if m and county is None:
                    county = m.group(1)
                m = re.match(r"^Printed:\s*(\d{1,2}/\s?\d{1,2}/\d{4})\s+(\S+)", text)
                if m and printed is None:
                    printed = f"{_iso(m.group(1))} {m.group(2)}"
                m = re.match(r"^Recording Date:\s*(\d{1,2}/\s?\d{1,2}/\d{4})\s+thru\s+(\d{1,2}/\s?\d{1,2}/\d{4})", text)
                if m and span is None:
                    span = f"{_iso(m.group(1))} thru {_iso(m.group(2))}"
            if "Reference" in texts or "Class" in texts:
                in_header = False                              # the column headings end the page header
                continue
        head = _head(row)
        if head:
            in_header = False
            current = IndexEntry(reference=head["reference"], recorded=head["recorded"], doc_type=head["doc_type"],
                                 instrument_date=head["instrument_date"], code=head["code"],
                                 extra={"time": head["time"], "pages": head["pages"]})
            entries.append(current)
            continue
        if "GRANTOR NAMES" in joined or "GRANTEE NAMES" in joined:
            in_header = False
            for r in row:
                if r["text"].startswith("GRANTOR NAMES"):
                    grantor_x = r["x0"]
                elif r["text"].startswith("GRANTEE NAMES"):
                    grantee_x = r["x0"]
            continue
        if in_header or current is None:
            continue
        label = next((n for n, r in enumerate(row) if r["text"].startswith("Property>")), None)
        if label is not None:
            # The label and its value can be separate runs on the same row.
            value = " ".join([row[label]["text"].split(">", 1)[1]] + [r["text"] for r in row[label + 1:]])
            current.property_ref = re.sub(r"\s+", " ", value).strip() or None
            row = row[:label]
        for r in row:
            text = r["text"]
            if _MARKER.match(text):
                continue                                       # the marker column
            elif grantor_x is not None and abs(r["x0"] - grantor_x) <= _COLUMN_SLACK:
                current.first_parties.append(_name(text, r["x1"], marker_ends))
            elif grantee_x is not None and abs(r["x0"] - grantee_x) <= _COLUMN_SLACK:
                name = _name(text, r["x1"], marker_ends)
                if name.upper() != current.doc_type.upper():   # "LIEN LIS PENDENS" printed as a placeholder
                    current.second_parties.append(name)
            else:
                skipped.append(f"{current.reference}: {text}")
    label = title + (f", {span}" if span else "") + (f", printed {printed}" if printed else "")
    return Listing(entries=entries, title=label, layout="recording_listing", county=county, skipped=skipped)


def _name(text: str, x1: float, marker_ends: set) -> str:
    """A party name without a marker letter printed into its last word."""
    if text.endswith("O") and any(abs(x1 - end) <= 1.5 for end in marker_ends):
        text = text[:-1]
    return re.sub(r"\s+", " ", text).strip()


# ── Layout registry ─────────────────────────────────────────────────────────
#
# To add a county whose reply is in a new layout: write a function that
# returns a Listing of IndexEntry records and add it here. Nothing else.

LAYOUTS: Dict[str, Tuple[str, Callable[[str], Listing]]] = {
    "recording_listing": ("PDF 'Recording Listing by Date and Time' with grantor and grantee name columns (Putnam)",
                          lambda path: parse_recording_listing(read_pdf_runs(path))),
}


def parse_file(layout: str, path: str) -> Listing:
    if layout not in LAYOUTS:
        raise ValueError(f"unknown layout {layout!r}; known: {', '.join(sorted(LAYOUTS))}")
    return LAYOUTS[layout][1](path)
