"""Who can sign for a company that owns a property.

Pure functions (no network, no database). The lane review modules call
find() with the dated records they already hold for a row and get back one
answer:

  verified  a dated record names a person in a role for this exact entity:
            "executed by Example Holdings, LLC, by its member, Jane Doe" in
            the trustee sale notice for the parcel, or a signature block
            "EXAMPLE HOLDINGS, LLC By: Jane Doe Its: Managing Member".
  path      a lead toward the signer that is NOT a signer: a registered
            agent, an attention or care-of line on the county mailing
            address, a guarantor, a person named beside the entity in a tax
            suit, or an attention name on another entity at the same P.O.
            box. Always labeled "Path to signer (not verified)".
  none      nothing in the records held.

Rules that keep this strict:

  - The entity must match exactly after normalizing punctuation and the
    legal suffix (LLC, Inc, Corp, Co). A longer or shorter company name is
    a different company.
  - The person must be tied to the entity by the document's own grammar
    ("by X, its Member", "By: X Its: Manager", "X, President of ENTITY").
    A shared surname, a co-borrower, an interested party, a lender's
    officer or a trustee is never a signer.
  - A name that is itself a company (a registered agent service, a bank, a
    title company) is never a signer.
  - A verified signer needs a dated source. An undated one is a path.

Nothing here reads the Tennessee Secretary of State site.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

VERSION = "entity-signer-1"
PATH_LABEL = "Path to signer (not verified)"

# ── names ────────────────────────────────────────────────────────────────────

_SUFFIX_FORMS = (
    (r"L\s*L\s*C|LIMITED LIABILITY (?:COMPANY|CO)", "LLC"),
    (r"INCORPORATED|INC", "INC"),
    (r"CORPORATION|CORP", "CORP"),
    (r"COMPANY|CO", "CO"),
    (r"LIMITED PARTNERSHIP|L\s*P", "LP"),
    (r"LIMITED|LTD", "LTD"),
)
_SUFFIX_TEXT = {"LLC": r"L\.?\s?L\.?\s?C\.?|Limited\s+Liability\s+Co(?:mpany|\.)?",
                "INC": r"Inc(?:orporated|\.)?", "CORP": r"Corp(?:oration|\.)?", "CO": r"Co(?:mpany|\.)?",
                "LP": r"L\.?\s?P\.?|Limited\s+Partnership", "LTD": r"Ltd\.?|Limited"}
_COMPANY_WORD = re.compile(
    r"\b(LLC|L L C|INC|INCORPORATED|CORP|CORPORATION|COMPANY|CO|LP|LLP|LTD|PLLC|PC|TRUST|BANK|N A|NA|FSB|ASSOCIATION|"
    r"ASSN|ASSOCIATES|SERVICES?|SERVICE CO|AGENTS?|TITLE|HOLDINGS?|PROPERTIES|PROPERTY|INVESTMENTS?|PARTNERS|"
    r"PARTNERSHIP|GROUP|FOUNDATION|MINISTRIES|CHURCH|MORTGAGE|LENDING|FINANCIAL|CAPITAL|FUND|MANAGEMENT|REALTY|"
    r"ENTERPRISES?|SYSTEMS?|SOLUTIONS|INN|SUITES|HOTEL|APARTMENTS?|HOMES|DEPARTMENT|COUNTY|CITY|STATE|GOVERNMENT|"
    r"AUTHORITY|UNKNOWN|OCCUPANTS?|TENANTS?|HEIRS|ESTATE|USA|AMERICA)\b")
_NAME_SUFFIX = re.compile(r"\b(JR|SR|II|III|IV)\b")


def _flat(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def entity_key(name: Any) -> str:
    """One key per legal entity name. "Stanley Village 66, LLC", "STANLEY
    VILLAGE 66 L.L.C." and "Stanley Village 66 Llc" agree; a name with one
    more or one fewer word does not."""
    s = re.sub(r"&", " AND ", str(name or "").upper())
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^THE ", "", s)
    for pattern, short in _SUFFIX_FORMS:
        s = re.sub(rf"\b(?:{pattern})$", short, s)
    return re.sub(r"\s+", " ", s).strip()


def same_entity(a: Any, b: Any) -> bool:
    ka, kb = entity_key(a), entity_key(b)
    return bool(ka) and ka == kb


def is_company_name(name: Any) -> bool:
    flat = re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper())
    return bool(_COMPANY_WORD.search(flat) or re.search(r"\d", flat))


def person_key(name: Any) -> str:
    """Order-independent key for a person, so "WILLIAMS JOE V III" and
    "Joe V. Williams III" count once. Initials and suffixes are dropped."""
    flat = _NAME_SUFFIX.sub(" ", re.sub(r"[^A-Z ]", " ", str(name or "").upper()))
    return " ".join(sorted(t for t in flat.split() if len(t) > 1))


def is_person_name(name: Any) -> bool:
    """A usable natural-person name: two or more words, at least two of them
    longer than an initial, and no company word or digit."""
    if is_company_name(name):
        return False
    return len(person_key(name).split()) >= 2


def _entity_regex(entity: str) -> Optional[re.Pattern]:
    """The entity as it may be printed in running text."""
    key = entity_key(entity)
    tokens = key.split()
    if len(tokens) < 2:
        return None
    parts = []
    for n, tok in enumerate(tokens):
        if n == len(tokens) - 1 and tok in _SUFFIX_TEXT:
            parts.append(f"(?:{_SUFFIX_TEXT[tok]})")
        elif tok == "AND":
            parts.append(r"(?:and|&)")
        else:
            parts.append(re.escape(tok))
    body = r"[\s,.\-]*".join(parts)
    return re.compile(rf"(?<![A-Za-z0-9])(?:The\s+)?{body}(?![A-Za-z0-9])", re.I)


# A word that may come right before a company name without being part of it.
_LEAD_IN = {"by", "from", "to", "and", "of", "that", "whereas", "said", "unto", "for", "borrower", "grantor",
            "mortgagor", "owner", "is", "was", "as", "property", "the", "debtor", "maker", "against", "or"}


def _stands_alone(text: str, start: int) -> bool:
    """False when the match is the tail of a longer name ("North Example
    Holdings, LLC" does not match "Example Holdings, LLC")."""
    before = text[:start].rstrip()
    if not before or before[-1] in ",;:.()[]\"'“”‘’—–-/":
        return True
    word = re.search(r"([A-Za-z0-9&]+)$", before)
    return bool(word) and word[1].lower() in _LEAD_IN


# ── signer wording ───────────────────────────────────────────────────────────

_ROLE_WORDS = (
    r"managing\s+member|sole\s+member|chief\s+manager|general\s+manager|managing\s+partner|general\s+partner|"
    r"authorized\s+(?:signatory|signer|agent|representative|member|officer|manager|person)|"
    r"chief\s+executive\s+officer|chief\s+financial\s+officer|chief\s+operating\s+officer|vice[\s-]president|"
    r"president|secretary|treasurer|member|manager|partner|owner|ceo|cfo|coo|officer|director|registered\s+agent")
_ROLE = rf"(?i:{_ROLE_WORDS})"
_STOP = (r"(?i:AND|OR|ITS|IT'S|AS|TO|THE|OF|BY|DATED|INDIVIDUALLY|TRUSTEE|FOR|AN|IN|ON|WITH|WHEREAS|TITLE|NAME|"
         r"PRINTED|DATE|STATE|COUNTY|NOTARY|SUBSTITUTE|SUCCESSOR|HIS|HER|THEIR)")
_TOKEN = rf"(?!{_STOP}\b)[A-Z][A-Za-z'’\-]*\.?"
_NAME = rf"{_TOKEN}(?:\s+{_TOKEN}){{1,3}}(?:,?\s+(?:Jr|JR|Sr|SR|II|III|IV)\b\.?)?"
_ITS = r"(?i:it[’'`]?s|their)"
_DESCRIPTOR = (r"(?:\s*,?\s*(?i:an?|the)\s+[A-Za-z\- ]{0,40}?(?i:limited\s+liability\s+company|non-?profit\s+corporation|"
               r"professional\s+corporation|corporation|limited\s+partnership|general\s+partnership|partnership|"
               r"company))?")
_LINK = r"\s*[,;]?\s*(?i:acting\s+)?(?i:herein\s+)?(?i:by\s+and\s+through|by|through)\s*:?\s*"
# After the entity: "..., a Tennessee limited liability company; by its member, Jane Doe"
_AFTER_ROLE_FIRST = re.compile(rf"^{_DESCRIPTOR}{_LINK}{_ITS}\s+(?P<role>{_ROLE})\s*[,:]?\s+(?P<name>{_NAME})")
# "..., by Jane Doe, its Managing Member" / "by Jane Doe, Authorized Signatory" / "By: Jane Doe Its: Manager"
_AFTER_NAME_FIRST = re.compile(
    rf"^{_DESCRIPTOR}{_LINK}(?P<name>{_NAME})\s*"
    rf"(?:,\s*(?:(?:{_ITS}|(?i:as|title|as\s+its|as\s+the))\s*:?\s+)?|\s+(?:{_ITS}|(?i:title))\s*:?\s+)(?P<role>{_ROLE})\b")
# Before the entity: "Jane Doe, Managing Member of Example Holdings, LLC"
_BEFORE = re.compile(rf"(?P<name>{_NAME})\s*,\s*(?:(?i:as|the|its|as\s+the)\s+)?(?P<role>{_ROLE})\s+(?i:of|for)\s+$")
_GUARANTOR = re.compile(rf"(?P<name>{_NAME})\s*(?:\(\s*(?i:limited\s+)?(?i:guarantor)\s*\)|,\s*(?i:limited\s+)?(?i:guarantor)\b)")
_PATH_ROLES = {"registered agent"}


def _role_text(role: str) -> str:
    return re.sub(r"[\s-]+", " ", role).strip().lower()


def _quote(text: str, start: int, end: int) -> str:
    a, b = max(0, start - 40), min(len(text), end + 40)
    return ("..." if a else "") + text[a:b].strip() + ("..." if b < len(text) else "")


def finding(*, name: str, role: str, kind: str, src: Optional[Dict[str, Any]], quote: str = "", note: str = "",
            verified: bool = False) -> Dict[str, Any]:
    return {"name": _flat(name).strip(" ,;."), "role": role, "kind": kind, "verified": bool(verified),
            "quote": _flat(quote)[:300], "note": note, "source": src}


def signers_in_text(text: Any, entity: Any, src: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """People a document names in a role for this exact entity.

    src is the dated source of the document (pilot_review.source()). A role
    with authority is verified when src carries a date; a registered agent
    and a guarantor are paths.
    """
    flat = _flat(text)
    pattern = _entity_regex(str(entity or ""))
    if not flat or pattern is None:
        return []
    dated = bool(src and src.get("date"))
    out: List[Dict[str, Any]] = []
    for m in pattern.finditer(flat):
        if not _stands_alone(flat, m.start()):
            continue
        hit = None
        after = flat[m.end():m.end() + 320]
        for rx in (_AFTER_ROLE_FIRST, _AFTER_NAME_FIRST):
            got = rx.match(after)
            if got:
                hit = (got["name"], got["role"], m.start(), m.end() + got.end())
                break
        if hit is None:
            got = _BEFORE.search(flat[max(0, m.start() - 200):m.start()])
            if got:
                hit = (got["name"], got["role"], max(0, m.start() - 200) + got.start(), m.end())
        if hit is None:
            continue
        name, role, a, b = hit
        if not is_person_name(name) or same_entity(name, entity):
            continue
        role = _role_text(role)
        if role in _PATH_ROLES:
            out.append(finding(name=name, role=role, kind="registered_agent", src=src, quote=_quote(flat, a, b),
                               note="A registered agent accepts service for the company; it is not shown to be a member, "
                                    "manager or officer."))
            continue
        out.append(finding(name=name, role=role, kind="signed_for_entity", src=src, quote=_quote(flat, a, b),
                           verified=dated, note="" if dated else "The document is undated, so this is a path, not a verified signer."))
    if out or pattern.search(flat):
        for g in _GUARANTOR.finditer(flat):
            if is_person_name(g["name"]):
                out.append(finding(name=g["name"], role="guarantor", kind="guarantor", src=src,
                                   quote=_quote(flat, g.start(), g.end()),
                                   note="A guarantor of the company's loan; the document does not say this person can sign for the company."))
    return out


# ── county mailing lines ─────────────────────────────────────────────────────

_CARE = re.compile(r"(?:(?<![A-Z])ATTN\b:?|(?<![A-Z])ATTENTION\b:?|\bC/O\b|(?:^|\s)%)\s*(?P<who>[A-Z0-9][A-Z0-9 .&'\-]*)", re.I)
_ADDRESS_START = re.compile(r"\s+(?:\d{1,6}\s+[A-Z]|P\.?\s?O\.?\s+BOX|PO\s+BOX|SUITE\b|STE\b|#)", re.I)


def care_of_names(line: Any) -> List[Tuple[str, str]]:
    """(marker, name) for every attention or care-of name on a county owner
    or mailing line. "COWART ST HFP/CHA LLCATTN E GULLEN" and "827 W MCKENNIE
    AVE C/O BE A HELPING HAND FOUNDATION, NASHVILLE TN" both yield one."""
    text = _flat(line).upper()
    text = re.sub(r"(?<=[A-Z])(ATTN|ATTENTION)\b", r" \1", text)     # "LLCATTN" as some rolls print it
    out = []
    for m in _CARE.finditer(text):
        who = m["who"].split(",")[0]
        cut = _ADDRESS_START.search(" " + who)
        if cut:
            who = (" " + who)[:cut.start()]
        who = who.strip(" .-&")
        if len(who) >= 3:
            marker = "attention line" if m.group(0).strip().upper().startswith("ATT") else "care-of line"
            out.append((marker, who))
    return out


def mailing_key(mailing: Any) -> Optional[str]:
    """Key for a P.O. box mailing address (box number plus ZIP). Street
    addresses are not keyed: an office building or a registered agent's
    address is shared by unrelated companies."""
    text = _flat(mailing).upper()
    box = re.search(r"\bP\.?\s?O\.?\s*BOX\s+(\d+)\b", text)
    zip5 = re.findall(r"(?<!\d)(\d{5})(?:-\d{4})?(?!\d)", text)
    return f"POBOX {box[1]}|{zip5[-1]}" if box and zip5 else None


def mail_paths(entity: Any, lines: Iterable[Tuple[Any, Optional[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
    out = []
    for line, src in lines:
        for marker, who in care_of_names(line):
            if same_entity(who, entity):
                continue
            person = is_person_name(who)
            out.append(finding(
                name=who, role=marker, kind="mailing_name", src=src, quote=_flat(line),
                note=(f"Named on the {marker} of the county mailing address. That shows who receives the company's mail, "
                      "not who can sign for it.") if person else
                     (f"The {marker} names another business or a partial name, not a person with a stated role.")))
    return out


def build_mailing_index(entries: Iterable[Tuple[Any, Any, Optional[Dict[str, Any]]]]) -> Dict[str, List[Dict[str, Any]]]:
    """{P.O. box key: [{name, entity, source}]} from (entity, mailing line,
    source) triples: the people named on an attention or care-of line at
    each box."""
    index: Dict[str, List[Dict[str, Any]]] = {}
    for entity, line, src in entries:
        key = mailing_key(line)
        if not key:
            continue
        for _, who in care_of_names(line):
            if is_person_name(who):
                slot = index.setdefault(key, [])
                if not any(person_key(s["name"]) == person_key(who) and same_entity(s["entity"], entity) for s in slot):
                    slot.append({"name": who, "entity": _flat(entity), "source": src})
    return index


def shared_mailing_paths(entity: Any, mailing: Any, index: Optional[Dict[str, List[Dict[str, Any]]]],
                         src: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for hit in (index or {}).get(mailing_key(mailing) or "", []):
        if same_entity(hit["entity"], entity):
            continue
        out.append(finding(
            name=hit["name"], role="attention name at the same P.O. box", kind="shared_mailing", src=hit["source"] or src,
            quote=_flat(mailing),
            note=(f"{hit['entity']} uses the same P.O. box and its county mailing line names this person. "
                  "An inference from a shared mailing address, weaker than a name on this company's own record.")))
    return out


def suit_party_paths(entity: Any, parties: Iterable[Any], src: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """People named beside the entity in a tax suit publication. The
    publication does not say what they are to the company."""
    return [finding(name=p, role="named with the company in the tax suit", kind="suit_party", src=src,
                    note="Named for service beside the company; the publication does not state the relationship "
                         "(officer, prior owner and lienholder are all possible).")
            for p in parties or [] if p and not same_entity(p, entity) and is_person_name(p)]


# ── one answer per entity ────────────────────────────────────────────────────

def resolve(entity: Any, findings: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Collapse findings to one answer. The same person found twice is kept
    once (the verified record wins); two different verified people are both
    kept. Stable across repeat runs: no timestamps."""
    signers: List[Dict[str, Any]] = []
    paths: List[Dict[str, Any]] = []
    for f in findings:
        if f.get("verified") and f.get("kind") == "signed_for_entity" and (f.get("source") or {}).get("date"):
            if not any(person_key(s["name"]) == person_key(f["name"]) for s in signers):
                signers.append(f)
    named = {person_key(s["name"]) for s in signers}
    for f in findings:
        if f.get("verified") and f in signers:
            continue
        key = person_key(f["name"]) or entity_key(f["name"])
        if key in named or any((person_key(p["name"]) or entity_key(p["name"])) == key and p["kind"] == f["kind"] for p in paths):
            continue
        paths.append({**f, "verified": False})
    return {"version": VERSION, "entity": _flat(entity), "status": "verified" if signers else "path" if paths else "none",
            "signers": signers, "paths": paths}


def find(entity: Any, *, texts: Sequence[Tuple[Any, Optional[Dict[str, Any]]]] = (),
         mail_lines: Sequence[Tuple[Any, Optional[Dict[str, Any]]]] = (),
         suit_parties: Optional[Tuple[Iterable[Any], Optional[Dict[str, Any]]]] = None,
         mailing: Any = None, mailing_index: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> Dict[str, Any]:
    """The signer answer for one company owner from the records held.

    texts:        (document text, dated source) pairs for THIS parcel: the
                  trustee sale notice, a recorded deed or deed of trust.
    mail_lines:   (county owner or mailing line, source) pairs.
    suit_parties: (names printed beside the owner in the tax suit, source).
    mailing:      the owner's county mailing address, for the shared P.O.
                  box lookup against mailing_index (build_mailing_index()).
    """
    found: List[Dict[str, Any]] = []
    for text, src in texts:
        found.extend(signers_in_text(text, entity, src))
    found.extend(mail_paths(entity, mail_lines))
    if suit_parties:
        found.extend(suit_party_paths(entity, suit_parties[0], suit_parties[1]))
    if mailing and mailing_index:
        found.extend(shared_mailing_paths(entity, mailing, mailing_index, mail_lines[0][1] if mail_lines else None))
    return resolve(entity, found)


# ── wording for cards ────────────────────────────────────────────────────────

def _cite(src: Optional[Dict[str, Any]]) -> str:
    if not src:
        return "source not recorded"
    return f"{src.get('label')}" + (f", {src['date']}" if src.get("date") else ", undated")


def signer_basis(answer: Dict[str, Any]) -> str:
    """Decider basis for a verified signer, quoting the record."""
    first = answer["signers"][0]
    text = (f"{_cite(first['source'])} names {first['name']} as {first['role']} of {answer['entity']}: "
            f"\"{first['quote']}\". Authority today is not confirmed against the Secretary of State record.")
    others = [f"{s['name']} ({s['role']})" for s in answer["signers"][1:]]
    if others:
        text += " Also named in a role for the company: " + "; ".join(others) + "."
    return text


def path_lines(answer: Optional[Dict[str, Any]]) -> List[str]:
    """One verify_next line per path, each labeled as not verified."""
    out = []
    for p in (answer or {}).get("paths") or []:
        out.append(f"{PATH_LABEL}: {p['name']}, {p['role']} ({_cite(p['source'])}). {p['note']} "
                   "Confirm the person's role for the company before treating them as the signer.")
    return out


def hold_extras(entity: Any, **sources: Any) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """(answer when a signer is verified else None, path lines) for a lane
    that only needs to know whether its company hold still applies."""
    answer = find(entity, **sources)
    return (answer if answer["status"] == "verified" else None), path_lines(answer)
