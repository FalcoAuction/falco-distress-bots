"""Who can sign for a company that owns a property.

Pure functions (no network, no database). The lane review modules call
find() with the dated records they hold for a row and get back one answer
with every person or contact placed in exactly one of three tiers:

  verified_authority  "Verified authority". A dated official record names
            the person in a role with authority for this exact entity:
            "executed by Example Holdings, LLC, by its member, Jane Doe" in
            the trustee sale notice for the parcel, or a signature block
            "EXAMPLE HOLDINGS, LLC By: Jane Doe Its: Managing Member".
            The only tier that may clear a signer hold.
  named_person        "Named person worth asking". An official record ties
            a person to the entity without stating authority: a care-of or
            attention name on the company's OWN county tax line, a person
            printed beside the company in a tax suit, a guarantor of the
            company's loan, the owner on a county business license issued
            in the company's exact name, or a role stated in an undated
            document.
  mailing_contact     "Registered agent or mailing contact". An agent, a
            care-of business, or an address-only link: an attention name on
            ANOTHER company at the same P.O. box, or a business license at
            the same address under a name without the entity type.

Rules that keep this strict:

  - The entity must match exactly after normalizing punctuation and the
    legal suffix (LLC, Inc, Corp, Co). A longer or shorter company name is
    a different company, and so is the same words with a different entity
    type ("Example Holdings LLC" is not "Example Holdings Inc").
  - The person must be tied to the entity by the document's own grammar
    ("by X, its Member", "By: X Its: Manager", "X, President of ENTITY").
    A shared surname, a co-borrower, an interested party, a lender's
    officer or a trustee is never a signer.
  - A name that is itself a company (a registered agent service, a bank, a
    title company) is never a signer.
  - A care-of line or a shared P.O. box can never establish authority. A
    shared P.O. box alone never lifts a person above mailing_contact.
  - Verified authority needs a dated source. An undated one is a named
    person.
  - Every finding carries its source, the role as printed, the document
    date and the next verification question.

Nothing here reads the Tennessee Secretary of State site.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

VERSION = "entity-signer-2"
VERIFIED, NAMED, CONTACT = "verified_authority", "named_person", "mailing_contact"
TIER_LABEL = {VERIFIED: "Verified authority",
              NAMED: "Named person worth asking (authority not verified)",
              CONTACT: "Registered agent or mailing contact (not a signer)"}
_TIER_RANK = {VERIFIED: 0, NAMED: 1, CONTACT: 2}
# Finding kind -> tier when the finding is not a verified signer.
_KIND_TIER = {"signed_for_entity": NAMED, "mailing_name": NAMED, "suit_party": NAMED, "guarantor": NAMED,
              "license_owner": NAMED, "registered_agent": CONTACT, "mailing_business": CONTACT,
              "shared_mailing": CONTACT, "license_address": CONTACT}

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
            verified: bool = False, ask: str = "") -> Dict[str, Any]:
    """One person or contact. role is the role as the record prints it, ask
    is the next verification question, and the document date is
    source.date."""
    verified = bool(verified and kind == "signed_for_entity" and (src or {}).get("date"))
    return {"name": _flat(name).strip(" ,;."), "role": role, "kind": kind, "verified": verified,
            "tier": VERIFIED if verified else _KIND_TIER.get(kind, CONTACT),
            "quote": _flat(quote)[:300], "note": note, "ask": ask, "source": src}


def signers_in_text(text: Any, entity: Any, src: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """People a document names in a role for this exact entity.

    src is the dated source of the document (pilot_review.source()). A role
    with authority is verified when src carries a date; a guarantor is a
    named person and a registered agent is a mailing contact.
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
                                    "manager or officer.",
                               ask=f"Ask {_flat(name)} who the members or managers of {_flat(entity)} are. An agent "
                                   "accepts mail and lawsuits and does not sign sale contracts."))
            continue
        out.append(finding(name=name, role=role, kind="signed_for_entity", src=src, quote=_quote(flat, a, b),
                           verified=dated,
                           note="" if dated else "The document is undated, so the role is not verified authority.",
                           ask=(f"Confirm with {_flat(name)} that the {role} role is still current, and ask for the "
                                "operating agreement or resolution that lets them sign a sale contract.") if dated else
                               (f"Find the dated instrument or filing that names {_flat(name)} as {role} of "
                                f"{_flat(entity)}.")))
    if out or pattern.search(flat):
        for g in _GUARANTOR.finditer(flat):
            if is_person_name(g["name"]):
                out.append(finding(name=g["name"], role="guarantor", kind="guarantor", src=src,
                                   quote=_quote(flat, g.start(), g.end()),
                                   note="A guarantor of the company's loan; the document does not say this person can "
                                        "sign for the company.",
                                   ask=f"Ask {_flat(g['name'])} whether they are a member, manager or officer of "
                                       f"{_flat(entity)} and who signs its contracts."))
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
    """Care-of and attention names on the company's OWN county owner or
    mailing line. A person is a named person worth asking; a business is a
    mailing contact. Neither is authority."""
    out = []
    for line, src in lines:
        for marker, who in care_of_names(line):
            if same_entity(who, entity):
                continue
            if is_person_name(who):
                out.append(finding(
                    name=who, role=marker, kind="mailing_name", src=src, quote=_flat(line),
                    note=f"Printed on the {marker} of this company's own county tax record. That shows who receives "
                         "the company's mail, not who can sign for it.",
                    ask=f"Ask {who} what their role is with {_flat(entity)} and who signs contracts for it, then "
                        "confirm the answer against the company's Secretary of State filing."))
            else:
                out.append(finding(
                    name=who, role=marker, kind="mailing_business", src=src, quote=_flat(line),
                    note=f"The {marker} names another business or a partial name, not a person with a stated role.",
                    ask=f"Call {who} and ask who the members or managers of {_flat(entity)} are."))
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
    """An attention name on ANOTHER company at the same P.O. box. Always a
    mailing contact: a shared box ties two mailing addresses together, not
    a person to this company."""
    out = []
    for hit in (index or {}).get(mailing_key(mailing) or "", []):
        if same_entity(hit["entity"], entity):
            continue
        out.append(finding(
            name=hit["name"], role=f"attention name on {hit['entity']} at the same P.O. box", kind="shared_mailing",
            src=hit["source"] or src, quote=_flat(mailing),
            note=(f"{hit['entity']} uses the same P.O. box and its county mailing line names this person. No record "
                  f"names this person for {_flat(entity)}; the link is the mailing address only."),
            ask=(f"Write to the box or ask {hit['name']} who the members or managers of {_flat(entity)} are. Do not "
                 "treat this person as tied to the company until a record of its own names them.")))
    return out


def suit_party_paths(entity: Any, parties: Iterable[Any], src: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """People named beside the entity in a tax suit publication. The
    publication does not say what they are to the company."""
    return [finding(name=p, role="named with the company in the tax suit", kind="suit_party", src=src,
                    note="Named for service beside the company; the publication does not state the relationship "
                         "(officer, prior owner and lienholder are all possible).",
                    ask=f"Read the tax suit complaint for how {_flat(p)} is described, or ask them what their "
                        f"relationship to {_flat(entity)} is.")
            for p in parties or [] if p and not same_entity(p, entity) and is_person_name(p)]


# ── county business license records ──────────────────────────────────────────

def base_key(name: Any) -> str:
    """entity_key without the legal suffix, for telling "same words, no
    entity type printed" apart from an exact match. Never used alone."""
    return re.sub(r"\s+(?:LLC|INC|CORP|CO|LP|LTD)$", "", entity_key(name)).strip()


def entity_type(name: Any) -> Optional[str]:
    m = re.search(r"\s(LLC|INC|CORP|CO|LP|LTD)$", entity_key(name))
    return m[1] if m else None


def clean_owner_name(name: Any) -> str:
    """The company's own name from a county owner line: "X INC & C/O JOE
    DOE", "X LLC ATTN E DOE" and "X INC ETAL" all give the company."""
    text = re.sub(r"(?<=[A-Z])(ATTN|ATTENTION)\b", r" \1", _flat(name).upper())
    text = re.split(r"\s+(?:&\s+)?(?:C/O|ATTN|ATTENTION|%)(?:\s|:|$)", text)[0]
    return re.sub(r"\s+(?:ET\s?AL|ET\s?UX|ET\s?VIR)\.?$", "", text).strip(" ,&")


def street_key(address: Any) -> Optional[str]:
    """House number plus ZIP of a street address ("202|37410"), or None for
    a P.O. box or an address without both."""
    text = _flat(address).upper()
    if re.search(r"\bP\.?\s?O\.?\s*BOX\b", text):
        return None
    num = re.search(r"(?<![A-Z0-9#])(\d{1,6})\s+[A-Z0-9]", text)
    zip5 = re.findall(r"(?<!\d)(\d{5})(?:-\d{4})?(?!\d)", text)
    return f"{num[1]}|{zip5[-1]}" if num and zip5 else None


def license_paths(entity: Any, lookup: Any, addresses: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    """Owners printed on county business license records (a cached lookup,
    see _signer_lookup). The license must be in the company's exact name,
    entity type included, for the owner to be a named person. When one of
    the two names prints no entity type and the license is at the company's
    own street address, it is an address link: mailing contact only. Same
    words with a different entity type is another company and yields
    nothing. A lookup stored for a different company name is ignored."""
    out: List[Dict[str, Any]] = []
    if not isinstance(lookup, dict):
        return out
    entity = clean_owner_name(entity)
    if lookup.get("entity_key") and lookup["entity_key"] != entity_key(entity):
        return out
    here = {k for k in (street_key(a) for a in addresses) if k}
    for rec in lookup.get("records") or []:
        owner, business = _flat(rec.get("owner")), _flat(rec.get("business"))
        if not owner or not business or not is_person_name(owner):
            continue
        src = {"label": f"{lookup.get('label') or 'County business license record'}, license for {business}",
               "url": lookup.get("url"), "date": rec.get("start") or None}
        state = _flat(rec.get("status")) or "status not printed"
        closed = f", closed {rec['close']}" if rec.get("close") else ""
        if same_entity(business, entity):
            out.append(finding(
                name=owner, role=f"Owner on the business license ({state}{closed})", kind="license_owner", src=src,
                quote=f"{business}; Owner: {owner}",
                note="The county business license in the company's exact name prints this person as owner. The "
                     "license does not say member, manager or officer.",
                ask=f"Ask {owner} whether they are a member or manager of {_flat(entity)} and can sign a sale "
                    "contract, then confirm against the company's Secretary of State filing."))
        elif (base_key(business) == base_key(entity) and (entity_type(business) is None) != (entity_type(entity) is None)
              and street_key(rec.get("business_address")) in here):
            out.append(finding(
                name=owner, role=f"Owner on a business license for {business} at the same address ({state}{closed})",
                kind="license_address", src=src, quote=f"{business}, {_flat(rec.get('business_address'))}; Owner: {owner}",
                note=f"The license is in the name {business}, which differs from {_flat(entity)} by the entity type, "
                     "at the company's street address. It is not shown to be the same company; the link is the "
                     "name and the address.",
                ask=f"Ask {owner} whether {business} was operated by {_flat(entity)} and who its members or "
                    "managers are."))
    return out


# ── one answer per entity ────────────────────────────────────────────────────

def _who(f: Dict[str, Any]) -> str:
    return person_key(f["name"]) or entity_key(f["name"])


def resolve(entity: Any, findings: Iterable[Dict[str, Any]], never: Iterable[Any] = ()) -> Dict[str, Any]:
    """Collapse findings to one answer. Each person appears once, in the
    strongest tier a record supports; two different verified people are
    both kept. never: names that can never sign for the owner (the lender,
    the trustee and substitute trustee, their attorney); any finding for
    one of them is dropped. Stable across repeat runs: no timestamps."""
    barred = {person_key(n) for n in never or [] if n and person_key(n)}
    found = [f for f in findings if _who(f) not in barred]
    signers: List[Dict[str, Any]] = []
    for f in found:
        if f.get("tier") == VERIFIED and not any(_who(s) == _who(f) for s in signers):
            signers.append(f)
    taken = {_who(s) for s in signers}
    paths: List[Dict[str, Any]] = []
    for tier in (NAMED, CONTACT):
        for f in found:
            if f.get("tier") != tier or _who(f) in taken:
                continue
            taken.add(_who(f))
            paths.append({**f, "verified": False})
    named = [p for p in paths if p["tier"] == NAMED]
    status = "verified" if signers else "named" if named else "contact" if paths else "none"
    return {"version": VERSION, "entity": _flat(entity), "status": status,
            "tier": {"verified": VERIFIED, "named": NAMED, "contact": CONTACT}.get(status),
            "signers": signers, "paths": paths}


def find(entity: Any, *, texts: Sequence[Tuple[Any, Optional[Dict[str, Any]]]] = (),
         mail_lines: Sequence[Tuple[Any, Optional[Dict[str, Any]]]] = (),
         suit_parties: Optional[Tuple[Iterable[Any], Optional[Dict[str, Any]]]] = None,
         mailing: Any = None, mailing_index: Optional[Dict[str, List[Dict[str, Any]]]] = None,
         licenses: Any = None, addresses: Iterable[Any] = (), never: Iterable[Any] = ()) -> Dict[str, Any]:
    """The signer answer for one company owner from the records held.

    texts:        (document text, dated source) pairs for THIS parcel: the
                  trustee sale notice, a recorded deed or deed of trust.
    mail_lines:   (county owner or mailing line, source) pairs.
    suit_parties: (names printed beside the owner in the tax suit, source).
    mailing:      the owner's county mailing address, for the shared P.O.
                  box lookup against mailing_index (build_mailing_index()).
    licenses:     a cached county business license lookup for this entity.
    addresses:    the company's own street addresses (mailing and situs).
    never:        lender, trustee and attorney names; never a signer.
    """
    found: List[Dict[str, Any]] = []
    for text, src in texts:
        found.extend(signers_in_text(text, entity, src))
    found.extend(mail_paths(entity, mail_lines))
    if suit_parties:
        found.extend(suit_party_paths(entity, suit_parties[0], suit_parties[1]))
    found.extend(license_paths(entity, licenses, addresses))
    if mailing and mailing_index:
        found.extend(shared_mailing_paths(entity, mailing, mailing_index, mail_lines[0][1] if mail_lines else None))
    return resolve(entity, found, never)


# ── wording for cards ────────────────────────────────────────────────────────

def _cite(src: Optional[Dict[str, Any]]) -> str:
    if not src:
        return "source not recorded"
    return f"{src.get('label')}" + (f", dated {src['date']}" if src.get("date") else ", undated")


def signer_basis(answer: Dict[str, Any]) -> str:
    """Decider basis for a verified signer, quoting the record."""
    first = answer["signers"][0]
    text = (f"{TIER_LABEL[VERIFIED]}: {_cite(first['source'])} names {first['name']} as {first['role']} of "
            f"{answer['entity']}: \"{first['quote']}\". Next: {first.get('ask') or 'Confirm the role is current.'}")
    others = [f"{s['name']} ({s['role']})" for s in answer["signers"][1:]]
    if others:
        text += " Also named in a role for the company: " + "; ".join(others) + "."
    return text


def path_lines(answer: Optional[Dict[str, Any]]) -> List[str]:
    """One verify_next line per person or contact that is not verified
    authority: tier label, name, role as printed, source and document date,
    then the next verification question."""
    out = []
    for p in (answer or {}).get("paths") or []:
        out.append(f"{TIER_LABEL[p.get('tier') or CONTACT]}: {p['name']}, {p['role']} ({_cite(p['source'])}). "
                   f"{p['note']} Next: {p.get('ask') or 'Confirm the role before treating this person as the signer.'}")
    return out


def hold_note(answer: Optional[Dict[str, Any]]) -> str:
    """Sentence for a hold reason: what is on file below verified authority."""
    paths = (answer or {}).get("paths") or []
    named = sum(1 for p in paths if p.get("tier") == NAMED)
    contacts = len(paths) - named
    bits = ([f"{named} named {'person' if named == 1 else 'people'} worth asking"] if named else []) + \
           ([f"{contacts} mailing contact{'' if contacts == 1 else 's'}"] if contacts else [])
    return f"On file: {' and '.join(bits)}; none is verified authority." if bits else ""


def hold_extras(entity: Any, **sources: Any) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """(answer when authority is verified else None, tiered lines) for a
    lane that only needs to know whether its company hold still applies."""
    answer = find(entity, **sources)
    return (answer if answer["status"] == "verified" else None), path_lines(answer)
