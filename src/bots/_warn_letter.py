"""Evidence from Tennessee's employment notices. Dates describe employment only."""
import re
from datetime import date, datetime


NOTICE = re.compile(r"\bWARN\s*#\s*(\d{9})\b", re.I)
MONTH = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
LETTER_DATE = re.compile(r"\b(" + MONTH + r")\s+(\d{1,2}),?\s+(20\d{2})\b", re.I)
TABLE_DATE = re.compile(r"(?<!\d)(\d{1,2})[-/](\d{1,2})[-/](\d{4}|\d{2})(?!\d)")
SITES = re.compile(
    r"\b(?:affected (?:worksites?|locations?|facilit(?:y|ies)) (?:is|are)(?: located at)?"
    r"|(?:this |the )?company is located at|employees at the following locations)"
    r"\s*[:,]?\s*(?:[A-Za-z][\w &'-]{1,65}(?:,\s*located at| located at|,)\s*)?"
    r"(\d.{3,850}?(?:TN|Tennessee),?\s+\d{5}(?:-\d{4})?)(?=[.]?(?:\s+(?:The |This |There |A |These )|$)|[.]\s)", re.I)
STREET = re.compile(r"(\d[^;]{2,180}?\b(?:TN|Tennessee),?\s+\d{5}(?:-\d{4})?)\b", re.I)


def dates(text, pattern):
    values = []
    for m, d, y in pattern.findall(text):
        try:
            month = int(m) if m.isdigit() else datetime.strptime(m.title(), "%B").month
            values.append(date(int(y) + (2000 if len(y) == 2 else 0), month, int(d)).isoformat())
        except ValueError:
            return []
    return sorted(set(values))


def kind(statement):
    # Consume the affirmative event at the beginning of the state's report.
    # A later reference to a closure can describe a different or negated event.
    subject = re.split(r"\s+(?:effective|beginning|without giving|on |starting)", statement.lower(), maxsplit=1)[0]
    subject = re.sub(r"^(?:a|an|the) ", "", subject).strip()
    if subject in {"permanent closure", "permanent closing", "mass layoff and permanent closure"}:
        return "permanent_closure"
    if subject in {"temporary closure", "temporary closing"}:
        return "temporary_closure"
    if re.fullmatch(r"(?:(temporary|partial|permanent|mass) )?(?:lay-?off|reduction in force)(?: or layoff)?", subject):
        return next((x + "_layoff" for x in ("temporary", "partial", "permanent", "mass")
                     if subject.startswith(x)), "layoff")
    return "review"


def letter_facts(text):
    body = " ".join(text.split())
    event = re.search(r"\bnotifying the agency (?:of|that)\s+([^.!?]{3,450})(?:[.!?]|$)", body, re.I)
    statement = event[1].strip() if event else ""
    tail = body[event.end():] if event else ""
    worksites = []
    for group in SITES.finditer(tail):
        # Only accept a whole list of printed TN addresses with explicit ZIPs.
        addresses = STREET.findall(group[1])
        remainder = STREET.sub("", group[1])
        if not re.sub(r"\band\b|[,;\s]", "", remainder, flags=re.I):
            worksites.extend(addresses)
    evidence = []
    for sentence in re.split(r"(?<=[.!?])\s+", tail):
        if re.search(r"\b(?:auction(?:eer)?|liquidat(?:ion|or)|asset sale|authorized to sell|equipment sale)\b",
                     sentence, re.I):
            evidence.append(sentence[:500])
    event_type = kind(statement)
    if re.search(r"\b(?:rescind(?:s|ed)?|withdraw(?:n|s)?|cancel(?:led|ed|s)?)\b.{0,50}\b(?:WARN|notice|layoff|closure)\b"
                 r"|\b(?:closure|layoff|notice)\s+(?:is|was|has been)\s+(?:rescinded|withdrawn|cancelled|canceled)\b",
                 body, re.I):
        event_type = "withdrawal_review"
    if event_type.endswith("closure") and re.search(r"\bnot (?:a|the result of a) (?:facility |site )?closure\b", body, re.I):
        event_type = "review"
    return statement or None, event_type, list(dict.fromkeys(worksites)), evidence


def review(rec, text, digest, today):
    statement, event_type, sites, evidence = letter_facts(text)
    ids = sorted(set(NOTICE.findall(text)))
    company = rec["company"]
    def name_key(value):
        return " ".join(re.findall(r"\w+", value.casefold()))

    body = " ".join(text.split())
    filer = re.search(r"\bWARN\s*#\s*\d{9}\s+([^#]{1,200}?)\s+has filed an official WARN", body, re.I)
    subject = re.search(r"Employee Dislocation Notification\s+(.{1,250}?)\s+WARN\s*#", " ".join(text.split()), re.I)
    names = [m[1] for m in (subject, filer) if m]
    status = "verified"
    if not text:
        status = "letter_unavailable"
    elif ids != [rec["notice"].lstrip("#")] or name_key(company) not in {name_key(n) for n in names}:
        status = "identity_review"
    elif not statement or event_type == "review":
        status = "event_review"
    listed = dates(rec["effective_raw"], TABLE_DATE)
    printed = dates(statement or "", LETTER_DATE)
    incomplete = re.search(MONTH + r"\s+20\d{2}", (statement or "") + " " + rec["effective_raw"], re.I)
    partial = bool(incomplete or re.search(r"\b(?:through|beginning|between|until)\b", statement or "", re.I)
                   and len(printed) < 2)
    timing = "unknown"
    if status == "verified" and printed and not partial:
        if listed and (not set(listed).intersection(printed) or len(listed) > 1 and listed[-1] != printed[-1]):
            timing = "date_conflict"
        else:
            timing = ("upcoming" if today < printed[0] else "elapsed" if today > printed[-1] else "in_window")
    if event_type == "withdrawal_review":
        timing = "unknown"
    authority = [{"basis": "statement_in_employment_memo", "statement": sentence,
                  "source_url": rec["letter_url"], "verification": "requires_underlying_document"}
                 for sentence in evidence if status == "verified" and re.search(
                     r"\b(?:authorized to sell|court[- ]appointed (?:receiver|liquidator)|retained.{0,70}auctioneer)\b",
                     sentence, re.I)]
    return dict(notice=rec["notice"], company=company, posted=rec["posted"], index_county=rec["county"],
                source_url=rec["letter_url"], checked_on=today, letter_sha256=digest, letter_notice_ids=ids,
                letter_employers=names, timing_basis="employment_notice",
                verification=status, event_type=event_type if status == "verified" else "review",
                event_statement=statement, worksites=sites if status == "verified" else [],
                worksite=sites[0] if status == "verified" and len(sites) == 1 else None,
                employment_window={"index_text": rec["effective_raw"], "index_dates": listed,
                                   "letter_dates": printed, "partial": partial}, timing=timing,
                asset_disposition="unverified", authority_evidence=authority,
                disposition_excerpts_for_review=evidence if status == "verified" else [])
