"""Evidence in the operative clauses of a Shelby publisher notice."""
import re
from datetime import datetime

MONTH_DATE = r"(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}"
REGISTER = r"register['’]s office"
RECORD = r"(?:instrument|document)\s+(?:number|no\.?)\s*[:#]?\s*"
APPOINTMENT = re.compile(
    rf"(?:\bappointment of (?:substitute|successor) trustee(?:, said appointment being)? "
    rf"(?:of record at|recorded(?: on {MONTH_DATE},?)?(?: in (?:the aforementioned {REGISTER}|the {REGISTER} for Shelby County, Tennessee))? (?:as|at))"
    rf"|\bappointed substitute trustee by the owner and holder of the note by instrument recorded "
    rf"in (?:deed book\s*[\d-]*, page\(s\)\s*[\d-]*\s+and/or as )?"
    rf"|\bappointed [A-Za-z. '’-]{{2,65}}, Esq\., as substitute trustee by document recorded {MONTH_DATE} as)"
    rf"\s*{RECORD}([0-9]{{8}})(?![\w/-]|[.,]\d)", re.I)
SALE = re.compile(r"\bwill proceed to sell the below-mentioned property on "
                  r"(\d{1,2}/\d{1,2}/\d{4}), at \d{1,2}:\d{2} [AP]M\b", re.I)

# Foundation's legal property description ends with this labeled address.
AKA = re.compile(r"\bALSO KNOWN AS:\s*(\d+[A-Za-z]? [^,;:]{1,90}),\s*([A-Za-z .'-]{2,50}),\s*"
                 r"TN\s+(\d{5})(?:-\d{4})?\s*(?=This sale is subject to)", re.I)
CONTROLS = {"substitute_trustee_appointment": "instrument_number", "published_sale_clause": "date",
            "foundation_legal_address": "address"}


def publisher_evidence(detail, county, fid, source_url):
    if county != "shelby" or detail.get("tdn_no") != fid:
        return {}
    body = " ".join((detail.get("body") or "").split())
    base = {"notice_id": fid, "source_url": source_url}
    evidence = {}
    if (detail.get("substitute_trustee") == "Foundation Legal Group, LLP"
            and detail.get("instrument_no") and len(re.findall(r"\bALSO KNOWN AS:", body, re.I)) == 1):
        alias = AKA.search(body)
        if alias:
            evidence["foundation_legal_address"] = {
                **base, "instrument_no": detail["instrument_no"], "street": alias[1], "city": alias[2],
                "zip": alias[3], "address": alias[0].strip()}
    matches = list(APPOINTMENT.finditer(body))
    numbers = {m[1] for m in matches}
    if (len(numbers) == 1 and matches[0][1] != detail.get("instrument_no")
            and not any(re.match(r"\s*(?:,|and|&)\s*(?:and\s+)?\d{8}\b", body[m.end():], re.I)
                        for m in matches)):
        evidence["substitute_trustee_appointment"] = {
            **base, "instrument_number": matches[0][1], "excerpt": matches[0][0]}
    sales = list(SALE.finditer(body))
    if (not detail.get("advertised_auction_date") and len(sales) == 1
            and not re.search(r"\b(?:was|is|has been) (?:hereby )?(?:withdrawn|cancelled|canceled|postponed|rescheduled)\b", body, re.I)):
        try:
            advertised = datetime.strptime(sales[0][1], "%m/%d/%Y").date()
        except ValueError:
            return evidence
        evidence["published_sale_clause"] = {**base, "date": advertised.isoformat(), "excerpt": sales[0][0]}
    return evidence


def repeat_evidence(lead, row, tracking, guards, patch):
    """Attach a publisher observation under the existing notice-tracking CAS."""
    from ._base import BotBase

    raw = lead.raw_payload or {}
    notice = raw.get("structured") or {}
    evidence = {k: raw[k] for k in CONTROLS if k in raw}
    if not evidence:
        return
    fid = raw["notice_id"]
    if (row.get("tdn") != fid or lead.pipeline_lead_key != BotBase.make_lead_key("memphis_daily_news", fid)
            or not notice.get("instrument_no") or row.get("loan") != notice["instrument_no"]
            or row.get("property_address") != lead.property_address or row.get("county") != "shelby"):
        return
    safe = {}
    for key, value in evidence.items():
        control = CONTROLS[key]
        for old in (tracking.get(key), row.get(key)):
            if old is not None and (not isinstance(old, dict) or old.get(control) != value[control]):
                break
        else:
            safe[key] = value
    if not safe:
        return
    guards.update({"raw_payload->structured->>tdn_no": fid,
                   "raw_payload->structured->>instrument_no": row["loan"], "county": "shelby",
                   "property_address": row["property_address"], "staging_status": "pending"})
    for key in safe:
        control = CONTROLS[key]
        prior = row.get(key) or {}
        guards[f"raw_payload->{key}->>{control}"] = prior.get(control)
    tracking.update(safe)
    if ("published_sale_clause" in safe and row["trustee_sale_date"] is None
            and not (row.get("phone_metadata") or {}).get("sale_status")
            and not tracking.get("published_sale_date")):
        guards["trustee_sale_date"] = None
        patch["trustee_sale_date"] = safe["published_sale_clause"]["date"]
