"""Collect foreclosure and estate notices from the dated issues linked by the Herald."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from typing import List, Optional
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import NAMESPACE_URL, uuid5

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload, _supabase
from ._court_notices import extract_probate, probate_review
from ._trustee_notice import sale_date


HCH_BASE = "https://www.hamiltoncountyherald.com"
HCH_INDEX = HCH_BASE + "/PublicNotices.aspx"
HCH_DETAIL = HCH_BASE + "/ViewNotice.aspx"

OPENCHILD_RE = re.compile(r"OpenChild\('(\d+)','([^']+)'\)")

# Notice body extraction (same as Nashville Ledger)
LENDER_RE = re.compile(r"payable to the order of\s+([^.]+?)\.", re.IGNORECASE)
LENDER_ALT_RE = re.compile(r"the holder of the (?:note|debt) is\s+([^.,]+)", re.IGNORECASE)
PARCEL_RE = re.compile(
    r"(?:Tax Parcel ID|MAP AND PARCEL|TAX MAP|PARCEL)\s*(?:NO\.?|NUMBER|ID)?[:\s]+([A-Z0-9][A-Z0-9\- \.]*\d[A-Z0-9\-\. ]*)",
    re.IGNORECASE,
)
PRINCIPAL_RE = re.compile(r"original principal (?:amount|sum) of \$?([\d,]+(?:\.\d{2})?)", re.IGNORECASE)
DELINQUENT_AMOUNT_RE = re.compile(r"entire amount delinquent[^$]*\$([\d,]+(?:\.\d{2})?)", re.IGNORECASE)
COMMONLY_KNOWN_RE = re.compile(r"Commonly known as[:\s]+(\d[^<\n]{0,160}?\b(?:TN|Tennessee)\.?[, ]+\d{5}(?:-\d{4})?)\b", re.IGNORECASE)
# Notices often run legal boilerplate after the address ("... TN 37404
# Street Address: The street address ... is believed to be ..."). Keep the
# address through the first ZIP so the assessor match can find it.
_ADDR_THROUGH_ZIP = re.compile(r"^(.*?\bTN\.?,?\s*\d{5}(?:-\d{4})?)", re.IGNORECASE)
_PARCEL_ID = re.compile(r"^\d{3}[A-Z]?(?:[ \-][A-Z])?[ \-]\d{3}(?:\.\d+)?")


def clean_address(a: Optional[str]) -> Optional[str]:
    if not a:
        return a
    m = _ADDR_THROUGH_ZIP.match(a.strip())
    return (m.group(1) if m else a).strip(" ,;")


def clean_parcel(p: Optional[str]) -> Optional[str]:
    if not p:
        return p
    m = _PARCEL_ID.match(p.strip().upper())
    return m.group(0).strip() if m else None


BORROWER_RE = re.compile(r"executed by\s+([A-Z][A-Za-z. ,'\-]+?),", re.IGNORECASE)

def _date(text, fmt="%m/%d/%Y"):
    try:
        return datetime.strptime(text or "", fmt).date()
    except ValueError:
        return None


def _printed_date(soup, selector):
    node = soup.select_one(selector)
    return _date(node.get_text(strip=True), "%A, %B %d, %Y") if node else None


# A reprint can add an issue date to the publication schedule immediately
# before the same Fr control. Keep the control and the entire operative text.
_PRINT_SCHEDULE = re.compile(
    r"(?:(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+"
    r"\d{1,2}(?:,\s*\d{1,2})*,\s*\d{4}\s+)+(?=Fr\d+\b)", re.I)


def _print_control(body):
    codes = set(re.findall(r"\bFr\d{5,}\b", body, flags=re.I))
    return codes.pop().upper() if len(codes) == 1 else None


def _document_key(kind, body, notice_key):
    text = _PRINT_SCHEDULE.sub("", " ".join(body.split()))
    control = _print_control(body) or notice_key
    return hashlib.sha256((kind + "|" + control + "|" + text).encode()).hexdigest()


def _advertised_sale(body, summary):
    # The standard conditional postponement clause describes a possibility;
    # retain actual cancellation/postponement notices for the shared parser.
    text = re.sub(r"\bif the sale is postponed\b", "if postponed", body, flags=re.I)
    printed = sale_date(text)
    reported = _date(summary)
    if summary and (not reported or reported.isoformat() != printed):
        return None
    return printed


class HamiltonCountyHeraldBot(BotBase):
    name = "hamilton_county_herald"
    description = "Chattanooga Hamilton County Herald foreclosure + probate notices"
    throttle_seconds = 1.5
    expected_min_yield = 3

    weeks_to_scan = 4

    def _document(self, url, **params):
        res = self.fetch(url, params=params, timeout=12)
        if res is None or res.status_code != 200 or res.url.split("?")[0].lower() != url.lower():
            raise RuntimeError(f"Herald document unavailable: {url} {params}")
        query = parse_qs(urlsplit(res.url).query)
        if any(query.get(key) != [value] for key, value in params.items()):
            raise ValueError(f"Herald redirected to a different issue or notice: {params}")
        return BeautifulSoup(res.content, "html.parser"), res.url

    def scrape(self) -> List[LeadPayload]:
        landing, _ = self._document(HCH_INDEX)
        latest = _printed_date(landing, "[id$=lblDate]")
        dates = set()
        for link in landing.select("a[href]"):
            href = urlsplit(link["href"])
            if href.path.lower() in ("publicnotices.aspx", "/publicnotices.aspx"):
                values = parse_qs(href.query).get("date", [])
                if len(values) == 1 and (issue_date := _date(values[0])):
                    dates.add(issue_date)
        today = date.today()
        if latest not in dates or not 0 <= (today - latest).days < self.weeks_to_scan * 7:
            raise ValueError("Unverified or stale Herald issue index")
        leads = {}
        for day in sorted(dates, reverse=True)[:self.weeks_to_scan]:
            if not 0 <= (today - day).days < self.weeks_to_scan * 7:
                continue
            param = f"{day.month}/{day.day}/{day.year}"
            issue = landing if day == latest else self._document(HCH_INDEX, date=param)[0]
            if _printed_date(issue, "[id$=lblDate]") != day:
                raise ValueError(f"Herald returned another issue for {day}")
            visited = set()
            for link in issue.select("a[href]"):
                match = OPENCHILD_RE.search(link["href"])
                if not match:
                    continue
                header = link.find_previous("h3")
                section = header.get_text(strip=True) if header else ""
                if section not in ("Foreclosures", "Courts"):
                    continue
                nid = match[1]
                if _date(unquote(match[2])) != day:
                    raise ValueError(f"Herald notice {nid} links to another edition")
                if nid in visited:
                    continue
                visited.add(nid)
                # The Courts table also indexes unrelated juvenile and general
                # sessions cases. Their published captions identify the court.
                row = link.find_parent("tr")
                if section == "Courts" and row and re.search(
                    r"\b(?:GENERAL SESSIONS|JUVENILE) COURT\b", row.get_text(" ", strip=True), re.I
                ) and not re.search(r"creditors|estate of|letters of", row.get_text(" ", strip=True), re.I):
                    continue
                lead = self._build_lead(nid, day, section)
                if lead:
                    digest = _document_key(lead.distress_type, lead.raw_payload["body"], lead.pipeline_lead_key)
                    leads.setdefault(digest, lead)
        return list(leads.values())

    def _write_staging(self, leads):
        # Re-reading a newspaper issue is not a fresh trustee confirmation.
        # Match historical keys across all staging statuses and printed reissues.
        if not leads:
            return 0, 0
        client = _supabase()
        if client is None:
            return 0, 0
        table = client.table("homeowner_requests_staging")
        keys, documents, print_controls = set(), set(), set()
        cursor = None
        while True:
            query = table.select("id,pipeline_lead_key,distress_type,body:raw_payload->>body").eq("bot_source", self.name)
            if cursor:
                query = query.gt("id", cursor)
            rows = query.order("id").limit(200).execute().data
            for row in rows:
                keys.add(row["pipeline_lead_key"])
                if row.get("body"):
                    documents.add(_document_key(row["distress_type"], row["body"], row["pipeline_lead_key"]))
                    if control := _print_control(row["body"]):
                        print_controls.add((row["distress_type"], control))
            if len(rows) < 200:
                break
            cursor = rows[-1]["id"]
        inserts = []
        for lead in leads:
            digest = _document_key(lead.distress_type, lead.raw_payload["body"], lead.pipeline_lead_key)
            if lead.pipeline_lead_key in keys or digest in documents:
                continue
            control = lead.raw_payload.get("print_control")
            if control and (lead.distress_type, control) in print_controls:
                self.logger.warning("Held changed Herald reprint %s, notice %s for review", control,
                                    lead.raw_payload["notice_id"])
                continue
            row = lead.as_db_row(scraper_run_id=self.run_id)
            # The table has a unique id, but no unique source/lead-key index.
            identity = control or lead.pipeline_lead_key
            row["id"] = str(uuid5(NAMESPACE_URL, f"{self.name}|{lead.distress_type}|{identity}"))
            inserts.append(row)
            keys.add(lead.pipeline_lead_key)
            documents.add(digest)
            if control:
                print_controls.add((lead.distress_type, control))
        added = 0
        for start in range(0, len(inserts), 50):
            result = table.upsert(inserts[start:start + 50], on_conflict="id",
                                  ignore_duplicates=True, default_to_null=False).execute()
            added += len(result.data)
        return added, len(leads) - added

    def _build_lead(self, nid, pub_date, section):
        param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        soup, url = self._document(HCH_DETAIL, id=nid, date=param)
        span, title = soup.select_one("#lblBody"), soup.select_one("#lblTitle")
        if (_printed_date(soup, "#lblDate") != pub_date or not title
                or title.get_text(strip=True) != section or not span or not span.get_text(strip=True)):
            raise ValueError(f"Unverified Herald detail {nid} in {pub_date}")
        lines = list(span.stripped_strings)
        if section == "Courts":
            return self._build_probate_lead(nid, pub_date, section, lines, url)
        fields = {}
        for elem, label in (("lbl1", "Borrower"), ("lbl2", "Address"),
                            ("lbl8", "Advertised Auction Date"), ("lbl9", "Date of First Public Notice"),
                            ("lbl11", "DR No.")):
            field = soup.find(id=elem)
            if field and field.find_parent("tr"):
                cells = field.find_parent("tr").find_all("td", recursive=False)
                if len(cells) == 2 and cells[0].get_text(strip=True).rstrip(":") == label:
                    fields[label] = field.get_text(strip=True)
        if fields.get("DR No.") and fields["DR No."] != nid:
            raise ValueError(f"Herald detail ID mismatch: {nid}")
        city = soup.select_one("#lbl3")
        if fields.get("Address") and city:
            locality = city.get_text(strip=True)
            if re.fullmatch(r"[A-Za-z .-]+,?\s+TN\s+\d{5}(?:-\d{4})?", locality, re.I):
                fields["location"] = f"{fields['Address']}, {locality}"
        return self._build_foreclosure_lead(nid, pub_date, section, " ".join(lines), url, fields)

    def _build_foreclosure_lead(self, nid: str, pub_date: date, title: str,
                                  body: str, url: str, fields: dict) -> Optional[LeadPayload]:
        if not re.search(r"\bHamilton County\b", body, re.I):
            return None
        # Borrower
        m = BORROWER_RE.search(body)
        borrower = (m.group(1).strip() if m else None) or fields.get("Borrower")
        # Address ("Commonly known as: 7711 E. Village Lane, Hixson, TN 37343")
        m = COMMONLY_KNOWN_RE.search(body)
        property_address = fields.get("location") or (clean_address(m.group(1)) if m else None)
        if not property_address:
            return None
        # Parcel
        m = PARCEL_RE.search(body)
        parcel = clean_parcel(m.group(1)) if m else None
        # Lender
        m = LENDER_RE.search(body) or LENDER_ALT_RE.search(body)
        lender = m.group(1).strip() if m else None
        # Principal
        m = PRINCIPAL_RE.search(body)
        principal = float(m.group(1).replace(",", "")) if m else None
        # Delinquent amount
        m = DELINQUENT_AMOUNT_RE.search(body)
        delinquent = float(m.group(1).replace(",", "")) if m else None

        advertised = _date(fields.get("Advertised Auction Date"))
        printed = _advertised_sale(body, fields.get("Advertised Auction Date"))
        first = _date(fields.get("Date of First Public Notice"))
        # A summary date preceding the first publication is not the auction
        # advertised by this notice (some publisher fields contain defaults).
        if first and advertised and advertised < first:
            advertised = printed = None
        admin_parts = [f"Hamilton Co Herald {nid}", f"pub {pub_date.isoformat()}"]
        if lender:
            admin_parts.append(f"lender={lender}")
        if parcel:
            admin_parts.append(f"parcel={parcel}")
        if delinquent:
            admin_parts.append(f"delinquent=${delinquent:,.0f}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, f"FL-{nid}"),
            property_address=property_address,
            county="hamilton",
            full_name=borrower,
            trustee_sale_date=printed if printed and printed >= date.today().isoformat() else None,
            distress_type="PRE_FORECLOSURE",
            admin_notes=" · ".join(admin_parts),
            source_url=url,
            raw_payload={
                "publication": "hamilton_county_herald",
                "title": title,
                "notice_id": nid,
                "print_control": _print_control(body),
                "first_publication_date": first.isoformat() if first and first <= pub_date else None,
                "advertised_auction_date": advertised.isoformat() if advertised else None,
                "notice_sale_date": printed,
                "borrower": borrower,
                "property_address": property_address,
                "parcel": parcel,
                "lender": lender,
                "original_principal": principal,
                "delinquent_amount": delinquent,
                "publication_date": pub_date.isoformat(),
                "body": body,
            },
        )

    # ── Probate lead ────────────────────────────────────────────────────────

    def _build_probate_lead(self, nid: str, pub_date: date, title: str,
                                  lines: list[str], url: str) -> Optional[LeadPayload]:
        body = " ".join(lines)
        if not re.search(r"NOTICE TO CREDITORS|Letters of (?:Administration|Testamentary|Authority)", body, re.I):
            return None
        fields = extract_probate(lines)
        decedent = fields["decedent"]
        if not decedent or fields["county"] != "hamilton":
            return None
        docket = fields["docket"]
        dod_raw = fields["date_of_death"]
        admin_parts = [f"Hamilton Co Herald {nid}", f"pub {pub_date.isoformat()}", "court=hamilton"]
        if docket:
            admin_parts.append(f"docket={docket}")
        if dod_raw:
            admin_parts.append(f"dod={dod_raw}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, f"CL-{nid}"),
            full_name=decedent,
            county="hamilton",
            distress_type="PROBATE",
            admin_notes=" · ".join(admin_parts),
            source_url=url,
            raw_payload={
                "publication": "hamilton_county_herald",
                "title": title,
                "notice_id": nid,
                "print_control": _print_control(body),
                "court_notice": fields,
                "probate_review": probate_review(lines, fields),
                "decedent": decedent,
                "county": "hamilton",
                "docket": docket,
                "date_of_death_raw": dod_raw,
                "publication_date": pub_date.isoformat(),
                "body": body,
            },
        )


def run() -> dict:
    bot = HamiltonCountyHeraldBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
