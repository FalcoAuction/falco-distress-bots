"""
Johnson City Board of Dwelling Standards scraper.

Source: Johnson City CivicWeb publishes Board of Dwelling Standards and
Review agenda packets as public PDFs. These packets are high-signal code
enforcement records: public hearings, show-cause hearings, repair/demolition
orders, and notices that structures are "unfit for human occupation or use."

The bot discovers recent meeting packets from the public meeting page, fetches
the latest PDFs, and extracts one lead per CEPM case/address.

Distress type: CODE_VIOLATION
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional
from urllib.parse import urljoin

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from ._base import BotBase, LeadPayload, _supabase
from ._probate_staging import guard_raw


MEETING_INFO_URL = "https://johnsoncitytn.civicweb.net/Portal/MeetingInformation.aspx?type=48"
BASE_URL = "https://johnsoncitytn.civicweb.net"

MEETING_BUTTON_RX = re.compile(
    r"MeetingButton(?P<id>\d+).*?meeting-list-item-button-date\">(?P<date>[^<]+)</div>"
    r".*?title=\"(?P<title>[^\"]+)\"",
    re.IGNORECASE | re.DOTALL,
)
PDF_LINK_RX = re.compile(r"href=\"(?P<href>/document/[^\"]+?\.pdf\?handle=[^\"]+)\"", re.IGNORECASE)
CASE_CAPTION = r"(?P<address>\d+[^\n]*?)[ \t]*[-:\u2013\u2014][ \t]*(?P<case>CEPM\d{6,})[ \t]*$"
AGENDA_ITEM_RX = re.compile(r"^[ \t]*\d+\.\d*[ \t]+" + CASE_CAPTION, re.I | re.M)
SUBJECT_RX = re.compile(r"^SUBJECT:[ \t]*" + CASE_CAPTION, re.I | re.M)
ATTACHMENT = re.compile(r"^(?:AGENDA SUMMARY|MINUTES)[ \t]*$", re.I | re.M)
END_AGENDA = re.compile(r"^\d+\.\s+ADJOURNMENT\b", re.I | re.M)
MEETING_DAY = re.compile(r"^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),[ \t]+"
                         r"(\w+ \d{1,2}, \d{4})\b", re.M)
REMOVAL = re.compile(r"^Motion to (?:rescind and )?remove ([^\n]+?) from the Board['\u2019]s docket"
                     r"([^.!?]{0,180})\.[ \t]*\nRESULT:[ \t]*Carried\b[^\n]*", re.I | re.M)

def iso_day(value):
    for fmt in ("%b %d %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except (ValueError, TypeError):
            pass
    return ""


def address_key(address):
    words = re.findall(r"[a-z0-9]+", (address or "").lower())
    suffix = {"street": "st", "avenue": "ave", "road": "rd"}
    return tuple(suffix.get(w, w) for w in words)


CASE_OPENED_RX = re.compile(r"Case Opened:\s*(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})", re.IGNORECASE)
HEARING_RX = re.compile(r"(?P<hearing>(?:FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH|SHOW CAUSE)\s+PUBLIC\s+HEARING|SHOW CAUSE HEARING)", re.IGNORECASE)
VIOLATION_DESC_RX = re.compile(
    r"Code Violation Description\s+(?P<desc>.+?)\s+Specific Violation Details",
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class Packet:
    meeting_id: str
    meeting_date: str
    title: str
    pdf_url: str


class JohnsonCityBdsrBot(BotBase):
    name = "johnson_city_bdsr"
    description = "Johnson City Board of Dwelling Standards code-enforcement packets"
    throttle_seconds = 1.5
    expected_min_yield = 5

    def scrape(self) -> List[LeadPayload]:
        if pdfplumber is None:
            self.logger.error("pdfplumber not installed; pip install pdfplumber")
            return []

        packets = self._discover_packets(limit=4)
        self.logger.info(f"discovered {len(packets)} recent BDSR packet(s)")

        leads: List[LeadPayload] = []
        removals = {}
        for packet in packets:
            res = self.fetch(packet.pdf_url, timeout=120)
            if res is None or res.status_code != 200:
                self.logger.warning(f"packet fetch failed: {packet.pdf_url}")
                continue
            if "pdf" not in res.headers.get("Content-Type", "").lower():
                self.logger.warning(f"packet not PDF: {packet.pdf_url}")
                continue
            try:
                text = self._extract_text(res.content)
            except Exception as e:
                self.logger.warning(f"pdf extract failed for {packet.pdf_url}: {e}")
                continue

            packet_leads = self._parse_packet(text, packet)
            for key, removal in self._minute_removals(text, packet):
                if key not in removals or removal["meeting_on"] > removals[key]["meeting_on"]:
                    removals[key] = removal
            self.logger.info(f"{packet.meeting_date}: {len(packet_leads)} lead(s)")
            leads.extend(packet_leads)

        deduped: Dict[str, LeadPayload] = {}
        conflicts = set()
        for lead in leads:
            key = lead.pipeline_lead_key
            evidence = lead.raw_payload[self.name]
            prior = deduped.get(key)
            if prior and address_key(prior.property_address) != address_key(lead.property_address):
                conflicts.add(key)
                continue
            if prior and prior.raw_payload[self.name]["meeting_on"] >= evidence["meeting_on"]:
                continue
            removal = removals.get((evidence["case_number"], address_key(evidence["address"])))
            if removal and removal["meeting_on"] >= evidence["meeting_on"]:
                evidence["stage"] = "docket_removal_reported"
                evidence["minutes_evidence"] = removal
                lead.admin_notes += " | Board docket removal reported in minutes"
            deduped[key] = lead
        return [lead for key, lead in deduped.items() if key not in conflicts]

    def _refresh_patch(self, row, lead):
        raw = row.get("raw_payload")
        old = raw.get(self.name) if isinstance(raw, dict) else None
        event = lead.raw_payload[self.name]
        if (row["staging_status"] != "pending" or not isinstance(old, dict)
                or old.get("case_number") != event["case_number"]
                or address_key(old.get("address")) != address_key(event["address"])
                or address_key(row["property_address"]) != address_key(lead.property_address)):
            return {}
        # Legacy dates included copied minutes. Only dates from the bounded
        # agenda parser establish a floor for subsequent refreshes.
        if old.get("meeting_on", "") > event["meeting_on"]:
            return {}
        removal = old.get("minutes_evidence") or {}
        incoming = event.get("minutes_evidence") or {}
        if (removal.get("meeting_on", "") >= event["meeting_on"]
                and removal.get("meeting_on", "") > incoming.get("meeting_on", "")):
            return {}
        patch = {}
        if old != event:
            patch["raw_payload"] = {**raw, self.name: event}
        if (row.get("source_url") or "").startswith(BASE_URL + "/document/") and row["source_url"] != lead.source_url:
            patch["source_url"] = lead.source_url
        notes = row.get("admin_notes") or ""
        prefix, separator, enrichment = notes.partition(" · ")
        # Recognize only a generated BDSR prefix. Retain appended enrichments.
        case = re.escape(event["case_number"])
        day = re.escape(old.get("meeting_on") or old.get("meeting_date") or "")
        template = (rf"BDSR {case}(?: \| \[HIGH-SIGNAL\])?(?: \| (?:[a-z ]*hearing|agenda item))?"
                    rf"(?: \| opened: [0-9-]+)?(?: \| violations: [^|]+)? \| meeting: {day}"
                    r"(?: \| Board docket removal reported in minutes)?")
        if re.fullmatch(template, prefix, re.I) and prefix != lead.admin_notes:
            patch["admin_notes"] = lead.admin_notes + separator + enrichment
        return patch

    def _write_staging(self, leads):
        if not leads:
            return 0, 0
        client = _supabase()
        if client is None:
            return 0, 0
        table = client.table("homeowner_requests_staging")
        by_key = {lead.pipeline_lead_key: lead for lead in leads}
        rows = (table.select("id,pipeline_lead_key,staging_status,property_address,raw_payload,source_url,admin_notes")
                .eq("bot_source", self.name).in_("pipeline_lead_key", list(by_key)).execute().data or [])
        existing = {}
        for row in rows:
            existing.setdefault(row["pipeline_lead_key"], []).append(row)
        refreshed = 0
        for key, matches in existing.items():
            if len(matches) != 1:
                continue
            row = matches[0]
            patch = self._refresh_patch(row, by_key[key])
            if not patch:
                continue
            update = (table.update(patch).eq("id", row["id"]).eq("bot_source", self.name)
                      .eq("pipeline_lead_key", key).eq("staging_status", "pending")
                      .eq("property_address", row["property_address"]))
            for field in ("source_url", "admin_notes"):
                if field in patch:
                    update = update.eq(field, row[field])
            if guard_raw(update, row["raw_payload"]).execute().data:
                refreshed += 1
        self.logger.info("Refreshed BDSR hearing evidence for %s pending rows", refreshed)
        new = [lead for key, lead in by_key.items() if key not in existing]
        candidates = [lead for lead in new if lead.raw_payload[self.name]["stage"] == "hearing_listed"]
        self.logger.info("Withheld %s previously unseen docket removals", len(new) - len(candidates))
        staged, duplicates = super()._write_staging(candidates)
        return staged, duplicates + len(leads) - len(by_key) + len(existing)

    def _discover_packets(self, limit: int) -> List[Packet]:
        res = self.fetch(MEETING_INFO_URL, timeout=60)
        if res is None or res.status_code != 200:
            self.logger.error(f"meeting page fetch failed: {res.status_code if res else 'none'}")
            return []

        page_html = res.text
        meetings = []
        seen_ids = set()
        for match in MEETING_BUTTON_RX.finditer(page_html):
            meeting_id = match.group("id")
            title = html.unescape(match.group("title")).strip()
            if meeting_id in seen_ids:
                continue
            seen_ids.add(meeting_id)
            if "Board of Dwelling Standards" not in title:
                continue
            if "Training" in title:
                continue
            meetings.append((meeting_id, match.group("date").strip(), title))

        packets: List[Packet] = []
        for meeting_id, meeting_date, title in meetings[: max(limit * 2, limit)]:
            pdf_url = self._packet_url_for_meeting(meeting_id)
            if not pdf_url:
                continue
            packets.append(Packet(meeting_id, meeting_date, title, pdf_url))
            if len(packets) >= limit:
                break
        return packets

    def _packet_url_for_meeting(self, meeting_id: str) -> Optional[str]:
        url = f"{BASE_URL}/Portal/MeetingInformation.aspx?Id={meeting_id}"
        res = self.fetch(url, timeout=60)
        if res is None or res.status_code != 200:
            return None
        match = PDF_LINK_RX.search(res.text)
        if not match:
            return None
        return urljoin(BASE_URL, html.unescape(match.group("href")))

    def _extract_text(self, pdf_bytes: bytes) -> str:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            return "\n".join((page.extract_text() or "") for page in pdf.pages)

    def _parse_packet(self, text: str, packet: Packet) -> List[LeadPayload]:
        # The published packet embeds previous meetings' minutes and notices.
        # Only numbered items before adjournment belong to this meeting.
        agenda = END_AGENDA.split(ATTACHMENT.split(text, maxsplit=1)[0], maxsplit=1)[0]
        day = MEETING_DAY.search(agenda)
        meeting_on = iso_day(packet.meeting_date)
        if not agenda.lstrip().startswith("AGENDA") or not day or not meeting_on or iso_day(day[1]) != meeting_on:
            return []
        sections = {}
        for block in ATTACHMENT.split(text)[1:]:
            subject = SUBJECT_RX.search(block)
            if subject:
                sections[(subject["case"].upper(), address_key(subject["address"]))] = block[subject.start():]
        leads = []
        for item in AGENDA_ITEM_RX.finditer(agenda):
            case = item["case"].upper()
            address = self._normalize_address(item["address"])
            if not address:
                continue
            # Hearing type is the line immediately following this agenda item.
            following = agenda[item.end():].lstrip().splitlines()
            hearing = HEARING_RX.fullmatch(following[0].strip()) if following else None
            section = sections.get((case, address_key(item["address"])), "")
            evidence = dict(case_number=case, meeting_id=packet.meeting_id, meeting_date=packet.meeting_date,
                            meeting_on=meeting_on, title=packet.title, address=address,
                            hearing=hearing[0] if hearing else None, stage="hearing_listed",
                            opened_on=self._extract_case_opened(section), violations=self._extract_violations(section),
                            sale_authority_verified=False, source_url=packet.pdf_url)
            # Packet letters are addressed to owners OR interested parties.
            leads.append(LeadPayload(bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, case),
                                     property_address=f"{address}, Johnson City, TN",
                                     distress_type="CODE_VIOLATION", source_url=packet.pdf_url,
                                     admin_notes=f"BDSR {case} | {evidence['hearing'] or 'agenda item'} | meeting: {meeting_on}",
                                     raw_payload={self.name: evidence}))
        return leads

    def _minute_removals(self, text, packet):
        # An attached draft reports a vote; it does not prove final approval,
        # remediation, a conveyance or a mandate to sell the subject property.
        for block in re.split(r"^MINUTES[ \t]*$", text, flags=re.M)[1:]:
            minutes = END_AGENDA.split(ATTACHMENT.split(block, maxsplit=1)[0], maxsplit=1)[0]
            day = MEETING_DAY.search(minutes[:360])
            if not day or not (meeting_on := iso_day(day[1])) or meeting_on > iso_day(packet.meeting_date):
                continue
            items = list(AGENDA_ITEM_RX.finditer(minutes))
            for i, item in enumerate(items):
                stop = items[i + 1].start() if i + 1 < len(items) else len(minutes)
                section = minutes[item.end():stop]
                for vote in REMOVAL.finditer(section):
                    printed, moved = address_key(item["address"]), address_key(vote[1])
                    if moved != printed:
                        continue
                    yield (item["case"].upper(), printed), dict(meeting_on=meeting_on, source_url=packet.pdf_url,
                                                               motion=" ".join(vote[0].split()))

    def _extract_case_opened(self, section: str) -> Optional[str]:
        match = CASE_OPENED_RX.search(section)
        if not match:
            return None
        try:
            return datetime.strptime(match.group("date"), "%B %d, %Y").date().isoformat()
        except ValueError:
            return match.group("date")

    def _extract_violations(self, section: str) -> str:
        found = []
        for match in VIOLATION_DESC_RX.finditer(section or ""):
            desc = self._clean(re.sub(r"\s+", " ", match.group("desc")))
            if desc and desc.lower() != "(code number)" and desc not in found:
                found.append(desc)
            if len(found) >= 4:
                break
        return "; ".join(found)

    @staticmethod
    def _normalize_address(raw: str) -> Optional[str]:
        value = re.sub(r"\([^)]*\)", "", raw or "")
        value = re.sub(r"\s+", " ", value).strip(" -.,")
        value = value.replace(" St.", " St").replace(" Rd.", " Rd").replace(" Ave.", " Ave")
        if not re.match(r"^\d+\s+\S+", value):
            return None
        return value.title()

    @staticmethod
    def _clean(value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()


def run() -> dict:
    bot = JohnsonCityBdsrBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
