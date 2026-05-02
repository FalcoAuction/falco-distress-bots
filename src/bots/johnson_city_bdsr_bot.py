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
from datetime import datetime, timezone
from io import BytesIO
from typing import Dict, List, Optional
from urllib.parse import urljoin

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from ._base import BotBase, LeadPayload


MEETING_INFO_URL = "https://johnsoncitytn.civicweb.net/Portal/MeetingInformation.aspx?type=48"
BASE_URL = "https://johnsoncitytn.civicweb.net"

MEETING_BUTTON_RX = re.compile(
    r"MeetingButton(?P<id>\d+).*?meeting-list-item-button-date\">(?P<date>[^<]+)</div>"
    r".*?title=\"(?P<title>[^\"]+)\"",
    re.IGNORECASE | re.DOTALL,
)
PDF_LINK_RX = re.compile(r"href=\"(?P<href>/document/[^\"]+?\.pdf\?handle=[^\"]+)\"", re.IGNORECASE)
AGENDA_ITEM_RX = re.compile(
    r"^\s*\d+\.\s+(?P<address>.+?)\s+-\s+(?P<case>CEPM\d{6,})\s*$",
    re.IGNORECASE | re.MULTILINE,
)
SUBJECT_RX = re.compile(
    r"SUBJECT:\s*(?P<address>.+?)\s+-\s+(?P<case>CEPM\d{6,})(?P<body>.*?)(?=\nBoard of Dwelling Standards and Review\nAGENDA SUMMARY|\Z)",
    re.IGNORECASE | re.DOTALL,
)
TO_OWNER_RX = re.compile(
    r"\nTO:\s*(?P<owner>.*?)(?:\nRE:\s*(?:Property at|Violation of City Code at):)",
    re.IGNORECASE | re.DOTALL,
)
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
            self.logger.info(f"{packet.meeting_date}: {len(packet_leads)} lead(s)")
            leads.extend(packet_leads)

        deduped: Dict[str, LeadPayload] = {}
        for lead in leads:
            deduped.setdefault(lead.pipeline_lead_key, lead)
        return list(deduped.values())

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
        case_sections = self._case_sections(text)
        leads: List[LeadPayload] = []

        for match in AGENDA_ITEM_RX.finditer(text):
            case_number = match.group("case").upper()
            address = self._normalize_address(match.group("address"))
            if not address:
                continue

            section = case_sections.get(case_number, "")
            owner = self._extract_owner(section)
            hearing = self._extract_hearing(section) or self._extract_hearing(match.group(0))
            opened = self._extract_case_opened(section)
            violations = self._extract_violations(section)
            high_signal = self._is_high_signal(section, hearing)

            notes_parts = [f"BDSR {case_number}"]
            if high_signal:
                notes_parts.append("[HIGH-SIGNAL]")
            if hearing:
                notes_parts.append(hearing.lower())
            if opened:
                notes_parts.append(f"opened: {opened}")
            if violations:
                notes_parts.append(f"violations: {violations[:160]}")
            notes_parts.append(f"meeting: {packet.meeting_date}")

            full_address = f"{address}, Johnson City, TN"
            leads.append(
                LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=self.make_lead_key(self.name, case_number),
                    property_address=full_address,
                    county="Washington County",
                    owner_name_records=owner,
                    distress_type="CODE_VIOLATION",
                    admin_notes=" | ".join(notes_parts),
                    source_url=packet.pdf_url,
                    raw_payload={
                        "johnson_city_bdsr": {
                            "case_number": case_number,
                            "meeting_id": packet.meeting_id,
                            "meeting_date": packet.meeting_date,
                            "title": packet.title,
                            "address": address,
                            "owner_block": owner,
                            "section_excerpt": section[:1500],
                        }
                    },
                )
            )
        return leads

    def _case_sections(self, text: str) -> Dict[str, str]:
        sections: Dict[str, str] = {}
        for match in SUBJECT_RX.finditer(text):
            sections[match.group("case").upper()] = match.group(0)
        return sections

    def _extract_owner(self, section: str) -> Optional[str]:
        match = TO_OWNER_RX.search(section)
        if not match:
            return None
        lines = [
            self._clean(line)
            for line in match.group("owner").splitlines()
            if self._clean(line)
        ]
        if not lines:
            return None
        # Keep the named owner/agent, not the full mailing address.
        owner_lines = []
        for line in lines:
            if re.search(r"\b\d{3,}\b|,\s*[A-Z]{2}\s+\d{5}", line):
                break
            owner_lines.append(line)
            if len(owner_lines) >= 2:
                break
        return " / ".join(owner_lines) or lines[0]

    def _extract_hearing(self, text: str) -> Optional[str]:
        match = HEARING_RX.search(text or "")
        return self._clean(match.group("hearing")) if match else None

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
            if desc and desc not in found:
                found.append(desc)
            if len(found) >= 4:
                break
        return "; ".join(found)

    def _is_high_signal(self, section: str, hearing: Optional[str]) -> bool:
        haystack = f"{section or ''} {hearing or ''}".upper()
        return any(
            term in haystack
            for term in (
                "UNFIT FOR HUMAN OCCUPATION",
                "DEMOLITION",
                "SHOW CAUSE",
                "DILAPIDATION",
                "BLIGHTED",
                "STRUCTURAL",
                "VACANT STRUCTURES",
                "PUBLIC HEARING",
            )
        )

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
