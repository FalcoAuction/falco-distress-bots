"""
Knoxville Public Officer Hearing (POH) PDF scraper.

Source: knoxvilletn.gov publishes monthly Public Officer Hearing PDFs
documenting code-enforcement cases (dilapidated, demolish, repair
orders) in Knoxville (Knox County). No public API — only PDFs.

We parse two PDFs:
  - agenda_poh_results.pdf  — most recent finished hearing
  - agenda_poh.pdf           — upcoming hearing agenda

Both are overwritten monthly. Cases include:
  - Boarding approvals list (addresses with boarding dates)
  - Detailed repair/demolition orders (full address, owner, parcel,
    violations, tax-delinquency status)

Distress type: CODE_VIOLATION
Owner data extracted from per-case sections is HIGH-quality (full name +
mailing address often DIFFERENT from property = absentee owner signal).
"""

from __future__ import annotations

import re
from io import BytesIO
from typing import List, Optional

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from ._base import BotBase, LeadPayload


KNOXVILLE_POH_PDFS = [
    (
        "results",
        "https://www.knoxvilletn.gov/UserFiles/Servers/Server_109478/"
        "File/Boards/betterbuilding/agenda_poh_results.pdf",
    ),
    (
        "agenda",
        "https://www.knoxvilletn.gov/UserFiles/Servers/Server_109478/"
        "File/Boards/betterbuilding/agenda_poh.pdf",
    ),
    (
        "bbb_results",
        "https://www.knoxvilletn.gov/UserFiles/Servers/Server_109478/"
        "File/Boards/betterbuilding/agenda_bbb_results.pdf",
    ),
    (
        "bbb_agenda",
        "https://www.knoxvilletn.gov/UserFiles/Servers/Server_109478/"
        "File/Boards/betterbuilding/agenda_bbb.pdf",
    ),
]


# Regex patterns for case-section parsing
_CASE_HEADER_RX = re.compile(
    r"^[A-Z]\.\s+([\d\-]+\s+[A-Z][A-Z0-9\s.,'\-/]+?)(?:\s+INCLUDING.*)?$",
    re.MULTILINE,
)
_PROPID_RX = re.compile(r"PROPERTY\s+IDENTIFICATION\s+NO:\s*(\S+)", re.IGNORECASE)
_OWNER_RX = re.compile(
    r"OWNERS?\s+AND\s+OTHER\s+INTERESTED\s+PARTIES:?\s*\n+\s*([A-Z][A-Z\s,.\-']+)",
    re.IGNORECASE | re.MULTILINE,
)
_VIOLATIONS_RX = re.compile(
    r"VIOLATIONS:?\s*\n?\s*([^\n]+(?:\n[^\n]+)?)",
    re.IGNORECASE,
)
_RESULTS_RX = re.compile(r"RESULTS:?\s*\n+\s*([^\n]+)", re.IGNORECASE)
_BOARDING_LINE_RX = re.compile(
    r"^([\d\-]+\s+[A-Z][A-Z0-9\s.,'\-/]+?)\s+[–\-]\s+Boarded\s+(\d{1,2}/\d{1,2}/\d{4})\s+(APPROVED|POSTPONED|DENIED)",
    re.MULTILINE | re.IGNORECASE,
)


class KnoxvillePohBot(BotBase):
    name = "knoxville_poh"
    description = "Knoxville Public Officer Hearing — code enforcement case dockets (PDFs)"
    throttle_seconds = 1.5
    expected_min_yield = 5  # PDFs publish monthly; small but high-signal

    def scrape(self) -> List[LeadPayload]:
        if pdfplumber is None:
            self.logger.error("pdfplumber not installed; pip install pdfplumber")
            return []

        all_leads: List[LeadPayload] = []
        for label, url in KNOXVILLE_POH_PDFS:
            self.logger.info(f"fetching {label}: {url}")
            res = self.fetch(url)
            if res is None or res.status_code != 200:
                self.logger.warning(f"  {label} fetch failed: {res.status_code if res else 'none'}")
                continue
            if "pdf" not in res.headers.get("Content-Type", "").lower():
                self.logger.warning(f"  {label} not PDF: {res.headers.get('Content-Type')}")
                continue

            try:
                pdf_text = self._extract_text(res.content)
            except Exception as e:
                self.logger.warning(f"  {label} pdfplumber failed: {e}")
                continue

            leads_from_pdf = self._parse_pdf_text(pdf_text, label, url)
            self.logger.info(f"  {label}: {len(leads_from_pdf)} leads parsed")
            all_leads.extend(leads_from_pdf)

        # Dedupe by address within this run (same address may appear in
        # boarding list AND case section)
        seen_addrs = set()
        deduped: List[LeadPayload] = []
        for lead in all_leads:
            key = (lead.property_address or "").strip().upper()
            if key in seen_addrs:
                continue
            seen_addrs.add(key)
            deduped.append(lead)

        self.logger.info(f"deduped to {len(deduped)} unique leads")
        return deduped

    def _extract_text(self, pdf_bytes: bytes) -> str:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            return "\n".join((page.extract_text() or "") for page in pdf.pages)

    def _parse_pdf_text(self, text: str, source_label: str, url: str) -> List[LeadPayload]:
        leads: List[LeadPayload] = []

        # 1. Boarding-approvals list lines
        for m in _BOARDING_LINE_RX.finditer(text):
            address_raw, boarded_date, status = m.group(1), m.group(2), m.group(3)
            address = self._normalize_address(address_raw)
            if not address:
                continue
            full_addr = f"{address}, Knoxville, TN"
            lead_id = f"poh-board-{address.lower().replace(' ', '-')}-{boarded_date}"
            leads.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=self.make_lead_key(self.name, lead_id),
                property_address=full_addr,
                county="Knox County",
                distress_type="CODE_VIOLATION",
                admin_notes=f"POH boarding {status} · boarded {boarded_date} · src={source_label}",
                source_url=url,
                raw_payload={"poh_section": "boarding", "status": status, "date": boarded_date},
            ))

        # 2. Per-case detailed sections
        # Split text on case headers (e.g. "A. 211 RICHMOND AVENUE")
        case_splits = re.split(r"^([A-Z])\.\s+([\d\-]+\s+[A-Z][^\n]+)$", text, flags=re.MULTILINE)
        # Result groups: [intro, letter, header, body, letter, header, body, ...]
        for i in range(1, len(case_splits) - 2, 3):
            header_address = case_splits[i + 1].strip()
            body = case_splits[i + 2]
            address = self._normalize_address(header_address.split("INCLUDING")[0])
            if not address:
                continue
            full_addr = f"{address}, Knoxville, TN"
            lead = self._build_case_lead(full_addr, body, source_label, url)
            if lead is not None:
                leads.append(lead)

        return leads

    def _build_case_lead(
        self, full_address: str, body: str, source_label: str, url: str
    ) -> Optional[LeadPayload]:
        # Extract structured fields from the case body
        prop_id_m = _PROPID_RX.search(body)
        prop_id = prop_id_m.group(1) if prop_id_m else None

        owner_m = _OWNER_RX.search(body)
        owner = owner_m.group(1).strip() if owner_m else None

        violations_m = _VIOLATIONS_RX.search(body)
        violations = violations_m.group(1).strip().replace("\n", " ") if violations_m else ""

        results_m = _RESULTS_RX.search(body)
        results = results_m.group(1).strip() if results_m else ""

        # Tax-delinquency hint (huge signal — owner can't pay city/county taxes)
        tax_hint = "tax delinquent" if re.search(r"UNPAID\s+\d{4}", body, re.IGNORECASE) else ""

        priority = " [HIGH-SIGNAL]" if any(
            kw in violations.upper() or kw in results.upper()
            for kw in ("DEMO", "STRUCTURAL", "DILAPIDATED", "UNFIT", "CONDEMNED")
        ) else ""

        notes_parts = [f"POH case{priority}"]
        if violations:
            notes_parts.append(f"violations: {violations[:120]}")
        if results:
            notes_parts.append(f"result: {results[:80]}")
        if tax_hint:
            notes_parts.append(tax_hint)
        if prop_id:
            notes_parts.append(f"parcel: {prop_id}")
        notes_parts.append(f"src={source_label}")

        lead_id = f"poh-case-{full_address.lower().replace(' ', '-')[:60]}"

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, lead_id),
            property_address=full_address,
            county="Knox County",
            owner_name_records=owner,
            distress_type="CODE_VIOLATION",
            admin_notes=" · ".join(notes_parts),
            source_url=url,
            raw_payload={"poh_section": "case", "body_excerpt": body[:400]},
        )

    @staticmethod
    def _normalize_address(raw: str) -> Optional[str]:
        s = re.sub(r"\s+", " ", raw or "").strip()
        # Strip common trailing junk like "INCLUDING ACCESSORY STRUCTURE"
        s = re.sub(r"\s+(INCLUDING|AND|WITH)\s+.*$", "", s, flags=re.IGNORECASE)
        # Must start with a number
        if not re.match(r"^\d", s):
            return None
        return s.title()  # Boring title-case for cleanliness


def run() -> dict:
    bot = KnoxvillePohBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
