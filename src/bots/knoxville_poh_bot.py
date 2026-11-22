"""
Knoxville hearing-petition acquisition from the official CivicPlus page.

Discover the currently linked POH/Better Building Board documents on each
run; old /UserFiles URLs no longer resolve. Current packets identify cases
by record number and CLT parcel. An agenda establishes a published petition,
not a final order, current ownership, unpaid tax balance, or sale authority.

Retain date/hash/URL and named-party evidence, refresh exact case/parcel
matches, and never infer a property closure from monthly document absence
or cancellation of a board meeting. Address-only boarding observations and
legacy parser output lack the controls required for new acquisition.
"""

from __future__ import annotations

import re
import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, quote
from bs4 import BeautifulSoup
from io import BytesIO
from typing import List, Optional

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from ._base import BotBase, LeadPayload, _supabase, guard_phone_metadata


KNOXVILLE_HEARINGS_URL = "https://www.knoxvilletn.gov/173/Better-Building-Board-Public-Officer-Hea"
REVIEW_KEY = "knoxville_hearing_review"


def hearing_date(text):
    # The cover date, never an inspection date or a later meeting mentioned in the body.
    match = re.search(r"\b(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{1,2}),?\s+(20\d{2})\b", text[:1600], re.I)
    if not match:
        return None
    try:
        return datetime.strptime(" ".join(match.groups()), "%B %d %Y").date().isoformat()
    except ValueError:
        return None


def discover_documents(html):
    documents = set()
    for link in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        label = link.get_text(" ", strip=True).lower()
        if "agenda" not in label and "results" not in label and "minutes" not in label:
            continue
        board = "poh" if "public officer" in label else "bbb" if "better building" in label else None
        if board is None:
            continue
        url = urljoin(KNOXVILLE_HEARINGS_URL, link["href"])
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in ("www.knoxvilletn.gov", "knoxvilletn.gov") or not parsed.path.startswith("/DocumentCenter/View/"):
            continue
        kind = "results" if "results" in label or "minutes" in label else "agenda"
        documents.add((board + "_" + kind, url))
    if not documents:
        raise RuntimeError("No official Knoxville hearing document links found")
    return sorted(documents)


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
        self._reviews = {}
        self.source_documents = []
        if pdfplumber is None:
            raise RuntimeError("pdfplumber is required")
        page = self.fetch(KNOXVILLE_HEARINGS_URL)
        if page is None or page.status_code != 200:
            raise RuntimeError("Official Knoxville hearing page unavailable")
        grouped = {}
        failed = 0
        for label, url in discover_documents(page.text):
            response = self.fetch(url)
            if response is None or response.status_code != 200 or not response.content.startswith(b"%PDF"):
                failed += 1
                continue
            try:
                text = self._extract_text(response.content)
                date = hearing_date(text)
                if not date:
                    raise ValueError("No explicit hearing date")
            except Exception as exc:
                failed += 1
                self.logger.warning("Knoxville document unavailable (%s)", type(exc).__name__)
                continue
            cancelled = bool(re.search(r"MEETING\s+HAS\s+BEEN\s+CANCEL(?:L)?ED", text, re.I))
            document = {"source_url": url, "source_page_url": KNOXVILLE_HEARINGS_URL,
                        "source_sha256": hashlib.sha256(response.content).hexdigest(),
                        "hearing_date": date, "document_kind": label,
                        "meeting_cancelled": cancelled,
                        "checked_on": datetime.now(timezone.utc).date().isoformat()}
            self.source_documents.append(document)
            if cancelled:
                continue  # Cancellation of a meeting is not closure of its properties.
            for lead in self._parse_pdf_text(text, label, url):
                raw = lead.raw_payload or {}
                if raw.get("poh_section") != "case" or not raw.get("case_number") or not raw.get("parcel_id"):
                    continue  # Address-only boarding lists cannot identify an enforcement case.
                evidence = {**document, "case_number": raw["case_number"], "parcel_id": raw["parcel_id"],
                            "property_address": lead.property_address,
                            "violations": raw["violations"], "named_parties": raw["named_parties"],
                            "status": "agenda_outcome_unverified" if label.endswith("agenda") else "published_result_review",
                            "owner_verified": False, "sale_authority_verified": False,
                            "current_order_verified": False}
                lead.raw_payload[REVIEW_KEY] = evidence
                grouped.setdefault(lead.pipeline_lead_key, []).append(lead)
        self.logger.info("Knoxville documents=%s failed=%s", len(self.source_documents), failed)
        if not self.source_documents:
            raise RuntimeError("No dated official Knoxville documents retrieved")
        leads = []
        for key, copies in grouped.items():
            if len({c.property_address for c in copies}) != 1:
                continue
            newest = max(copies, key=lambda c: (c.raw_payload[REVIEW_KEY]["hearing_date"], c.raw_payload[REVIEW_KEY]["document_kind"].endswith("results")))
            peers = [c for c in copies if c.raw_payload[REVIEW_KEY]["hearing_date"] == newest.raw_payload[REVIEW_KEY]["hearing_date"] and c.raw_payload[REVIEW_KEY]["document_kind"] == newest.raw_payload[REVIEW_KEY]["document_kind"]]
            if len({c.raw_payload[REVIEW_KEY]["source_sha256"] for c in peers}) > 1:
                continue
            leads.append(newest)
            self._reviews[key] = newest.raw_payload[REVIEW_KEY]
        return leads

    def _write_staging(self, leads):
        if not getattr(self, "_reviews", None):
            return 0, 0
        client = _supabase()
        if client is None:
            return 0, 0
        keys = list(self._reviews)
        existing = []
        for offset in range(0, len(keys), 80):
            existing.extend(client.table("homeowner_requests_staging").select("pipeline_lead_key").eq("bot_source", self.name).in_("pipeline_lead_key", keys[offset:offset+80]).execute().data or [])
        known = {r["pipeline_lead_key"] for r in existing}
        fresh = [lead for lead in leads if lead.pipeline_lead_key not in known]
        staged, _ = super()._write_staging(fresh) if fresh else (0, 0)
        self.refresh_counts = {"updated": 0, "failed": 0, "protected": 0, "oversized": 0, "conflicts": 0}
        for offset in range(0, len(keys), 80):
            for table in ("homeowner_requests_staging", "homeowner_requests"):
                q = client.table(table).select("id,pipeline_lead_key,property_address,county,phone_metadata")
                q = q.eq("bot_source", self.name) if table.endswith("_staging") else q.eq("source", "bot")
                try:
                    rows = q.in_("pipeline_lead_key", keys[offset:offset+80]).execute().data or []
                except Exception as exc:
                    self.refresh_counts["failed"] += 1
                    self.logger.warning("Knoxville hydration failed (%s)", type(exc).__name__)
                    continue
                for row in rows:
                    evidence = self._reviews[row["pipeline_lead_key"]]
                    patch = review_patch(row, evidence)
                    if patch is None:
                        self.refresh_counts["protected"] += 1
                        continue
                    old = row.get("phone_metadata")
                    if max(len(quote(json.dumps(v), safe="")) for v in (old, patch["phone_metadata"])) > 6000:
                        self.refresh_counts["oversized"] += 1
                        continue
                    update = client.table(table).update(patch).eq("id", row["id"]).eq("pipeline_lead_key", row["pipeline_lead_key"])
                    update = update.eq("bot_source", self.name) if table.endswith("_staging") else update.eq("source", "bot")
                    update = update.eq("property_address", row["property_address"]).eq("county", row["county"])
                    try:
                        written = guard_phone_metadata(update, old).execute().data
                        self.refresh_counts["updated" if written else "conflicts"] += 1
                    except Exception as exc:
                        self.refresh_counts["failed"] += 1
                        self.logger.warning("Knoxville update failed (%s)", type(exc).__name__)
        from ._knox_poh_assessor import refresh
        self.assessor_counts = refresh(client, self._reviews, self.fetch)
        self.logger.info("Knox County tax roll %s", self.assessor_counts)
        return staged, len(leads) - len(fresh)

    def _extract_text(self, pdf_bytes: bytes) -> str:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            return "\n".join((page.extract_text() or "") for page in pdf.pages)

    def _parse_pdf_text(self, text: str, source_label: str, url: str) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        # Current CivicPlus packet layout has explicit case numbers and CLT parcels.
        headers = list(re.finditer(r"^ADDRESS:\s*(.+?)\s+RECORD NUMBER:\s*([A-Z0-9-]+)\s*$", text, re.M))
        for index, header in enumerate(headers):
            body = text[header.end():headers[index + 1].start() if index + 1 < len(headers) else len(text)]
            parcel = re.search(r"CLT NUMBER:[ \t]*([0-9][A-Z0-9.-]*)\b", body)
            violation = re.search(r"^VIOLATIONS:\s*([^\n]+)", body, re.M)
            parties = re.search(r"OWNERS AND OTHER INTERESTED PARTIES:\s*\n(.*?)(?=\nVIOLATIONS:)", body, re.S)
            address = self._normalize_address(header.group(1))
            if not parcel or not violation or not address:
                continue
            case = header.group(2)
            names = [re.sub(r"^[•●\-]\s*", "", line).strip() for line in (parties.group(1).splitlines() if parties else []) if line.strip()]
            leads.append(LeadPayload(bot_source=self.name,
                pipeline_lead_key=self.make_lead_key(self.name, f"poh-record-{case}-{parcel.group(1)}"),
                property_address=f"{address}, Knoxville, TN", county="Knox County",
                distress_type="CODE_VIOLATION", owner_name_records=None,
                admin_notes=f"POH published petition {case}; violations: {violation.group(1).strip()}; outcome unverified",
                source_url=url, raw_payload={"poh_section": "case", "case_number": case,
                    "parcel_id": parcel.group(1), "violations": violation.group(1).strip(),
                    "named_parties": names, "body_excerpt": body[:400]}))
        if headers:
            return leads

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



def review_patch(row, evidence):
    """Refresh exact case/parcel keys only; an absent monthly agenda is not closure."""
    pm = row.get("phone_metadata")
    if pm is not None and not isinstance(pm, dict):
        return None
    expected = BotBase.make_lead_key("knoxville_poh", f"poh-record-{evidence['case_number']}-{evidence['parcel_id']}")
    if row.get("pipeline_lead_key") != expected or row.get("property_address", "").casefold() != evidence["property_address"].casefold() or row.get("county", "").casefold() != "knox county":
        return None
    previous = (pm or {}).get(REVIEW_KEY)
    if isinstance(previous, dict):
        if previous.get("hearing_date", "") > evidence["hearing_date"] or previous.get("checked_on", "") > evidence["checked_on"]:
            return None
        if previous.get("hearing_date") == evidence["hearing_date"] and previous.get("document_kind", "").endswith("results") and evidence["document_kind"].endswith("agenda"):
            return None
    metadata = {**(pm or {}), REVIEW_KEY: evidence}
    return None if metadata == pm else {"phone_metadata": metadata}

def run() -> dict:
    bot = KnoxvillePohBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
