"""Track Shelby Environmental Court receiver-eligible cases for research."""
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import re

import pdfplumber

from ._base import BotBase, BotTimeout, _supabase
from ._owner_lookup import LegacyTLSAdapter
from ._shelby_receiver_property import property_record
from ._shelby_receiver_crm import publish, reviews
from .shelby_receiver_history import collect_history
from .shelby_tax_sale_bot import EXTRACT, SCHEDULE, scheduled_parcels


SOURCE = "https://www.shelbycountytn.gov/DocumentCenter/View/35671"
WARRANT = re.compile(r"[0-9]{8}")
HEARING = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})\s*[-\u2013\u2014]+\s*\d")
PARCEL = re.compile(r"\bParcel ID\s*(\d{6})\s*([A-Z]?\d{4,5}[A-Z]?)(?=\s|$)", re.I)


def read_roster(content):
    cases = {}
    with pdfplumber.open(BytesIO(content)) as doc:
        stamp = re.match(r"D:(\d{8})", doc.metadata.get("CreationDate", ""))
        if not stamp:
            raise ValueError("Missing county PDF date")
        publication = datetime.strptime(stamp[1], "%Y%m%d").date()
        for number, page in enumerate(doc.pages, 1):
            table = page.extract_table() or []
            text = page.extract_text(use_text_flow=True) or ""
            if number == 1:
                if not table or not table[0][0].startswith("As of " + publication.strftime("%B ") + str(publication.day) + ","):
                    raise ValueError("County roster date needs verification")
                if len(table) < 2 or table[1][:2] != ["ADDRESS", "CIVIL WARRANT"] or table[1][3] != "NEXT COURT":
                    raise ValueError("Unrecognized receiver roster")
                table = table[2:]
            if not table:
                raise ValueError(f"Missing roster table on page {number}")
            prefix = " ".join(text.split())
            for row in table:
                if len(row) != 5:
                    raise ValueError("Receiver roster columns changed")
                address, warrant, _, hearing, note = row
                address = " ".join(address.split())
                if not WARRANT.fullmatch(warrant.strip()) or not re.match(r"^\d+\S*\s+\w", address):
                    raise ValueError("Unverified roster identity")
                warrant = warrant.strip()
                if (warrant, address) in cases:
                    raise ValueError(f"Duplicate case/address {warrant} {address}")
                date_match = HEARING.match(hearing)
                hearing_date = datetime.strptime(date_match[1], "%m/%d/%Y").date() if date_match else None
                # In some PDFs the defendant column overlaps adjacent rows visually.
                # Use content order, bounded by the same warrant and hearing date.
                identity = f"{address} {warrant} "
                parcel = None
                if date_match and prefix.count(identity) == 1:
                    tail = prefix.split(identity, 1)[1]
                    details, sep, _ = tail.partition(date_match[1])
                    found = PARCEL.findall(details)
                    if sep and len(found) == 1 and not re.search(r"\b\d{8}\b", details):
                        parcel = " ".join(found[0])
                cases[warrant, address] = {"warrant": warrant, "address": address, "parcel": parcel,
                                  "hearing_listed": hearing_date.isoformat() if hearing_date else None,
                                  "county_note": " ".join((note or "").split()), "page": number}
    return {"source_url": SOURCE, "source_date": publication.isoformat(), "sha256": sha256(content).hexdigest(),
            "disposition": "receiver_eligibility_research", "cases": list(cases.values())}


def attach_trustee_list(receipt, leads):
    by_parcel = {}
    for lead in leads:
        raw = lead.raw_payload
        key = "".join(raw["parcel"].split()).upper()
        by_parcel.setdefault(key, []).append({
            "parcel": raw["parcel"], "alt_parcel": raw["alt_parcel"],
            "trustee_street": (lead.property_address or "").split(",")[0],
            "sale_id": raw["sale_id"], "advertised_date": lead.trustee_sale_date, "source_url": lead.source_url,
        })
    for case in receipt["cases"]:
        if case["parcel"]:
            key = "".join(case["parcel"].split()).upper()
            case["current_trustee_listings"] = by_parcel.get(key, [])
    receipt["trustee_schedule_url"] = SCHEDULE
    receipt["trustee_extract_url"] = EXTRACT


class ShelbyReceiverBot(BotBase):
    name = "shelby_receiver_roster"
    description = "Shelby Environmental Court receiver-eligible property research"

    def __init__(self):
        super().__init__()
        host = "https://apps2.shelbycountytrustee.com/"
        self.session.mount(host, LegacyTLSAdapter(max_retries=self.session.get_adapter(host).max_retries))

    def attach_property_records(self, receipt, day):
        receipt.update(receiver_appointment_verified=False, sale_authority_verified=False)
        for case in receipt["cases"]:
            listings = case.get("current_trustee_listings", [])
            for listing in listings:
                if len(listings) != 1:
                    listing["property_review"] = {"status": "ambiguous_listing"}
                    continue
                response = self.fetch(listing["source_url"], timeout=15)
                if response is None or not response.ok:
                    listing["property_review"] = {"status": "source_unavailable", "checked_on": day.isoformat()}
                    continue
                listing["property_review"] = property_record(response.text, case, listing, day)

    def run(self, *, archive_seed=None):
        started = datetime.now(timezone.utc)
        self._report_health("running", started, None, 0, 0, 0, 0)
        receipt, error, status = None, None, "ok"
        try:
            response = self.fetch(SOURCE)
            if response is None or response.status_code != 200 or not response.content.startswith(b"%PDF"):
                raise ValueError("County roster PDF unavailable")
            receipt = read_roster(response.content)
            schedule = self.fetch(SCHEDULE)
            extract = self.fetch(EXTRACT)
            if schedule is None or not schedule.ok or extract is None or not extract.ok:
                raise ValueError("Trustee list unavailable for roster cross-check")
            leads = scheduled_parcels(schedule.text, extract.content.decode("utf-8-sig"), started.date())
            attach_trustee_list(receipt, leads)
            self.attach_property_records(receipt, started.date())
            client = _supabase()
            if client is None:
                raise RuntimeError("Receiver staging unavailable")
            if archive_seed is None:
                last = (client.table("bot_run_health").select("notes->sale_book_history")
                        .eq("bot_source", self.name).eq("status", "ok").order("started_at", desc=True)
                        .limit(1).execute().data or [])
                archive_seed = last[0].get("sale_book_history") if last else None
            receipt["sale_book_history"] = collect_history(receipt, self.session, previous=archive_seed,
                                                          max_books=2, day=started.date())
            receipt.update(publish(client, reviews(receipt, started.date()), started.date()))

        except BotTimeout as exc:
            status, error = "timed_out", str(exc)
        except Exception as exc:
            status, error = "failed", str(exc)
        count = len(receipt["cases"]) if receipt else 0
        self._report_health(status, started, datetime.now(timezone.utc), count, count, 0, 0,
                            error_message=error, notes=receipt)
        return {"name": self.name, "run_id": self.run_id, "status": status, "fetched": count,
                "staged": 0, "duplicates": 0, "error": error}


def run():
    return ShelbyReceiverBot().run()
