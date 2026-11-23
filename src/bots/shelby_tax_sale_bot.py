"""Shelby Trustee's scheduled tax-sale inventory, keyed by sale and exact parcel."""
import csv
import base64
import io
import json
import re
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import unquote, urlencode
from uuid import NAMESPACE_URL, uuid5

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload, _supabase
from ._owner_lookup import LegacyTLSAdapter
from ._tax_tract_review import CARD_KEY, STATUS_KEY, next_status, patch_for, shelby_card, shelby_wants_card

SCHEDULE = "https://shelbycountytrustee.com/191/Tax-Sale-Schedule"
EXTRACT = "https://scgpublic.s3.amazonaws.com/TaxSaleExtract.csv"
REMOVED = "Parcel no longer on current Shelby trustee tax sale list"
SALE_PASSED = "Shelby tax sale date passed; sale result not verified"
STAGING = "homeowner_requests_staging"
DETAIL_BUDGET = 12          # Trustee parcel pages per run, new and refreshed together
REFRESH_AFTER_DAYS = 7      # a first-trace card's balance is re-read after this long


def scheduled_parcels(html, contents, today):
    page = BeautifulSoup(html, "html.parser")
    if not page.find("a", href=EXTRACT):
        raise ValueError("Shelby schedule no longer links the known sale extract")
    sales = {}
    for row in page.select("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.select("td")]
        if len(cells) >= 2 and re.fullmatch(r"\d{4}", cells[0]):
            sales["TS" + cells[0]] = datetime.strptime(cells[1], "%m/%d/%Y").date()
    reader = csv.DictReader(io.StringIO(contents))
    required = {"ParcelID", "Alt_Parcel", "Street Number", "Street Name", "Tax Sale"}
    if not sales or not required.issubset(reader.fieldnames or []):
        raise ValueError("Missing Shelby schedule or parcel columns")
    leads = {}
    for row in reader:
        code = row["Tax Sale"].strip()
        parcel = row["ParcelID"].strip()
        alt = row["Alt_Parcel"].strip()
        if code not in sales or not parcel or not re.fullmatch(r"[0-9A-Z]{14}", alt):
            raise ValueError("Unrecognized Shelby sale or parcel")
        if sales[code] < today:
            continue
        key = BotBase.make_lead_key("shelby_tax_sale", f"{code}|{alt}")
        street = row["Street Name"].strip()
        number = row["Street Number"].strip()
        address = f"{number} {street}, Shelby County, TN" if re.fullmatch(r"[1-9]\d*", number) and street else None
        lead = LeadPayload(bot_source="shelby_tax_sale", pipeline_lead_key=key, county="shelby",
                           distress_type="TAX_LIEN", property_address=address,
                           trustee_sale_date=sales[code].isoformat(),
                           source_url="https://apps2.shelbycountytrustee.com/Parcel?" + urlencode({"parcel": parcel}),
                           raw_payload={"sale_id": code, "parcel": parcel, "alt_parcel": alt, "street": street,
                                        "schedule_url": SCHEDULE, "list_url": EXTRACT})
        if key in leads and leads[key] != lead:
            raise ValueError("Conflicting Shelby parcel rows")
        leads[key] = lead
    if not reader.line_num or reader.line_num <= 1:
        raise ValueError("Empty Shelby extract; cannot verify removals")
    return list(leads.values())


def staging_row(lead, run_id):
    return {**lead.as_db_row(run_id), "id": str(uuid5(NAMESPACE_URL, "shelby_tax_sale/" + lead.pipeline_lead_key))}


def parcel_evidence(html, facts, as_of):
    """Accept the trustee's detail only when the parcel AND sale designation agree."""
    page = BeautifulSoup(html, "html.parser")
    words = list(page.stripped_strings)
    def field(label):
        if label in words and words.index(label) + 1 < len(words):
            return words[words.index(label) + 1]
        return ""
    if (" ".join(field("Parcel ID:").split()) != " ".join(facts["parcel"].split())
            or field("Tax Sale ID:") != facts["sale_id"]):
        return {}
    evidence = {"checked_on": as_of.isoformat()}
    owner = field("Primary Owner")
    if owner and owner.upper() not in {"UNKNOWN", "N/A", "LOCATION"}:
        evidence["primary_owner"] = owner
    balance = field("Total Balance")
    if re.fullmatch(r"\$[\d,]+\.\d{2}", balance):
        evidence["total_tax_balance"] = float(balance[1:].replace(",", ""))
    for tr in page.select("tr"):
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if (len(cells) == 8 and cells[2] == "Shelby" and cells[1].isdigit()
                and int(cells[1]) < as_of.year and re.fullmatch(r"\$[\d,]+\.\d{2}", cells[6])):
            due = float(cells[6][1:].replace(",", ""))
            if due > 0:
                evidence.setdefault("visible_prior_year_balances", {})[cells[1]] = due
        if len(cells) != 8 or cells[1:3] != [str(as_of.year), "Shelby"]:
            continue
        link = tr.select_one('a[href*="billNumberData="]')
        if not link:
            continue
        encoded = unquote(link["href"].split("billNumberData=", 1)[1])
        try:
            bill = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        except (ValueError, UnicodeError):
            continue
        if (bill.get("altParcelId") != facts["alt_parcel"] or str(bill.get("year")) != cells[1]
                or "REALTY" not in str(bill.get("description", "")).upper()):
            continue
        assessed = bill.get("assessment")
        if not isinstance(assessed, (int, float)) or assessed <= 0 or cells[7] != f"${assessed:,.2f}":
            continue
        code = (bill.get("classification") or "").strip()
        evidence.update(tax_year=as_of.year, assessed_value=assessed, tax_class=code)
        ratio = {"R": 0.25, "I": 0.40, "C": 0.40}.get(code)
        if ratio:
            evidence.update(assessment_ratio=ratio, estimated_appraisal=round(assessed / ratio))
    return evidence


class ShelbyTaxSaleBot(BotBase):
    name = "shelby_tax_sale"
    description = "Upcoming real property tax sales on the Shelby County Trustee parcel list"
    expected_min_yield = 0
    frozen_after_runs = 60

    def __init__(self):
        super().__init__()
        # Only the trustee's parcel host needs the older initial TLS handshake.
        host = "https://apps2.shelbycountytrustee.com/"
        self.session.mount(host, LegacyTLSAdapter(max_retries=self.session.get_adapter(host).max_retries))

    def scrape(self):
        self.snapshot_complete = False
        self.outage = "Shelby trustee inventory unavailable"
        schedule = self.fetch(SCHEDULE)
        extract = self.fetch(EXTRACT)
        if schedule is None or not schedule.ok or extract is None or not extract.ok:
            raise ValueError(self.outage)
        try:
            leads = scheduled_parcels(schedule.text, extract.content.decode("utf-8-sig"), date.today())
        except ValueError as exc:
            self.outage = "Shelby trustee list unreadable: " + str(exc)
            raise
        try:
            self.list_date = parsedate_to_datetime(extract.headers.get("Last-Modified", "")).date().isoformat()
        except (TypeError, ValueError):
            self.list_date = None
        self.snapshot_complete = True
        return leads

    def _write_staging(self, leads):
        # BotBase also calls this method after a failed scrape. A failed or
        # malformed snapshot retires nothing: every tracked tract is dated
        # "unknown" for today and keeps its last confirmed status.
        if not getattr(self, "snapshot_complete", False):
            client = _supabase()
            if client is not None:
                self._review_tracts(client, None, date.today(), getattr(self, "outage", None))
            return 0, 0
        client = _supabase()
        if client is None:
            raise RuntimeError("Staging unavailable")
        table = client.table(STAGING)
        today = date.today()
        rows = [staging_row(lead, self.run_id) for lead in leads]
        current = {row["id"]: row for row in rows}
        inserted = 0
        for start in range(0, len(rows), 100):
            result = table.upsert(rows[start:start + 100], on_conflict="id", ignore_duplicates=True,
                                  default_to_null=False).execute()
            inserted += len(result.data or [])
        after = "00000000-0000-0000-0000-000000000000"
        while True:
            page = (table.select("id,staging_status,rejection_reason,trustee_sale_date")
                    .eq("bot_source", self.name).gt("id", after).order("id").limit(500).execute().data or [])
            for old in page:
                listed = current.get(old["id"])
                patch = {}
                if listed and old["staging_status"] == "rejected" and old["rejection_reason"] == REMOVED:
                    patch = {"staging_status": "pending", "rejection_reason": None,
                             "trustee_sale_date": listed["trustee_sale_date"]}
                elif old["staging_status"] == "pending":
                    if not listed:
                        passed = (old["trustee_sale_date"] or "9999") < today.isoformat()
                        patch = {"staging_status": "rejected", "rejection_reason": SALE_PASSED if passed else REMOVED}
                    elif old["trustee_sale_date"] != listed["trustee_sale_date"]:
                        patch = {"trustee_sale_date": listed["trustee_sale_date"]}
                if patch:
                    query = table.update(patch).eq("id", old["id"]).eq("staging_status", old["staging_status"])
                    if old["staging_status"] == "rejected":
                        query = query.eq("rejection_reason", REMOVED)
                    query.execute()
            if len(page) < 500:
                break
            after = page[-1]["id"]
        self._fill_details(client, current)
        self._review_tracts(client, current, today)
        return inserted, len(rows) - inserted

    def _fill_details(self, client, current):
        from .free_county_completion_bot import apply
        records = (client.table("homeowner_requests_staging")
                   .select("id,raw_payload,owner_name_records,property_value,property_value_source")
                   .eq("bot_source", self.name).eq("staging_status", "pending")
                   .is_("raw_payload->trustee_detail", "null").order("id").limit(12).execute().data or [])
        self.detail_fetches = 0
        for row in records:
            listed = current.get(row["id"])
            if not listed:
                continue
            self.detail_fetches += 1
            res = self.fetch(listed["source_url"], timeout=12)
            if res is None or not res.ok:
                break
            evidence = parcel_evidence(res.text, listed["raw_payload"], date.today())
            if not evidence:
                continue
            patch = {"raw_payload": {**row["raw_payload"], "trustee_detail": evidence}}
            if evidence.get("primary_owner") and not row.get("owner_name_records"):
                patch["owner_name_records"] = evidence["primary_owner"]
            if evidence.get("estimated_appraisal") and row.get("property_value") is None:
                patch.update(property_value=evidence["estimated_appraisal"],
                             property_value_source="shelby_trustee_assessment_estimate")
            apply(client, "homeowner_requests_staging", row, patch)

    def _refresh_details(self, client, rows, current, today, *, ids=None, budget=None, max_age_days=REFRESH_AFTER_DAYS):
        """Re-read the Trustee balance behind the oldest first-trace cards so
        the tax debt on a card carries a recent as-of date."""
        budget = DETAIL_BUDGET - getattr(self, "detail_fetches", 0) if budget is None else budget
        cutoff = (today - timedelta(days=max_age_days)).isoformat()

        def due(row):
            raw = row["raw_payload"] or {}
            if row["id"] not in current or not raw.get("trustee_detail"):
                return False
            if ids is not None:
                return row["id"] in ids
            return ((raw.get(CARD_KEY) or {}).get("route") == "first_trace_review"
                    and raw["trustee_detail"].get("checked_on", "") <= cutoff)

        oldest = sorted(filter(due, rows), key=lambda r: r["raw_payload"]["trustee_detail"].get("checked_on", ""))
        refreshed = []
        for row in oldest[:max(budget, 0)]:
            listed = current[row["id"]]
            res = self.fetch(listed["source_url"], timeout=12)
            if res is None or not res.ok:
                break
            raw = row["raw_payload"]
            old = raw["trustee_detail"]
            evidence = parcel_evidence(res.text, listed["raw_payload"], today)
            if evidence:
                if old.get("total_tax_balance") is not None:
                    evidence["previous_total_tax_balance"] = {"amount": old["total_tax_balance"],
                                                              "as_of": old.get("checked_on")}
                patch = {"trustee_detail": evidence, "trustee_detail_recheck": None}
            else:
                patch = {"trustee_detail_recheck": {"checked_on": today.isoformat(),
                                                    "status": "parcel_or_sale_not_confirmed"}}
            client.rpc("apply_staging_enrichment", {"p_rows": [{"id": row["id"], "raw_patch": patch}]}).execute()
            row["raw_payload"] = {**raw, **patch}
            refreshed.append(row["id"])
        return refreshed

    def _review_tracts(self, client, current, today, reason=None, *, write=True):
        """Date every tracked tract from this run's list and keep its review
        card current. current is None when the county list could not be read."""
        table = client.table(STAGING)
        rows, after = [], "00000000-0000-0000-0000-000000000000"
        while True:
            page = (table.select("id,staging_status,rejection_reason,property_address,owner_name_records,"
                                 "property_value,property_value_source,trustee_sale_date,source_url,raw_payload")
                    .eq("bot_source", self.name).gt("id", after).order("id").limit(500).execute().data or [])
            rows.extend(page)
            if len(page) < 500:
                break
            after = page[-1]["id"]
        if current is not None and write:
            self._refresh_details(client, rows, current, today)
        patches = []
        for row in rows:
            raw = row["raw_payload"] or {}
            listed = current.get(row["id"]) if current is not None else None
            prior = raw.get(STATUS_KEY)
            if prior is None and row["staging_status"] == "pending":
                # First dated check of a row the collector already carried as listed.
                prior = {"status": "listed", "sale_date": row["trustee_sale_date"]}
            status = next_status(prior, {
                "available": current is not None, "reason": reason, "listed": bool(listed),
                "sale_date": listed["trustee_sale_date"] if listed else None,
                "source_url": EXTRACT, "source_date": getattr(self, "list_date", None)}, today)
            status.setdefault("sale_id", raw.get("sale_id"))
            card = shelby_card(row, status, today, EXTRACT) if shelby_wants_card(row) else None
            patch = patch_for(raw, status, card)
            if patch:
                patches.append({"id": row["id"], "raw_patch": patch})
        if write:
            for start in range(0, len(patches), 200):
                client.rpc("apply_staging_enrichment", {"p_rows": patches[start:start + 200]}).execute()
        self.review_patches = patches
        return patches


def run():
    return ShelbyTaxSaleBot().run()
