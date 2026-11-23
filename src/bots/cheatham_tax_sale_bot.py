"""Follow Cheatham County delinquent tax sale tracts on the county's own notice.

The tracts are staged by court_notices from the court's published sale notice.
This bot stages nothing. Each run it reads the Notice of Delinquent Tax Sale
the County Trustee posts, and dates every staged tract:

  noticed    still printed on the posted notice. Cheatham County publishes no
             payment or release list, so this is NOT a confirmation that the
             tract is still set for sale; the card stays on research hold and
             names the Clerk and Master as the office to ask.
  removed    the posted notice for the same case no longer prints the tract
  sale_held  the advertised sale date has passed
  unknown    the notice could not be read, or the county has posted a notice
             for a different case; the prior status is kept

A changed sale date on the posted notice is carried to the staged row, with
the earlier date kept in the status.
"""
import re
from datetime import date, datetime, timezone
from hashlib import sha256
from io import BytesIO

import pdfplumber

from ._base import BotBase, BotTimeout, _supabase
from ._tax_tract_review import STATUS_KEY, cheatham_card, match_tract, next_status, patch_for

NOTICE = "https://www.cheathamcountytrustee.gov/pdfs/delinquent_tax_sale.pdf"
STAGING = "homeowner_requests_staging"
REMOVED = "Tract no longer on the Cheatham County posted tax sale notice"
SALE_PASSED = "Cheatham tax sale date passed; sale result not verified"


def read_notice(content):
    """Case, sale date and tracts from the posted notice. Raises ValueError
    when any of them cannot be read, so a changed layout is an unknown and
    never a removal."""
    with pdfplumber.open(BytesIO(content)) as doc:
        meta = doc.metadata or {}
        text = "\n".join((page.extract_text() or "") for page in doc.pages)
    text = text.translate({0x2013: "-", 0x2014: "-", 0x2010: "-", 0x2011: "-", 0xFFFD: "-", 0x2019: "'"})
    flat = " ".join(text.split())
    if "NOTICE OF DELINQUENT TAX SALE" not in flat.upper() or "CHANCERY COURT FOR CHEATHAM COUNTY" not in flat.upper():
        raise ValueError("Not a Cheatham delinquent tax sale notice")
    docket = re.search(r"Civil Action No\.\s*(\d+)", flat, re.I)
    scheduled = re.search(r"\d{1,2}:\d{2}\s*[ap]\.m\. on \w+, (\w+ \d{1,2}, \d{4})", flat, re.I)
    if not docket or not scheduled:
        raise ValueError("Cheatham notice is missing its case number or sale date")
    try:
        sale = datetime.strptime(scheduled[1], "%B %d, %Y").date()
    except ValueError:
        raise ValueError("Cheatham notice sale date is unreadable")
    entries, started, expected = [], False, "A"
    for line in (l.strip() for l in text.splitlines()):
        if not started:
            started = "described tracts" in line
            continue
        if line.startswith("Penalties, interest"):
            break
        if re.match(re.escape(expected) + r"\.\s+\S", line):
            entries.append([expected, line[2:].strip()])
            expected = chr(ord(expected) + 1)
        elif entries:
            entries[-1][1] += " " + line
    tracts = []
    for letter, entry in entries:
        entry = " ".join(entry.split())
        parcel = re.search(r"Parcel\s+([0-9A-Z][0-9A-Za-z.\- ]*?)\s*\(([^)]*)\)", entry)
        amount = re.search(r"\$\s*([\d,]+\.\d{2})\s*$", entry)
        owners = re.search(r"Current Owners?\s*-\s*(.+?)\s*-\s*\$\s*[\d,]+\.\d{2}\s*$", entry)
        location = re.search(r"having an address of (.+?)\s*[,\-]?\s*-?\s*Current Owner", entry)
        if not parcel or not amount:
            raise ValueError("Cheatham notice tract " + letter + " is unreadable")
        named = owners[1].split(" - Period of delinquency")[0].strip() if owners else ""
        if named.startswith("Defendant "):
            named = named[len("Defendant "):]
        short = " ".join(parcel[1].split())
        lot = short.replace(" ", "").split("-")[-1]
        tracts.append({"letter": letter, "parcel_printed": short, "parcel_long": " ".join(parcel[2].split()),
                       "parcel_forms_agree": lot in parcel[2].replace(" ", ""),
                       "location": location[1].strip() if location else None, "owners": named,
                       "amount": float(amount[1].replace(",", ""))})
    if not tracts:
        raise ValueError("Cheatham notice lists no tracts")
    stamp = re.match(r"D:(\d{8})", meta.get("ModDate") or meta.get("CreationDate") or "")
    return {"docket": docket[1], "sale_date": sale.isoformat(),
            "document_date": datetime.strptime(stamp[1], "%Y%m%d").date().isoformat() if stamp else None,
            "sha256": sha256(content).hexdigest(), "source_url": NOTICE, "tracts": tracts}


def review_rows(rows, notice, day, reason=None):
    """Status, card and column changes for each staged tract. notice is None
    when the county's posted notice could not be read."""
    today = day.isoformat()
    out = []
    for row in rows:
        raw = row["raw_payload"] or {}
        prior = raw.get(STATUS_KEY) or {"status": "noticed", "sale_date": row["trustee_sale_date"]}
        tract = None
        if notice is None:
            seen = {"available": False, "reason": reason}
        elif str(notice["docket"]) != str(raw.get("docket")) and (prior.get("sale_date") or "9999") >= today:
            seen = {"available": False, "reason": "County has posted a sale notice for a different case ("
                                                  + str(notice["docket"]) + ")"}
        else:
            tract = match_tract(raw, notice)
            seen = {"available": True, "listed": bool(tract), "sale_date": notice["sale_date"] if tract else None,
                    "release_list": False, "source_url": NOTICE, "source_date": notice["document_date"]}
        status = next_status(prior, seen, day)
        status.update(case=raw.get("docket"), tract=(tract or {}).get("letter") or raw.get("tract"))
        if tract:
            status.update(notice_sha256=notice["sha256"], parcel_forms_agree=tract["parcel_forms_agree"])
        card = cheatham_card(row, status, day, NOTICE, tract)
        columns = {}
        if row["staging_status"] == "pending":
            if status["status"] == "removed":
                columns = {"staging_status": "rejected", "rejection_reason": REMOVED}
            elif status["status"] == "sale_held":
                columns = {"staging_status": "rejected", "rejection_reason": SALE_PASSED}
            elif tract and row["trustee_sale_date"] != notice["sale_date"]:
                columns = {"trustee_sale_date": notice["sale_date"]}
        elif (row["staging_status"] == "rejected" and row.get("rejection_reason") == REMOVED and tract
              and notice["sale_date"] >= today):
            columns = {"staging_status": "pending", "rejection_reason": None, "trustee_sale_date": notice["sale_date"]}
        out.append({"id": row["id"], "status": status, "card": card, "columns": columns,
                    "raw_patch": patch_for(raw, status, card)})
    return out


class CheathamTaxSaleBot(BotBase):
    name = "cheatham_tax_sale"
    description = "Dated status for Cheatham County delinquent tax sale tracts from the county's posted notice"

    def tracked(self, client):
        return (client.table(STAGING)
                .select("id,staging_status,rejection_reason,property_address,owner_name_records,property_value,"
                        "property_value_source,trustee_sale_date,source_url,raw_payload")
                .eq("bot_source", "court_notices").eq("county", "cheatham").eq("distress_type", "TAX_LIEN")
                .eq("raw_payload->>kind", "cheatham_tax_sale").order("id").limit(500).execute().data or [])

    def read_posted_notice(self):
        response = self.fetch(NOTICE)
        if response is None or response.status_code != 200 or not response.content.startswith(b"%PDF"):
            return None, "Cheatham County posted sale notice unavailable"
        try:
            return read_notice(response.content), None
        except ValueError as exc:
            return None, str(exc)
        except Exception as exc:  # a damaged PDF is an unreadable source, not a removal
            return None, "Cheatham County posted sale notice unreadable: " + type(exc).__name__

    def apply(self, client, changes):
        table = client.table(STAGING)
        patches = [{"id": c["id"], "raw_patch": c["raw_patch"]} for c in changes if c["raw_patch"]]
        for start in range(0, len(patches), 200):
            client.rpc("apply_staging_enrichment", {"p_rows": patches[start:start + 200]}).execute()
        for change in changes:
            if change["columns"]:
                # Only a row still in the state this run read is moved.
                (table.update(change["columns"]).eq("id", change["id"]).eq("bot_source", "court_notices")
                 .eq("staging_status", "rejected" if change["columns"].get("staging_status") == "pending" else "pending")
                 .execute())
        return len(patches)

    def run(self, *, write=True, day=None):
        started = datetime.now(timezone.utc)
        day = day or started.date()
        self._report_health("running", started, None, 0, 0, 0, 0)
        status, error, notes, changes = "ok", None, None, []
        try:
            client = _supabase()
            if client is None:
                raise RuntimeError("Staging unavailable")
            notice, reason = self.read_posted_notice()
            rows = self.tracked(client)
            changes = review_rows(rows, notice, day, reason)
            if write:
                self.apply(client, changes)
            counts = {}
            for change in changes:
                counts[change["status"]["status"]] = counts.get(change["status"]["status"], 0) + 1
            notes = {"notice": {k: v for k, v in (notice or {}).items() if k != "tracts"} or None,
                     "notice_tracts": len(notice["tracts"]) if notice else None,
                     "unread_reason": reason, "tracked": len(rows), "by_status": counts,
                     "written": sum(1 for c in changes if c["raw_patch"]) if write else 0}
            if notice is None:
                status, error = "failed", reason
        except BotTimeout as exc:
            status, error = "timed_out", str(exc)
        except Exception as exc:
            status, error = "failed", str(exc)
        self.changes = changes
        self._report_health(status, started, datetime.now(timezone.utc), len(changes), len(changes), 0, 0,
                            error_message=error, notes=notes)
        return {"name": self.name, "run_id": self.run_id, "status": status, "fetched": len(changes),
                "staged": 0, "duplicates": 0, "error": error}


def run():
    return CheathamTaxSaleBot().run()
