"""Read-only sale-book research. Run with python -m src.bots.shelby_receiver_history RECEIPT.json."""
import argparse
import json
import re
from hashlib import sha256
from pathlib import Path
from urllib.parse import urljoin

import pypdfium2
import requests
from bs4 import BeautifulSoup

from ._shelby_receiver_property import street_matches

INDEX = "https://www.shelbycountytn.gov/330/Tax-Sale-Information"
BOOK = re.compile(r"TX[- ]20\d\d\s+Tax Sale\s+\d+", re.I)
HEADER = re.compile(r"Parent Parcel\s+([A-Z0-9]+),\s*Parcel No\.\s*([A-Z0-9]+),\s*Location:\s*([^,]+),", re.I)
BUYER = re.compile(r"\bThe property sold to:\s*(.+?)\s+whose address is:", re.I)


def matched_parcels(receipt):
    targets = {}
    for case in receipt["cases"]:
        listings = case.get("current_trustee_listings", [])
        if len(listings) != 1:
            continue
        listing = listings[0]
        review = listing.get("property_review", {})
        if review.get("status") == "matched_county_bill" and review.get("parcel") == listing["parcel"]:
            targets.setdefault(listing["alt_parcel"], []).append({
                "parcel": listing["parcel"], "address": case["address"], "warrant": case["warrant"],
            })
    return targets


def book_results(pages, targets):
    # A parcel entry can cross a page break. Stop at the next entry even when its
    # header is malformed, so its buyer cannot be attributed to the prior unit.
    text = "\f".join(" ".join(p.split()) for p in pages)
    for entry in re.finditer(r"\bParent Parcel\b.*?(?=\bParent Parcel\b|\Z)", text, re.I | re.S):
        body = " ".join(entry[0].split())
        header = HEADER.match(body)
        if not header or header[1] != header[2] or header[2] not in targets:
            continue
        unit = re.search(r"\bLot No\.\s+UNIT\s+([A-Z0-9-]+)(?=[,\s]|$)", body, re.I)
        buyers = BUYER.findall(body)
        outcome, buyer = "historical_result_unresolved", None
        if len(buyers) == 1 and body.lower().count("parcel no.") == 1:
            name = buyers[0].upper()
            if name == "NO BID BY COUNTY, PROPERTY NOT SOLD":
                outcome = "historical_unsold"
            elif re.fullmatch(r"SHELBY COUNTY (?:GOVERNMENT|GOVT|GOV'T)(?:, C/O SHELBY COUNTY TRUSTEE)?", name):
                outcome, buyer = "historical_county_purchase", buyers[0]
            elif re.fullmatch(r"[A-Z][A-Z .,'&-]+", name) and not re.search(
                r"\b(NO|NOT|BID|WITHDRAWN|REDEEMED|REMOVED|UNKNOWN|CANCELLED|STRUCK|VOID)\b", name
            ):
                outcome, buyer = "historical_purchase", buyers[0]
        for target in targets[header[2]]:
            if street_matches(target["address"], header[3].strip()):
                yield {**target, "alt_parcel": header[2], "location": header[3].strip(),
                       "printed_unit": unit[1] if unit else None,
                       "page": text.count("\f", 0, entry.start()) + 1, "outcome": outcome,
                       "historical_buyer": buyer}


def read_pages(content):
    pages = []
    with pypdfium2.PdfDocument(content) as doc:
        for page in doc:
            text = page.get_textpage()
            try:
                pages.append(text.get_text_bounded())
            finally:
                text.close()
                page.close()
    return pages


def collect_history(receipt, session):
    targets = matched_parcels(receipt)
    report = {"index_url": INDEX, "roster_sha256": receipt["sha256"], "targets": targets,
              "books": [], "receiver_appointment_verified": False, "sale_authority_verified": False}
    if not targets:
        return report
    response = session.get(INDEX, timeout=30)
    response.raise_for_status()
    seen = set()
    for a in BeautifulSoup(response.content, "html.parser").select("a[href]"):
        label = " ".join(a.get_text(" ", strip=True).split())
        if not BOOK.match(label):
            continue
        url = urljoin(INDEX, a["href"]).replace("http://", "https://", 1)
        if url in seen or not re.match(r"https://(?:www\.)?shelbycountytn\.gov/DocumentCenter/View/\d+", url, re.I):
            continue
        seen.add(url)
        book = {"sale": label, "url": url}
        report["books"].append(book)
        try:
            response = session.get(url, timeout=45)
            response.raise_for_status()
            pages = read_pages(response.content)
        except (requests.RequestException, pypdfium2.PdfiumError) as exc:
            book.update(status="unavailable", error=str(exc))
            continue
        book.update(status="searched_extractable_text", sha256=sha256(response.content).hexdigest(),
                    pages=len(pages), text_pages=sum(bool(p.strip()) for p in pages),
                    matches=list(book_results(pages, targets)))
    if not seen:
        raise ValueError("No recognized county sale books")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    with requests.Session() as session:
        print(json.dumps(collect_history(json.loads(args.receipt.read_text(encoding="utf-8-sig")), session), indent=2))
