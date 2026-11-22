"""Read the city complaint status. The status is independent of a hearing ruling."""
from datetime import datetime
import re
from time import monotonic
from urllib.parse import urlencode


DETAIL = "https://knoxvilleworksearch.knoxvilletn.gov/recorddetails.html"
STREET_WORDS = {"ROAD": "RD", "AVENUE": "AVE", "STREET": "ST", "DRIVE": "DR",
                "BOULEVARD": "BLVD", "LANE": "LN", "EAST": "E", "WEST": "W",
                "NORTH": "N", "SOUTH": "S"}


def street(address):
    return " ".join(STREET_WORDS.get(w, w) for w in address.split(",")[0].upper().split())


def read_status(review, addresses, records):
    case = review["case_number"]
    matches = [r for r in records if len(r) == 7 and r[0] == case and r[2] == "CE Structure Complaint"]
    actual = {(street(r[0]), r[1].strip()) for r in addresses if len(r) >= 2}
    if actual != {(street(review["property_address"]), "")} or len(matches) != 1:
        return None
    _, _, _, description, _, status, day = matches[0]
    parcels = re.findall(r"\bParcel\s*=\s*([A-Z0-9.-]+)", description, re.I)
    if any(p.upper() != review["parcel_id"] for p in parcels):
        return None
    try:
        day = datetime.strptime(day, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None
    if not status or len(status) > 65 or day > review["checked_on"]:
        return None
    return {"case_number": case, "parcel_id": review["parcel_id"], "status": status,
            "status_date": day, "checked_on": review["checked_on"],
            "source_url": DETAIL + "?" + urlencode({"RecordID": case})}


def attach_city_records(reviews, logger):
    from playwright.sync_api import Error, sync_playwright

    # Only published municipal case output is needed. Keep reporter names,
    # contact fields and unstructured complaint narratives out of staging.
    cells = "rows => rows.map(r => Array.from(r.cells, c => c.innerText.trim()))"
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page()
            deadline = monotonic() + 90
            for review in reviews:
                if monotonic() >= deadline:
                    logger.warning("Knoxville city status review reached its time budget")
                    break
                case = review["case_number"]
                url = DETAIL + "?" + urlencode({"RecordID": case})
                try:
                    response = page.goto(url, timeout=15000, wait_until="domcontentloaded")
                    if not response or response.status != 200 or page.url != url:
                        continue
                    page.locator("#resultTable").get_by_text(case, exact=True).first.wait_for(timeout=9000)
                    page.locator("#addressResultTable tr").nth(1).wait_for(timeout=9000)
                    address_rows = page.locator("#addressResultTable tr").evaluate_all(cells)[1:]
                    record_rows = page.locator("#resultTable tr").evaluate_all(cells)[1:]
                    result = read_status(review, address_rows, record_rows)
                    if result:
                        review["city_record"] = result
                except Error as error:
                    logger.warning("Knoxville city status unavailable for %s (%s)", case, type(error).__name__)
            browser.close()
    except Error as error:
        logger.warning("Knoxville case browser unavailable (%s)", type(error).__name__)
