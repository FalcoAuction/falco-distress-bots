"""Recheck Memphis Daily News notices against Foundation Legal Group's Shelby calendar."""
import re
from datetime import date, datetime, timedelta, timezone

import requests
from dateutil.tz import gettz
from bs4 import BeautifulSoup

from ._base import BotBase, _supabase, guard_phone_metadata

URL = "https://sales.thefoundationlegalgroup.com/"
FIRM = "Foundation Legal Group, LLP"
KEY = "memphis_trustee_calendar"
HEADERS = ["Date", "Time", "Prior Sale Date", "Address", "City", "County", "State",
           "Zip", "Location", "Auctioneer"]
SUFFIX = {"road": "rd", "drive": "dr", "street": "st", "cove": "cv", "lane": "ln",
          "court": "ct", "avenue": "ave", "circle": "cir", "trail": "trl"}


def street(value):
    parts = value.lower().strip().split()
    if parts:
        index = -1
        directions = {"east": "e", "west": "w", "north": "n", "south": "s"}
        if len(parts) >= 4 and parts[-1] in (*directions, *directions.values()):
            parts[-1] = directions.get(parts[-1], parts[-1])
            index = -2
        suffix = parts[index].rstrip(".")
        parts[index] = SUFFIX.get(suffix, suffix)
    return " ".join(parts)


def calendar_date(value):
    return datetime.strptime(value, "%m/%d/%Y").date()


def parse_calendar(page, start, end):
    # A reset form, validation message or partial page cannot establish an omission.
    for field, expected in (("txtRangeBegin", start.isoformat()), ("txtRangeEnd", end.isoformat())):
        if not page.select_one(f'#{field}[value="{expected}"]'):
            raise ValueError("calendar date range not acknowledged")
    for field, expected in (("ddlState", "TN"), ("ddlCounty", "Shelby")):
        if not page.select_one(f'#{field} option[selected][value="{expected}"]'):
            raise ValueError("calendar geography not acknowledged")
    trs = page.select("#gvSales tr")
    if not trs or [th.get_text(strip=True) for th in trs[0].select("th")] != HEADERS:
        raise ValueError("missing calendar grid")
    rows = []
    for tr in trs[1:]:
        cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(cells) != len(HEADERS) or cells[5:7] != ["Shelby", "TN"]:
            raise ValueError("unexpected calendar row")
        current, prior, address, zipcode = cells[0], cells[2], cells[3], cells[7]
        if not address or not re.fullmatch(r"\d{5}(-\d{4})?", zipcode):
            raise ValueError("incomplete calendar identity")
        rows.append(dict(address=address, city=cells[4], zip=zipcode[:5],
                         date=calendar_date(current).isoformat(), time=cells[1],
                         prior_date=None if prior == "--" else calendar_date(prior.split()[0]).isoformat()))
    if not rows:
        raise ValueError("empty calendar grid")
    return rows


def load_calendar(start, end):
    filters = {"ddlState": "TN", "txtRangeBegin": start.isoformat(), "txtRangeEnd": end.isoformat()}
    with requests.Session() as session:
        response = session.get(URL, timeout=12)
        for control, data in [
            ("cbAcceptTerms", {"__EVENTTARGET": "cbAcceptTerms", "cbAcceptTerms": "on"}),
            ("btnAcceptTerms", {"cbAcceptTerms": "on", "btnAcceptTerms": "View Property Listings"}),
            ("ddlState", {"__EVENTTARGET": "ddlState", **filters}),
            ("btnSearch", {"btnSearch": "Search", "ddlCounty": "Shelby", **filters}),
        ]:
            response.raise_for_status()
            page = BeautifulSoup(response.content, "html.parser")
            if not page.select_one("#" + control):
                raise ValueError("calendar form changed")
            hidden = {i["name"]: i.get("value", "") for i in page.select("input[type=hidden][name]")}
            response = session.post(URL, data={**hidden, **data}, timeout=12)
        response.raise_for_status()
        return parse_calendar(BeautifulSoup(response.content, "html.parser"), start, end)


def review(row, listings, today, end):
    raw = row["raw_payload"]
    notice = raw.get("structured") or {}
    fid = notice.get("tdn_no") or ""
    if (row["bot_source"] != "memphis_daily_news" or row["county"] != "shelby"
            or notice.get("substitute_trustee") != FIRM or not re.fullmatch(r"F[DN]\d+", fid)
            or row["pipeline_lead_key"] != BotBase.make_lead_key("memphis_daily_news", fid)):
        return None
    evidence = dict(status="unresolved", checked_on=today.isoformat(), valid_on=today.isoformat(),
                    source_url=URL, notice_id=fid, advertised_date=row["trustee_sale_date"],
                    property_address=row["property_address"], window_end=end.isoformat())
    try:
        advertised = calendar_date(notice.get("advertised_auction_date") or "")
    except ValueError:
        return evidence
    evidence["advertised_date"] = advertised.isoformat()
    address = row["property_address"] or ""
    zipcode = re.search(r"\bTN\s+(\d{5})(?:-\d{4})?$", address, re.I)
    target = street(address.split(",")[0])
    if (not zipcode or not re.match(r"^\d+\s", target) or target != street(notice.get("address") or "")
            or advertised > end):
        return evidence
    # Keep one dated anchor across omissions to recognize a subsequent continuance.
    # Bind it to the original evidence controls before it can support a new date.
    prior = row.get("prior") or {}
    anchor = None
    if all(prior.get(k) == evidence[k] for k in
           ("source_url", "notice_id", "property_address", "advertised_date")):
        anchor = prior.get("last_linked_date")
        if anchor:
            evidence["last_linked_date"] = anchor
    prior_listing = prior.get("listing") or {}
    accepted_dates = {advertised.isoformat()}
    if anchor:
        accepted_dates.add(anchor)
        if prior_listing.get("prior_date"):
            accepted_dates.add(prior_listing["prior_date"])
    if row["trustee_sale_date"] not in accepted_dates:
        return {**evidence, "reason": "stored_date_conflict"}
    pm = row.get("phone_metadata") or {}
    if not isinstance(pm, dict) or not isinstance(pm.get("notice_tracking", {}), dict):
        return {**evidence, "reason": "invalid_notice_metadata"}
    tracking = pm.get("notice_tracking", {})
    aliases = [a for a in (raw.get("foundation_legal_address"), tracking.get("foundation_legal_address"))
               if a is not None]
    alias = None
    if aliases:
        alias = aliases[-1]
        if (not isinstance(alias, dict) or any(not isinstance(a, dict) or a.get("address") != alias.get("address")
                or not re.fullmatch(r"https://www\.memphisdailynews\.com/Search/Details/ViewNotice\.aspx"
                                    rf"\?id={fid}&date=\d{{1,2}}/\d{{1,2}}/\d{{4}}", a.get("source_url") or "")
                or a.get("notice_id") != fid or not notice.get("instrument_no")
                or a.get("instrument_no") != notice["instrument_no"] for a in aliases)
                or alias.get("zip") != zipcode[1] or not alias.get("street") or not alias.get("city")
                or alias["street"].split()[0] != target.split()[0]):
            return {**evidence, "reason": "legal_address_conflict"}
        target = street(alias["street"])
        evidence["publisher_legal_address"] = alias
    candidates = [r for r in listings if street(r["address"]) == target]
    if candidates:
        if (len(candidates) != 1 or candidates[0]["zip"] != zipcode[1]
                or (alias and candidates[0]["city"].casefold() != alias["city"].casefold())):
            return {**evidence, "reason": "ambiguous_identity"}
        listing = candidates[0]
        observed = date.fromisoformat(listing["date"])
        evidence["listing"] = listing
        if observed == advertised:
            status = "corroborated"
        elif observed > advertised and (
                listing["prior_date"] == advertised.isoformat()
                or (anchor and (listing["date"] == anchor or listing["prior_date"] == anchor)
                    and listing["date"] >= anchor)):
            status = "postponed"
        else:
            return {**evidence, "reason": "date_conflict"}
        return {**evidence, "last_linked_date": listing["date"],
                "status": "expired" if observed < today else status}
    # A likely spelling difference is a research hold, never a fuzzy match.
    if any(r["zip"] == zipcode[1] and street(r["address"]).split()[0] == target.split()[0]
           for r in listings):
        return {**evidence, "reason": "street_discrepancy"}
    return {**evidence, "status": "expired" if (anchor or advertised.isoformat()) < today.isoformat() else "omitted"}


def handoff(table, row, evidence, observed_at):
    """CRM reads the staging date and sync_sale_dates orders observations by last_seen_at."""
    listing = evidence.get("listing") or {}
    previous = row["trustee_sale_date"]
    if (evidence["status"] != "postponed" or listing.get("prior_date") != previous
            or listing["date"] <= previous):
        return False
    # CRM sync also joins street + ZIP across sources. Never export a unit-specific
    # or alternate-street date under the less precise staging address.
    if street(listing["address"]) != street(row["property_address"].split(",")[0]):
        return False
    pm = row.get("phone_metadata")
    if (pm is not None and not isinstance(pm, dict)) or (pm or {}).get("sale_status"):
        return False
    tracking = (pm or {}).get("notice_tracking", {})
    if not isinstance(tracking, dict):
        return False
    if tracking.get("last_seen_at"):
        try:
            if datetime.fromisoformat(tracking["last_seen_at"].replace("Z", "+00:00")) > observed_at:
                return False
        except (ValueError, TypeError, AttributeError):
            return False
    notice = row.get("structured") or (row.get("raw_payload") or {}).get("structured") or {}
    if not notice.get("instrument_no"):
        return False
    update = {"trustee_sale_date": listing["date"], "phone_metadata": {
        **(pm or {}), "notice_tracking": {**tracking, "last_seen_at": observed_at.isoformat(),
                                          "last_seen_by": KEY}}}
    query = table.update(update)
    guards = {"id": row["id"], "pipeline_lead_key": row["pipeline_lead_key"], "bot_source": "memphis_daily_news",
              "staging_status": row["staging_status"], "county": "shelby", "property_address": row["property_address"],
              "trustee_sale_date": previous, "raw_payload->structured->>tdn_no": evidence["notice_id"],
              "raw_payload->structured->>substitute_trustee": FIRM,
              "raw_payload->structured->>instrument_no": notice["instrument_no"],
              "raw_payload->structured->>advertised_auction_date": notice["advertised_auction_date"],
              f"raw_payload->{KEY}->>status": "postponed", f"raw_payload->{KEY}->>checked_on": evidence["checked_on"],
              f"raw_payload->{KEY}->listing->>prior_date": previous, f"raw_payload->{KEY}->listing->>date": listing["date"]}
    for key, value in guards.items():
        query = query.eq(key, value)
    return bool(guard_phone_metadata(query, pm).execute().data)


def run():
    today = datetime.now(timezone.utc).astimezone(gettz("America/Chicago")).date()
    end = today + timedelta(days=120)
    observed_at = datetime.now(timezone.utc)
    listings = load_calendar(today, end)
    client = _supabase()
    if client is None:
        raise RuntimeError("staging unavailable")
    last = None
    fetched = staged = dates_updated = 0
    while True:
        query = (client.table("homeowner_requests_staging")
                 .select("id,bot_source,pipeline_lead_key,county,property_address,trustee_sale_date,staging_status,phone_metadata,"
                         "legal_address:raw_payload->foundation_legal_address,"
                         "structured:raw_payload->structured,prior:raw_payload->memphis_trustee_calendar")
                 .eq("bot_source", "memphis_daily_news").eq("county", "shelby")
                 .eq("raw_payload->structured->>substitute_trustee", FIRM)
                 .in_("staging_status", ["pending", "promoted"]))
        if last:
            query = query.gt("id", last)
        rows = query.order("id").limit(100).execute().data or []
        patches = []
        postponed = []
        for row in rows:
            prior = row["prior"] or {}
            # Revisit elapsed notices for continuances, including previously observed extensions.
            cutoff = (today - timedelta(days=120)).isoformat()
            latest = max(row["trustee_sale_date"] or "", prior.get("last_linked_date") or "")
            if latest and latest < cutoff and prior.get("status") in (None, "expired"):
                continue
            raw = {"structured": row["structured"], "foundation_legal_address": row.get("legal_address")}
            evidence = review({**row, "raw_payload": raw}, listings, today, end)
            if evidence:
                fetched += 1
                if evidence["status"] == "postponed" and row["trustee_sale_date"] != evidence["listing"]["date"]:
                    postponed.append((row, evidence))
                if evidence != prior:
                    patches.append({"id": row["id"], "raw_patch": {KEY: evidence}})
        if patches:
            count = client.rpc("apply_staging_enrichment", {"p_rows": patches}).execute().data
            if count != len(patches):
                raise RuntimeError("incomplete calendar evidence update")
            staged += count
        for row, evidence in postponed:
            dates_updated += handoff(client.table("homeowner_requests_staging"), row, evidence, observed_at)
        if len(rows) < 100:
            break
        last = rows[-1]["id"]
    return {"status": "success", "fetched": fetched, "staged": staged, "dates_updated": dates_updated}
