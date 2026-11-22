"""Join published, parcel-specific tax evidence to ownership signals for operator review."""
from collections import defaultdict
from datetime import date, datetime

from ._land_identity import parcel_control


SOURCES = {
    "hardeman_tax_notice": ("hardeman",),
    "main_street_media": ("dickson", "wilson", "rutherford"),
    "tn_public_notice": ("henry", "madison"),
    "davidson_tax_sale": ("davidson",),
    "montgomery_tax_sale": ("montgomery",),
}
RAW_KEYS = ("kind", "assessment", "parcel", "comptroller_parcel", "publication_date", "case_number",
            "named_defendant", "tax_sale", "wilson_tax_review", "davidson_tax_list_review", "court_notice")
FIELDS = ("id,county,distress_type,bot_source,staging_status,rejection_reason,source_url,trustee_sale_date,"
          + ",".join(f"e_{field}:raw_payload->{field}" for field in RAW_KEYS))
ACTIVE = {"tax_suit_notice", "published_delinquency", "scheduled_tax_sale"}


def obj(value):
    return value if isinstance(value, dict) else {}


def key(value):
    return " ".join(value.upper().split()) if isinstance(value, str) else ""


def is_land_signal(row):
    raw = obj(row.get("raw_payload"))
    return (row.get("distress_type") in {"HEIRS_PROPERTY", "ABSENTEE_LAND"} or row.get("bot_source") == "land_lane"
            or isinstance(raw.get("land"), dict) or raw.get("property_lane") == "land"
            or "land_qualification" in obj(raw.get("property_lane")))


def _day(value):
    for fmt in ("%Y-%m-%d", "%A, %B %d, %Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except (TypeError, ValueError):
            pass
    return None


def source_event(row, today):
    """Only consume actual producer shapes; address/name similarity cannot join parcels."""
    src = row.get("bot_source")
    county = key(row.get("county")).lower()
    raw = obj(row.get("raw_payload")) or {field: row.get("e_" + field) for field in RAW_KEYS}
    link = row.get("source_url")
    if not isinstance(link, str) or not link.startswith("https://"):
        return None
    if row.get("distress_type") == "PROBATE":
        roll = obj(raw.get("comptroller_parcel"))
        if (src not in {"court_notices", "tn_public_notice"} or roll.get("method") not in {"owner_name", "address"}
                or key(roll.get("county")).lower() != county or not roll.get("parcelid")):
            return None
        published = _day(raw.get("publication_date"))
        if not published or published > today:
            return None
        return {"staging_id": row["id"], "source": src, "county": county, "parcel_id": roll["parcelid"],
                "parcel_namespace": "comptroller_roll", "evidence_level": "assessor_link_only",
                "printed_parcel": None, "owner": roll.get("owner"), "status": "estate_notice_assessor_link",
                "source_url": link, "publication_date": published.isoformat(), "sale_date": None,
                "case_id": obj(raw.get("court_notice")).get("docket"), "match_method": roll["method"],
                "review_source_url": None, "rejection_reason": row.get("rejection_reason")}
    if row.get("distress_type") != "TAX_LIEN" or county not in SOURCES.get(src, ()):
        return None
    sale = obj(raw.get("tax_sale"))
    printed = raw.get("parcel")
    parcel = None
    party = None
    published = _day(raw.get("publication_date"))
    sale_day = _day(row.get("trustee_sale_date"))
    result = obj(raw.get("wilson_tax_review") or raw.get("davidson_tax_list_review"))
    status = "tax_suit_notice"
    if src in ("hardeman_tax_notice", "tn_public_notice"):
        if raw.get("kind") != ("tax_collection_suit" if src == "hardeman_tax_notice" else "county_tax_table"):
            return None
        # The tax producer joins the PRINTED county parcel to an unambiguous
        # assessment version. The trailing roll year is part of its full ID.
        parcel = obj(raw.get("assessment")).get("parcel_id")
        if not printed or key(parcel).rsplit(" ", 1)[0] != key(printed):
            return None
        if src == "tn_public_notice":
            status = "published_delinquency"
    elif src == "main_street_media":
        if raw.get("kind") not in {"tax_collection_suit", "chancery_tax_notice"} or not printed:
            return None
        if county in ("dickson", "wilson"):
            comp = obj(raw.get("comptroller_parcel"))
            if (comp.get("method") != county + "_tax_parcel" or key(comp.get("county")).lower() != county
                    or key(comp.get("printed_parcel")) != key(printed)):
                return None
            parcel, party = comp.get("parcelid"), comp.get("owner")
        else:
            parcel, party = printed, raw.get("named_defendant")
    else:
        if sale.get("list_url") != link or not _day(sale.get("sale_date")):
            return None
        if sale_day and sale_day != _day(sale["sale_date"]):
            return None
        sale_day = _day(sale["sale_date"])
        parcel = printed = sale.get("parcel")
        party = sale.get("owner")
        status = "scheduled_tax_sale"
    if not key(parcel) or (published and published > today) or (not published and not sale_day):
        return None
    outcome = result.get("status")
    if sale.get("sold") is True and sale.get("buyer") and sale.get("sold_for"):
        status = "sold"
    elif outcome:
        status = str(outcome)
    elif row.get("staging_status") not in ("pending", "verified", "promoted"):
        status = "source_record_held"
    elif sale_day and sale_day < today:
        status = "past_sale_date"
    return {"staging_id": row["id"], "source": src, "county": county, "parcel_id": parcel,
            "parcel_namespace": "county_gis" if county in {"davidson", "rutherford", "montgomery"} else "comptroller_roll",
            "evidence_level": "notice_parcel", "printed_parcel": printed, "owner": party, "status": status, "source_url": link,
            "publication_date": published.isoformat() if published else None,
            "sale_date": sale_day.isoformat() if sale_day else None, "case_id": raw.get("case_number"),
            "review_source_url": result.get("source_url") or result.get("final_list_url"),
            "rejection_reason": row.get("rejection_reason")}


def collect_events(client, today=None):
    index = defaultdict(list)
    today = today or date.today()
    table = client.table("homeowner_requests_staging")
    # Source and UUID have indexes. Never filter a statewide table scan by
    # a JSON expression: even the last empty page can hit statement_timeout.
    for source in (*SOURCES, "court_notices"):
        after = "00000000-0000-0000-0000-000000000000"
        while True:
            page = (table.select("id,county,distress_type,kind:raw_payload->>kind,"
                                 "method:raw_payload->comptroller_parcel->>method")
                    .eq("bot_source", source).gt("id", after).order("id").limit(500).execute().data or [])
            ids = []
            for summary in page:
                if summary["distress_type"] == "PROBATE":
                    if source in ("court_notices", "tn_public_notice") and summary["method"] in ("address", "owner_name"):
                        ids.append(summary["id"])
                elif (summary["distress_type"] == "TAX_LIEN" and summary["county"] in SOURCES.get(source, ())
                      and (source != "tn_public_notice" or summary["kind"] == "county_tax_table")):
                    ids.append(summary["id"])
            for offset in range(0, len(ids), 80):
                rows = table.select(FIELDS).in_("id", ids[offset:offset + 80]).execute().data or []
                for row in rows:
                    event = source_event(row, today)
                    if event:
                        control = parcel_control(event["parcel_namespace"], event["county"], event["parcel_id"])
                        index[control].append(event)
            if len(page) < 500:
                break
            after = page[-1]["id"]
    return index


def qualify(row, events, today=None):
    today = today or date.today()
    raw = obj(row.get("raw_payload"))
    land, roll = obj(raw.get("land")), obj(raw.get("comptroller_parcel"))
    county = key(land.get("county") or row.get("county")).lower()
    parcel = land.get("parcel_id") or roll.get("parcelid")
    owner = land.get("owner_of_record") or roll.get("owner")
    identity = parcel_control(raw.get("source"), county, parcel)
    reasons = []
    if row.get("staging_status") == "rejected":
        reasons.append("land_record_rejected")
    if land.get("status") != "current" or land.get("review_required") is not False:
        reasons.append("acquisition_review")
    if (not identity or not owner or not county or key(roll.get("county")) != key(county)
            or key(row.get("county")) != key(county) or key(roll.get("owner")) != key(owner)
            or key(row.get("owner_name_records")) != key(owner) or roll.get("parcelid") != parcel):
        reasons.append("parcel_or_owner_mismatch")
    matched = sorted((e for e in events if isinstance(e, dict) and e.get("county") == county
                      and identity and parcel_control(e.get("parcel_namespace"), county, e.get("parcel_id")) == identity), key=lambda e: e["staging_id"])
    direct = [e for e in matched if e.get("evidence_level") == "notice_parcel"]
    if not direct:
        reasons.append("assessor_link_only" if matched else "no_independent_parcel_event")
    for event in direct:
        pub, sale = _day(event.get("publication_date")), _day(event.get("sale_date"))
        if (not event.get("staging_id") or not event.get("printed_parcel") or not event.get("source_url")
                or event.get("rejection_reason") or not (pub or sale) or (pub and pub > today)):
            reasons.append("source_record_held")
        if event.get("owner") and key(event["owner"]) != key(owner):
            reasons.append("event_owner_mismatch")
        if event.get("status") not in ACTIVE:
            reasons.append(event.get("status") or "source_record_held")
        elif event.get("sale_date") and (_day(event["sale_date"]) or date.min) < today:
            reasons.append("past_sale_date")
    return {"status": "research_only" if reasons else "corroborated_opportunity",
            "reasons": sorted(set(reasons)), "county": county, "parcel_id": parcel,
            "parcel_namespace": raw.get("source"), "parcel_control": identity[2] if identity else None,
            "owner_of_record": owner, "source_url": land.get("source_url"), "events": matched,
            "sale_authority_verified": False, "outreach_eligible": False}


# Narrow JSON projections for contact providers that never needed the full payload.
GATE_FIELDS = ("lane_tag:raw_payload->property_lane,land_input:raw_payload->land,"
               "land_roll:raw_payload->comptroller_parcel,land_source:raw_payload->>source")


def land_gate(row, today=None, client=None):
    if "raw_payload" not in row:
        row = {**row, "raw_payload": {"property_lane": row.get("lane_tag"), "land": row.get("land_input"),
                                      "source": row.get("land_source"), "comptroller_parcel": row.get("land_roll")}}
    if not is_land_signal(row):
        return True
    lane = obj(obj(row.get("raw_payload")).get("property_lane"))
    prior = obj(lane.get("land_qualification"))
    review = qualify(row, prior.get("events") or [], today)
    if (review["status"] != "corroborated_opportunity" or prior != review or lane.get("lane") != "land"
            or obj(lane.get("readiness")).get("ready") is not True or lane.get("exclude_reason")):
        return False
    if client is None:
        return True
    # The county collector can retire a source row between grading and spend.
    ids = [event["staging_id"] for event in review["events"]]
    rows = (client.table("homeowner_requests_staging").select(FIELDS)
            .in_("id", ids).execute().data or [])
    current = [event for r in rows if (event := source_event(r, today or date.today()))]
    return qualify(row, current, today) == review
