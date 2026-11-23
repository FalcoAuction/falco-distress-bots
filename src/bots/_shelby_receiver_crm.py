"""Project receiver research onto the existing exact Trustee staging identity."""
from uuid import NAMESPACE_URL, uuid5

from ._base import BotBase
from ._shelby_receiver_property import street_matches


FIELD = "shelby_receiver_review"
SOURCE = "shelby_tax_sale"


def reviews(receipt, day):
    result = {}
    history = receipt["sale_book_history"]
    books = history["books"]
    for case in receipt["cases"]:
        listings = case.get("current_trustee_listings", [])
        if len(listings) != 1:
            continue
        listing = listings[0]
        prop = listing["property_review"]
        key = BotBase.make_lead_key(SOURCE, listing["sale_id"] + "|" + listing["alt_parcel"])
        rid = str(uuid5(NAMESPACE_URL, SOURCE + "/" + key))
        if prop.get("status") != "matched_county_bill":
            result.setdefault(rid, dict(status="property_review_unverified", checked_on=day.isoformat(),
                                        parcel=listing["parcel"], alt_parcel=listing["alt_parcel"], sale_id=listing["sale_id"],
                                        cases=[{"address": case["address"], "warrant": case["warrant"]}],
                                        receiver_appointment_verified=False, sale_authority_verified=False))
            continue
        if rid not in result or result[rid]["status"] != "receiver_eligibility_research":
            matches = []
            for book in books:
                for hit in book.get("matches", []):
                    if hit["parcel"] == listing["parcel"] and hit["alt_parcel"] == listing["alt_parcel"]:
                        evidence = {k: hit[k] for k in ("outcome", "historical_buyer", "printed_unit", "page")}
                        evidence.update(source_url=book["url"] + "#page=" + str(hit["page"]), sale=book["sale"])
                        if evidence not in matches:
                            matches.append(evidence)
            result[rid] = dict(status="receiver_eligibility_research", checked_on=day.isoformat(),
                               roster_url=receipt["source_url"], roster_date=receipt["source_date"],
                               parcel=listing["parcel"], alt_parcel=listing["alt_parcel"], sale_id=listing["sale_id"],
                               advertised_date=listing["advertised_date"], cases=[], property_review=prop,
                               sale_history=matches, books_searched=sum(b.get("status") == "searched_extractable_text" for b in books),
                               books_indexed=len(books), receiver_appointment_verified=False, sale_authority_verified=False)
        result[rid]["cases"].append({k: case[k] for k in ("warrant", "address", "hearing_listed", "page")})
    return result


def publish(client, current, day):
    table = client.table("homeowner_requests_staging")
    # The set is source-scoped. Only previously active cards can need retirement.
    active = set()
    after = "00000000-0000-0000-0000-000000000000"
    while True:
        page = (table.select("id").eq("bot_source", SOURCE).eq("raw_payload->shelby_receiver_review->>status",
                "receiver_eligibility_research").gt("id", after).order("id").limit(200).execute().data or [])
        active.update(row["id"] for row in page)
        if len(page) < 200:
            break
        after = page[-1]["id"]
    ids = sorted(active | current.keys())
    updated = []
    found = set()
    for start in range(0, len(ids), 80):
        rows = (table.select("id,raw_payload,property_address").eq("bot_source", SOURCE)
                .in_("id", ids[start:start + 80]).execute().data or [])
        for row in rows:
            rid, raw = row["id"], row["raw_payload"]
            found.add(rid)
            prior = raw.get(FIELD) or {}
            review = current.get(rid)
            if review and (any(review[k] != raw.get(k) for k in ("sale_id", "alt_parcel", "parcel"))
                           or not street_matches(review["cases"][0]["address"], (row["property_address"] or "").split(",")[0])):
                raise ValueError("Conflicting Trustee staging identity: " + rid)
            if review is None:
                review = dict(status="no_current_roster_match", checked_on=day.isoformat(),
                              receiver_appointment_verified=False, sale_authority_verified=False)
            if prior == review or prior.get("checked_on", "") > day.isoformat():
                continue
            # Merge the one review key inside PostgreSQL. The row also carries the
            # tax status and review card, so the whole payload no longer fits an
            # equality filter in the request URL, and a key merge cannot lose
            # another writer's keys.
            merged = client.rpc("apply_staging_enrichment", {"p_rows": [{"id": rid, "raw_patch": {FIELD: review}}]}).execute()
            if not merged.data:
                raise RuntimeError("Receiver review was not written: " + rid)
            updated.append(rid)
    return {"updated_staging_ids": updated, "missing_staging_ids": sorted(current.keys() - found)}
