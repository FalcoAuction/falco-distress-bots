import copy
import unittest
from datetime import date

from src.bots import _hamilton_tax_sale as tax_sale
from src.bots import _run_new
from src.bots._land_review import (build_card, holder_remains, owner_group, review, same_card, same_person,
                                   staged_events, tract)
from src.bots._pilot_review import build_review, dedupe_worklist, source
from src.bots.land_review_bot import LandReviewBot

TODAY = date(2026, 11, 22)
RUTHERFORD_PARCEL = "102 01400 R0012345"


def land_row(owner="SMITH JOHN A & MARY B", parcel=RUTHERFORD_PARCEL, county="rutherford", ns="county_gis", **extra):
    row = {"id": "land-1", "bot_source": "land_lane", "county": county, "distress_type": "ABSENTEE_LAND",
           "staging_status": "pending", "owner_name_records": owner, "property_address": "100 FARM RD, Rutherford County, TN",
           "property_value": 400000,
           "raw_payload": {"source": ns, "property_lane": {"lane": "land", "grade": "B"},
                           "land": {"county": county, "parcel_id": parcel, "owner_of_record": owner, "acres": 42.5,
                                    "signals": ["absentee"], "source_url": "https://example.gov/parcels"},
                           "comptroller_parcel": {"parcelid": parcel, "owner": owner, "county": county, "appraisal": 400000,
                                                  "last_sale_date": "19950301"}}}
    row.update(extra)
    return row


def tax_row(parcel=RUTHERFORD_PARCEL, defendant="SMITH JOHN A & MARY B", **extra):
    row = {"id": "tax-1", "bot_source": "main_street_media", "county": "rutherford", "distress_type": "TAX_LIEN",
           "staging_status": "pending", "rejection_reason": None, "trustee_sale_date": None,
           "source_url": "https://mainstreetmediatn.com/notice/1",
           "raw_payload": {"kind": "tax_collection_suit", "parcel": parcel, "named_defendant": defendant,
                           "publication_date": "2026-11-10", "case_number": "24CV-100"}}
    row.update(extra)
    return row


def probate_row(decedent, roll_owner, parcel="043 061    01800 000 2027", county="humphreys", reps=(), published="2026-09-30"):
    return {"id": "probate-1", "bot_source": "court_notices", "county": county, "distress_type": "PROBATE",
            "staging_status": "pending", "full_name": decedent, "source_url": "https://example.com/notice",
            "raw_payload": {"publication_date": published,
                            "court_notice": {"decedent": decedent, "docket": "2026-PR-53",
                                             "representatives": [{"name": n, "role": "executor"} for n in reps]},
                            "comptroller_parcel": {"method": "owner_name", "county": county.upper(), "parcelid": parcel,
                                                   "owner": roll_owner}}}


def hamilton_row(owner="ASH MARCUS DEVON", assessor_owner=None, parcel="135B-A-036", grade="A"):
    return {"id": "ham-1", "bot_source": "hamilton_tax_delinquent", "county": "hamilton", "distress_type": "TAX_LIEN",
            "staging_status": "verified", "owner_name_records": owner, "property_address": "249 Box Turtle Ln, Chattanooga, TN",
            "property_value": 36000,
            "raw_payload": {"county": "hamilton", "parcel": parcel, "cumulative_owed": 838.93, "years_delinquent": 3,
                            "last_bill_year": 2025, "property_lane": {"lane": "land", "grade": grade},
                            "hamilton_assessor": {"parcel": parcel, "owner": assessor_owner or owner, "appraised": 36000,
                                                  "calc_acres": 1, "land_use": "UNDEVELOPED LAND AND WATER AREAS",
                                                  "last_sale_date": "2020-03-12"}}}


def card_for(row, events, roll=None):
    land = tract(row)
    return build_card(row, land, review(row, land, events), TODAY, roll), review(row, land, events)


SALE_LIST = """REVISED 6/4/2026
DOCKET ITEM # PROPERTY # PROPERTY ADDRESS MAP GROUP PARCEL MINIMUM BID
PAID 11256 159070 3358 EASTON AVE 118H F 029.01 $3,153.93
11256 65454 2836 ANDERSON TER 139H A 033.02 $3,465.85
REMOVED 11256 174460 5350 SPALDING DR 141 026 S002 $20,597.91
PAID 11255 1763 900 MOUNTAIN CREEK RD 117O A 011 C158 $6,508.33
"""


class ExactTractRule(unittest.TestCase):
    def test_exact_tract_tax_match_is_first_trace(self):
        row = land_row()
        events = staged_events(tax_row(), TODAY)
        self.assertEqual(len(events), 1)
        card, result = card_for(row, events)
        self.assertEqual(len(result["matched"]), 1)
        self.assertEqual(card["route"], "first_trace_review")
        self.assertEqual(card["lane"], "land")
        self.assertIn("24CV-100", card["event"]["what"])
        self.assertEqual(card["event"]["status"], "active")
        self.assertTrue(card["decider"]["verified"])
        self.assertEqual(card["economics"]["value"], 400000)
        self.assertIn("not a market price", card["economics"]["value_basis"])
        self.assertTrue(any("Access not verified" in u for u in card["economics"]["unknowns"]))

    def test_same_surname_on_a_different_parcel_is_not_a_candidate(self):
        row = land_row()
        events = staged_events(tax_row(parcel="102 01400 R0099999", defendant="SMITH JOHN A & MARY B"), TODAY)
        self.assertEqual(len(events), 1)                       # a real tax event, on another tract
        card, result = card_for(row, events)
        self.assertEqual((result["matched"], result["rejected"]), ([], []))
        self.assertEqual(card["route"], "research_hold")
        self.assertNotIn("24CV-100", str(card))
        self.assertIn("ownership signal", card["route_reason"])

    def test_name_only_foreclosure_link_is_rejected_with_the_reason(self):
        row = land_row(parcel="074 058    12100 000 2027", county="robertson", ns="comptroller_roll")
        notice = {"id": "fc-1", "bot_source": "tn_lis_pendens", "county": "robertson", "distress_type": "PRE_FORECLOSURE",
                  "staging_status": "pending", "trustee_sale_date": "2026-12-15", "source_url": "https://example.com/n",
                  "owner_name_records": "John Smith",
                  "raw_payload": {"comptroller_parcel": {"method": "debtor_name", "county": "ROBERTSON",
                                                         "parcelid": "074 058    12100 000 2027"}}}
        card, result = card_for(row, staged_events(notice, TODAY))
        self.assertEqual(result["rejected"][0]["reason"], "name_link_only")
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("only by a name match", card["route_reason"])
        by_address = copy.deepcopy(notice)
        by_address["raw_payload"]["comptroller_parcel"]["method"] = "address"
        card, result = card_for(row, staged_events(by_address, TODAY))
        self.assertEqual(card["route"], "first_trace_review")
        self.assertEqual(card["economics"]["deadline"], "2026-12-15")

    def test_past_sale_and_held_source_do_not_count(self):
        row = land_row()
        past = tax_row(source_url="https://example.gov/list", bot_source="davidson_tax_sale", county="davidson",
                       raw_payload={"tax_sale": {"list_url": "https://example.gov/list", "sale_date": "2026-10-01",
                                                 "parcel": RUTHERFORD_PARCEL, "owner": "SMITH JOHN A & MARY B"}})
        events = staged_events(past, TODAY)
        self.assertEqual(events[0]["detail"], "past_sale_date")
        held = staged_events(tax_row(staging_status="rejected", rejection_reason="duplicate"), TODAY)
        card, result = card_for(row, held)
        self.assertEqual(card["route"], "research_hold")
        self.assertEqual(result["matched"], [])


class EstateRule(unittest.TestCase):
    PARCEL = "043 061    01800 000 2027"

    def row(self, owner):
        return land_row(owner=owner, parcel=self.PARCEL, county="humphreys", ns="comptroller_roll")

    def test_names(self):
        self.assertTrue(same_person("Gary Lee Adcock", "ADCOCK GARY L ET AL"))
        self.assertFalse(same_person("Robert Adcock", "ADCOCK GARY L ET AL"))        # surname only
        self.assertFalse(same_person("Gary Wayne Adcock", "ADCOCK GARY L"))           # middle disagrees
        self.assertFalse(owner_group("RIGGINS NAOMI P ETAL & MAYES PEGGY R")["complete"])
        self.assertTrue(owner_group("SMITH JOHN A & MARY B")["complete"])
        self.assertFalse(owner_group("LUFFMAN DELORIS JANE HEIRS")["complete"])

    def test_sole_decedent_with_named_representative_advances(self):
        row = self.row("LUFFMAN DELORIS JANE HEIRS")
        events = staged_events(probate_row("Deloris Jane Luffman", "LUFFMAN DELORIS JANE HEIRS", reps=["Ann Luffman"]), TODAY)
        card, _ = card_for(row, events)
        self.assertEqual(card["route"], "first_trace_review")
        self.assertEqual(card["decider"]["name"], "Ann Luffman")
        self.assertFalse(card["decider"]["verified"])
        self.assertTrue(any("Power of sale" in u for u in card["economics"]["unknowns"]))

    def test_estate_without_a_named_representative_or_with_co_owners_holds(self):
        row = self.row("LUFFMAN DELORIS JANE HEIRS")
        card, result = card_for(row, staged_events(probate_row("Deloris Jane Luffman", "LUFFMAN DELORIS JANE HEIRS"), TODAY))
        self.assertEqual(len(result["matched"]), 1)
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(any("personal representative" in v for v in card["availability"]["verify_next"]))
        co = self.row("RIGGINS NAOMI P ETAL & MAYES PEGGY R")
        card, _ = card_for(co, staged_events(probate_row("Peggy R Mayes", "RIGGINS NAOMI P ETAL & MAYES PEGGY R",
                                                         reps=["Margaret Williams"]), TODAY))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("ET AL", card["decider"]["basis"])

    def test_decedent_with_only_the_surname_in_common_is_rejected(self):
        row = self.row("LUFFMAN DELORIS JANE HEIRS")
        events = staged_events(probate_row("Harold Luffman", "LUFFMAN DELORIS JANE HEIRS"), TODAY)
        _, result = card_for(row, events)
        self.assertEqual(result["rejected"][0]["reason"], "decedent_not_owner_of_record")

    def test_stale_estate_notice_does_not_count(self):
        row = self.row("LUFFMAN DELORIS JANE HEIRS")
        events = staged_events(probate_row("Deloris Jane Luffman", "LUFFMAN DELORIS JANE HEIRS", published="2025-06-01"), TODAY)
        _, result = card_for(row, events)
        self.assertEqual(result["rejected"][0]["reason"], "stale_notice")


class HamiltonTaxSale(unittest.TestCase):
    def events(self, sale_date=None, today=TODAY):
        return tax_sale.sale_events(tax_sale.parse_list(SALE_LIST), 2026, tax_sale.list_url(2026), sale_date, today)

    def test_parse_marks_and_units(self):
        parsed = tax_sale.parse_list(SALE_LIST)
        self.assertEqual(parsed["revised"], "2026-06-04")
        self.assertEqual(parsed["unparsed"], 0)
        self.assertEqual(parsed["entries"]["118H-F-029.01"]["mark"], "PAID")
        self.assertIsNone(parsed["entries"]["139H-A-033.02"]["mark"])
        self.assertIn("141--026 S002", parsed["entries"])            # a unit never matches the parent parcel
        self.assertNotIn("141--026", parsed["entries"])
        self.assertIsNone(tax_sale.parse_sale_date("tax sale auction is on Thursday, June 05, 2025.", 2026))
        self.assertEqual(tax_sale.parse_sale_date("tax sale auction is on Thursday, June 03, 2027.", 2027), "2027-06-03")

    def test_paid_listing_does_not_count(self):
        row = hamilton_row(owner="PHILLIPS ELINORE & TIMOTHY", parcel="118H-F-029.01")
        roll = {"file_date": "2026-11-01", "parcels": {"118H-F-029.01": {"total": 436.75, "years": 1, "latest_year": 2025}}}
        card, result = card_for(row, self.events(), roll)
        self.assertEqual(result["rejected"][0]["reason"], "paid_before_sale")
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("docket 11256", card["event"]["what"])
        self.assertIn("marked PAID", card["route_reason"])
        self.assertEqual(card["economics"]["known_debt"][0]["kind"], "delinquent_tax")
        self.assertEqual(card["economics"]["known_debt"][0]["amount"], 436.75)

    def test_open_listing_counts_only_before_a_known_sale_date(self):
        row = hamilton_row(owner="ROMERO HUMBERTO", parcel="139H-A-033.02")
        roll = {"file_date": "2027-05-20", "parcels": {"139H-A-033.02": {"total": 602.31, "years": 4, "latest_year": 2025}}}
        card, _ = card_for(row, self.events(), roll)
        self.assertEqual(card["route"], "research_hold")              # no sale date: outcome unknown
        open_sale = tax_sale.sale_events(tax_sale.parse_list(SALE_LIST), 2027, tax_sale.list_url(2027), "2027-06-03", date(2027, 5, 20))
        land = tract(row)
        card = build_card(row, land, review(row, land, open_sale), date(2027, 5, 20), roll)
        self.assertEqual(card["route"], "first_trace_review")
        self.assertEqual(card["economics"]["deadline"], "2027-06-03")

    def test_paid_off_roll_and_sold_rows_are_removed(self):
        row = hamilton_row(owner="REYES GERARDO & GONZALEZ JUAN", parcel="127H-D-012")
        card, _ = card_for(row, [], {"file_date": "2026-11-01", "parcels": {}})
        self.assertEqual(card["route"], "removed")
        self.assertIn("no longer on the Hamilton County Trustee delinquent tax file", card["route_reason"])
        sold = hamilton_row(owner="ROMERO HUMBERTO", assessor_owner="ROMERO ARIEL J GARCIA & TORRES DENISSE BARRAGAN",
                            parcel="139H-A-033.02")
        card, _ = card_for(sold, self.events(), {"file_date": "2026-11-01", "parcels": {}})
        self.assertEqual(card["route"], "removed")
        self.assertIn("Ownership changed", card["route_reason"])

    def test_spouse_dropped_from_the_roll_is_a_hold_not_a_removal(self):
        self.assertTrue(holder_remains("LEDFORD WILLIAM JOHN & ZINA LYN", "LEDFORD ZINA LYN"))
        self.assertFalse(holder_remains("ROMERO HUMBERTO", "ROMERO ARIEL J GARCIA & TORRES DENISSE BARRAGAN"))
        row = hamilton_row(owner="LEDFORD WILLIAM JOHN & ZINA LYN", assessor_owner="LEDFORD ZINA LYN")
        card, _ = card_for(row, [], {"file_date": "2026-11-01", "parcels": {"135B-A-036": {"total": 900.0, "years": 1, "latest_year": 2025}}})
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(card["availability"]["conflicts"])

    def test_a_grade_lot_with_tax_only_is_a_hold_that_says_what_would_qualify(self):
        card, _ = card_for(hamilton_row(), [], {"file_date": "2026-11-01", "parcels": {"135B-A-036": {"total": 838.93, "years": 3, "latest_year": 2025}}})
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("only a tax delinquency", card["route_reason"])
        self.assertTrue(any("Qualifies when" in v for v in card["availability"]["verify_next"]))
        self.assertNotIn("—", str(card))


class CrossLaneKeys(unittest.TestCase):
    def test_land_and_tax_cards_for_one_tract_count_once(self):
        parcel = "074 058    12100 000 2027"
        row = land_row(parcel=parcel, county="robertson", ns="comptroller_roll")
        tax = tax_row()
        tax["raw_payload"]["comptroller_parcel"] = {"method": "address", "county": "ROBERTSON", "parcelid": parcel}
        notice = {"id": "fc-1", "bot_source": "nashville_ledger", "county": "robertson", "distress_type": "PRE_FORECLOSURE",
                  "staging_status": "pending", "trustee_sale_date": "2026-12-15", "source_url": "https://example.com/n",
                  "raw_payload": {"comptroller_parcel": {"method": "address", "county": "ROBERTSON", "parcelid": parcel}}}
        land_card, _ = card_for(row, staged_events(notice, TODAY))
        src = source("County tax notice", "https://example.com/t", TODAY)
        tax_card = build_review(
            lane="tax", route="first_trace_review", route_reason="On the sale list.", reviewed_by="tax_lane", today=TODAY,
            county="Robertson", owner_names="SMITH JOHN A & MARY B",
            event={"what": "Tax sale", "status": "active", "checked_on": TODAY, "source": src},
            asset={"description": "Tract", "parcel": "074 058 12100 000", "tie": "Printed parcel", "source": src},
            decider={"name": "SMITH JOHN A & MARY B", "role": "Owner of record", "basis": "County roll", "source": src},
            economics={"value": 400000, "value_basis": "County appraisal", "value_source": src},
            availability={"evidence": "Still listed", "first_call_question": "Selling?"})
        self.assertEqual(land_card["property_key"], tax_card["property_key"])
        self.assertEqual(land_card["owner_group_key"], tax_card["owner_group_key"])
        groups = dedupe_worklist([land_card, tax_card])
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]["assets"]), 1)
        self.assertEqual(sorted(groups[0]["lanes"]), ["land", "tax"])


class FakeQuery:
    def __init__(self, store, op, payload=None):
        self.store, self.op, self.payload, self.filters = store, op, payload, []

    def select(self, *_):
        return self

    def eq(self, field, value):
        self.filters.append(("eq", field, value))
        return self

    def is_(self, field, value):
        self.filters.append(("is", field, value))
        return self

    def limit(self, *_):
        return self

    def _match(self, row):
        for kind, field, value in self.filters:
            if field == "raw_payload->pilot_review->>reviewed_at":
                got = (row["raw_payload"].get("pilot_review") or {}).get("reviewed_at")
            elif field == "raw_payload->pilot_review":
                got = row["raw_payload"].get("pilot_review")
            elif field == "raw_payload":
                import json
                got = json.dumps(row["raw_payload"], separators=(",", ":"))
            else:
                got = row.get(field)
            if (kind == "is" and got is not None) or (kind == "eq" and got != value):
                return False
        return True

    def execute(self):
        rows = [r for r in self.store.rows if self._match(r)]
        if self.op == "update":
            for r in rows:
                r.update(copy.deepcopy(self.payload))
            self.store.updates += len(rows)
        return type("R", (), {"data": copy.deepcopy(rows)})()


class FakeClient:
    def __init__(self, rows):
        self.rows, self.updates = rows, 0

    def table(self, _):
        client = self
        return type("T", (), {"select": lambda s, *a: FakeQuery(client, "select"),
                              "update": lambda s, payload: FakeQuery(client, "update", payload)})()


class WritePath(unittest.TestCase):
    def item(self, row):
        land = tract(row)
        return {"id": row["id"], "identity": list(land["identity"]),
                "card": build_card(row, land, review(row, land, staged_events(tax_row(), TODAY)), TODAY)}

    def test_repeat_run_writes_once(self):
        row = land_row()
        client = FakeClient([copy.deepcopy(row)])
        bot = LandReviewBot()
        first = bot.write_cards(client, [self.item(row)])
        self.assertEqual(first["written"], ["land-1"])
        self.assertEqual(client.rows[0]["raw_payload"]["pilot_review"]["route"], "first_trace_review")
        self.assertIn("land", client.rows[0]["raw_payload"])            # the rest of the payload is kept
        again = self.item(row)
        again["card"]["reviewed_at"] = "2026-11-23T09:00:00+00:00"
        second = bot.write_cards(client, [again])
        self.assertEqual(second["unchanged"], ["land-1"])
        self.assertEqual(client.updates, 1)
        self.assertTrue(same_card(client.rows[0]["raw_payload"]["pilot_review"], again["card"]))

    def test_manual_and_stronger_cards_are_kept(self):
        row = land_row()
        stored = copy.deepcopy(row)
        stored["raw_payload"]["pilot_review"] = {"manual": True, "route": "removed", "reviewed_by": "patrick"}
        client = FakeClient([stored])
        out = LandReviewBot().write_cards(client, [self.item(row)])
        self.assertEqual(out["kept_existing"], ["land-1"])
        self.assertEqual(client.updates, 0)
        other = copy.deepcopy(row)
        other["raw_payload"]["pilot_review"] = {"route": "removed", "reviewed_by": "tax_lane", "reviewed_at": "x"}
        client = FakeClient([other])
        self.assertEqual(LandReviewBot().write_cards(client, [self.item(row)])["kept_existing"], ["land-1"])

    def test_row_that_changed_parcel_is_not_written(self):
        row = land_row()
        moved = copy.deepcopy(row)
        moved["raw_payload"]["land"]["parcel_id"] = moved["raw_payload"]["comptroller_parcel"]["parcelid"] = "102 01400 R0099999"
        client = FakeClient([moved])
        self.assertEqual(LandReviewBot().write_cards(client, [self.item(row)])["conflict"], ["land-1"])
        self.assertEqual(client.updates, 0)

    def test_only_ids_scopes_a_backfill(self):
        row = land_row()
        client = FakeClient([copy.deepcopy(row)])
        out = LandReviewBot().write_cards(client, [self.item(row)], only_ids={"someone-else"})
        self.assertEqual(client.updates, 0)
        self.assertEqual(out["written"], [])


class ScheduledOrder(unittest.TestCase):
    def test_land_runs_early_and_review_follows_acquisition(self):
        names = [n for n, _ in _run_new.select_bots()]
        self.assertEqual(names[0], "bot_health_monitor")
        self.assertEqual(names.count("land_lane"), 1)
        self.assertEqual(names.index("land_review"), names.index("land_lane") + 1)
        self.assertLess(names.index("land_review"), names.index("hud_reo"))


if __name__ == "__main__":
    unittest.main()
