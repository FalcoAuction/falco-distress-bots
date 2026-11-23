"""Register index review: roll match, notice link, cards and the pass inside
preforeclosure_review. No network; a small made-up roll in a temp index."""

from __future__ import annotations

import copy
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test")

from src.bots import _register_index_review as review  # noqa: E402
from src.bots._comptroller import SCHEMA, ParcelIndex, name_tokens, street_key_from_comptroller  # noqa: E402
from src.bots._preforeclosure_review import LINK_KEY  # noqa: E402
from src.bots._register_index_review import (ROLL_KEY, Roll, appointment_card, link_notice, lis_pendens_card,  # noqa: E402
                                             notice_identity, roll_match, street_evidence)
from src.bots.preforeclosure_review_bot import PreforeclosureReviewBot  # noqa: E402

TODAY = date(2026, 11, 23)
NOW = datetime(2026, 11, 23, 18, 0, tzinfo=timezone.utc)
EVIDENCE = {"kind": "records_request_reply", "office": "Putnam County Register of Deeds", "received": "2026-11-23",
            "listing": "Recording Listing by Date and Time, 2026-10-01 thru 2026-11-20", "from": "Deputy Clerk (email)"}


def parcel(pid, owner, owner2="", address="", city="COOKEVILLE", appraisal=200000.0, county="PUTNAM", mail=None):
    o, o2 = owner.upper(), owner2.upper()
    number = address.split()[-1] if address else ""
    street = " ".join(address.split()[:-1])
    mail = mail or (f"{number} {street}" if address else "")
    return (county, pid, street_key_from_comptroller(address), city, o, o2, (name_tokens(o) or [None])[0],
            (name_tokens(o2) or [None])[0], mail, city, "TN", "38501", "00 RESIDENTIAL", "11 - HOUSEHOLD UNITS", appraisal,
            0.0, 0.0, 1, "", 0.0, "", "", address)


PARCELS = [
    parcel("071 001 00100 000 2027", "ROE STACY LOUISE", "ETVIR THURMAN SANFORD ROE", "WEST HAVEN DR 2960", appraisal=369500.0),
    parcel("071 002 00100 000 2027", "DOE ALTON R", "", "ELLEN CIR 4747", appraisal=339400.0),
    parcel("071 003 00100 000 2027", "POE EDWARD", "", "PENTHOUSE RD 5138", appraisal=190200.0),
    parcel("071 004 00100 000 2027", "GRAY MARY NELL", "AND CRISSY LEANN ROBBINS ETAL", "OLD BAXTER RD 9219"),
    # A common name: three owners, one of them with the borrower's middle initial.
    parcel("071 005 00100 000 2027", "SMITH JOHN A", "", "OAK ST 1"),
    parcel("071 005 00200 000 2027", "SMITH JOHN B", "", "ELM ST 2"),
    parcel("071 005 00300 000 2027", "SMITH JOHN", "ETUX LINDA SMITH", "PINE ST 3"),
    # One owner, a house and the lot beside it.
    parcel("071 006 00100 000 2027", "LANE PAT Q", "", "MILL RD 10"),
    parcel("071 006 00200 000 2027", "LANE PAT Q", "", "MILL RD 12", appraisal=30000.0, mail="10 MILL RD"),
    # Two owners told apart only by the middle initial.
    parcel("071 010 00100 000 2027", "KING JOHN A", "", "FIRST ST 1"),
    parcel("071 010 00200 000 2027", "KING JOHN B", "", "SECOND ST 2"),
    parcel("071 007 00100 000 2027", "FIRST SAMPLE BANK", "", "BANK ST 100"),
    parcel("071 008 00100 000 2027", "ACME RENTALS LLC", "", "SAMPLE PL 120"),
    parcel("071 009 00100 000 2027", "WEST CAROL ANN", "", "RIDGE RD 55", appraisal=250000.0),
]


def make_roll(tmp):
    path = os.path.join(tmp, "index.sqlite")
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    con.executemany(f"INSERT INTO parcels VALUES ({','.join('?' * 23)})", PARCELS)
    con.commit()
    con.close()
    return Roll(ParcelIndex(path), {"PUTNAM": "2026-11-16"}, "2026-11-17", {"PUTNAM": "https://example.test/Putnam.zip"})


class RollCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.roll = make_roll(cls.tmp.name)

    @classmethod
    def tearDownClass(cls):
        cls.roll.con.close()
        cls.tmp.cleanup()

    def match(self, *borrowers, county="Putnam"):
        return roll_match(self.roll, county, list(borrowers))


class RollMatchTests(RollCase):
    def test_both_borrowers_on_one_parcel_is_confirmed(self):
        found = self.match("ROE THURMAN SANFORD", "ROE STACY LOUISE")
        self.assertEqual((found["status"], found["strength"], found["parcel_id"], found["appraisal"]),
                         ("confirmed", "joint", "071 001 00100 000 2027", 369500.0))
        self.assertEqual(found["situs"], "2960 WEST HAVEN DR, COOKEVILLE, TN 38501")
        self.assertEqual((found["roll_date"], found["roll_source"]), ("2026-11-16", "https://example.test/Putnam.zip"))

    def test_middle_initial_and_sole_owner_by_that_name_is_confirmed(self):
        found = self.match("DOE ALTON R")
        self.assertEqual((found["status"], found["strength"], found["roll_owner"]), ("confirmed", "middle_initial", "DOE ALTON R"))

    def test_first_and_last_name_alone_is_not_verification(self):
        found = self.match("POE EDWARD")
        self.assertEqual((found["status"], found["parcel_count"]), ("name_only", 1))

    def test_common_name_is_held_even_with_a_matching_middle_initial(self):
        found = self.match("KING JOHN A")
        self.assertEqual((found["status"], found["parcel_count"], found["owner_count"]), ("common_name", 2, 2))
        loose = self.match("SMITH JOHN A")                      # "SMITH JOHN" with no initial could be the same man
        self.assertEqual((loose["status"], loose["parcel_count"], loose["owner_count"]), ("ambiguous", 2, 2))
        plain = self.match("SMITH JOHN")
        self.assertEqual((plain["status"], plain["parcel_count"], plain["owner_count"]), ("ambiguous", 3, 3))
        self.assertNotIn("parcel_id", plain)

    def test_one_owner_with_two_parcels_is_not_an_exact_parcel(self):
        found = self.match("LANE PAT Q")
        self.assertEqual((found["status"], found["parcel_count"]), ("multiple_parcels", 2))

    def test_roll_owner_not_on_the_appointment_holds(self):
        found = self.match("GRAY MARY NELL", "GRAY LOYD")
        self.assertEqual(found["status"], "co_owner_mismatch")
        self.assertEqual((found["extra_owners"], found["et_al"], found["missing_borrowers"]),
                         (["CRISSY LEANN ROBBINS"], True, ["GRAY LOYD"]))

    def test_lender_among_the_borrowers_is_never_looked_up(self):
        found = self.match("FIRST SAMPLE BANK")
        self.assertEqual(found["status"], "no_person_borrower")
        self.assertNotIn("roll_owner", found)
        mixed = self.match("FIRST SAMPLE BANK", "DOE ALTON R")
        self.assertEqual((mixed["status"], mixed["roll_owner"], mixed["searched"]), ("confirmed", "DOE ALTON R", ["DOE ALTON R"]))

    def test_no_owner_by_that_name_and_county_not_on_file(self):
        self.assertEqual(self.match("LINDEN THOMAS")["status"], "no_match")
        self.assertEqual(self.match("DOE ALTON R", county="Davidson")["status"], "county_not_on_file")


def appointment(**over):
    base = {"county": "Putnam", "borrowers": ["DOE ALTON R"], "trustees": ["CLEAR SAMPLE LLC"], "recorded": "2026-10-06",
            "roll": {"status": "confirmed", "parcel_count": 1, "situs": "4747 ELLEN CIR, COOKEVILLE, TN 38501"}}
    return {**base, **over}


def notice(**over):
    base = {"id": "n1", "bot_source": "tn_public_notice", "county": "putnam", "full_name": "Alton R. Doe",
            "borrowers": ["Alton R. Doe"], "property_address": "4747 Ellen Cir, Cookeville, TN 38501",
            "trustee_sale_date": "2026-12-15", "publication_date": "Thursday, October 15, 2026",
            "staged_at": "2026-10-16T12:00:00+00:00", "source_url": "https://example.test/n1",
            "excerpt": "SUBSTITUTE TRUSTEE'S SALE ... Clear Sample, LLC, Substitute Trustee", "staging_status": "pending"}
    return notice_identity({**base, **over})


class LinkTests(unittest.TestCase):
    def test_borrower_and_street_link_with_days_gained(self):
        link = link_notice(appointment(trustees=[]), [notice(excerpt="")])
        self.assertEqual((link["status"], link["first_advertised"], link["appointment_recorded"], link["days_gained"]),
                         ("linked", "2026-10-15", "2026-10-06", 9))
        self.assertEqual((link["sale_date"], link["first_advertised_basis"]), ("2026-12-15", "publication date on the notice"))
        self.assertIn("street of the borrower's parcel", link["basis"])

    def test_borrower_and_trustee_link_without_a_roll_match(self):
        link = link_notice(appointment(roll={"status": "no_match"}), [notice()])
        self.assertEqual(link["status"], "linked")
        self.assertIn("substitute trustee this appointment appointed", link["basis"])

    def test_name_alone_is_a_candidate_not_a_link(self):
        link = link_notice(appointment(roll={"status": "name_only", "parcel_count": 1, "situs": "9 OTHER RD, COOKEVILLE, TN"}),
                           [notice(excerpt="Some Other Firm, Substitute Trustee")])
        self.assertEqual(link["status"], "candidate")
        self.assertEqual(link["candidates"][0]["staging_id"], "n1")
        self.assertNotIn("days_gained", link)

    def test_other_people_and_other_counties_do_not_link(self):
        self.assertEqual(link_notice(appointment(), [notice(full_name="Alton Smith", borrowers=["Alton Smith"])])["status"], "none")
        self.assertEqual(link_notice(appointment(), [notice(borrowers=["Alton Smith", "Mary Doe"])])["status"], "none")
        self.assertEqual(link_notice(appointment(), [notice(county="overton")])["status"], "none")
        self.assertEqual(link_notice(appointment(), [notice(publication_date="2026-05-01", trustee_sale_date="2026-06-01")])["status"], "none")

    def test_trustee_list_without_a_borrower_needs_street_and_firm(self):
        listed = dict(bot_source="mackie_wolf_trustee", borrowers=[], full_name=None, excerpt=None, publication_date=None,
                      report_date="2026-10-30")
        link = link_notice(appointment(trustees=["MACKIE WOLF ZIENTZ & MANN PC"]), [notice(**listed)])
        self.assertEqual((link["status"], link["days_gained"], link["first_advertised_basis"]),
                         ("linked", 24, "date of the trustee's sale report"))
        self.assertEqual(link_notice(appointment(), [notice(**listed)])["status"], "none")                 # another firm
        self.assertEqual(link_notice(appointment(trustees=["MACKIE WOLF ZIENTZ & MANN PC"]),
                                     [notice(**listed, property_address="1 Other St, Cookeville, TN")])["status"], "none")

    def test_confirmed_parcel_on_another_street_is_a_conflict(self):
        link = link_notice(appointment(), [notice(property_address="99 Elsewhere Rd, Cookeville, TN 38501")])
        self.assertEqual(link["status"], "conflict")
        self.assertIn("99 Elsewhere Rd", link["reason"])

    def test_recording_after_the_advertisement_gains_nothing(self):
        link = link_notice(appointment(recorded="2026-10-20"), [notice()])
        self.assertEqual((link["status"], link["days_gained"]), ("linked", -5))


def staged(**raw_over):
    raw = {"book_page": "RB 1736-282", "code": "SUBTR", "recorded": "2026-10-06", "appointment_date": "2026-09-29",
           "lenders": ["FIRST SAMPLE BANK"], "borrowers": ["DOE ALTON R"], "substitute_trustees": ["CLEAR SAMPLE LLC"],
           "party_basis": "listing grantors, lender names split by keyword", "evidence": EVIDENCE}
    raw.update(raw_over)
    return {"id": "r1", "bot_source": "putnam_register_index", "county": "Putnam", "distress_type": "SUBSTITUTION_OF_TRUSTEE",
            "pipeline_lead_key": "k1", "property_address": None, "source_url": None, "staging_status": "pending",
            "staged_at": "2026-11-23T16:13:15+00:00", "raw": raw}


class CardTests(RollCase):
    def card(self, row=None, link=None, **kwargs):
        row = row or staged()
        found = roll_match(self.roll, "Putnam", row["raw"]["borrowers"])
        return appointment_card(row, found, link or {"status": "none"}, TODAY, **kwargs)

    def linked(self, **over):
        return {**link_notice(appointment(), [notice()]), **over}

    def test_confirmed_appointment_with_no_sale_date_is_an_early_filing_hold(self):
        card = self.card(ranked=False)
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(card["route_reason"].startswith("Early filing: owner DOE ALTON R on the Putnam County roll"))
        self.assertIn("No sale date is advertised yet", card["route_reason"])
        self.assertEqual((card["decider"]["name"], card["decider"]["verified"]), ("DOE ALTON R", True))
        self.assertEqual((card["asset"]["parcel"], card["economics"]["value"], card["economics"]["deadline"]),
                         ("071 002 00100 000 2027", 339400.0, None))
        self.assertEqual(card["owner_group_key"], "tn|putnam|P:DOE A")
        self.assertIsNone(card["event"]["source"]["url"])
        self.assertIn("Putnam County Register of Deeds", card["event"]["source"]["label"])
        self.assertEqual(card["event"]["source"]["date"], "2026-11-23")

    def test_name_only_match_says_so_and_names_no_asset(self):
        card = self.card(staged(borrowers=["POE EDWARD"]))
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(card["route_reason"].startswith("Owner match by name only, 1 parcel"))
        self.assertEqual((card["decider"]["name"], card["decider"]["verified"]), ("POE EDWARD", False))
        self.assertEqual((card["asset"]["parcel"], card["asset"]["address"], card["economics"]["value"]), (None, None, None))
        self.assertIsNone(card["property_key"])

    def test_common_name_and_two_parcels_count_the_parcels(self):
        common = self.card(staged(borrowers=["SMITH JOHN A"]))
        self.assertTrue(common["route_reason"].startswith("Owner match by name only, 2 parcels under 2 different owners"))
        self.assertFalse(common["decider"]["verified"])
        two = self.card(staged(borrowers=["LANE PAT Q"]))
        self.assertTrue(two["route_reason"].startswith("Owner match by name only, 2 parcels"))
        self.assertIsNone(two["asset"]["parcel"])

    def test_lender_and_trustee_are_never_the_owner(self):
        for row in (staged(), staged(borrowers=[]), staged(borrowers=["FIRST SAMPLE BANK"]), staged(borrowers=["LINDEN THOMAS"])):
            card = self.card(row)
            text = " ".join(str(x) for x in (card["decider"]["name"], card["owner_names"], card["owner_group_key"]))
            self.assertNotIn("BANK", text)
            self.assertNotIn("CLEAR SAMPLE", text)
        none = self.card(staged(borrowers=["FIRST SAMPLE BANK"]))
        self.assertEqual((none["route"], none["decider"]["name"], none["decider"]["role"]), ("research_hold", None, "Not identified"))
        self.assertIn("names no individual borrower", none["route_reason"])

    def test_co_owner_mismatch_and_no_match_hold(self):
        card = self.card(staged(borrowers=["GRAY MARY NELL", "GRAY LOYD"]))
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("CRISSY LEANN ROBBINS", card["route_reason"])
        self.assertIn("GRAY LOYD is on the appointment but not on the roll", card["route_reason"])
        self.assertTrue(card["availability"]["conflicts"])
        missing = self.card(staged(borrowers=["LINDEN THOMAS"]))
        self.assertIn("No owner named LINDEN THOMAS is on the Putnam County roll", missing["route_reason"])

    def test_linked_appointment_is_a_duplicate_of_its_carded_notice(self):
        card = self.card(link=self.linked(), notice_carded=True, ranked=False)
        self.assertEqual(card["route"], "removed")
        self.assertIn("Duplicate of the sale notice record", card["route_reason"])
        self.assertIn("9 days before the first advertisement", card["route_reason"])

    def test_linked_appointment_whose_notice_has_no_card(self):
        held = self.card(link=self.linked(), ranked=False)
        self.assertEqual(held["route"], "research_hold")
        self.assertIn("not on the ranked list", held["route_reason"])
        ready = self.card(link=self.linked(), ranked=True)
        self.assertEqual((ready["route"], ready["economics"]["deadline"]), ("first_trace_review", "2026-12-15"))
        self.assertIn("9 days before the first advertisement", ready["event"]["what"])
        passed = self.card(link=self.linked(sale_date="2026-11-10"), ranked=True)
        self.assertEqual(passed["route"], "removed")

    def test_unconfirmed_owner_is_never_first_trace_even_with_a_sale_date(self):
        card = self.card(staged(borrowers=["POE EDWARD"]), link=self.linked(), ranked=True)
        self.assertEqual(card["route"], "research_hold")
        self.assertTrue(card["route_reason"].startswith("Owner match by name only"))

    def test_candidate_notice_and_conflict_hold(self):
        candidate = {"status": "candidate", "candidates": [{"staging_id": "n1", "source": "tn_public_notice"}], "reason": "x"}
        card = self.card(link=candidate, ranked=True)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("is not linked: name only", card["route_reason"])
        conflict = self.card(link={"status": "conflict", "reason": "notice names 99 Elsewhere Rd", "notice_ids": ["x"]})
        self.assertEqual(conflict["route"], "research_hold")
        self.assertIn("different property", conflict["route_reason"])

    def test_notice_street_confirms_or_contradicts_the_owner(self):
        earlier = roll_match(self.roll, "Putnam", ["WEST CAROL"])
        self.assertEqual(earlier["status"], "name_only")
        found = street_evidence(self.roll, "Putnam", ["WEST CAROL"], "55 Ridge Rd, Cookeville, TN 38501", earlier)
        self.assertEqual((found["status"], found["strength"], found["parcel_id"]), ("confirmed", "notice_street", "071 009 00100 000 2027"))
        other = street_evidence(self.roll, "Putnam", ["WEST CAROL"], "120 Sample Pl, Cookeville, TN", earlier)
        self.assertEqual((other["status"], other["roll_owner"]), ("owner_differs", "ACME RENTALS LLC"))
        card = appointment_card(staged(borrowers=["WEST CAROL"]), other, self.linked(), TODAY, ranked=True)
        self.assertEqual(card["route"], "research_hold")
        self.assertEqual(card["decider"]["name"], "WEST CAROL")                 # the roll's company is not staged as owner
        self.assertEqual(street_evidence(self.roll, "Putnam", ["WEST CAROL"], "1 Nowhere Ln, Cookeville, TN", earlier), earlier)

    def test_lis_pendens_is_research_with_no_owner(self):
        row = {"id": "p1", "county": "Putnam", "property_address": "120 Sample Place, Putnam County, TN",
               "raw": {"book_page": "RB 1739-269", "recorded": "2026-10-20", "first_parties": ["POE ANNA", "POE BELLA"],
                       "second_parties": ["ACME RENTALS LLC"], "property_as_indexed": "120 SAMPLE PLACE; Lot: 81",
                       "owner_not_established": True, "evidence": EVIDENCE}}
        hit = self.roll.index.match_address(row["property_address"], "Putnam")
        card = lis_pendens_card(row, TODAY, hit, "2026-11-16")
        self.assertEqual((card["route"], card["decider"]["name"], card["decider"]["role"]), ("research_hold", None, "Not established"))
        self.assertIn("does not say who sued whom", card["route_reason"])
        self.assertEqual((card["owner_names"], card["owner_group_key"]), ([], None))
        self.assertIn("bulk street match", card["asset"]["tie"])
        self.assertEqual(lis_pendens_card(row, TODAY)["asset"]["parcel"], None)


class Staging:
    """Staging rows behind the query chain the pass uses, plus the write RPC."""

    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def table(self, _):
        return Chain(self)

    def rpc(self, name, params):
        self.calls.append(copy.deepcopy(params["p_rows"]))
        for item in params["p_rows"]:
            for row in self.rows:
                if row["id"] == item["id"]:
                    row["raw_payload"] = {**row["raw_payload"], **item["raw_patch"]}

        class Result:
            def execute(self):
                return type("R", (), {"data": len(params["p_rows"])})()
        return Result()


class Chain:
    def __init__(self, db):
        self.db, self.tests = db, []

    def select(self, *_):
        return self

    def eq(self, key, value):
        self.tests.append(lambda r, k=key, v=value: r.get(k) == v)
        return self

    def in_(self, key, values):
        self.tests.append(lambda r, k=key, v=set(values): r.get(k) in v)
        return self

    def gte(self, key, value):
        self.tests.append(lambda r, k=key, v=value: (r.get(k) or "") >= v)
        return self

    def rows(self):
        return [r for r in self.db.rows if all(t(r) for t in self.tests)]


class PassTests(RollCase):
    def setUp(self):
        def row(ident, borrowers, book):
            base = staged(borrowers=borrowers, book_page=book)
            return {**{k: v for k, v in base.items() if k != "raw"}, "id": ident, "raw_payload": base["raw"]}
        self.confirmed = row("r1", ["DOE ALTON R"], "RB 1736-282")
        self.by_name = row("r2", ["POE EDWARD"], "RB 1740-30")
        self.lis = {"id": "p1", "bot_source": "putnam_register_index", "county": "Putnam", "distress_type": "LIS_PENDENS",
                    "property_address": "120 Sample Place, Putnam County, TN", "staging_status": "pending",
                    "staged_at": "2026-11-23T16:13:15+00:00",
                    "raw_payload": {"book_page": "RB 1739-269", "recorded": "2026-10-20", "first_parties": ["POE ANNA"],
                                    "second_parties": [], "property_as_indexed": "120 SAMPLE PLACE", "evidence": EVIDENCE}}
        self.notice = {"id": "n1", "bot_source": "mackie_wolf_trustee", "county": "Putnam", "full_name": None, "borrowers": [],
                       "property_address": "4747 Ellen Cir, Cookeville, TN 38501", "trustee_sale_date": "2026-12-21",
                       "report_date": "2026-11-20", "staged_at": "2026-11-20T20:00:00+00:00", "staging_status": "pending",
                       "source_url": "https://example.test/report.pdf", "owner_name_records": None, "property_value": None,
                       "property_value_source": None,
                       "notice_tracking": {"last_seen_at": "2026-11-23T15:00:00+00:00", "last_seen_by": "mackie_wolf_trustee"}}
        self.db = Staging([self.confirmed, self.by_name, self.lis])
        self.sale_rows = []
        self.bot = PreforeclosureReviewBot()
        for name, value in (("_appointments", lambda c, t, only: []), ("_notices", lambda c, t: []),
                            ("_sales", lambda c, t, source: [r for r in self.sale_rows if r["bot_source"] == source]),
                            ("_ranked", lambda c, ids: set()), ("_page", lambda build: copy.deepcopy(build().rows()))):
            self.enterContext(patch.object(self.bot, name, side_effect=value))
        self.enterContext(patch.object(review, "open_roll", return_value=self.roll))

    def run_once(self, **kwargs):
        return self.bot.review(self.db, now=NOW, register_index=True, **kwargs)

    def test_first_run_writes_cards_and_a_repeat_writes_nothing(self):
        first = self.run_once()
        self.assertEqual((first["register_appointments"], first["register_lis_pendens"], first["register_owner_confirmed"],
                          first["cards_written"], first["rows_changed"]), (2, 1, 1, 3, 3))
        self.assertEqual(self.confirmed["raw_payload"]["pilot_review"]["route"], "research_hold")
        self.assertEqual(self.confirmed["raw_payload"][ROLL_KEY]["status"], "confirmed")
        self.assertTrue(self.by_name["raw_payload"]["pilot_review"]["route_reason"].startswith("Owner match by name only, 1 parcel"))
        self.assertEqual(self.lis["raw_payload"]["pilot_review"]["decider"]["name"], None)
        second = self.run_once()
        self.assertEqual((second["rows_changed"], second["cards_written"], second["unchanged"]), (0, 0, 3))
        self.assertEqual(len(self.db.calls), 1)

    def test_off_by_default_and_dry_run_write_nothing(self):
        off = self.bot.review(self.db, now=NOW)
        self.assertEqual((off["rows_changed"], "register_appointments" in off), (0, False))
        plan = self.run_once(dry_run=True)
        self.assertEqual(sorted(p["id"] for p in plan["plan"]), ["p1", "r1", "r2"])
        self.assertEqual(self.db.calls, [])
        self.assertNotIn("pilot_review", self.confirmed["raw_payload"])

    def test_appointment_that_duplicates_a_carded_notice_is_removed(self):
        self.confirmed["raw_payload"]["substitute_trustees"] = ["MACKIE WOLF ZIENTZ & MANN PC"]
        self.db.rows.append(self.notice)
        self.sale_rows.append(self.notice)
        result = self.run_once(dry_run=True)
        self.assertEqual(result["register_linked"], 1)
        patches = result["patches"]
        self.assertEqual(patches["r1"]["pilot_review"]["route"], "removed")
        self.assertEqual((patches["r1"][LINK_KEY]["staging_id"], patches["r1"][LINK_KEY]["days_gained"]), ("n1", 45))
        self.assertIn("Putnam County Register RB 1736-282", patches["n1"]["pilot_review"]["event"]["what"])
        self.assertIn("45 days before the first advertisement", patches["n1"]["pilot_review"]["event"]["what"])
        self.assertNotEqual(patches["r2"]["pilot_review"]["route"], "removed")

    def test_manual_card_is_kept(self):
        self.confirmed["raw_payload"]["pilot_review"] = {"manual": True, "route": "first_trace_review", "reviewed_by": "patrick"}
        result = self.run_once()
        self.assertEqual(result["kept_existing"], 1)
        self.assertEqual(self.confirmed["raw_payload"]["pilot_review"]["reviewed_by"], "patrick")

    def test_a_cautious_card_from_another_lane_is_not_replaced(self):
        self.lis["raw_payload"]["pilot_review"] = {"route": "removed", "reviewed_by": "cre_review"}
        self.run_once()
        self.assertEqual(self.lis["raw_payload"]["pilot_review"]["reviewed_by"], "cre_review")

    def test_no_roll_or_a_failure_changes_nothing(self):
        with patch.object(review, "open_roll", return_value=None):
            result = self.run_once()
        self.assertEqual((result["rows_changed"], result["register_skipped_no_roll"]), (0, 1))
        with patch.object(review, "roll_match", side_effect=RuntimeError("boom")):
            failed = self.run_once()
        self.assertEqual((failed["rows_changed"], failed["register_failed"]), (0, 1))
        self.assertEqual(self.db.calls, [])


if __name__ == "__main__":
    unittest.main()
