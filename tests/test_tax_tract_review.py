import copy
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

from src.bots import cheatham_tax_sale_bot as cheatham
from src.bots._pilot_review import dedupe_worklist, property_key
from src.bots._tax_tract_review import (CARD_KEY, STATUS_KEY, fold_parcel, match_tract, next_status, patch_for,
                                        shelby_card, shelby_wants_card)
from src.bots.shelby_tax_sale_bot import EXTRACT, REMOVED, ShelbyTaxSaleBot, scheduled_parcels, staging_row

DAY = date(2026, 11, 22)
PAGE = f'<a href="{EXTRACT}">CSV</a><table><tr><td>2302</td><td>04/13/2027</td></tr></table>'
HEADER = 'ParcelID,Alt_Parcel,Street Number,Street Name,Tax Sale\n'
LISTED = {"available": True, "listed": True, "sale_date": "2027-04-13", "source_url": EXTRACT, "source_date": "2026-11-22"}
GONE = {"available": True, "listed": False, "source_url": EXTRACT, "source_date": "2026-11-23"}
DOWN = {"available": False, "reason": "Shelby trustee inventory unavailable"}


def shelby_row(**over):
    row = {
        "id": "row-1", "staging_status": "pending", "rejection_reason": None,
        "property_address": "214 MOUNTAIN BROOK, Shelby County, TN", "owner_name_records": "HERIN JOHN B & TINA",
        "property_value": 2489800, "property_value_source": "shelby_trustee_assessment_estimate",
        "trustee_sale_date": "2027-04-13", "source_url": "https://apps2.shelbycountytrustee.com/Parcel?parcel=D0222+++00514C",
        "raw_payload": {
            "sale_id": "TS2302", "parcel": "D0222   00514C", "alt_parcel": "D022200000514C",
            "trustee_detail": {"checked_on": "2026-11-21", "primary_owner": "HERIN JOHN B & TINA", "tax_year": 2026,
                               "estimated_appraisal": 2489800,
                               "total_tax_balance": 90794.06,
                               "visible_prior_year_balances": {"2024": 22998.96, "2025": 19004.34}},
            "publication_evidence": {"parties": ["HERIN JOHN B & TINA", "MADISON C HERIN"],
                                     "url": "https://www.memphisdailynews.com/notices/2026/Nov/18/"},
        },
    }
    row.update(over)
    return row


def with_raw(**keys):
    row = shelby_row()
    row["raw_payload"] = {**row["raw_payload"], **keys}
    return row


class StatusTests(unittest.TestCase):
    def test_still_listed_tract_is_active_and_keeps_tax_apart_from_other_debt(self):
        status = next_status({"status": "listed", "sale_date": "2027-04-13"}, LISTED, DAY)
        self.assertEqual((status["status"], status["last_confirmed_on"]), ("listed", "2026-11-22"))
        card = shelby_card(shelby_row(), status, DAY, EXTRACT)
        self.assertEqual((card["lane"], card["route"], card["event"]["status"]), ("tax", "first_trace_review", "active"))
        self.assertEqual(card["economics"]["runway_days"], 142)
        self.assertIn("redemption", card["economics"]["deadline_kind"])
        self.assertIn("before the sale", card["economics"]["deadline_kind"])
        self.assertEqual([(d["kind"], d["amount"], d["as_of"]) for d in card["economics"]["known_debt"]],
                         [("delinquent_tax", 42003.3, "2026-11-21")])
        self.assertIn("Not a payoff", card["economics"]["known_debt"][0]["note"])
        self.assertTrue(any(u.startswith("Mortgage payoff") for u in card["economics"]["unknowns"]))
        self.assertEqual(card["decider"]["role"], "owner of record on the county tax roll")
        self.assertFalse(card["decider"]["verified"])
        self.assertNotIn("—", str(card))

    def test_removed_tract_is_dated_and_says_only_what_the_list_shows(self):
        listed = next_status({"status": "listed", "sale_date": "2027-04-13"}, LISTED, DAY)
        gone = next_status(listed, GONE, date(2026, 11, 23))
        self.assertEqual((gone["status"], gone["removed_on"], gone["last_listed_on"]),
                         ("removed", "2026-11-23", "2026-11-22"))
        self.assertIn("consistent with payment or withdrawal", gone["note"])
        self.assertNotIn("paid in full", gone["note"])
        card = shelby_card(shelby_row(staging_status="rejected", rejection_reason=REMOVED), gone, date(2026, 11, 23), EXTRACT)
        self.assertEqual((card["route"], card["event"]["status"]), ("removed", "resolved"))
        later = next_status(gone, GONE, date(2026, 11, 30))
        self.assertEqual((later["status"], later["removed_on"]), ("removed", "2026-11-23"))
        back = next_status(later, LISTED, date(2026, 12, 1))
        self.assertEqual((back["status"], back["relisted_on"]), ("listed", "2026-12-01"))
        self.assertNotIn("removed_on", back)

    def test_postponed_sale_keeps_the_earlier_date(self):
        listed = next_status(None, LISTED, DAY)
        moved = next_status(listed, {**LISTED, "sale_date": "2027-06-08"}, date(2026, 12, 1))
        self.assertEqual((moved["status"], moved["sale_date"], moved["prior_sale_date"], moved["postponed_noted_on"]),
                         ("postponed", "2027-06-08", "2027-04-13", "2026-12-01"))
        again = next_status(moved, {**LISTED, "sale_date": "2027-06-08"}, date(2026, 12, 2))
        self.assertEqual((again["status"], again["prior_sale_date"]), ("postponed", "2027-04-13"))
        card = shelby_card(shelby_row(trustee_sale_date="2027-06-08"), again, date(2026, 12, 2), EXTRACT)
        self.assertEqual((card["route"], card["event"]["status"], card["economics"]["deadline"]),
                         ("first_trace_review", "postponed", "2027-06-08"))

    def test_completed_sale_is_not_called_paid_or_removed_from_list(self):
        listed = next_status(None, LISTED, DAY)
        held = next_status(listed, {"available": True, "listed": False}, date(2027, 4, 14))
        self.assertEqual((held["status"], held["sale_date"], held["result"]), ("sale_held", "2027-04-13", None))
        self.assertIn("redemption", held["note"])
        card = shelby_card(shelby_row(), held, date(2027, 4, 14), EXTRACT)
        self.assertEqual((card["route"], card["event"]["status"]), ("removed", "resolved"))
        self.assertIn("has passed", card["route_reason"])

    def test_county_list_outage_is_unknown_not_paid_and_not_eligible(self):
        listed = next_status(None, LISTED, DAY)
        down = next_status(listed, DOWN, date(2026, 11, 23))
        self.assertEqual((down["status"], down["last_known_status"], down["last_confirmed_on"]),
                         ("unknown", "listed", "2026-11-22"))
        self.assertEqual(down["failed_checks"], [{"on": "2026-11-23", "reason": DOWN["reason"]}])
        self.assertNotIn("removed_on", down)
        card = shelby_card(shelby_row(), down, date(2026, 11, 23), EXTRACT)
        self.assertEqual((card["route"], card["event"]["status"]), ("research_hold", "unknown"))
        down2 = next_status(down, DOWN, date(2026, 11, 24))
        self.assertEqual((down2["last_known_status"], len(down2["failed_checks"])), ("listed", 2))
        back = next_status(down2, LISTED, date(2026, 11, 25))
        self.assertEqual(back["status"], "listed")
        self.assertNotIn("last_known_status", back)
        # An outage that hides a postponement is still seen when the list returns.
        moved = next_status(down2, {**LISTED, "sale_date": "2027-06-08"}, date(2026, 11, 25))
        self.assertEqual((moved["status"], moved["prior_sale_date"]), ("postponed", "2027-04-13"))

    def test_repeat_run_writes_nothing_and_manual_card_is_kept(self):
        row = shelby_row()
        status = next_status({"status": "listed", "sale_date": "2027-04-13"}, LISTED, DAY)
        first = patch_for(row["raw_payload"], status, shelby_card(row, status, DAY, EXTRACT))
        self.assertEqual(set(first), {STATUS_KEY, CARD_KEY})
        row["raw_payload"].update(copy.deepcopy(first))
        status2 = next_status(row["raw_payload"][STATUS_KEY], LISTED, DAY)
        self.assertEqual(patch_for(row["raw_payload"], status2, shelby_card(row, status2, DAY, EXTRACT)), {})
        manual = {"manual": True, "route": "removed", "reviewed_by": "patrick"}
        row["raw_payload"][CARD_KEY] = manual
        status3 = next_status(status2, LISTED, date(2026, 11, 23))
        change = patch_for(row["raw_payload"], status3, shelby_card(row, status3, date(2026, 11, 23), EXTRACT))
        self.assertEqual(set(change), {STATUS_KEY})

    def test_holds_for_entity_heirs_receiver_and_missing_prior_year_bill(self):
        status = next_status(None, LISTED, DAY)
        entity = shelby_row(owner_name_records="SOUTHWIND LODGING LLC")
        entity["raw_payload"]["trustee_detail"]["primary_owner"] = "SOUTHWIND LODGING LLC"
        entity["raw_payload"]["publication_evidence"] = {"parties": ["SOUTHWIND LODGING LLC"]}
        card = shelby_card(entity, status, DAY, EXTRACT)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("entity", card["route_reason"])
        self.assertIn("Secretary of State", card["availability"]["verify_next"][0])
        heirs = with_raw(publication_evidence={"parties": ["HERIN JOHN B & TINA", "KNOWN AND UNKNOWN HEIRS OF JOHN HERIN"]})
        card = shelby_card(heirs, status, DAY, EXTRACT)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("deceased", card["route_reason"])
        receiver = with_raw(shelby_receiver_review={"status": "receiver_eligibility_research", "cases": [
            {"warrant": "20400196", "hearing_listed": "2026-11-24"}]})
        card = shelby_card(receiver, status, DAY, EXTRACT)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("next check 2026-11-24", card["availability"]["verify_next"][0])
        current = shelby_row()
        current["raw_payload"]["trustee_detail"]["visible_prior_year_balances"] = {}
        self.assertEqual(shelby_card(current, status, DAY, EXTRACT)["route"], "research_hold")
        copied = shelby_row(property_value_source="xref_avm_enricher")
        del copied["raw_payload"]["trustee_detail"]["estimated_appraisal"]
        card = shelby_card(copied, status, DAY, EXTRACT)
        self.assertEqual(card["route"], "research_hold")
        self.assertIn("not the county assessment of this parcel", card["route_reason"])
        self.assertIn("Copied from another record", card["economics"]["value_basis"])
        small = shelby_row(property_value=40000)
        self.assertFalse(shelby_wants_card(small))
        small["raw_payload"]["shelby_receiver_review"] = {"status": "receiver_eligibility_research", "cases": []}
        self.assertTrue(shelby_wants_card(small))

    def test_duplicate_parcel_in_alternate_format_is_one_tract_and_one_owner_group(self):
        spaced = 'D0222   00514C,D022200000514C,214,MOUNTAIN BROOK,TS2302\n'
        lead, = scheduled_parcels(PAGE, HEADER + spaced * 2, DAY)
        self.assertEqual(staging_row(lead, "a")["id"], staging_row(lead, "b")["id"])
        self.assertEqual(property_key("shelby", "D0222   00514C"), property_key("Shelby County", "D0222 00514C"))
        status = next_status(None, LISTED, DAY)
        one = shelby_card(shelby_row(), status, DAY, EXTRACT)
        other = shelby_row(id="row-2", property_address="216 MOUNTAIN BROOK, Shelby County, TN")
        other["raw_payload"].update(parcel="D0222   00515C", alt_parcel="D022200000515C")
        two = shelby_card(other, status, DAY, EXTRACT)
        groups = dedupe_worklist([one, copy.deepcopy(one), two])
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]["assets"]), 2)


class ShelbyBotTests(unittest.TestCase):
    def client(self, rows):
        client = MagicMock()
        table = client.table.return_value
        (table.select.return_value.eq.return_value.gt.return_value.order.return_value.limit.return_value
         .execute.return_value.data) = rows
        return client

    def test_outage_dates_every_tract_unknown_and_retires_nothing(self):
        row = shelby_row()
        row["raw_payload"][STATUS_KEY] = next_status(None, LISTED, date(2026, 11, 21))
        client = self.client([row])
        bot = ShelbyTaxSaleBot()
        bot.fetch = MagicMock(return_value=None)
        with self.assertRaises(ValueError):
            bot.scrape()
        with patch("src.bots.shelby_tax_sale_bot._supabase", return_value=client):
            self.assertEqual(bot._write_staging([]), (0, 0))
        client.table.return_value.update.assert_not_called()
        client.table.return_value.upsert.assert_not_called()
        (name, args), = [c.args for c in client.rpc.call_args_list]
        self.assertEqual(name, "apply_staging_enrichment")
        written, = args["p_rows"]
        self.assertEqual(written["raw_patch"][STATUS_KEY]["status"], "unknown")
        self.assertEqual(written["raw_patch"][STATUS_KEY]["last_known_status"], "listed")
        self.assertEqual(written["raw_patch"][CARD_KEY]["route"], "research_hold")

    def test_review_pass_is_idempotent_and_marks_a_missing_parcel_removed(self):
        row = shelby_row()
        bot = ShelbyTaxSaleBot()
        current = {"row-1": {"trustee_sale_date": "2027-04-13", "source_url": row["source_url"], "raw_payload": row["raw_payload"]}}
        client = self.client([row])
        first = bot._review_tracts(client, current, DAY)
        self.assertEqual(first[0]["raw_patch"][CARD_KEY]["route"], "first_trace_review")
        row["raw_payload"].update(copy.deepcopy(first[0]["raw_patch"]))
        self.assertEqual(bot._review_tracts(client, current, DAY), [])
        row.update(staging_status="rejected", rejection_reason=REMOVED)
        gone = bot._review_tracts(client, {}, date(2026, 11, 23))
        self.assertEqual(gone[0]["raw_patch"][STATUS_KEY]["removed_on"], "2026-11-23")
        self.assertEqual(gone[0]["raw_patch"][CARD_KEY]["route"], "removed")

    def test_balance_refresh_keeps_the_prior_balance_and_flags_an_unconfirmed_page(self):
        row = shelby_row()
        row["raw_payload"][CARD_KEY] = {"route": "first_trace_review", "reviewed_by": "shelby_tax_sale"}
        current = {"row-1": {"trustee_sale_date": "2027-04-13", "source_url": row["source_url"], "raw_payload": row["raw_payload"]}}
        bot, client = ShelbyTaxSaleBot(), MagicMock()
        page = ('<span>Parcel ID:</span>D0222   00514C<span>Primary Owner</span>HERIN JOHN B &amp; TINA'
                '<span>Total Balance</span>$80,000.00<span>Tax Sale ID:</span>TS2302')
        bot.fetch = MagicMock(return_value=MagicMock(ok=True, text=page))
        self.assertEqual(bot._refresh_details(client, [row], current, date(2026, 11, 29)), ["row-1"])
        detail = row["raw_payload"]["trustee_detail"]
        self.assertEqual((detail["total_tax_balance"], detail["checked_on"]), (80000.0, "2026-11-29"))
        self.assertEqual(detail["previous_total_tax_balance"], {"amount": 90794.06, "as_of": "2026-11-21"})
        self.assertEqual(bot._refresh_details(client, [row], current, date(2026, 11, 30)), [])
        bot.fetch = MagicMock(return_value=MagicMock(ok=True, text=page.replace("TS2302", "")))
        bot._refresh_details(client, [row], current, date(2026, 12, 7))
        self.assertEqual(row["raw_payload"]["trustee_detail_recheck"]["status"], "parcel_or_sale_not_confirmed")
        card = shelby_card(row, next_status(None, LISTED, date(2026, 12, 7)), date(2026, 12, 7), EXTRACT)
        self.assertEqual(card["route"], "research_hold")


def notice(**over):
    base = {"docket": "19076", "sale_date": "2026-12-16", "document_date": "2026-11-16", "sha256": "abc",
            "source_url": cheatham.NOTICE, "tracts": [
                {"letter": "D", "parcel_printed": "018-043.00", "parcel_long": "04-018-018-043.00-000",
                 "parcel_forms_agree": True, "location": "1825 Mosley Ferry Road, Chapmansboro, TN 37035",
                 "owners": "Michelle K. Carroll (aka Michelle Barrett)", "amount": 5363.88},
                {"letter": "B", "parcel_printed": "018-043.01", "parcel_long": "04-018-018-043.01-000",
                 "parcel_forms_agree": True, "location": "Mosley Ferry Road (off), Chapmansboro, TN 37035",
                 "owners": "Michelle K. Barrett (aka Michelle K. Carroll)", "amount": 2463.26},
                {"letter": "E", "parcel_printed": "096J-A-011.00", "parcel_long": "11-096-O -096-J -A -011.00- -000",
                 "parcel_forms_agree": True, "location": "1070 Patterson Drive, Kingston Springs, TN 37082",
                 "owners": "Robin A. Davis", "amount": 9281.80},
            ]}
    base.update(over)
    return base


def cheatham_row(tract, parcel, owner, roll_owner, amount, address, rid=None):
    return {"id": rid or "c-" + tract, "staging_status": "pending", "rejection_reason": None,
            "property_address": address, "owner_name_records": owner, "property_value": 232200,
            "property_value_source": "comptroller", "trustee_sale_date": "2026-12-16",
            "source_url": "https://example.invalid/notice",
            "raw_payload": {"kind": "cheatham_tax_sale", "docket": "19076", "tract": tract, "parcel_printed": parcel,
                            "owners_printed": owner, "published_judgment": amount, "publication_date": "2026-11-20",
                            "assessment_review": {"owner": roll_owner, "parcelid": "011 096J A 01100 000 2027",
                                                  "source_url": "https://comptroller.tn.gov/Cheatham.zip"}}}


DAVIS = ("E", "096J-A-0l 1.00", "Robin A. Davis", "DAVIS ROBIN A", 9281.8, "1070 Patterson Drive, Kingston Springs, TN 37082")


class CheathamTests(unittest.TestCase):
    def test_printed_tract_is_noticed_but_release_is_unknown(self):
        self.assertEqual(fold_parcel("096J-A-0l 1.00"), fold_parcel("096J-A-011.00"))
        self.assertEqual(fold_parcel("0S0A-B-070.00"), fold_parcel("050A-B-070.00"))
        change, = cheatham.review_rows([cheatham_row(*DAVIS)], notice(), DAY)
        self.assertEqual(change["status"]["status"], "noticed")
        card = change["card"]
        self.assertEqual((card["route"], card["event"]["status"], card["economics"]["runway_days"]),
                         ("research_hold", "unknown", 24))
        self.assertIn("(615) 792-4620", card["availability"]["verify_next"][0])
        self.assertIn("Clerk and Master", card["availability"]["verify_next"][0])
        self.assertEqual([(d["kind"], d["amount"], d["as_of"]) for d in card["economics"]["known_debt"]],
                         [("delinquent_tax", 9281.8, "2026-11-16")])
        self.assertEqual(card["property_key"], property_key("cheatham", "011 096J A 01100 000"))
        self.assertEqual(change["columns"], {})
        # Same day, same notice: nothing more to write.
        row = cheatham_row(*DAVIS)
        row["raw_payload"].update(copy.deepcopy(change["raw_patch"]))
        self.assertEqual(cheatham.review_rows([row], notice(), DAY)[0]["raw_patch"], {})

    def test_tract_dropped_from_a_revised_notice_is_removed_and_letter_alone_never_matches(self):
        row = cheatham_row(*DAVIS)
        first, = cheatham.review_rows([row], notice(), DAY)
        row["raw_payload"].update(copy.deepcopy(first["raw_patch"]))
        revised = notice(document_date="2026-11-30", sha256="def")
        revised["tracts"] = revised["tracts"][:2] + [dict(revised["tracts"][0], letter="E")]
        self.assertIsNone(match_tract(row["raw_payload"], revised))
        change, = cheatham.review_rows([row], revised, date(2026, 12, 1))
        self.assertEqual((change["status"]["status"], change["status"]["removed_on"]), ("removed", "2026-12-01"))
        self.assertEqual(change["card"]["route"], "removed")
        self.assertEqual(change["columns"], {"staging_status": "rejected", "rejection_reason": cheatham.REMOVED})

    def test_unreadable_notice_or_a_different_case_is_unknown(self):
        row = cheatham_row(*DAVIS)
        first, = cheatham.review_rows([row], notice(), DAY)
        row["raw_payload"].update(copy.deepcopy(first["raw_patch"]))
        for posted, reason in ((None, "Cheatham County posted sale notice unavailable"), (notice(docket="20001"), None)):
            change, = cheatham.review_rows([row], posted, date(2026, 11, 29), reason)
            self.assertEqual((change["status"]["status"], change["status"]["last_known_status"]), ("unknown", "noticed"))
            self.assertEqual(change["card"]["route"], "research_hold")
            self.assertEqual(change["columns"], {})

    def test_new_sale_date_is_carried_and_a_passed_sale_is_closed(self):
        row = cheatham_row(*DAVIS)
        first, = cheatham.review_rows([row], notice(), DAY)
        row["raw_payload"].update(copy.deepcopy(first["raw_patch"]))
        moved, = cheatham.review_rows([row], notice(sale_date="2027-01-20"), date(2026, 12, 10))
        self.assertEqual(moved["columns"], {"trustee_sale_date": "2027-01-20"})
        self.assertEqual((moved["status"]["sale_date"], moved["status"]["prior_sale_date"]), ("2027-01-20", "2026-12-16"))
        self.assertIn("was 2026-12-16", moved["card"]["event"]["what"])
        for posted in (notice(), notice(docket="20001"), None):
            held, = cheatham.review_rows([row], posted, date(2026, 12, 17), "down")
            if posted is None:
                self.assertEqual(held["status"]["status"], "unknown")
                continue
            self.assertEqual(held["status"]["status"], "sale_held")
            self.assertEqual(held["columns"]["rejection_reason"], cheatham.SALE_PASSED)
            self.assertEqual(held["card"]["route"], "removed")

    def test_one_owner_under_two_names_is_one_group_and_heirs_are_held(self):
        d = cheatham_row("D", "018-043.00", "Michelle K. Carroll (aka Michelle Barrett)", "CARROLL MICHELLE K",
                         5363.88, "1825 Mosley Ferry Road, Chapmansboro, TN 37035")
        b = cheatham_row("B", "018-043.01", "Michelle K. Barrett (aka Michelle K. Carroll)", "BARRETT MICHELLE K",
                         2463.26, None)
        one, two = cheatham.review_rows([d, b], notice(), DAY)
        self.assertEqual(one["card"]["owner_group_key"], two["card"]["owner_group_key"])
        self.assertIn("no street number", two["card"]["route_reason"])
        heirs = cheatham_row("E", "096J-A-011.00", "Heirs of Robin A. Davis", "DAVIS ROBIN A", 9281.8, DAVIS[5])
        estate = notice()
        estate["tracts"][2]["owners"] = "Heirs of Robin A. Davis"
        card = cheatham.review_rows([heirs], estate, DAY)[0]["card"]
        self.assertIn("deceased", card["route_reason"])
        joint = cheatham_row("H", "049N-H-004.0l", "Roger and Banessa (aka Banesha) Hamlin", "HAMLIN ROGER & BANESA HAMLIN",
                             2991.36, "222 Brook Hollow Drive, Ashland City, TN 37015")
        posted = notice()
        posted["tracts"].append({"letter": "H", "parcel_printed": "049N-H-004.01", "parcel_long": "x004.01x",
                                 "parcel_forms_agree": True, "location": joint["property_address"],
                                 "owners": joint["owner_name_records"], "amount": 2991.36})
        self.assertIn("p:hamlin r", cheatham.review_rows([joint], posted, DAY)[0]["card"]["owner_group_key"].lower())

    def test_notice_text_is_parsed_and_a_changed_layout_raises(self):
        text = ("IN THE CHANCERY COURT FOR CHEATHAM COUNTY, TENNESSEE\nNOTICE OF DELINQUENT TAX SALE\n"
                "Civil Action No. 19076, notice is hereby given that I will sell for cash to\nthe highest and best bidder at\n"
                "10:00 a.m. on Wednesday, December 16, 2026, in the\nHistoric Chancery Courtroom\n"
                "the following described tracts of land, subject to the equity of redemption\n"
                "A. Keisha Marie Anderson Property – Parcel 011-022.03 (04-011- -011- - -022.03- -000),\n"
                "having an address of 1360 John Ashton Road, Pleasant View 37146, Current\n"
                "Owners – Defendant Keisha Marie Anderson – $4243.96\n"
                "B. Jeffrey McPherson Parcel 090-070.02 (10-090- -090- - -007.02- -000), having an\n"
                "address of 1000 Dogwood Lane, Kingston Springs, TN 37082– Current Owners –\n"
                "Jeffrey A. McPherson - $15,044.90\n"
                "Penalties, interest, and costs accruing from October 6, 2026\n")

        def opened(body):
            doc = MagicMock()
            doc.__enter__.return_value = doc
            doc.metadata = {"ModDate": "D:20261116094656-06'00'"}
            doc.pages = [MagicMock(extract_text=MagicMock(return_value=body))]
            return doc
        with patch.object(cheatham.pdfplumber, "open", return_value=opened(text)):
            read = cheatham.read_notice(b"%PDF")
        self.assertEqual((read["docket"], read["sale_date"], read["document_date"]), ("19076", "2026-12-16", "2026-11-16"))
        a, b = read["tracts"]
        self.assertEqual((a["parcel_printed"], a["owners"], a["amount"], a["location"]),
                         ("011-022.03", "Keisha Marie Anderson", 4243.96, "1360 John Ashton Road, Pleasant View 37146"))
        self.assertEqual((b["owners"], b["amount"], b["parcel_forms_agree"]), ("Jeffrey A. McPherson", 15044.90, False))
        for broken in (text.replace("Civil Action No. 19076", ""), text.replace("$4243.96", "pending"),
                       text.replace("described tracts", "listed land")):
            with patch.object(cheatham.pdfplumber, "open", return_value=opened(broken)):
                with self.assertRaises(ValueError):
                    cheatham.read_notice(b"%PDF")

    def test_run_reports_failure_and_writes_unknown_when_the_notice_is_down(self):
        row = cheatham_row(*DAVIS)
        client = MagicMock()
        bot = cheatham.CheathamTaxSaleBot()
        bot.fetch = MagicMock(return_value=None)
        bot.tracked = MagicMock(return_value=[row])
        bot._report_health = MagicMock()
        with patch("src.bots.cheatham_tax_sale_bot._supabase", return_value=client):
            result = bot.run(day=DAY)
        self.assertEqual(result["status"], "failed")
        written, = client.rpc.call_args.args[1]["p_rows"]
        self.assertEqual(written["raw_patch"][STATUS_KEY]["status"], "unknown")
        client.table.return_value.update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
