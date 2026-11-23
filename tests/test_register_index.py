"""County Register index loader: the Recording Listing layout, the party
split, lead keys and repeat loads. No network; names are made up."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test")

from reportlab.pdfgen.canvas import Canvas  # noqa: E402

from src.bots import _base  # noqa: E402
from src.bots._register_index import (IndexEntry, Listing, build_leads, canonical_county, classify, is_institution,  # noqa: E402
                                      lead_key, parse_file, parse_recording_listing, register_sources, source_name,
                                      split_parties)
from src.bots.register_index_loader import RegisterIndexLoader  # noqa: E402

GRANTOR_X, MARKER_X, GRANTEE_X = 373.0, 534.9, 571.0


def run(page, top, x0, text, width=None):
    return {"page": page, "top": top, "x0": x0, "x1": x0 + (width if width is not None else 4.5 * len(text)), "text": text}


def header(page):
    return [run(page, 37, 317, "Recording Listing by Date and Time"), run(page, 37, 36, "Printed: 11/23/2026 9:19a"),
            run(page, 48, 368.3, "Putnam County"), run(page, 57, 363.8, "Register of Deeds"),
            run(page, 68, 354, "Jane Sample, Register"), run(page, 77, 318, "Recording Date: 10/ 1/2026 thru 11/20/2026"),
            run(page, 104, 36, "Class"), run(page, 104, 99, "Reference"), run(page, 104, 612, "Instrument Type")]


def head(page, top, ref, recorded, time, code, dated, doc_type):
    return [run(page, top, 36, "RB"), run(page, top, 99, ref), run(page, top, 185, recorded), run(page, top, 235, time),
            run(page, top, 261, code), run(page, top, 320, dated), run(page, top, 383, "3"), run(page, top, 421, "$0.00"),
            run(page, top, 612, doc_type)]


def names(page, top, grantors=(), grantees=()):
    out = [run(page, top, GRANTOR_X, "GRANTOR NAMES AND ADDRESS"), run(page, top, 539.3, "AP"),
           run(page, top - 0.2, GRANTEE_X, "GRANTEE NAMES AND ADDRESS"), run(page, top - 0.2, 733, "AP")]
    for n in range(max(len(grantors), len(grantees))):
        y = top + 10 + 9 * n
        if n < len(grantors):
            out += [run(page, y, GRANTOR_X, grantors[n]), run(page, y, MARKER_X, "O", 5.4)]
        if n < len(grantees):
            out += [run(page, y - 0.2, GRANTEE_X, grantees[n])]
    return out


def sample_runs():
    runs = header(1)
    runs += head(1, 119, "1736-282", "10/6/2026", "1:11p", "SUBTR", "9/29/2026", "Substitute of Trustee")
    runs += names(1, 129, ["DOE JANE Q", "ACME HOME LENDING LLC"], ["RECON TRUSTEE CO PLLC"])
    runs += head(1, 170, "1736-314", "10/7/2026", "8:07a", "APTR", "9/21/2026", "Appoint Trustee/Sub Trustee")
    runs += names(1, 180, ["FIRST SAMPLE BANK", "ROE RICHARD ALLEN", "ROE MARY BETH"], ["CLEAR SAMPLE LLC"])
    runs += head(1, 230, "1739-269", "10/20/2026", "3:03p", "LLP", "10/20/2026", "Lien Lis Pendens")
    runs += names(1, 240, ["POE ANNA", "POE BELLA"], ["LIEN LIS PENDENS"])
    runs += [run(1, 270, 402, "Property>"), run(1, 270, 432, "120 SAMPLE PLACE; Phase: IV; Lot: 81")]
    runs += head(1, 300, "1740-1", "10/21/2026", "9:00a", "WD", "10/20/2026", "Warranty Deed")
    runs += names(1, 310, ["SELLER SAM"], ["BUYER BOB"])
    # An entry that runs onto the next page; the page header sits near the grantor column.
    runs += head(1, 700, "1744-284", "11/10/2026", "10:28a", "LLP", "11/10/2026", "Lien Lis Pendens")
    runs += names(1, 710, ["GRAY PAT"], ["SAMPLE HOLDINGS LLC"])
    runs += header(2)
    runs += [run(2, 119, GRANTEE_X, "SAMPLE RENTALS LLC"), run(2, 119, 733, "O", 5.4),
             run(2, 128, GRANTEE_X, "BROWN SAM"), run(2, 137, 402, "Property> 1794 EAST SAMPLE STREET; Acres: 2.48")]
    # A long lender name printed into the marker column.
    runs += head(2, 170, "1746-279", "11/19/2026", "3:28p", "SUBTR", "11/19/2026", "Substitute of Trustee")
    runs += names(2, 180, ["LANE TOM"], ["SAMPLE CREDIT UNION"])
    runs += [run(2, 199, GRANTOR_X, "SAMPLE HUMAN FEDERAL CREDIT UNION AKAO", MARKER_X + 5.4 - GRANTOR_X)]
    return runs


class LayoutTests(unittest.TestCase):
    def setUp(self):
        self.listing = parse_recording_listing(sample_runs())
        self.by_ref = {e.reference: e for e in self.listing.entries}

    def test_header_and_entries(self):
        self.assertEqual(self.listing.title,
                         "Recording Listing by Date and Time, 2026-10-01 thru 2026-11-20, printed 2026-11-23 9:19a")
        self.assertEqual(self.listing.county, "Putnam")
        self.assertEqual(len(self.listing.entries), 6)
        self.assertEqual(self.listing.skipped, [])
        entry = self.by_ref["RB 1736-282"]
        self.assertEqual((entry.recorded, entry.instrument_date, entry.code, entry.doc_type, entry.kind),
                         ("2026-10-06", "2026-09-29", "SUBTR", "Substitute of Trustee", "appointment"))
        self.assertEqual(entry.first_parties, ["DOE JANE Q", "ACME HOME LENDING LLC"])
        self.assertEqual(entry.second_parties, ["RECON TRUSTEE CO PLLC"])

    def test_columns_are_split_by_position_and_the_placeholder_is_dropped(self):
        lis = self.by_ref["RB 1739-269"]
        self.assertEqual((lis.kind, lis.first_parties, lis.second_parties), ("lis_pendens", ["POE ANNA", "POE BELLA"], []))
        self.assertEqual(lis.property_ref, "120 SAMPLE PLACE; Phase: IV; Lot: 81")

    def test_entry_continues_across_a_page_break_without_the_page_header(self):
        lis = self.by_ref["RB 1744-284"]
        self.assertEqual(lis.first_parties, ["GRAY PAT"])
        self.assertEqual(lis.second_parties, ["SAMPLE HOLDINGS LLC", "SAMPLE RENTALS LLC", "BROWN SAM"])
        self.assertEqual(lis.property_ref, "1794 EAST SAMPLE STREET; Acres: 2.48")

    def test_marker_letter_printed_into_a_name_is_removed(self):
        self.assertEqual(self.by_ref["RB 1746-279"].first_parties, ["LANE TOM", "SAMPLE HUMAN FEDERAL CREDIT UNION AKA"])

    def test_other_instrument_types_are_parsed_but_not_staged(self):
        self.assertEqual(self.by_ref["RB 1740-1"].kind, "other")
        leads = build_leads(self.listing, "Putnam", "2026-11-23")
        self.assertEqual(len(leads), 5)
        self.assertNotIn(lead_key("Putnam", "RB 1740-1"), {x["pipeline_lead_key"] for x in leads})

    def test_pdf_is_read_through_the_same_parser(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "listing.pdf")
            canvas = Canvas(path, pagesize=(792, 612))
            canvas.setFont("Helvetica", 8)
            for item in sample_runs():
                if item["page"] == 1 and item["top"] < 280:
                    canvas.drawString(item["x0"], 612 - item["top"], item["text"])
            canvas.save()
            listing = parse_file("recording_listing", path)
        self.assertEqual([e.reference for e in listing.entries], ["RB 1736-282", "RB 1736-314", "RB 1739-269"])
        self.assertEqual(listing.entries[1].first_parties, ["FIRST SAMPLE BANK", "ROE RICHARD ALLEN", "ROE MARY BETH"])
        self.assertEqual(listing.entries[1].second_parties, ["CLEAR SAMPLE LLC"])
        self.assertEqual(listing.entries[2].property_ref, "120 SAMPLE PLACE; Phase: IV; Lot: 81")
        with self.assertRaises(ValueError):
            parse_file("no_such_layout", "x")


class PartyTests(unittest.TestCase):
    def test_lender_is_never_a_borrower_wherever_it_is_listed(self):
        for grantors in (["FIRST SAMPLE BANK", "ROE RICHARD ALLEN"], ["ROE RICHARD ALLEN", "FIRST SAMPLE BANK"]):
            parties = split_parties(IndexEntry("RB 1-1", "2026-10-01", "Substitute of Trustee", grantors, ["CLEAR SAMPLE LLC"]))
            self.assertEqual((parties["borrowers"], parties["lenders"], parties["trustees"]),
                             (["ROE RICHARD ALLEN"], ["FIRST SAMPLE BANK"], ["CLEAR SAMPLE LLC"]))
            self.assertIn("keyword", parties["basis"])

    def test_institution_words(self):
        for name in ("WELLS FARGO", "US BANK TRUST", "NEWREZ LLC", "SAMPLE FEDERAL CREDIT UNION AKA", "CU FACTORY BUILT LENDING LP",
                     "SECRETARY OF HOUSING AND URBAN DEVELOPMENT", "TENNESSEE HOUSING DEVELOPMENT AGENCY", "SMITH FAMILY TRUST",
                     "21ST MORTGAGE", "MERS", ""):
            self.assertTrue(is_institution(name), name)
        for name in ("MARCUM ALTON R", "SHORES MARY NELL", "DE LA CRUZ MARIA", "BANKS JOHN"):
            self.assertFalse(is_institution(name), name)

    def test_lender_without_a_borrower_stages_no_name(self):
        listing = Listing([IndexEntry("RB 2-2", "2026-10-01", "Substitute of Trustee", ["WELLS FARGO", "ACME PROPERTIES LLC"],
                                      ["WELLS FARGO"])], "Listing", "recording_listing")
        lead = build_leads(listing, "Putnam", "2026-11-23")[0]
        self.assertIsNone(lead["full_name"])
        self.assertIsNone(lead["owner_name_records"])
        self.assertEqual(lead["raw_payload"]["borrowers"], [])

    def test_a_grantee_repeated_as_grantor_is_not_a_borrower(self):
        parties = split_parties(IndexEntry("RB 3-3", "2026-10-01", "Substitute of Trustee", ["SMITH JOHN", "DOE JANE"], ["SMITH JOHN"]))
        self.assertEqual(parties["borrowers"], ["DOE JANE"])

    def test_classify(self):
        self.assertEqual(classify("Appoint Trustee/Sub Trustee", "APTR"), "appointment")
        self.assertEqual(classify("SUBSTITUTION OF TRUSTEE"), "appointment")
        self.assertEqual(classify("Lien Lis Pendens", "LLP"), "lis_pendens")
        self.assertEqual(classify("Trust Deed", "TD"), "other")
        self.assertEqual(classify("Release of Lis Pendens"), "lis_pendens")   # staged and held, never dropped silently


class LeadTests(unittest.TestCase):
    def setUp(self):
        self.leads = {x["raw_payload"]["book_page"]: x
                      for x in build_leads(parse_recording_listing(sample_runs()), "Putnam", "2026-11-23", "Deputy Clerk (email)")}

    def test_keys_match_the_first_putnam_load(self):
        # Key of the row staged in production on 2026-11-23 for Putnam RB 1736-282.
        self.assertEqual(lead_key("Putnam", "RB 1736-282"), "52f501c9c2f49a9d990924077c45a4f162ced9f9")
        self.assertEqual(lead_key("putnam county", "RB 1739-269"), "e244457b97754b52bcedd5865fffcb52d5ffafb8")
        self.assertEqual(source_name("Putnam"), "putnam_register_index")
        self.assertEqual(source_name("van buren"), "van_buren_register_index")
        self.assertIn("dekalb_register_index", register_sources())
        self.assertEqual(len(register_sources()), 95)

    def test_appointment_shape(self):
        lead = self.leads["RB 1736-314"]
        raw = lead["raw_payload"]
        self.assertEqual((lead["bot_source"], lead["county"], lead["distress_type"]),
                         ("putnam_register_index", "Putnam", "SUBSTITUTION_OF_TRUSTEE"))
        self.assertEqual((lead["full_name"], lead["owner_name_records"]), ("ROE RICHARD ALLEN", "ROE RICHARD ALLEN; ROE MARY BETH"))
        self.assertIsNone(lead["property_address"])
        self.assertEqual((raw["lenders"], raw["substitute_trustees"], raw["recorded"], raw["appointment_date"], raw["code"]),
                         (["FIRST SAMPLE BANK"], ["CLEAR SAMPLE LLC"], "2026-10-07", "2026-09-21", "APTR"))
        self.assertEqual(raw["evidence"], {
            "kind": "records_request_reply", "office": "Putnam County Register of Deeds", "received": "2026-11-23",
            "listing": "Recording Listing by Date and Time, 2026-10-01 thru 2026-11-20, printed 2026-11-23 9:19a",
            "from": "Deputy Clerk (email)"})
        self.assertNotIn("FIRST SAMPLE BANK", lead["owner_name_records"])

    def test_lis_pendens_has_no_owner(self):
        lead = self.leads["RB 1744-284"]
        self.assertEqual(lead["distress_type"], "LIS_PENDENS")
        self.assertEqual(lead["property_address"], "1794 East Sample Street, Putnam County, TN")
        self.assertNotIn("full_name", lead)
        self.assertNotIn("owner_name_records", lead)
        self.assertTrue(lead["raw_payload"]["owner_not_established"])
        self.assertEqual(lead["raw_payload"]["first_parties"], ["GRAY PAT"])
        self.assertIn("does not say who sued whom", lead["admin_notes"])

    def test_wrong_county_or_date_is_refused(self):
        listing = parse_recording_listing(sample_runs())
        with self.assertRaises(ValueError):
            build_leads(listing, "Overton", "2026-11-23")          # the listing is headed Putnam County
        with self.assertRaises(ValueError):
            build_leads(listing, "Putnam", "11/23/2026")
        with self.assertRaises(ValueError):
            canonical_county("Putnum")


class StagingTable:
    """The slice of the Supabase client _write_staging uses, in memory."""

    def __init__(self):
        self.rows = []

    def table(self, name):
        return Query(self, name)


class Query:
    def __init__(self, db, name):
        self.db, self.name, self.filters, self.op, self.payload = db, name, [], "select", None

    def select(self, *_):
        return self

    def insert(self, payload):
        self.op, self.payload = "insert", payload
        return self

    def update(self, payload):
        self.op, self.payload = "update", payload
        return self

    def eq(self, key, value):
        self.filters.append(lambda r, k=key, v=value: r.get(k) == v)
        return self

    def in_(self, key, values):
        self.filters.append(lambda r, k=key, v=set(values): r.get(k) in v)
        return self

    def is_(self, key, _):
        self.filters.append(lambda r, k=key: r.get(k) is None)
        return self

    def filter(self, *_):
        return self

    def execute(self):
        if self.name != "homeowner_requests_staging":
            return type("R", (), {"data": []})()
        if self.op == "insert":
            new = self.payload if isinstance(self.payload, list) else [self.payload]
            self.db.rows.extend({"id": f"row{len(self.db.rows) + n}", **row} for n, row in enumerate(new))
            return type("R", (), {"data": new})()
        hits = [r for r in self.db.rows if all(f(r) for f in self.filters)]
        if self.op == "update":
            for r in hits:
                r.update(self.payload)
        return type("R", (), {"data": hits})()


class RepeatLoadTests(unittest.TestCase):
    def test_second_load_is_all_duplicates(self):
        db = StagingTable()
        listing = parse_recording_listing(sample_runs())
        with patch.object(_base, "_supabase", return_value=db), \
                patch("src.bots.register_index_loader.parse_file", return_value=listing):
            loader = RegisterIndexLoader("Putnam", "recording_listing", "reply.pdf", "2026-11-23")
            self.assertEqual(loader.name, "putnam_register_index")
            before = loader.preview(db)
            self.assertEqual((before["would_insert"], before["already_staged"]), (5, 0))
            self.assertEqual(before["not_staged_types"], ["Warranty Deed"])
            self.assertEqual(db.rows, [])                                  # a preview writes nothing
            self.assertEqual(loader._write_staging(loader.scrape()), (5, 0))
            again = RegisterIndexLoader("Putnam", "recording_listing", "reply.pdf", "2026-11-23")
            self.assertEqual(again._write_staging(again.scrape()), (0, 5))
            after = again.preview(db)
            self.assertEqual((after["would_insert"], after["already_staged"]), (0, 5))
        self.assertEqual(len(db.rows), 5)
        self.assertEqual({r["bot_source"] for r in db.rows}, {"putnam_register_index"})


if __name__ == "__main__":
    unittest.main()
