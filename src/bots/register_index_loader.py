"""
Load a county Register of Deeds index reply into staging. Run by hand when
a records request reply lands; not on the schedule.

    python -m src.bots.register_index_loader --county Putnam --layout recording_listing \
        --received 2026-11-23 --file reply.pdf            # preview, writes nothing
    python -m src.bots.register_index_loader ... --write   # stage

The preview parses the file, prints every row it would stage and counts how
many are already in staging. --write stages through the normal bot path:
a row already staged under the same key is a duplicate, so loading the same
reply (or an updated list that overlaps it) twice inserts nothing twice.

Rows land under bot_source <county>_register_index. Appointments of
substitute trustee are staged as SUBSTITUTION_OF_TRUSTEE with borrower,
lender and trustee from the listing; lis pendens as LIS_PENDENS with the
owner not established. preforeclosure_review writes their review cards on
its next run. See _register_index.py for the shape and how to add a layout.

The reply file is untrusted input: it is parsed for text only.

Distress type: SUBSTITUTION_OF_TRUSTEE, LIS_PENDENS.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload, _supabase
from ._register_index import LAYOUTS, Listing, build_leads, canonical_county, parse_file, source_name


class RegisterIndexLoader(BotBase):
    description = "County Register listing of substitute trustee and lis pendens recordings, by records request"
    expected_min_yield = 0
    frozen_after_runs = 10_000           # a reply is loaded once or twice, never refreshed daily

    def __init__(self, county: str, layout: str, path: str, received: str, sender: Optional[str] = None):
        self.county = canonical_county(county)
        self.name = source_name(self.county)
        self.layout, self.path, self.received, self.sender = layout, path, received, sender
        self.listing: Optional[Listing] = None
        super().__init__()

    def scrape(self) -> List[LeadPayload]:
        self.listing = parse_file(self.layout, self.path)
        return [LeadPayload(**fields) for fields in build_leads(self.listing, self.county, self.received, self.sender)]

    def preview(self, client=None) -> Dict[str, Any]:
        """Parse and compare with staging. Reads only."""
        leads = self.scrape()
        staged: Optional[set] = None
        if client is not None and leads:
            staged = set()
            keys = [lead.pipeline_lead_key for lead in leads]
            for offset in range(0, len(keys), 200):
                rows = (client.table("homeowner_requests_staging").select("pipeline_lead_key")
                        .eq("bot_source", self.name).in_("pipeline_lead_key", keys[offset:offset + 200]).execute().data) or []
                staged.update(r["pipeline_lead_key"] for r in rows)
        kinds: Dict[str, int] = {}
        for entry in self.listing.entries:
            kinds[entry.kind] = kinds.get(entry.kind, 0) + 1
        return {"source": self.name, "listing": self.listing.title, "entries": len(self.listing.entries),
                "by_kind": kinds, "not_staged_types": sorted({e.doc_type for e in self.listing.entries if e.kind == "other"}),
                "unplaced_lines": self.listing.skipped, "leads": leads,
                "already_staged": None if staged is None else sum(1 for lead in leads if lead.pipeline_lead_key in staged),
                "would_insert": None if staged is None else sum(1 for lead in leads if lead.pipeline_lead_key not in staged)}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Stage a county Register of Deeds index reply (preview unless --write)")
    ap.add_argument("--county", required=True, help="Tennessee county, e.g. Putnam")
    ap.add_argument("--layout", required=True, choices=sorted(LAYOUTS),
                    help="; ".join(f"{name}: {text}" for name, (text, _) in sorted(LAYOUTS.items())))
    ap.add_argument("--file", required=True, help="the reply as received (PDF, spreadsheet or CSV, per layout)")
    ap.add_argument("--received", required=True, help="day the reply arrived, YYYY-MM-DD")
    ap.add_argument("--from", dest="sender", help="who sent it, e.g. 'Deputy Clerk, Putnam County Register of Deeds (email)'")
    ap.add_argument("--write", action="store_true", help="stage the rows (default: preview only)")
    args = ap.parse_args(argv)

    loader = RegisterIndexLoader(args.county, args.layout, args.file, args.received, args.sender)
    if args.write:
        result = loader.run()
        print({k: v for k, v in result.items() if k != "error"}, "error:", bool(result.get("error")))
        return 1 if result.get("error") else 0
    plan = loader.preview(_supabase())
    for lead in plan["leads"]:
        print(f"{lead.distress_type} | {lead.raw_payload['book_page']} | {lead.raw_payload['recorded']} | "
              f"borrower: {lead.owner_name_records or '-'} | property: {lead.property_address or '-'}")
    print({k: v for k, v in plan.items() if k != "leads"})
    print("Preview only. Add --write to stage.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
