"""
Statewide pilot worklist: one row per owner group from the shared review cards.

Reads every staging row that carries raw_payload.pilot_review, keeps the
first-trace recommendations, collapses them to one row per owner group with
each separately identified property under it (a property reached by two lanes
counts once), and compares the result with an earlier shortlist so the report
can say which groups are new, which were already known and which dropped out.

    python -m src.bots.pilot_worklist OUT_DIR [BASELINE.json]

Writes OUT_DIR/pilot-worklist.json, .csv and .xlsx. Read-only against the
database. BASELINE.json is the Nov 22 owner shortlist format: a list of
{owner_names_as_sourced, county, qualification, assets:[{parcel_id,
asset_address, staging_id}]}.

Distress type: N/A (report only).
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._pilot_review import ROUTES, dedupe_worklist, owner_group_key, property_key, rekey, runway_days

CRM = "https://falco.llc/crm/leads/"
LANE_LABEL = {"pre_foreclosure": "Pre-foreclosure", "tax": "Delinquent tax", "cre": "Commercial",
              "probate": "Probate", "land": "Land"}
COLUMNS = ["order", "owner_group", "signer_path", "lanes", "county", "asset", "parcel", "why_now", "event_checked",
           "deadline", "days_left", "value", "value_basis", "known_debt", "missing_evidence",
           "first_call_question", "vs_baseline", "crm_link", "event_source", "other_records"]


def _src_url(s: Any) -> str:
    return (s or {}).get("url") or "" if isinstance(s, dict) else ""


def _debt_text(debts: Iterable[Dict[str, Any]]) -> str:
    label = {"delinquent_tax": "delinquent taxes", "mortgage_original": "original loan (not a payoff)",
             "mortgage_estimate": "estimated loan balance", "judgment": "judgment", "lien": "lien", "other": "other"}
    out = []
    for d in debts or []:
        amount = f"${d['amount']:,.0f}" if d.get("amount") is not None else "amount unknown"
        as_of = f" as of {d['as_of']}" if d.get("as_of") else ""
        out.append(f"{label.get(d.get('kind'), 'other')}: {amount}{as_of}")
    return "; ".join(out) or "none recorded"


def baseline_keys(baseline: List[Dict[str, Any]], qualification: str = "candidate_for_first_trace_review"
                  ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Index an earlier shortlist three ways: by owner key, by property key and
    by staging id. A row matches the baseline if any of the three agree, so a
    name spelled differently or a missing parcel does not create a false "new"."""
    by_owner: Dict[str, Dict[str, Any]] = {}
    by_prop: Dict[str, Dict[str, Any]] = {}
    by_id: Dict[str, Dict[str, Any]] = {}
    for row in baseline or []:
        if row.get("qualification") != qualification:
            continue
        ok = owner_group_key(row.get("owner_names_as_sourced") or row.get("owner_name"), row.get("county"))
        if ok:
            by_owner[ok] = row
        for a in row.get("assets") or []:
            pk = property_key(a.get("county") or row.get("county"), a.get("parcel_id"), a.get("asset_address"))
            if pk:
                by_prop[pk] = row
            if a.get("staging_id"):
                by_id[a["staging_id"]] = row
    return by_owner, by_prop, by_id


def build_worklist(rows: List[Dict[str, Any]], baseline: Optional[List[Dict[str, Any]]] = None,
                   today: Optional[date] = None) -> Dict[str, Any]:
    """rows: [{"id", "county", "pilot_review"}]. Returns the ordered worklist,
    per-lane and per-route counts, and the baseline comparison."""
    today = today or date.today()
    cards = []
    route_counts: Dict[str, Dict[str, int]] = {}
    for r in rows:
        card = r.get("pilot_review")
        if not isinstance(card, dict) or card.get("version") != "pilot-review-1":
            continue
        card = {**rekey(card, r.get("county")), "_id": r["id"], "_county": r.get("county")}
        lane = route_counts.setdefault(card.get("lane", "?"), {})
        lane[card.get("route", "?")] = lane.get(card.get("route", "?"), 0) + 1
        cards.append(card)

    by_owner, by_prop, by_id = baseline_keys(baseline or [])
    seen_baseline: set = set()
    groups = dedupe_worklist(cards)

    def soonest(g: Dict[str, Any]) -> Tuple[int, int]:
        days = [runway_days(c["economics"].get("deadline"), today) for a in g["assets"] for c in a["cards"]]
        days = [d for d in days if d is not None]
        return (0, min(days)) if days else (1, 0)

    groups.sort(key=soonest)
    out_rows: List[Dict[str, Any]] = []
    new_groups = strengthened = 0
    for n, g in enumerate(groups, 1):
        matched = None
        if g["owner_group_key"] in by_owner:
            matched = by_owner[g["owner_group_key"]]
        for a in g["assets"]:
            if a["property_key"] in by_prop:
                matched = matched or by_prop[a["property_key"]]
            for c in a["cards"]:
                if c["_id"] in by_id:
                    matched = matched or by_id[c["_id"]]
        if matched is not None:
            seen_baseline.add(id(matched))
            strengthened += 1
        else:
            new_groups += 1
        for a in g["assets"]:
            c = a["cards"][0]
            eco, av, dec, ev, ast = c["economics"], c["availability"], c["decider"], c["event"], c["asset"]
            out_rows.append({
                "order": n,
                "owner_group": dec.get("name") or "Decision-maker not identified",
                "signer_path": f"{dec.get('role') or 'unknown role'}{'' if dec.get('verified') else ' (not verified)'}: {dec.get('basis') or ''}".strip(),
                "lanes": " + ".join(dict.fromkeys(LANE_LABEL.get(x["lane"], x["lane"]) for x in a["cards"])),
                "county": c.get("_county") or "",
                "asset": ast.get("address") or ast.get("description") or "",
                "parcel": ast.get("parcel") or "",
                "why_now": ev.get("what") or "",
                "event_checked": ev.get("checked_on") or "",
                "deadline": eco.get("deadline") or "unknown",
                "days_left": runway_days(eco.get("deadline"), today),
                "value": eco.get("value"),
                "value_basis": ("Estimate: " if eco.get("value_is_estimate") else "") + (eco.get("value_basis") or ""),
                "known_debt": _debt_text(eco.get("known_debt")),
                "missing_evidence": "; ".join([*(eco.get("unknowns") or []), *(av.get("verify_next") or [])]),
                "first_call_question": av.get("first_call_question") or "",
                "vs_baseline": "already on the Nov 22 list" if matched is not None else "new since Nov 22",
                "crm_link": CRM + c["_id"],
                "event_source": _src_url(ev.get("source")),
                "other_records": " ".join(CRM + x["_id"] for x in a["cards"][1:]),
            })

    dropped = []
    for row in (baseline or []):
        if row.get("qualification") == "candidate_for_first_trace_review" and id(row) not in seen_baseline:
            dropped.append({"owner": row.get("owner_name"), "county": row.get("county"),
                            "opportunity_type": row.get("opportunity_type"),
                            "staging_ids": [a.get("staging_id") for a in row.get("assets") or []]})
    return {
        "built_on": today.isoformat(),
        "owner_groups": len(groups),
        "properties": sum(len(g["assets"]) for g in groups),
        "first_trace_cards": sum(len(a["cards"]) for g in groups for a in g["assets"]),
        "cards_by_lane_and_route": route_counts,
        "new_owner_groups_vs_baseline": new_groups,
        "baseline_groups_still_qualified": strengthened,
        "baseline_groups_not_on_worklist": dropped,
        "rows": out_rows,
    }


def fetch_cards(client) -> List[Dict[str, Any]]:
    """Every staging row with a pilot card. Read one route at a time so the
    query uses the partial index on the card route (a bare "has a card" filter
    scans all of staging and hits the API timeout)."""
    rows: List[Dict[str, Any]] = []
    for route in ROUTES:
        start = 0
        while True:
            page = (client.table("homeowner_requests_staging")
                    .select("id,county,pilot_review:raw_payload->pilot_review")
                    .eq("raw_payload->pilot_review->>route", route)
                    .order("id").range(start, start + 199).execute().data) or []
            rows.extend(page)
            if len(page) < 200:
                break
            start += 200
    return rows


def write_outputs(result: Dict[str, Any], out_dir: Path) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = [out_dir / "pilot-worklist.json", out_dir / "pilot-worklist.csv"]
    paths[0].write_text(json.dumps(result, indent=1), encoding="utf-8")
    with paths[1].open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(result["rows"])
    try:
        from openpyxl import Workbook
    except ImportError:
        return paths
    wb = Workbook()
    ws = wb.active
    ws.title = "First trace review"
    ws.append([c.replace("_", " ").title() for c in COLUMNS])
    for r in result["rows"]:
        ws.append([r.get(c) for c in COLUMNS])
    ws.freeze_panes = "C2"
    summary = wb.create_sheet("Summary")
    summary.append(["Built on", result["built_on"]])
    summary.append(["Owner groups", result["owner_groups"]])
    summary.append(["Properties", result["properties"]])
    summary.append(["New owner groups since baseline", result["new_owner_groups_vs_baseline"]])
    summary.append(["Baseline groups still qualified", result["baseline_groups_still_qualified"]])
    summary.append(["Baseline groups not on this worklist", len(result["baseline_groups_not_on_worklist"])])
    summary.append([])
    summary.append(["Lane", "Route", "Cards"])
    for lane, routes in sorted(result["cards_by_lane_and_route"].items()):
        for route, n in sorted(routes.items()):
            summary.append([LANE_LABEL.get(lane, lane), route.replace("_", " "), n])
    paths.append(out_dir / "pilot-worklist.xlsx")
    wb.save(paths[-1])
    return paths


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    from ._base import _supabase
    client = _supabase()
    if client is None:
        print("No database credentials.", file=sys.stderr)
        return 1
    baseline = json.loads(Path(argv[2]).read_text(encoding="utf-8")) if len(argv) > 2 else None
    result = build_worklist(fetch_cards(client), baseline)
    for p in write_outputs(result, Path(argv[1])):
        print(p)
    print(f"{result['owner_groups']} owner groups, {result['properties']} properties; "
          f"{result['new_owner_groups_vs_baseline']} new, {result['baseline_groups_still_qualified']} already known, "
          f"{len(result['baseline_groups_not_on_worklist'])} baseline groups not on this list")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
