import json
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.packaging.data_quality import assess_packet_data
from src.settings import get_dts_window


QUERY = """
WITH latest AS (
  SELECT
    lead_key,
    avm_value,
    avm_low,
    avm_high,
    status,
    enriched_at,
    attom_raw_json,
    ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY enriched_at DESC) AS rn
  FROM attom_enrichments
)
SELECT
  l.lead_key,
  l.address,
  l.county,
  l.state,
  l.distress_type,
  l.falco_score_internal,
  l.auction_readiness,
  l.equity_band,
  l.dts_days,
  l.uw_ready,
  le.avm_value,
  le.avm_low,
  le.avm_high,
  le.status AS attom_status,
  le.attom_raw_json
FROM leads l
LEFT JOIN latest le
  ON le.lead_key = l.lead_key AND le.rn = 1
WHERE l.dts_days IS NOT NULL
  AND l.dts_days BETWEEN ? AND ?
ORDER BY l.dts_days ASC, l.lead_key ASC
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main() -> int:
    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    dts_min, dts_max = get_dts_window("PACKET_QUALITY_REPORT")
    limit = int(os.environ.get("FALCO_PACKET_QUALITY_REPORT_LIMIT", "50"))

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(QUERY, (dts_min, dts_max)).fetchall()
    con.close()

    leads: List[Dict[str, Any]] = []
    blocker_counts: Counter[str] = Counter()
    batchdata_targets: Counter[str] = Counter()

    for row in rows[:limit]:
        fields = dict(row)
        fields["value_anchor_low"] = fields.get("avm_low")
        fields["value_anchor_mid"] = fields.get("avm_value")
        fields["value_anchor_high"] = fields.get("avm_high")
        quality = assess_packet_data(fields)
        lead = {
            "lead_key": fields.get("lead_key"),
            "address": fields.get("address"),
            "county": fields.get("county"),
            "dts_days": fields.get("dts_days"),
            "auction_readiness": fields.get("auction_readiness"),
            "packet_completeness_pct": quality["packet_completeness_pct"],
            "vault_publish_ready": quality["vault_publish_ready"],
            "vault_publish_blockers": quality["vault_publish_blockers"],
            "batchdata_fallback_targets": quality["batchdata_fallback_targets"],
        }
        leads.append(lead)
        blocker_counts.update(quality["vault_publish_blockers"])
        batchdata_targets.update(quality["batchdata_fallback_targets"])

    report = {
        "generated_at": _utc_now(),
        "db_path": db_path,
        "dts_window": {"min": dts_min, "max": dts_max},
        "lead_count": len(leads),
        "vault_ready_count": sum(1 for lead in leads if lead["vault_publish_ready"]),
        "top_blockers": blocker_counts.most_common(10),
        "top_batchdata_targets": batchdata_targets.most_common(10),
        "leads": leads,
    }

    out_dir = os.path.join("out", "reports")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "packet_quality_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(json.dumps({
        "ok": True,
        "out_path": os.path.abspath(out_path),
        "lead_count": report["lead_count"],
        "vault_ready_count": report["vault_ready_count"],
        "top_blockers": report["top_blockers"][:5],
        "top_batchdata_targets": report["top_batchdata_targets"][:5],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
