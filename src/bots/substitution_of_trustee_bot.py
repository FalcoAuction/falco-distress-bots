# src/bots/substitution_of_trustee_bot.py
# Substitution of Trustee ingestion bot.
# Enable with: FALCO_ENABLE_SOT=1

import os
import sqlite3
from typing import Dict


def run() -> Dict[str, int]:
    if os.environ.get("FALCO_ENABLE_SOT", "").strip() != "1":
        print("[SubstitutionOfTrusteeBot] disabled (set FALCO_ENABLE_SOT=1 to enable)")
        return {"status": "disabled"}

    db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    summary: Dict[str, int] = {
        "candidates_seen": 0,
        "leads_updated":   0,
        "missing_leads":   0,
    }

    try:
        con = sqlite3.connect(db)
        try:
            rows = con.execute(
                """
                SELECT DISTINCT lead_key FROM ingest_events
                WHERE source = 'SUBSTITUTION_OF_TRUSTEE'
                ORDER BY id DESC
                LIMIT 200
                """
            ).fetchall()

            summary["candidates_seen"] = len(rows)

            for (lead_key,) in rows:
                exists = con.execute(
                    "SELECT 1 FROM leads WHERE lead_key = ? LIMIT 1", (lead_key,)
                ).fetchone()
                if not exists:
                    summary["missing_leads"] += 1
                    continue
                con.execute(
                    """
                    UPDATE leads
                    SET distress_type      = 'SOT',
                        auction_readiness  = COALESCE(auction_readiness, 'UPSTREAM')
                    WHERE lead_key = ?
                    """,
                    (lead_key,),
                )
                summary["leads_updated"] += 1

            con.commit()
        finally:
            con.close()
    except Exception as e:
        print(f"[SubstitutionOfTrusteeBot] ERROR {type(e).__name__}: {e}")

    print(
        f"[SubstitutionOfTrusteeBot] summary "
        f"candidates_seen={summary['candidates_seen']} "
        f"leads_updated={summary['leads_updated']} "
        f"missing_leads={summary['missing_leads']}"
    )
    return summary
