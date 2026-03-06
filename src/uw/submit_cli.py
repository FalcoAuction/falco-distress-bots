# src/uw/submit_cli.py
from __future__ import annotations

import json
import os
import sqlite3
from typing import Optional

from .schema import UWSubmission


def _db_path() -> str:
    # IMPORTANT: keep consistent with sqlite_store.py
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _prompt(msg: str, default: Optional[str] = None) -> str:
    if default is None:
        s = input(f"{msg}: ").strip()
    else:
        s = input(f"{msg} [{default}]: ").strip()
        if not s:
            s = default
    return s.strip()


def _prompt_int(msg: str, default: Optional[int] = None) -> Optional[int]:
    s = _prompt(msg, str(default) if default is not None else None)
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _prompt_float(msg: str, default: Optional[float] = None) -> Optional[float]:
    s = _prompt(msg, str(default) if default is not None else None)
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _ensure_cols(con: sqlite3.Connection) -> None:
    # Defensive: ensure columns exist even if DB is older
    cols = {r[1] for r in con.execute("PRAGMA table_info(leads)").fetchall()}
    if "uw_ready" not in cols:
        con.execute("ALTER TABLE leads ADD COLUMN uw_ready INTEGER")
    if "uw_json" not in cols:
        con.execute("ALTER TABLE leads ADD COLUMN uw_json TEXT")


def submit_uw(lead_key: str) -> bool:
    lead_key = (lead_key or "").strip()
    if not lead_key:
        print("[UW] missing lead_key")
        return False

    db = _db_path()
    if not os.path.isfile(db):
        print(f"[UW] sqlite db not found at {db}")
        return False

    con = sqlite3.connect(db)
    try:
        con.row_factory = sqlite3.Row
        _ensure_cols(con)

        row = con.execute(
            "SELECT lead_key, address, county, state, dts_days, auction_readiness, equity_band, falco_score_internal "
            "FROM leads WHERE lead_key=?",
            (lead_key,),
        ).fetchone()

        if not row:
            print(f"[UW] lead_key not found in leads: {lead_key}")
            return False

        print("\n=== UW SUBMIT ===")
        print(f"lead_key: {row['lead_key']}")
        print(f"address : {row['address']}")
        print(f"county  : {row['county']}, {row['state']}")
        print(f"dts_days: {row['dts_days']} | readiness={row['auction_readiness']} | equity_band={row['equity_band']} | score={row['falco_score_internal']}")
        print("")

        uw = UWSubmission()

        # Gate
        uw.uw_ready = 1 if _prompt("UW_READY? (1=yes,0=no)", "0") in ("1", "yes", "y", "true") else 0
        uw.uw_confidence = _prompt_int("UW_CONFIDENCE 1-5 (blank=skip)", None)
        uw.uw_blocker = _prompt("UW_BLOCKER (none/title/bankruptcy/occupancy/condition/liens/other)", "none")

        # Reality
        uw.occupancy = _prompt("Occupancy (unknown/owner/tenant/vacant)", "unknown")
        uw.condition = _prompt("Condition (unknown/light/medium/heavy)", "unknown")
        uw.title_notes = _prompt("Title / legal notes (short)", "")

        # Numbers
        uw.manual_arv = _prompt_float("Manual ARV (USD, blank=skip)", None)
        uw.manual_bid_cap = _prompt_float("Manual BID CAP (USD, blank=skip)", None)
        uw.repair_estimate = _prompt_float("Repair estimate (USD, blank=skip)", None)
        uw.lien_estimate_total = _prompt_float("Lien estimate total (USD, blank=skip)", None)

        # Thesis
        uw.exit_strategy = _prompt("Exit strategy (auction_retail/wholesale/investor/flip/hold)", "auction_retail")
        uw.partner_action = _prompt("Partner action (1-3 bullets in one line)", "")

        uw.operator = os.environ.get("USERNAME") or os.environ.get("USER") or None
        payload = uw.to_json_dict()
        payload_json = json.dumps(payload, ensure_ascii=False)

        con.execute(
            "UPDATE leads SET uw_ready=?, uw_json=? WHERE lead_key=?",
            (int(payload["uw_ready"]), payload_json, lead_key),
        )
        con.commit()

        print("\n[UW] saved")
        print(f"[UW] uw_ready={payload['uw_ready']} bytes={len(payload_json)}")
        return True

    except Exception as e:
        print(f"[UW] ERROR {type(e).__name__}: {e}")
        try:
            con.rollback()
        except Exception:
            pass
        return False
    finally:
        con.close()


if __name__ == "__main__":
    lk = os.environ.get("FALCO_LEAD_KEY", "").strip()
    if not lk:
        lk = _prompt("Enter lead_key")
    ok = submit_uw(lk)
    raise SystemExit(0 if ok else 1)
