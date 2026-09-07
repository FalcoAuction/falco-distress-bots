"""
Preflight for the daily pipeline.

Prove the run can do what the workflow assumes BEFORE spending 75 minutes
finding out. Bots used to discover a missing secret or a missing browser
binary forty minutes in, record `skipped_no_creds`, and the job stayed
green. This prints one table up front, writes out/reports/preflight.json
for the artifact, and exits 1 only when Supabase is unreachable: every
bot no-ops without it, so that run would be a green no-op.

    python -m src.bots._preflight
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from dotenv import load_dotenv
    _here = Path(__file__).resolve()
    for _parent in [_here.parent, *_here.parents]:
        if (_parent / ".env").exists():
            load_dotenv(_parent / ".env", override=False)
            break
except ImportError:
    pass

OUT_PATH = Path("out/reports/preflight.json")

# (group, env vars, what stops working without them, required?)
ENV_GROUPS: List[Tuple[str, List[str], str, bool]] = [
    ("supabase", ["NEXT_PUBLIC_SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"],
     "everything (staging writes + health rows)", True),
    ("enformion", ["FALCO_ENFORMION_AP_NAME", "FALCO_ENFORMION_AP_PASSWORD"],
     "enformion_skip_trace (phones)", False),
    ("twilio", ["TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN"],
     "middle_tn_twilio_lookup + auto_promoter phone validation", False),
    ("openai", ["OPENAI_API_KEY"], "decision_engine LLM grading", False),
    ("batchdata", ["FALCO_BATCHDATA_API_KEY"], "middle_tn_skiptrace", False),
    ("attom", ["FALCO_ATTOM_API_KEY"], "legacy AVM enrichment", False),
    ("resend", ["RESEND_API_KEY"], "pipeline alert email (_alert.py)", False),
    ("rod_davidson", ["FALCO_DAVIDSON_ROD_USER", "FALCO_DAVIDSON_ROD_PASSWORD"],
     "mtn_lis_pendens_rod (Davidson)", False),
    ("rod_williamson", ["FALCO_WILLIAMSON_ROD_USER", "FALCO_WILLIAMSON_ROD_PASSWORD"],
     "mtn_lis_pendens_rod (Williamson)", False),
    ("rod_hamilton", ["FALCO_HAMILTON_ROD_USER", "FALCO_HAMILTON_ROD_PASSWORD"],
     "mtn_lis_pendens_rod (Hamilton)", False),
]

# (import name, which bots break without it)
IMPORTS: List[Tuple[str, str]] = [
    ("supabase", "everything"),
    ("requests", "everything"),
    ("bs4", "HTML scrapers"),
    ("pdfplumber", "mackie_wolf_trustee, bankruptcy_schedule_d"),
    ("phonenumbers", "phone_classifier, auto_promoter"),
    ("openai", "decision_engine"),
    ("twilio", "middle_tn_twilio_lookup"),
    ("playwright.sync_api", "mtn_lis_pendens_rod"),
]


def _mark(ok: bool, required: bool = False) -> str:
    if ok:
        return "[ok]"
    return "[!!]" if required else "[--]"


def check_env() -> List[Dict[str, Any]]:
    rows = []
    for group, names, purpose, required in ENV_GROUPS:
        missing = [n for n in names if not (os.environ.get(n) or "").strip()]
        rows.append({
            "check": f"env:{group}",
            "ok": not missing,
            "required": required,
            "detail": purpose if not missing else f"missing {', '.join(missing)} -> {purpose} off",
        })
    return rows


def check_imports() -> List[Dict[str, Any]]:
    rows = []
    for mod, purpose in IMPORTS:
        try:
            importlib.import_module(mod)
            rows.append({"check": f"import:{mod}", "ok": True, "required": False, "detail": purpose})
        except Exception as e:
            rows.append({"check": f"import:{mod}", "ok": False, "required": False,
                         "detail": f"{type(e).__name__}: {e} -> {purpose} off"})
    return rows


def check_chromium() -> Dict[str, Any]:
    """playwright the package can be installed while the browser binary is
    not (that is exactly the failure that lost the lis pendens source)."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            path = p.chromium.executable_path
        ok = bool(path) and os.path.exists(path)
        return {"check": "playwright:chromium", "ok": ok, "required": False,
                "detail": path if ok else f"binary missing at {path}; run: python -m playwright install chromium"}
    except Exception as e:
        return {"check": "playwright:chromium", "ok": False, "required": False,
                "detail": f"{type(e).__name__}: {e}"}


def check_supabase() -> Dict[str, Any]:
    url = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        return {"check": "supabase:reachable", "ok": False, "required": True, "detail": "no creds"}
    try:
        from supabase import create_client
        client = create_client(url, key)
        res = client.table("bot_run_health").select("id").limit(1).execute()
        rows = getattr(res, "data", None)
        return {"check": "supabase:reachable", "ok": rows is not None, "required": True,
                "detail": "bot_run_health readable"}
    except Exception as e:
        return {"check": "supabase:reachable", "ok": False, "required": True,
                "detail": f"{type(e).__name__}: {str(e)[:200]}"}


def run() -> Tuple[List[Dict[str, Any]], bool]:
    rows = check_env() + check_imports() + [check_chromium(), check_supabase()]
    fatal = any(r["required"] and not r["ok"] for r in rows)
    return rows, fatal


def main() -> int:
    rows, fatal = run()
    print("=== Preflight ===")
    for r in rows:
        print(f"  {_mark(r['ok'], r['required'])} {r['check']:26s} {r['detail']}")
    off = [r["check"] for r in rows if not r["ok"] and not r["required"]]
    if off:
        print(f"  optional features off this run: {len(off)}")
    try:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(json.dumps({"rows": rows, "fatal": fatal}, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"  could not write {OUT_PATH}: {e}")
    if fatal:
        print("  FATAL: required check failed; the pipeline would be a no-op. Aborting.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
