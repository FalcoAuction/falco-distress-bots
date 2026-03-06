# src/uw/uw_web.py
# Local UW web form (no dependencies). Writes UW JSON + uw_ready into SQLite.
# Run:
#   .\.venv\Scripts\python.exe -m src.uw.uw_web
# Open:
#   http://127.0.0.1:8844/

import json
import os
import sqlite3
import urllib.parse
from dataclasses import dataclass
from datetime import date
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional, Tuple


DEFAULT_DB = os.path.join("data", "falco.db")


def _db_path() -> str:
    # Keep compatible with your repo conventions
    return os.environ.get("FALCO_SQLITE_PATH") or os.environ.get("FALCO_DB_PATH") or DEFAULT_DB


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con


def _today() -> date:
    return date.today()


def _parse_iso_date(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    try:
        return date.fromisoformat(str(d).strip())
    except Exception:
        return None


def _calc_dts(sale_date: Optional[str]) -> Optional[int]:
    sd = _parse_iso_date(sale_date)
    if not sd:
        return None
    return (sd - _today()).days


def _safe_int(v: str, default: Optional[int] = None) -> Optional[int]:
    v = (v or "").strip()
    if v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v: str, default: Optional[float] = None) -> Optional[float]:
    v = (v or "").strip().replace(",", "")
    if v == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


def _pick_next_lead(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """
    Brutal shortlist (edit as you learn):
      - must have sale_date
      - dts in [21, 60] preferred
      - readiness GREEN preferred (YELLOW allowed)
      - NOT already UW_READY=1 (we only show those needing UW)
    """
    # Ensure uw_ready/uw_json exist (best-effort)
    try:
        con.execute("ALTER TABLE leads ADD COLUMN uw_ready INTEGER")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE leads ADD COLUMN uw_json TEXT")
    except Exception:
        pass

    # Grab latest sale_date per lead (from ingest_events)
    q = """
    WITH latest_ie AS (
      SELECT lead_key, MAX(id) AS max_ie_id
      FROM ingest_events
      WHERE sale_date IS NOT NULL AND sale_date != ''
      GROUP BY lead_key
    ),
    ie AS (
      SELECT e.lead_key, e.sale_date, e.source_url
      FROM ingest_events e
      JOIN latest_ie li ON li.max_ie_id = e.id
    ),
    latest_ae AS (
      SELECT lead_key, MAX(id) AS max_ae_id
      FROM attom_enrichments
      GROUP BY lead_key
    ),
    ae AS (
      SELECT a.lead_key, a.status AS attom_status, a.avm_value, a.avm_low, a.avm_high, a.enriched_at
      FROM attom_enrichments a
      JOIN latest_ae la ON la.max_ae_id = a.id
    )
    SELECT
      l.lead_key,
      l.address,
      l.county,
      l.state,
      l.auction_readiness,
      l.falco_score_internal,
      l.equity_band,
      l.dts_days,
      l.uw_ready,
      l.uw_json,
      ie.sale_date,
      ie.source_url,
      ae.attom_status,
      ae.avm_value,
      ae.avm_low,
      ae.avm_high,
      ae.enriched_at
    FROM leads l
    JOIN ie ON ie.lead_key = l.lead_key
    LEFT JOIN ae ON ae.lead_key = l.lead_key
    WHERE (l.uw_ready IS NULL OR l.uw_ready != 1)
      AND (l.address IS NOT NULL AND l.address != '')
      AND (l.county IS NOT NULL AND l.county != '')
      AND (l.auction_readiness IN ('GREEN','YELLOW'))
    ORDER BY
      CASE WHEN l.auction_readiness='GREEN' THEN 0 ELSE 1 END,
      CASE
        WHEN l.dts_days BETWEEN 21 AND 60 THEN 0
        WHEN l.dts_days BETWEEN 61 AND 75 THEN 1
        ELSE 2
      END,
      COALESCE(l.falco_score_internal, 0) DESC
    LIMIT 1
    """
    row = con.execute(q).fetchone()
    return row


def _load_lead_by_key(con: sqlite3.Connection, lead_key: str) -> Optional[sqlite3.Row]:
    """Load any single lead by exact lead_key, regardless of readiness/uw_ready state."""
    # Ensure columns exist (same guard as _pick_next_lead)
    try:
        con.execute("ALTER TABLE leads ADD COLUMN uw_ready INTEGER")
    except Exception:
        pass
    try:
        con.execute("ALTER TABLE leads ADD COLUMN uw_json TEXT")
    except Exception:
        pass

    q = """
    WITH latest_ie AS (
      SELECT lead_key, MAX(id) AS max_ie_id
      FROM ingest_events
      WHERE sale_date IS NOT NULL AND sale_date != ''
      GROUP BY lead_key
    ),
    ie AS (
      SELECT e.lead_key, e.sale_date, e.source_url
      FROM ingest_events e
      JOIN latest_ie li ON li.max_ie_id = e.id
    ),
    latest_ae AS (
      SELECT lead_key, MAX(id) AS max_ae_id
      FROM attom_enrichments
      GROUP BY lead_key
    ),
    ae AS (
      SELECT a.lead_key, a.status AS attom_status, a.avm_value, a.avm_low, a.avm_high, a.enriched_at
      FROM attom_enrichments a
      JOIN latest_ae la ON la.max_ae_id = a.id
    )
    SELECT
      l.lead_key,
      l.address,
      l.county,
      l.state,
      l.auction_readiness,
      l.falco_score_internal,
      l.equity_band,
      l.dts_days,
      l.uw_ready,
      l.uw_json,
      ie.sale_date,
      ie.source_url,
      ae.attom_status,
      ae.avm_value,
      ae.avm_low,
      ae.avm_high,
      ae.enriched_at
    FROM leads l
    LEFT JOIN ie  ON ie.lead_key  = l.lead_key
    LEFT JOIN ae  ON ae.lead_key  = l.lead_key
    WHERE l.lead_key = ?
    """
    return con.execute(q, (lead_key,)).fetchone()


def _save_uw(con: sqlite3.Connection, lead_key: str, uw_ready: int, uw_json: str) -> None:
    con.execute(
        "UPDATE leads SET uw_ready=?, uw_json=? WHERE lead_key=?",
        (int(uw_ready), uw_json, lead_key),
    )
    con.commit()


def _write_manual_uw(con: sqlite3.Connection, lead_key: str, payload: Dict[str, Any]) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS manual_underwriting (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_key   TEXT NOT NULL,
            analyst    TEXT,
            value_low  REAL,
            value_high REAL,
            max_bid    REAL,
            occupancy  TEXT,
            condition  TEXT,
            strategy   TEXT,
            notes      TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    _notes = " | ".join(
        p for p in (payload.get("title_notes") or "", payload.get("partner_action") or "")
        if p
    ) or None
    con.execute(
        """
        INSERT INTO manual_underwriting
            (lead_key, analyst, value_low, value_high, max_bid, occupancy, condition, strategy, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_key,
            None,                                  # analyst — no form field yet
            None,                                  # value_low — no form field yet
            payload.get("manual_arv"),             # value_high ← ARV
            payload.get("manual_bid_cap"),         # max_bid
            payload.get("occupancy") or None,
            payload.get("condition") or None,
            payload.get("exit_strategy") or None,  # strategy
            _notes,
        ),
    )
    con.commit()


def _html_escape(s: Any) -> str:
    s = "" if s is None else str(s)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _render(row: Optional[sqlite3.Row], msg: str = "", error: str = "") -> bytes:
    _load_box = """
    <div class="card" style="padding:12px 16px;">
      <form method="GET" action="/" style="display:flex;gap:8px;align-items:center;margin:0;">
        <span style="font-size:13px;color:#9ca3af;white-space:nowrap;">Load lead key</span>
        <input name="lead_key" placeholder="paste lead_key here" style="margin:0;flex:1;"/>
        <button type="submit" style="white-space:nowrap;">Go</button>
      </form>
    </div>
    """
    _error_banner = (
        f"<div style='background:#7f1d1d;color:#fecaca;padding:10px 16px;"
        f"border-radius:10px;margin-bottom:14px;font-weight:bold;'>"
        f"{_html_escape(error)}</div>"
    ) if error else ""

    if not row:
        body = _load_box + _error_banner + """
        <h2>No UW candidates found.</h2>
        <p>That means either: no sale_date leads, or all candidates already have uw_ready=1, or readiness isn't GREEN/YELLOW.</p>
        <p><a href="/">Refresh</a></p>
        """
        return _wrap_page("FALCO UW", body).encode("utf-8")

    sale_date = row["sale_date"]
    dts = row["dts_days"]
    if dts is None:
        dts = _calc_dts(sale_date)

    avm_mid = row["avm_value"]
    avm_low = row["avm_low"]
    avm_high = row["avm_high"]

    # preload existing uw_json if any
    uw_existing = {}
    try:
        if row["uw_json"]:
            uw_existing = json.loads(row["uw_json"])
    except Exception:
        uw_existing = {}

    def pre(key: str, default: str = "") -> str:
        v = uw_existing.get(key, default)
        return "" if v is None else str(v)

    header = f"""
    <div class="card">
      <div class="kvs">
        <div><span class="k">lead_key</span><span class="v mono">{_html_escape(row["lead_key"])}</span></div>
        <div><span class="k">address</span><span class="v">{_html_escape(row["address"])}</span></div>
        <div><span class="k">county</span><span class="v">{_html_escape(row["county"])}</span></div>
        <div><span class="k">sale_date</span><span class="v">{_html_escape(sale_date)}</span></div>
        <div><span class="k">dts_days</span><span class="v">{_html_escape(dts)}</span></div>
        <div><span class="k">readiness</span><span class="v pill {('g' if row['auction_readiness']=='GREEN' else 'y')}">{_html_escape(row["auction_readiness"])}</span></div>
        <div><span class="k">score</span><span class="v">{_html_escape(row["falco_score_internal"])}</span></div>
        <div><span class="k">equity_band</span><span class="v">{_html_escape(row["equity_band"])}</span></div>
        <div><span class="k">ATTOM</span><span class="v">{_html_escape(row["attom_status"])}</span></div>
        <div><span class="k">AVM</span><span class="v">{_html_escape(avm_low)} / {_html_escape(avm_mid)} / {_html_escape(avm_high)}</span></div>
        <div><span class="k">source_url</span><span class="v"><a href="{_html_escape(row["source_url"])}" target="_blank">open notice</a></span></div>
      </div>
    </div>
    """

    form = f"""
    <form method="POST" action="/save" class="card">
      <input type="hidden" name="lead_key" value="{_html_escape(row["lead_key"])}"/>

      <div class="grid">
        <label>UW_READY (1=yes, 0=no)
          <select name="uw_ready">
            <option value="0" {"selected" if str(row["uw_ready"] or 0) != "1" else ""}>0</option>
            <option value="1" {"selected" if str(row["uw_ready"] or 0) == "1" else ""}>1</option>
          </select>
        </label>

        <label>UW_CONFIDENCE (1-5)
          <input name="uw_confidence" value="{_html_escape(pre("uw_confidence",""))}" placeholder=""/>
        </label>

        <label>UW_BLOCKER
          <select name="uw_blocker">
            { _opt("none", pre("uw_blocker","none")) }
            { _opt("title", pre("uw_blocker","none")) }
            { _opt("bankruptcy", pre("uw_blocker","none")) }
            { _opt("occupancy", pre("uw_blocker","none")) }
            { _opt("condition", pre("uw_blocker","none")) }
            { _opt("liens", pre("uw_blocker","none")) }
            { _opt("other", pre("uw_blocker","none")) }
          </select>
        </label>

        <label>Occupancy
          <select name="occupancy">
            { _opt("unknown", pre("occupancy","unknown")) }
            { _opt("owner", pre("occupancy","unknown")) }
            { _opt("tenant", pre("occupancy","unknown")) }
            { _opt("vacant", pre("occupancy","unknown")) }
          </select>
        </label>

        <label>Condition
          <select name="condition">
            { _opt("unknown", pre("condition","unknown")) }
            { _opt("light", pre("condition","unknown")) }
            { _opt("medium", pre("condition","unknown")) }
            { _opt("heavy", pre("condition","unknown")) }
          </select>
        </label>

        <label>Manual ARV (USD)
          <input name="manual_arv" value="{_html_escape(pre("manual_arv",""))}" placeholder="e.g. 525000"/>
        </label>

        <label>Manual BID CAP (USD)
          <input name="manual_bid_cap" value="{_html_escape(pre("manual_bid_cap",""))}" placeholder="e.g. 385000"/>
        </label>

        <label>Repair estimate (USD)
          <input name="repair_estimate" value="{_html_escape(pre("repair_estimate",""))}" placeholder=""/>
        </label>

        <label>Lien estimate total (USD)
          <input name="lien_estimate_total" value="{_html_escape(pre("lien_estimate_total",""))}" placeholder=""/>
        </label>

        <label>Exit strategy
          <select name="exit_strategy">
            { _opt("auction_retail", pre("exit_strategy","auction_retail")) }
            { _opt("wholesale", pre("exit_strategy","auction_retail")) }
            { _opt("investor", pre("exit_strategy","auction_retail")) }
            { _opt("flip", pre("exit_strategy","auction_retail")) }
            { _opt("hold", pre("exit_strategy","auction_retail")) }
          </select>
        </label>
      </div>

      <label>Title / legal notes (short)
        <input name="title_notes" value="{_html_escape(pre("title_notes",""))}" placeholder="one-liner"/>
      </label>

      <label>Partner action (1–3 bullets in one line)
        <input name="partner_action" value="{_html_escape(pre("partner_action",""))}" placeholder="e.g. Call trustee; verify occupancy; pull deed/mortgage"/>
      </label>

      <div class="row">
        <button type="submit">Save UW</button>
        <a class="btn" href="/">Skip / Next</a>
      </div>

      {"<p class='msg'>"+_html_escape(msg)+"</p>" if msg else ""}
    </form>
    """

    return _wrap_page("FALCO UW", _load_box + _error_banner + header + form).encode("utf-8")


def _opt(val: str, current: str) -> str:
    sel = "selected" if str(current).strip().lower() == val.lower() else ""
    return f'<option value="{_html_escape(val)}" {sel}>{_html_escape(val)}</option>'


def _wrap_page(title: str, inner: str) -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{_html_escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; background:#0b1220; color:#e5e7eb; margin:0; padding:24px; }}
    a {{ color:#93c5fd; }}
    .card {{ background:#0f1a2e; border:1px solid #1f2a44; border-radius:14px; padding:16px; margin-bottom:14px; }}
    .kvs {{ display:grid; grid-template-columns: 1fr 1fr; gap:10px; }}
    .k {{ display:inline-block; width:110px; color:#9ca3af; }}
    .v {{ color:#e5e7eb; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 12px; }}
    .pill {{ padding:2px 8px; border-radius:999px; font-weight:700; font-size:12px; display:inline-block; }}
    .pill.g {{ background:#064e3b; }}
    .pill.y {{ background:#78350f; }}
    .grid {{ display:grid; grid-template-columns: 1fr 1fr 1fr; gap:12px; margin-top:10px; }}
    label {{ display:block; font-size:12px; color:#cbd5e1; }}
    input, select {{ width:100%; margin-top:6px; padding:10px; border-radius:10px; border:1px solid #24314f; background:#0b1325; color:#e5e7eb; }}
    .row {{ display:flex; gap:10px; margin-top:12px; align-items:center; }}
    button, .btn {{ background:#1e40af; color:white; border:none; padding:10px 14px; border-radius:10px; cursor:pointer; text-decoration:none; display:inline-block; }}
    .btn {{ background:#374151; }}
    .msg {{ color:#a7f3d0; margin-top:10px; }}
  </style>
</head>
<body>
  <h2 style="margin-top:0;">FALCO — Manual Underwriting</h2>
  <p style="color:#9ca3af;margin-top:-6px;">Local-only. Writes to SQLite. No Notion. No ATTOM calls.</p>
  {inner}
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, data: bytes, content_type: str = "text/html; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path not in ("/", "/index.html"):
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return
        try:
            qs = urllib.parse.parse_qs(parsed.query)
            lead_key_qs = (qs.get("lead_key", [""])[0] or "").strip()
            con = _connect()
            error = ""
            if lead_key_qs:
                row = _load_lead_by_key(con, lead_key_qs)
                if row is None:
                    error = f"Lead key not found: {lead_key_qs}"
                    row = _pick_next_lead(con)
            else:
                row = _pick_next_lead(con)
            data = _render(row, error=error)
            con.close()
            self._send(200, data)
        except Exception as e:
            self._send(500, f"error: {type(e).__name__}: {e}".encode("utf-8"), "text/plain; charset=utf-8")

    def do_POST(self):
        if self.path != "/save":
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length).decode("utf-8", errors="replace")
            form = urllib.parse.parse_qs(raw)

            lead_key = (form.get("lead_key", [""])[0] or "").strip()
            if not lead_key:
                self._send(400, b"missing lead_key", "text/plain; charset=utf-8")
                return

            uw_ready = _safe_int(form.get("uw_ready", ["0"])[0], 0) or 0

            payload: Dict[str, Any] = {
                "uw_ready": int(uw_ready),
                "uw_confidence": _safe_int(form.get("uw_confidence", [""])[0], None),
                "uw_blocker": (form.get("uw_blocker", ["none"])[0] or "none").strip(),
                "occupancy": (form.get("occupancy", ["unknown"])[0] or "unknown").strip(),
                "condition": (form.get("condition", ["unknown"])[0] or "unknown").strip(),
                "title_notes": (form.get("title_notes", [""])[0] or "").strip(),
                "partner_action": (form.get("partner_action", [""])[0] or "").strip(),
                "manual_arv": _safe_float(form.get("manual_arv", [""])[0], None),
                "manual_bid_cap": _safe_float(form.get("manual_bid_cap", [""])[0], None),
                "repair_estimate": _safe_float(form.get("repair_estimate", [""])[0], None),
                "lien_estimate_total": _safe_float(form.get("lien_estimate_total", [""])[0], None),
                "exit_strategy": (form.get("exit_strategy", ["auction_retail"])[0] or "auction_retail").strip(),
            }

            uw_json = json.dumps(payload, ensure_ascii=False)

            con = _connect()
            _save_uw(con, lead_key, uw_ready, uw_json)
            _write_manual_uw(con, lead_key, payload)

            # Render next
            row = _pick_next_lead(con)
            data = _render(row, msg=f"Saved UW for {lead_key} (uw_ready={uw_ready}).")
            con.close()
            self._send(200, data)

        except Exception as e:
            self._send(500, f"error: {type(e).__name__}: {e}".encode("utf-8"), "text/plain; charset=utf-8")


def main():
    host = "127.0.0.1"
    port = int(os.environ.get("FALCO_UW_PORT", "8844") or "8844")
    httpd = HTTPServer((host, port), Handler)
    print(f"[UW_WEB] db={_db_path()}")
    print(f"[UW_WEB] listening http://{host}:{port}/  (CTRL+C to stop)")
    httpd.serve_forever()


if __name__ == "__main__":
    main()