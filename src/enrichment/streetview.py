# src/enrichment/streetview.py
#
# Google Street View Static API helper.
# Feature is OFF by default. Requires explicit opt-in via env vars.
# Metadata endpoint checked first (free tier per Google docs).
# Images cached on disk by lead_key; no re-fetch if cached file > 5 KB.

import json
import logging
import os
from typing import Optional

_LOG = logging.getLogger(__name__)

_META_URL  = "https://maps.googleapis.com/maps/api/streetview/metadata"
_IMAGE_URL = "https://maps.googleapis.com/maps/api/streetview"
_STATICMAP_URL = "https://maps.googleapis.com/maps/api/staticmap"


# ─── Env helpers ──────────────────────────────────────────────────────────────

def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


# ─── Location builder ─────────────────────────────────────────────────────────

def _build_location(fields: dict) -> str:
    """
    Returns a geocodable one-line location string.
    Prefers attom_detail['address'] (one-line); falls back to fields['address'].

    IMPORTANT:
    - leads.address in our DB already tends to include city/state/zip.
    - leads table does NOT guarantee a separate city column.
    - Avoid duplicating the state (e.g., "... TN 37072, TN").
    """
    detail = fields.get("attom_detail")
    if isinstance(detail, dict):
        for key in ("address", "oneLine", "singleLineAddress"):
            v = detail.get(key)
            if isinstance(v, dict):
                for nested_key in ("oneLine", "line1", "singleLineAddress"):
                    nested_val = v.get(nested_key)
                    if nested_val and str(nested_val).strip():
                        if nested_key == "line1" and v.get("line2"):
                            return f"{str(nested_val).strip()}, {str(v.get('line2')).strip()}"
                        return str(nested_val).strip()
            if v and str(v).strip():
                return str(v).strip()

    addr = (fields.get("address") or "").strip()
    st   = (fields.get("state") or "").strip()

    if not addr:
        return ""

    # If address already ends with state (or contains ", TN"), don't append again.
    # Common cases: "... , TN 37072" or "... TN" etc.
    if st:
        addr_upper = addr.upper()
        st_upper   = st.upper()

        # If we already see ", TN" anywhere near the end, or it ends with " TN" or " TN <zip>"
        if (f", {st_upper}" in addr_upper) or addr_upper.endswith(f" {st_upper}") or addr_upper.endswith(f", {st_upper}"):
            return addr

        # Otherwise append state for extra geocoding context
        return f"{addr}, {st}"

    return addr


# ─── Public interface ─────────────────────────────────────────────────────────

def get_streetview_image_path(fields: dict, run_budget: dict) -> Optional[str]:
    """
    Returns a local JPEG path for a Street View image of the property, or None.

    Guards (all return None):
      - FALCO_STREETVIEW_ENABLE != "1"
      - FALCO_GMAPS_API_KEY not set
      - run_budget["used"] >= run_budget["max"]
      - Metadata status != "OK"
      - Any network / IO error

    run_budget is mutated: ["used"] is incremented on each IMAGE fetch only
    (metadata calls are free and do not count against the cap).
    """
    # ── 1. Feature gate ───────────────────────────────────────────────────────
    if _env("FALCO_STREETVIEW_ENABLE") != "1":
        _LOG.debug("[SV] disabled (FALCO_STREETVIEW_ENABLE != 1)")
        return None

    api_key = _env("FALCO_GMAPS_API_KEY")
    if not api_key:
        _LOG.warning("[SV] missing API key — set FALCO_GMAPS_API_KEY to enable Street View")
        return None

    # ── 2. Per-run budget check ───────────────────────────────────────────────
    used = run_budget.get("used", 0)
    cap  = run_budget.get("max", 0)
    if used >= cap:
        _LOG.info("[SV] cap hit (used=%d max=%d) — skipping image fetch", used, cap)
        return None

    # ── 3. Cache setup ────────────────────────────────────────────────────────
    lead_key = (fields.get("lead_key") or "").strip()
    if not lead_key:
        _LOG.warning("[SV] no lead_key — cannot cache; skipping")
        return None

    cache_dir = _env(
        "FALCO_STREETVIEW_CACHE_DIR",
        os.path.join("out", "images", "streetview"),
    )
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{lead_key}.jpg")

    # ── 4. Cache hit ──────────────────────────────────────────────────────────
    sidecar_path = cache_path.replace(".jpg", ".meta.json")
    if os.path.isfile(cache_path) and os.path.getsize(cache_path) > 5_120:
        if os.path.isfile(sidecar_path):
            try:
                with open(sidecar_path) as fh:
                    meta = json.load(fh)
                fields["streetview_status"]       = meta.get("status", "unknown")
                fields["streetview_imagery_date"]  = meta.get("date")
                fields["streetview_pano_id"]       = meta.get("pano_id")
                fields["property_image_source"]    = meta.get("source", "street_view")
                print(f"[SV] cache hit: {cache_path} (date={meta.get('date')})")
            except Exception as exc:
                _LOG.warning("[SV] could not read sidecar for lead_key=%s: %s", lead_key, exc)
        else:
            _LOG.info("[SV] cache hit (no sidecar): %s", cache_path)
        return cache_path

    # ── 5. Location string ────────────────────────────────────────────────────
    location = _build_location(fields)
    if not location:
        _LOG.warning("[SV] could not build location string for lead_key=%s", lead_key)
        return None

    # ── 6. Request params ─────────────────────────────────────────────────────
    timeout  = max(1, int(_env("FALCO_STREETVIEW_TIMEOUT_S", "8") or "8"))
    radius   = _env("FALCO_STREETVIEW_RADIUS_M", "50") or "50"
    fov      = _env("FALCO_STREETVIEW_FOV",      "80") or "80"
    pitch    = _env("FALCO_STREETVIEW_PITCH",    "0")  or "0"
    heading  = _env("FALCO_STREETVIEW_HEADING")

    try:
        import requests as _req

        # ── 7. Metadata check (free — does not consume image quota) ──────────
        meta_params: dict = {
            "location": location,
            "radius":   radius,
            "key":      api_key,
        }
        _LOG.info("[SV] metadata check for %r (lead_key=%s)", location, lead_key)
        meta_r = _req.get(_META_URL, params=meta_params, timeout=timeout)
        meta_r.raise_for_status()
        meta_json  = meta_r.json()
        status     = meta_json.get("status", "UNKNOWN")
        img_date   = meta_json.get("date")
        pano_id    = meta_json.get("pano_id")
        _LOG.info("[SV] metadata status=%s for lead_key=%s", status, lead_key)
        print(f"[SV] meta status={status} date={img_date} pano={pano_id}")

        if status != "OK":
            fallback_enabled = _env("FALCO_STATICMAP_FALLBACK_ENABLE", "1") == "1"
            if not fallback_enabled:
                return None

            map_params: dict = {
                "size": "640x360",
                "maptype": "satellite",
                "center": location,
                "zoom": _env("FALCO_STATICMAP_ZOOM", "18") or "18",
                "key": api_key,
                "markers": f"size:small|color:{_env('FALCO_STATICMAP_MARKER_COLOR', '0x13c296')}|{location}",
            }
            map_r = _req.get(_STATICMAP_URL, params=map_params, timeout=timeout)
            map_r.raise_for_status()
            if "image" not in map_r.headers.get("content-type", ""):
                return None
            with open(cache_path, "wb") as fh:
                fh.write(map_r.content)
            if os.path.getsize(cache_path) <= 5_120:
                try:
                    os.remove(cache_path)
                except OSError:
                    pass
                return None

            run_budget["used"] += 1
            fields["streetview_status"] = status or "unknown"
            fields["streetview_imagery_date"] = None
            fields["streetview_pano_id"] = None
            fields["property_image_source"] = "satellite_map"
            try:
                with open(sidecar_path, "w") as fh:
                    json.dump(
                        {"status": status, "date": None, "pano_id": None, "source": "satellite_map"},
                        fh,
                    )
            except Exception as exc:
                _LOG.warning("[SV] could not write static map sidecar: %s", exc)
            print(f"[SV] fallback static map saved: {cache_path}")
            return cache_path

        # ── 8. Budget re-check before image fetch (race safety) ───────────────
        if run_budget.get("used", 0) >= run_budget.get("max", 0):
            _LOG.info("[SV] cap hit after metadata — not fetching image")
            return None

        # ── 9. Image fetch ────────────────────────────────────────────────────
        img_params: dict = {
            "size":     "640x360",
            "location": location,
            "radius":   radius,
            "fov":      fov,
            "pitch":    pitch,
            "key":      api_key,
        }
        if heading:
            img_params["heading"] = heading

        _LOG.info("[SV] fetching image for lead_key=%s", lead_key)
        img_r = _req.get(_IMAGE_URL, params=img_params, timeout=timeout)
        img_r.raise_for_status()

        content_type = img_r.headers.get("content-type", "")
        if "image" not in content_type:
            _LOG.warning(
                "[SV] unexpected content-type %r for lead_key=%s — discarding",
                content_type, lead_key,
            )
            return None

        raw = img_r.content
        with open(cache_path, "wb") as fh:
            fh.write(raw)

        # Google returns a small "no imagery" tile on some failures
        if os.path.getsize(cache_path) <= 5_120:
            _LOG.warning(
                "[SV] image too small (%d bytes) — likely error tile; discarding",
                os.path.getsize(cache_path),
            )
            os.remove(cache_path)
            return None

        run_budget["used"] += 1
        _LOG.info(
            "[SV] image saved: %s (budget used=%d/%d)",
            cache_path, run_budget["used"], run_budget.get("max", 0),
        )

        # Persist metadata sidecar so cache-hit runs don't need a re-fetch
        try:
            with open(sidecar_path, "w") as fh:
                json.dump({"status": status, "date": img_date, "pano_id": pano_id}, fh)
        except Exception as exc:
            _LOG.warning("[SV] could not write sidecar: %s", exc)

        # Write into fields so callers (pdf_builder) can render imagery date
        fields["streetview_status"]      = status or "unknown"
        fields["streetview_imagery_date"] = img_date
        fields["streetview_pano_id"]      = pano_id
        fields["property_image_source"]   = "street_view"

        return cache_path

    except Exception as exc:
        _LOG.warning("[SV] error for lead_key=%s: %s: %s", lead_key, type(exc).__name__, exc)
        return None
