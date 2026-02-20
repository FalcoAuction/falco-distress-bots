# src/enrichment/attom_client.py
"""
ATTOM Data Property API (Premium) client.

Key changes vs prior:
- Default base URL uses v1.0.0 (common in ATTOM gateway deployments)
- Adds lightweight debug sampling of non-200 responses (first few only)
- Central call counting + per-path breakdown
- requests-only, no heavy deps
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import requests


def _truthy(val: str) -> bool:
    return str(val or "").strip() not in ("", "0", "false", "False", "no", "No")


def _clip(s: str, n: int = 500) -> str:
    s = s or ""
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


DEFAULT_BASE_URL = os.getenv(
    "FALCO_ATTOM_BASE_URL",
    # NOTE: many ATTOM gateway tenants use v1.0.0
    "https://api.gateway.attomdata.com/propertyapi/v1.0.0",
).rstrip("/")


class AttomError(RuntimeError):
    pass


@dataclass
class AttomClient:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_s: int = 30
    max_retries: int = 2
    retry_backoff_s: float = 1.2
    debug: bool = field(default_factory=lambda: _truthy(os.getenv("FALCO_ENRICH_DEBUG", "")))

    # counters
    call_count: int = 0
    call_count_by_path: Dict[str, int] = field(default_factory=dict)

    # debug sampling (avoid log spam)
    _debug_fail_samples_left: int = 5

    def _headers(self) -> Dict[str, str]:
        # ATTOM gateway expects "apikey" header
        return {
            "Accept": "application/json",
            "apikey": self.api_key,
        }

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        params = params or {}

        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                self.call_count += 1
                self.call_count_by_path[path] = self.call_count_by_path.get(path, 0) + 1

                r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout_s)

                if r.status_code == 200:
                    return r.json() if r.text else {}

                # debug sample a few failures so we can see root cause quickly
                if self.debug and self._debug_fail_samples_left > 0:
                    self._debug_fail_samples_left -= 1
                    print(
                        f"[ATTOM][DEBUG] non200 path={path} status={r.status_code} "
                        f"params={params} body={_clip(r.text, 600)}"
                    )

                # retryable statuses
                if r.status_code in (429, 500, 502, 503, 504):
                    raise AttomError(f"ATTOM {r.status_code} retryable: {_clip(r.text, 300)}")

                # non-retryable
                raise AttomError(f"ATTOM {r.status_code}: {_clip(r.text, 300)}")

            except Exception as e:
                last_err = e
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_backoff_s * (attempt + 1))

        raise AttomError(str(last_err) if last_err else "ATTOM request failed")

    # -----------------------
    # Endpoint wrappers
    # -----------------------

    def property_detail(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        params: Dict[str, Any] = {"address1": address1}
        if address2:
            params["address2"] = address2
        if postalcode:
            params["postalcode"] = postalcode
        return self._request("/property/detail", params=params)

    def property_detail_mortgage(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        params: Dict[str, Any] = {"address1": address1}
        if address2:
            params["address2"] = address2
        if postalcode:
            params["postalcode"] = postalcode
        return self._request("/property/detailmortgage", params=params)

    def property_detail_owner(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        params: Dict[str, Any] = {"address1": address1}
        if address2:
            params["address2"] = address2
        if postalcode:
            params["postalcode"] = postalcode
        return self._request("/property/detailowner", params=params)

    def avm_detail(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        params: Dict[str, Any] = {"address1": address1}
        if address2:
            params["address2"] = address2
        if postalcode:
            params["postalcode"] = postalcode
        return self._request("/attomavm/detail", params=params)

    def valuation_home_equity(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        params: Dict[str, Any] = {"address1": address1}
        if address2:
            params["address2"] = address2
        if postalcode:
            params["postalcode"] = postalcode
        return self._request("/valuation/homeequity", params=params)

    def sales_comparables(
        self,
        *,
        address1: str,
        address2: str = "",
        postalcode: str = "",
        radius_miles: float = 1.0,
        days_back: int = 180,
        pagesize: int = 10,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"address1": address1, "pagesize": int(pagesize)}
        if address2:
            params["address2"] = address2
        if postalcode:
            params["postalcode"] = postalcode

        params["radius"] = float(radius_miles)

        # Many ATTOM gateway tenants accept startdate as YYYY-MM-DD
        try:
            from datetime import date, timedelta

            start = (date.today() - timedelta(days=int(days_back))).isoformat()
            params["startdate"] = start
        except Exception:
            pass

        return self._request("/salescomparables", params=params)

    # -----------------------
    # Response helpers
    # -----------------------

    @staticmethod
    def first_property(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        prop = payload.get("property")
        if isinstance(prop, list) and prop:
            return prop[0] if isinstance(prop[0], dict) else None
        if isinstance(prop, dict):
            return prop
        return None

    @staticmethod
    def status_ok(payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        status = payload.get("status")
        if isinstance(status, dict):
            code = status.get("code")
            if code in (0, "0"):
                return True
            msg = str(status.get("msg") or status.get("message") or "").lower()
            if "success" in msg:
                return True
        return bool(payload.get("property"))
