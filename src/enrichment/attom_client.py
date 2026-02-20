# src/enrichment/attom_client.py
"""
ATTOM Data Property API (Premium) client.

Design goals:
- requests-only
- safe no-op behavior handled by caller (enricher checks key)
- centralized call counting for cost control
- resilient parsing helpers
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import requests


DEFAULT_BASE_URL = os.getenv("FALCO_ATTOM_BASE_URL", "https://api.gateway.attomdata.com/propertyapi/v1.0").rstrip("/")


class AttomError(RuntimeError):
    pass


def _is_truthy_env(val: str) -> bool:
    return val.strip() not in ("", "0", "false", "False", "no", "No")


def _clip_json(obj: Any, max_chars: int = 4500) -> str:
    try:
        import json
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1] + "…"


@dataclass
class AttomClient:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_s: int = 30
    max_retries: int = 2
    retry_backoff_s: float = 1.2
    debug: bool = field(default_factory=lambda: _is_truthy_env(os.getenv("FALCO_ENRICH_DEBUG", "")))

    # counters
    call_count: int = 0
    call_count_by_path: Dict[str, int] = field(default_factory=dict)

    def _headers(self) -> Dict[str, str]:
        # ATTOM uses "apikey" header for the gateway.
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

                # retryable
                if r.status_code in (429, 500, 502, 503, 504):
                    raise AttomError(f"ATTOM {r.status_code} retryable: {r.text[:400]}")
                raise AttomError(f"ATTOM {r.status_code}: {r.text[:400]}")
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

        # Many ATTOM implementations accept "startdate" (yyyy-mm-dd)
        try:
            from datetime import date, timedelta
            start = (date.today() - timedelta(days=int(days_back))).isoformat()
            params["startdate"] = start
        except Exception:
            pass

        return self._request("/salescomparables", params=params)

    # -----------------------
    # Parsing helpers
    # -----------------------

    @staticmethod
    def first_property(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        prop = payload.get("property")
        if isinstance(prop, list) and prop:
            if isinstance(prop[0], dict):
                return prop[0]
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
        if payload.get("property"):
            return True
        return False
