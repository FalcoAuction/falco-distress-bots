# src/enrichment/attom_client.py

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import requests


def _truthy(val: str) -> bool:
    return str(val or "").strip() not in ("", "0", "false", "False", "no", "No")


def _clip(s: str, n: int = 700) -> str:
    s = s or ""
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


DEFAULT_BASE_URL = os.getenv(
    "FALCO_ATTOM_BASE_URL",
    "https://api.gateway.attomdata.com/propertyapi/v1.0.0",
).rstrip("/")


class AttomError(RuntimeError):
    pass


@dataclass
class AttomClient:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_s: int = 30
    max_retries: int = 1
    retry_backoff_s: float = 1.1
    debug: bool = field(default_factory=lambda: _truthy(os.getenv("FALCO_ENRICH_DEBUG", "")))

    call_count: int = 0
    call_count_by_path: Dict[str, int] = field(default_factory=dict)
    _debug_fail_samples_left: int = 10

    def _headers(self) -> Dict[str, str]:
        return {"Accept": "application/json", "apikey": self.api_key}

    @staticmethod
    def _clean_address2(address2: str) -> str:
        """
        ATTOM wants Address2 like: "Nashville, TN"
        """
        a2 = (address2 or "").strip()

        # remove any zip fragments
        a2 = " ".join([t for t in a2.replace(",", " ").split() if not (t.isdigit() and len(t) == 5)])

        # normalize to "City, ST" if possible
        tokens = a2.split()
        if len(tokens) >= 2 and len(tokens[-1]) == 2 and tokens[-1].isalpha():
            st = tokens[-1].upper()
            city = " ".join(tokens[:-1]).strip()
            if city:
                return f"{city}, {st}"
        return a2

    def _request_once(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"

        self.call_count += 1
        self.call_count_by_path[path] = self.call_count_by_path.get(path, 0) + 1

        r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout_s)

        if r.status_code == 200:
            return r.json() if r.text else {}

        if self.debug and self._debug_fail_samples_left > 0:
            self._debug_fail_samples_left -= 1
            print(f"[ATTOM][DEBUG] non200 path={path} status={r.status_code} params={params} body={_clip(r.text)}")

        if r.status_code in (429, 500, 502, 503, 504):
            raise AttomError(f"ATTOM {r.status_code} retryable: {_clip(r.text, 300)}")

        raise AttomError(f"ATTOM {r.status_code}: {_clip(r.text, 300)}")

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        last: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                return self._request_once(path, params)
            except Exception as e:
                last = e
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_backoff_s * (attempt + 1))
        raise AttomError(str(last) if last else "ATTOM request failed")

    def _params(self, *, address1: str, address2: str) -> Dict[str, Any]:
        a1 = (address1 or "").strip()
        a2 = self._clean_address2(address2)
        if not a1 or not a2:
            raise AttomError("ATTOM requires address1 and address2")
        return {"address1": a1, "address2": a2}

    # -----------------------
    # Endpoint wrappers (YOUR TENANT: address1+address2 only)
    # -----------------------

    def property_detail(self, *, address1: str, address2: str) -> Dict[str, Any]:
        return self._request("/property/detail", self._params(address1=address1, address2=address2))

    def property_detail_mortgage(self, *, address1: str, address2: str) -> Dict[str, Any]:
        return self._request("/property/detailmortgage", self._params(address1=address1, address2=address2))

    def property_detail_owner(self, *, address1: str, address2: str) -> Dict[str, Any]:
        return self._request("/property/detailowner", self._params(address1=address1, address2=address2))

    def avm_detail(self, *, address1: str, address2: str) -> Dict[str, Any]:
        return self._request("/attomavm/detail", self._params(address1=address1, address2=address2))

    def valuation_home_equity(self, *, address1: str, address2: str) -> Dict[str, Any]:
        return self._request("/valuation/homeequity", self._params(address1=address1, address2=address2))

    def sales_comparables(
        self,
        *,
        address1: str,
        address2: str,
        radius_miles: float = 1.0,
        days_back: int = 180,
        pagesize: int = 10,
    ) -> Dict[str, Any]:
        params = self._params(address1=address1, address2=address2)
        params["pagesize"] = int(pagesize)
        params["radius"] = float(radius_miles)
        try:
            from datetime import date, timedelta

            params["startdate"] = (date.today() - timedelta(days=int(days_back))).isoformat()
        except Exception:
            pass
        return self._request("/salescomparables", params)

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
