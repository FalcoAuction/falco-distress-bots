# src/enrichment/attom_client.py

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import requests


def _truthy(val: str) -> bool:
    return str(val or "").strip() not in ("", "0", "false", "False", "no", "No")


def _clip(s: str, n: int = 600) -> str:
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
    max_retries: int = 2
    retry_backoff_s: float = 1.2
    debug: bool = field(default_factory=lambda: _truthy(os.getenv("FALCO_ENRICH_DEBUG", "")))

    call_count: int = 0
    call_count_by_path: Dict[str, int] = field(default_factory=dict)
    _debug_fail_samples_left: int = 8

    def _headers(self) -> Dict[str, str]:
        return {"Accept": "application/json", "apikey": self.api_key}

    @staticmethod
    def _clean_address2(address2: str) -> str:
        # ATTOM often likes "CITY STATE" or "CITY, STATE" (no zip).
        a2 = (address2 or "").strip()
        # remove any zip fragments just in case
        a2 = " ".join([t for t in a2.replace(",", " ").split() if not (t.isdigit() and len(t) == 5)])
        # normalize back to "City, ST" if possible
        tokens = a2.split()
        if len(tokens) >= 2 and len(tokens[-1]) == 2:
            city = " ".join(tokens[:-1]).strip()
            st = tokens[-1].upper()
            return f"{city}, {st}".strip(", ")
        return a2

    def _build_param_variants(self, *, address1: str, address2: str = "", postalcode: str = "") -> Tuple[Dict[str, Any], ...]:
        """
        ATTOM can return code -4 "Invalid Parameter Combination" if you send
        address1+address2+postalcode together.

        We try safe combos in order:
          1) address1 + postalcode (most precise)
          2) address1 + address2
          3) address1 only (last resort)
        """
        a1 = (address1 or "").strip()
        a2 = self._clean_address2(address2)
        z = (postalcode or "").strip()

        variants: list[Dict[str, Any]] = []
        if a1 and z:
            variants.append({"address1": a1, "postalcode": z})
        if a1 and a2:
            variants.append({"address1": a1, "address2": a2})
        if a1:
            variants.append({"address1": a1})
        return tuple(variants)

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

        # treat 429/5xx as retryable
        if r.status_code in (429, 500, 502, 503, 504):
            raise AttomError(f"ATTOM {r.status_code} retryable: {_clip(r.text, 300)}")

        raise AttomError(f"ATTOM {r.status_code}: {_clip(r.text, 300)}")

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        last_err: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                return self._request_once(path, params)
            except Exception as e:
                last_err = e
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_backoff_s * (attempt + 1))
        raise AttomError(str(last_err) if last_err else "ATTOM request failed")

    def _request_with_fallbacks(self, path: str, variants: Tuple[Dict[str, Any], ...]) -> Dict[str, Any]:
        """
        Try multiple valid parameter combinations.
        Stops on first 200 response.
        """
        last_err: Optional[Exception] = None
        for params in variants:
            try:
                return self._request(path, params)
            except AttomError as e:
                last_err = e
                # keep trying next variant
                continue
        raise AttomError(str(last_err) if last_err else "ATTOM request failed")

    # -----------------------
    # Endpoint wrappers
    # -----------------------

    def property_detail(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        variants = self._build_param_variants(address1=address1, address2=address2, postalcode=postalcode)
        return self._request_with_fallbacks("/property/detail", variants)

    def property_detail_mortgage(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        variants = self._build_param_variants(address1=address1, address2=address2, postalcode=postalcode)
        return self._request_with_fallbacks("/property/detailmortgage", variants)

    def property_detail_owner(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        variants = self._build_param_variants(address1=address1, address2=address2, postalcode=postalcode)
        return self._request_with_fallbacks("/property/detailowner", variants)

    def avm_detail(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        variants = self._build_param_variants(address1=address1, address2=address2, postalcode=postalcode)
        return self._request_with_fallbacks("/attomavm/detail", variants)

    def valuation_home_equity(self, *, address1: str, address2: str = "", postalcode: str = "") -> Dict[str, Any]:
        variants = self._build_param_variants(address1=address1, address2=address2, postalcode=postalcode)
        return self._request_with_fallbacks("/valuation/homeequity", variants)

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
        # build safe base variants first, then add comps params to each
        base_variants = self._build_param_variants(address1=address1, address2=address2, postalcode=postalcode)
        variants: list[Dict[str, Any]] = []
        for v in base_variants:
            vv = dict(v)
            vv["pagesize"] = int(pagesize)
            vv["radius"] = float(radius_miles)
            try:
                from datetime import date, timedelta

                vv["startdate"] = (date.today() - timedelta(days=int(days_back))).isoformat()
            except Exception:
                pass
            variants.append(vv)
        return self._request_with_fallbacks("/salescomparables", tuple(variants))

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
