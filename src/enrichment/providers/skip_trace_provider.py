from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class SkipTraceResult:
    owner_phone_primary: Optional[str] = None
    owner_phone_secondary: Optional[str] = None
    owner_phone_source: Optional[str] = None
    owner_phone_confidence: Optional[str] = None


class SkipTraceProvider:
    def trace(self, address: str, owner_name: Optional[str] = None) -> SkipTraceResult:
        raise NotImplementedError


class NullSkipTraceProvider(SkipTraceProvider):
    def trace(self, address: str, owner_name: Optional[str] = None) -> SkipTraceResult:
        return SkipTraceResult()


class BatchDataProvider(SkipTraceProvider):
    _URL = "https://api.batchdata.com/api/v1/property/skip-trace"

    def __init__(self, api_key: str) -> None:
        self._key = api_key

    def _parse_address(self, address: str) -> dict[str, str]:
        raw = (address or "").strip()
        if not raw:
            return {}

        parts = [part.strip() for part in raw.split(",") if part.strip()]
        street = parts[0] if parts else raw
        city = ""
        state = "TN"
        zip_code = ""

        if len(parts) >= 2:
            city = re.sub(r"\bcounty\b", "", parts[1], flags=re.I).strip()
            city = re.sub(r"\s+", " ", city).strip(" ,")

        if len(parts) >= 3:
            state_zip = parts[2]
            match = re.search(r"\b([A-Z]{2})\b", state_zip.upper())
            if match:
                state = match.group(1)
            zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", state_zip)
            if zip_match:
                zip_code = zip_match.group(1)

        if not city and len(parts) == 1:
            city_match = re.match(r"^(.*?),\s*([^,]+)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?$", raw, flags=re.I)
            if city_match:
                street = city_match.group(1).strip()
                city = city_match.group(2).strip()
                state = city_match.group(3).upper()
                zip_code = city_match.group(4)

        payload = {"street": street, "state": state}
        if city:
            payload["city"] = city
        if zip_code:
            payload["zip"] = zip_code
        return payload

    @staticmethod
    def _select_phone(phones: list[dict]) -> tuple[Optional[str], Optional[str], Optional[str]]:
        ranked = []
        for phone in phones:
            if not isinstance(phone, dict):
                continue
            number = str(phone.get("number") or phone.get("phone") or "").strip()
            if not number:
                continue
            score = int(phone.get("score") or 0)
            tested = bool(phone.get("tested"))
            reachable = bool(phone.get("reachable"))
            dnc = bool(phone.get("dnc"))
            ranked.append(
                (
                    1 if not dnc else 0,
                    1 if reachable else 0,
                    1 if tested else 0,
                    score,
                    number,
                    "high" if reachable and not dnc else "medium" if not dnc else "low",
                )
            )

        ranked.sort(reverse=True)
        if not ranked:
            return None, None, None

        primary = ranked[0][4]
        confidence = ranked[0][5]
        secondary = ranked[1][4] if len(ranked) > 1 else None
        return primary, secondary, confidence

    def trace(self, address: str, owner_name: Optional[str] = None) -> SkipTraceResult:
        property_address = self._parse_address(address)
        if not property_address:
            return SkipTraceResult()

        payload = {"requests": [{"propertyAddress": property_address}]}
        if owner_name:
            payload["requests"][0]["ownerName"] = owner_name

        resp = requests.post(
            os.environ.get("FALCO_BATCHDATA_SKIPTRACE_URL", self._URL),
            headers={"Authorization": f"Bearer {self._key}", "Content-Type": "application/json"},
            json=payload,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results") or {}
        persons = results.get("persons") if isinstance(results, dict) else None
        first = persons[0] if isinstance(persons, list) and persons else {}
        phones = first.get("phoneNumbers") or first.get("phones") or first.get("ownerPhones") or []
        primary, secondary, confidence = self._select_phone(phones if isinstance(phones, list) else [])

        return SkipTraceResult(
            owner_phone_primary=primary,
            owner_phone_secondary=secondary,
            owner_phone_source="BatchData",
            owner_phone_confidence=confidence,
        )


def get_skip_trace_provider() -> SkipTraceProvider:
    default_provider = "batchdata" if os.environ.get("FALCO_BATCHDATA_API_KEY", "").strip() else "null"
    prov = os.environ.get("FALCO_SKIP_TRACE_PROVIDER", default_provider).strip().lower()
    if prov == "batchdata":
        api_key = os.environ.get("FALCO_SKIP_TRACE_API_KEY", "").strip() or os.environ.get("FALCO_BATCHDATA_API_KEY", "").strip()
        if not api_key:
            return NullSkipTraceProvider()
        return BatchDataProvider(api_key)
    return NullSkipTraceProvider()
