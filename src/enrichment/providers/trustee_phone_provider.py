# src/enrichment/providers/trustee_phone_provider.py
#
# Tier-2 contact enrichment: public trustee / law-firm phone lookup.
#
# Providers
# ---------
#   NullTrusteePhoneProvider   — always returns None (for FALCO_TRUSTEE_PHONE_PROVIDER=null)
#   TableTrusteePhoneProvider  — built-in TN foreclosure firm table (default)
#
# Env var
# -------
#   FALCO_TRUSTEE_PHONE_PROVIDER = "table" | "null"   (default: "table")

from __future__ import annotations

import os
import re
from typing import Optional


class TrusteePhoneProvider:
    def lookup(self, firm_name: str) -> Optional[str]:
        raise NotImplementedError


class NullTrusteePhoneProvider(TrusteePhoneProvider):
    def lookup(self, firm_name: str) -> Optional[str]:
        return None


# ---------------------------------------------------------------------------
# Built-in TN foreclosure trustee / law-firm table
# ---------------------------------------------------------------------------
# Each entry is (normalized_token, NXX-NXX-XXXX).
# Token match is substring: if token appears anywhere in the normalized firm name,
# the phone is returned.  Entries are ordered from most-specific to least-specific
# so the first match wins.
# ---------------------------------------------------------------------------
_TN_FIRM_TABLE: list[tuple[str, str]] = [
    # ── National default service firms (active in TN) ───────────────────────
    ("rubin lublin",              "615-390-7585"),
    ("wilson & associates",       "501-219-9388"),
    ("wilson and associates",     "501-219-9388"),
    ("logs legal group",          "770-220-2535"),
    ("logs",                      "770-220-2535"),
    ("shapiro ingle",             "704-333-8107"),
    ("shapiro & ingle",           "704-333-8107"),
    ("brock & scott",             "704-943-7741"),
    ("brock and scott",           "704-943-7741"),
    ("brock scott",               "704-943-7741"),
    ("robertson anschutz",        "877-462-7323"),
    ("robertson & anschutz",      "877-462-7323"),
    ("clear recon corp",          "858-750-7600"),
    ("clear recon",               "858-750-7600"),
    ("lerner sampson",            "704-896-7166"),
    ("lerner & sampson",          "704-896-7166"),
    ("mcmichael taylor gray",     "678-855-4062"),
    ("mcmichael & taylor",        "678-855-4062"),
    ("pendergast law",            "866-999-5059"),
    ("pendergast",                "866-999-5059"),
    ("aldridge pite",             "858-750-7600"),
    ("aldridge & pite",           "858-750-7600"),
    ("howard law",                "855-225-2036"),
    ("mediant management",        "972-535-3085"),
    # ── TN-specific firms ───────────────────────────────────────────────────
    ("mackie wolf zientz",        "615-726-5600"),
    ("mackie wolf",               "615-726-5600"),
    ("western progressive tennessee", "877-237-7878"),
    ("western progressive",       "877-237-7878"),
    ("winchester sellers foster", "615-265-6380"),
    ("hughes watters",            "713-254-3500"),
    ("stites & harbison",         "615-782-2200"),
    ("stites harbison",           "615-782-2200"),
    ("baker donelson",            "615-726-5600"),
    ("bone mcallester",           "615-238-3900"),
    ("bone & mcallester",         "615-238-3900"),
    ("miller & martin",           "423-785-8000"),
    ("miller martin",             "423-785-8000"),
    ("waller lansden",            "615-244-6380"),
    ("waller",                    "615-244-6380"),
    # ── Generic fallback tokens (very low specificity — put last) ───────────
    ("tennessee foreclosure",     "615-259-3800"),
]


def _norm_firm(s: str) -> str:
    """Normalize a firm name for token-based matching."""
    s = s.lower().strip()
    # strip common legal suffixes that add noise
    s = re.sub(r"\b(p\.?l\.?l\.?c\.?|l\.?l\.?c\.?|p\.?c\.?|inc\.?|ltd\.?|llp)\b", " ", s)
    s = re.sub(r"[,\.]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


class TableTrusteePhoneProvider(TrusteePhoneProvider):
    def __init__(self) -> None:
        self._table = list(_TN_FIRM_TABLE)

    def lookup(self, firm_name: str) -> Optional[str]:
        if not firm_name:
            return None
        norm = _norm_firm(firm_name)
        for token, phone in self._table:
            # Word-boundary match to prevent false positives:
            # "wilson" should match "wilson & associates" but not "williamson county"
            if re.search(r"\b" + re.escape(token) + r"\b", norm):
                return phone
        return None


def get_trustee_phone_provider() -> TrusteePhoneProvider:
    prov = os.environ.get("FALCO_TRUSTEE_PHONE_PROVIDER", "table").strip().lower()
    if prov == "null":
        return NullTrusteePhoneProvider()
    return TableTrusteePhoneProvider()
