"""
Radaris.com skip-trace — free public-records phone discovery.

Source: radaris.com/ng/search returns name+address+phone rows for free.
Matched by street-name + house-number against the lead's property
address (radaris also returns mailing address which sometimes differs
from property address — we accept either).

Stores in phone_metadata.radaris_lookup:
  {
    "matched_address": "...",
    "matched_name": "...",
    "phones": [{"phone": "+16158760298", "rank": 0}, ...],
    "resolved_at": "...",
  }

If primary `phone` column is empty AND we have a high-confidence
address match, write the top phone to `phone`.

Run:
  python radaris_skiptrace.py [--dry-run] [--limit N] [--target STATE]
"""
import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.bots._base import _supabase

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

FOCUS_COUNTIES = {
    "davidson", "williamson", "sumner", "rutherford", "wilson",
    "maury", "montgomery",
}


def _norm_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


LLC_MARKERS = re.compile(
    r"\b(LLC|L\.L\.C\.|INC|INCORPORATED|CORP|CORPORATION|CO\.|COMPANY|"
    r"PARTNERS|PARTNERSHIP|LP|L\.P\.|LLP|TRUST|FOUNDATION|"
    r"PROPERTIES|HOLDINGS|GROUP|ENTERPRISES|VENTURES|INVESTMENTS|"
    r"CHURCH|MINISTRY|ESTATE OF|DECEASED|BANK|HOA|"
    r"REVOCABLE|IRREVOCABLE|LIVING TRUST|FAMILY TRUST)\b",
    re.IGNORECASE,
)


def _is_individual(name: Optional[str]) -> bool:
    """Return True only if name looks like a real person (not LLC/trust/etc)."""
    if not name:
        return False
    if LLC_MARKERS.search(name):
        return False
    # Reject all-uppercase short tokens that look like business codes
    if re.match(r"^[A-Z0-9 &\-]+$", name) and len(name.split()) > 4:
        return False
    return True


def _parse_owner_name(name: Optional[str]) -> Tuple[str, str]:
    """Best-effort first / last name split. Skips initials/suffixes."""
    if not name:
        return "", ""
    name = re.sub(r"\bJR\.?$|\bSR\.?$|\bI{2,3}$|\bIV$", "", name.strip(), flags=re.I)
    parts = name.strip().split()
    parts = [p for p in parts if len(p) > 1]  # drop "M" middle initials
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def _normalize_addr_token(s: str) -> str:
    """Strip punctuation + lowercase + collapse street suffixes."""
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Collapse common street-type variants
    s = re.sub(r"\b(road|rd|drive|dr|street|st|avenue|ave|av|"
               r"boulevard|blvd|court|ct|lane|ln|place|pl|circle|cir|"
               r"parkway|pkwy|terrace|ter|trail|trl|highway|hwy|way|"
               r"point|pt|park|pk)\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _addr_matches(lead_addr: str, radaris_addr: str) -> bool:
    """Return True if street-number + street-name match."""
    if not lead_addr or not radaris_addr:
        return False
    lead_first = lead_addr.split(",")[0].strip()
    rad_first = radaris_addr.split(",")[0].strip()
    # Compare house numbers
    lm = re.match(r"^(\d+)", lead_first)
    rm = re.match(r"^(\d+)", rad_first)
    if not lm or not rm or lm.group(1) != rm.group(1):
        return False
    lead_norm = _normalize_addr_token(re.sub(r"^\d+\s*", "", lead_first))
    rad_norm = _normalize_addr_token(re.sub(r"^\d+\s*", "", rad_first))
    # Tokens must overlap meaningfully (intersection >= 1 word)
    lead_tokens = set(t for t in lead_norm.split() if len(t) > 1)
    rad_tokens = set(t for t in rad_norm.split() if len(t) > 1)
    return len(lead_tokens & rad_tokens) >= 1


def _state_from_addr(addr: str) -> str:
    """Extract 2-letter state from address tail."""
    m = re.search(r",\s*([A-Z]{2})\s+\d{5}", addr or "")
    return m.group(1) if m else "TN"


def _fmt_phone_e164(s: str) -> Optional[str]:
    digits = re.sub(r"\D", "", s)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def _radaris_search(
    session: requests.Session, first: str, last: str, state: str
) -> List[Dict[str, Any]]:
    """Hit radaris and return list of {name, address, phones[]}."""
    url = (
        f"https://radaris.com/ng/search?"
        f"ff={requests.utils.quote(first)}"
        f"&fl={requests.utils.quote(last)}"
        f"&fs={state}"
    )
    try:
        r = session.get(url, timeout=15)
    except Exception as e:
        print(f"    radaris fetch error: {e}")
        return []
    if r.status_code == 429:
        print(f"    radaris 429 — backing off 60s")
        time.sleep(60)
        return []
    if r.status_code != 200:
        print(f"    radaris status {r.status_code}")
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select("tr.name-address-phone_report")
    out: List[Dict[str, Any]] = []
    for row in rows:
        name_el = row.select_one(".name-address-phone__name")
        addr_el = row.select_one(".name-address-phone__address")
        phone_el = row.select_one(".name-address-phone__phone")
        if not (name_el and addr_el):
            continue
        name = name_el.get_text(" ", strip=True)
        # Strip "(age XX)" suffix
        name = re.sub(r",?\s*age\s+\d+\s*$", "", name, flags=re.I).strip()
        addr = addr_el.get_text(" ", strip=True)
        phones_raw = phone_el.get_text(" ", strip=True) if phone_el else ""
        # Phones separated by bullet/diamond chars in radaris
        phones = re.findall(r"\(\d{3}\)\s?\d{3}-\d{4}", phones_raw)
        out.append({"name": name, "address": addr, "phones": phones})
    return out


def _candidates(client) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for table in ("homeowner_requests", "homeowner_requests_staging"):
        page = 0
        while True:
            try:
                q = (
                    client.table(table)
                    .select(
                        "id, county, property_address, "
                        "owner_name_records, full_name, phone, phone_metadata"
                    )
                    .not_.is_("property_address", "null")
                    .range(page * 1000, (page + 1) * 1000 - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                for r in rows:
                    if _norm_county(r.get("county")) not in FOCUS_COUNTIES:
                        continue
                    # Eligibility:
                    # - must have an owner name (and look like an individual)
                    # - either no phone OR phone is empty string
                    # - skip if already radaris-checked
                    owner = (
                        r.get("owner_name_records") or r.get("full_name") or ""
                    )
                    if not owner or not _is_individual(owner):
                        continue
                    pm = r.get("phone_metadata") or {}
                    if isinstance(pm, dict) and pm.get("radaris_lookup"):
                        continue
                    phone = (r.get("phone") or "").strip()
                    if phone and len(re.sub(r"\D", "", phone)) >= 10:
                        # Already has good phone — still useful as alt-phone source
                        # but lower priority
                        r["__has_phone__"] = True
                    else:
                        r["__has_phone__"] = False
                    r["__table__"] = table
                    out.append(r)
                if len(rows) < 1000:
                    break
                page += 1
            except Exception as e:
                print(f"[warn] candidate fetch {table}: {e}")
                break
    # Phone-less leads first
    out.sort(key=lambda r: (r.get("__has_phone__", False), r["id"]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument(
        "--phoneless-only", action="store_true",
        help="Only hit leads currently missing a phone",
    )
    args = ap.parse_args()

    client = _supabase()
    if client is None:
        print("[fatal] no supabase client")
        sys.exit(1)

    candidates = _candidates(client)
    if args.phoneless_only:
        candidates = [c for c in candidates if not c.get("__has_phone__")]
    print(f"[info] {len(candidates)} MTN leads to check on radaris")

    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Accept": "text/html",
        "Accept-Language": "en-US,en;q=0.9",
    })

    matched = 0
    no_match = 0
    new_phone = 0
    update_failed = 0
    skipped_no_name = 0

    for i, lead in enumerate(candidates[: args.limit]):
        addr = lead["property_address"]
        owner = lead.get("owner_name_records") or lead.get("full_name") or ""
        first, last = _parse_owner_name(owner)
        if not first or not last:
            skipped_no_name += 1
            continue
        state = _state_from_addr(addr)

        results = _radaris_search(session, first, last, state)
        if not results:
            print(f"  [{i+1}/{args.limit}] {owner[:25]:27s} {addr[:50]:53s} no radaris hit")
            no_match += 1
            time.sleep(4 + (i % 3))  # polite — radaris has rate limit signals
            continue

        # Find best address match
        best = None
        for r in results:
            if _addr_matches(addr, r["address"]):
                best = r
                break
        if not best:
            # Fallback: zip-code only match
            zip_m = re.search(r"\b(\d{5})\b", addr)
            if zip_m:
                lead_zip = zip_m.group(1)
                for r in results:
                    if lead_zip in r["address"]:
                        best = r
                        break
        if not best:
            no_match += 1
            print(
                f"  [{i+1}/{args.limit}] {owner[:25]:27s} {addr[:50]:53s} "
                f"no addr match ({len(results)} TN results)"
            )
            time.sleep(4 + (i % 3))
            continue

        # Build phone list (E.164)
        e164_phones = []
        for p in best["phones"]:
            e = _fmt_phone_e164(p)
            if e and e not in e164_phones:
                e164_phones.append(e)

        # Update phone_metadata
        pm = lead.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}
        pm["radaris_lookup"] = {
            "matched_address": best["address"],
            "matched_name": best["name"],
            "phones": [{"phone": p, "rank": idx} for idx, p in enumerate(e164_phones)],
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }

        update: Dict[str, Any] = {"phone_metadata": pm}

        # If lead has no primary phone, write the top radaris phone
        primary_phone = (lead.get("phone") or "").strip()
        wrote_primary = False
        if (not primary_phone or len(re.sub(r"\D", "", primary_phone)) < 10) and e164_phones:
            update["phone"] = e164_phones[0]
            wrote_primary = True

        if args.dry_run:
            star = " *NEW*" if wrote_primary else ""
            print(
                f"  [{i+1}/{args.limit}] {owner[:25]:27s} MATCH: {best['address'][:45]:48s} "
                f"phones={e164_phones[:2]}{star} (DRY)"
            )
            matched += 1
            if wrote_primary:
                new_phone += 1
            time.sleep(4 + (i % 3))
            continue

        try:
            client.table(lead["__table__"]).update(update).eq(
                "id", lead["id"]
            ).execute()
            matched += 1
            if wrote_primary:
                new_phone += 1
            star = " *NEW*" if wrote_primary else ""
            print(
                f"  [{i+1}/{args.limit}] {owner[:25]:27s} MATCH: "
                f"{best['address'][:45]:48s} phones={e164_phones[:2]}{star}"
            )
        except Exception as e:
            update_failed += 1
            print(
                f"  [{i+1}/{args.limit}] {owner[:25]:27s} update_failed: {e}"
            )
        time.sleep(4 + (i % 3))

    print()
    print(
        f"matched={matched} new_phone={new_phone} no_match={no_match} "
        f"skipped_no_name={skipped_no_name} update_failed={update_failed}"
    )


if __name__ == "__main__":
    main()
