# src/bots/foreclosure_tennessee_bot.py

import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from ..config import FORECLOSURE_TN_SEED_URL, FORECLOSURE_TN_MAX_PAGES
from ..utils import fetch, make_lead_key
from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..scoring import days_to_sale, detect_risk_flags, triage, score_v2, label


_DATE_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")


def _parse_mmddyyyy(s: str) -> str | None:
    if not s:
        return None
    m = _DATE_RE.match(s.strip())
    if not m:
        return None
    mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
    return f"{yyyy}-{int(mm):02d}-{int(dd):02d}"


def _extract_table_rows(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []

    for a in soup.select('a[href*="Foreclosure-Listing.aspx"][href]'):
        listing_url = urljoin(base_url, a["href"])
        tr = a.find_parent("tr")

        if tr:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
        else:
            cells = [(a.find_parent().get_text(" ", strip=True) if a.find_parent() else "")]

        row_text = " | ".join([c for c in cells if c])
        parts = [p.strip() for p in re.split(r"\s{2,}|\|", row_text) if p.strip()]

        dates = [p for p in parts if _DATE_RE.match(p)]
        sale_date_raw = dates[0] if len(dates) >= 1 else ""
        cont_date_raw = dates[1] if len(dates) >= 2 else ""

        # Remove date parts
        parts_wo_dates = [p for p in parts if p not in set(dates)]

        city = parts_wo_dates[0] if len(parts_wo_dates) > 0 else ""
        address = parts_wo_dates[1] if len(parts_wo_dates) > 1 else ""
        zip_code = parts_wo_dates[2] if len(parts_wo_dates) > 2 else ""
        county = parts_wo_dates[3] if len(parts_wo_dates) > 3 else ""
        trustee = parts_wo_dates[4] if len(parts_wo_dates) > 4 else ""

        out.append(
            {
                "sale_date_iso": _parse_mmddyyyy(sale_date_raw),
                "continuance_date_iso": _parse_mmddyyyy(cont_date_raw),
                "city": city,
                "address": address,
                "zip": zip_code,
                "county": county,
                "trustee": trustee,
                "listing_url": listing_url,
            }
        )

    return out


def run():
    print(f"[ForeclosureTNBot] seed={FORECLOSURE_TN_SEED_URL}")

    if not FORECLOSURE_TN_SEED_URL:
        print("[ForeclosureTNBot] No seed set.")
        return

    created = 0
    updated = 0

    skipped_expired = 0
    skipped_kill = 0
    skipped_no_date = 0

    monitor_written = 0
    green_written = 0
    total_written = 0

    for page in range(1, FORECLOSURE_TN_MAX_PAGES + 1):
        url = FORECLOSURE_TN_SEED_URL if page == 1 else f"{FORECLOSURE_TN_SEED_URL}?page={page}"

        try:
            html = fetch(url)
        except Exception as e:
            print(f"[ForeclosureTNBot] fetch failed page={page} url={url}: {e}")
            if page == 1:
                return
            break

        rows = _extract_table_rows(html, base_url=url)
        print(f"[ForeclosureTNBot] page={page} rows={len(rows)}")
        if page > 1 and len(rows) == 0:
            break

        for r in rows:
            # Prefer continuance date (usually the forward-looking scheduled date)
            sale_date_iso = r["continuance_date_iso"] or r["sale_date_iso"]
            if not sale_date_iso:
                skipped_no_date += 1
                continue

            county = (r["county"] or "TN").strip()
            address = (r["address"] or "").strip()
            trustee = (r["trustee"] or "").strip()
            listing_url = r["listing_url"]

            dts = days_to_sale(sale_date_iso)

            if dts is not None and dts < 0:
                skipped_expired += 1
                continue

            flags = detect_risk_flags(" ".join([address, county, trustee]))
            override_status, reason = triage(dts, flags)
            if override_status == "KILL":
                skipped_kill += 1
                continue

            distress_type = "Foreclosure"
            score = score_v2(distress_type, county, dts, True)

            # Status assignment rule-set
            if dts is not None and dts < 30:
                status = "MONITOR"
            else:
                status = "MONITOR" if override_status == "MONITOR" else label(
                    distress_type, county, dts, flags, score, True
                )

            if status == "MONITOR":
                monitor_written += 1
            else:
                green_written += 1

            title = f"Foreclosure ({status}) ({county})"

            lead_key = make_lead_key(
                "FORECLOSURETN",
                listing_url,
                county,
                sale_date_iso,
                address,
            )

            props = build_properties(
                title=title,
                source="ForeclosureTennessee",
                distress_type=distress_type,
                county=county,
                address=address,
                sale_date_iso=sale_date_iso,
                trustee_attorney=trustee,
                contact_info=trustee if trustee else (reason or ""),
                raw_snippet=f"City={r['city']} Zip={r['zip']} (continuance={r['continuance_date_iso']} orig={r['sale_date_iso']})",
                url=listing_url,
                score=score,
                status=status,
                lead_key=lead_key,
            )

            existing_id = find_existing_by_lead_key(lead_key)
            if existing_id:
                update_lead(existing_id, props)
                updated += 1
            else:
                create_lead(props)
                created += 1

            total_written += 1

    print(
        "[ForeclosureTNBot] summary "
        f"total_written={total_written} green_written={green_written} monitor_written={monitor_written} "
        f"created={created} updated={updated} "
        f"skipped_no_date={skipped_no_date} skipped_expired={skipped_expired} skipped_kill={skipped_kill}"
    )
    print("[ForeclosureTNBot] Done.")
