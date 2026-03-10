import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from src.packaging.data_quality import assess_packet_data

MAIN_REPO = Path(r"C:\code\falco-distress-bots")
SITE_REPO = Path(r"C:\code\falco-site")

DB_PATH = MAIN_REPO / "data" / "falco.db"
PACKETS_ROOT = MAIN_REPO / "out" / "packets"
SITE_PACKET_DIR = SITE_REPO / "private" / "vault" / "packets"
SITE_DATA_DIR = SITE_REPO / "data"
SITE_LISTINGS_FILE = SITE_DATA_DIR / "vault_listings.ndjson"

MAX_LISTINGS = 25


def slugify(text: str) -> str:
    s = (text or "").lower().strip()
    out = []
    prev_dash = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append("-")
                prev_dash = True
    slug = "".join(out).strip("-")
    return slug or "listing"


def ensure_dirs() -> None:
    SITE_PACKET_DIR.mkdir(parents=True, exist_ok=True)
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not SITE_LISTINGS_FILE.exists():
        SITE_LISTINGS_FILE.write_text("", encoding="utf-8")


def load_existing_listings() -> dict[str, dict]:
    rows: dict[str, dict] = {}
    if not SITE_LISTINGS_FILE.exists():
        return rows

    for line in SITE_LISTINGS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            if isinstance(row, dict) and row.get("slug"):
                rows[row["slug"]] = row
        except Exception:
            continue
    return rows


def write_listings(rows: list[dict]) -> None:
    payload = "\n".join(json.dumps(r) for r in rows)
    SITE_LISTINGS_FILE.write_text(payload + ("\n" if payload else ""), encoding="utf-8")


def packet_for_lead(lead_key: str) -> Path | None:
    candidates = sorted(
        PACKETS_ROOT.rglob(f"{lead_key}.pdf"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def latest_prov_text(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> str | None:
    row = cur.execute(
        """
        SELECT field_value_text
        FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def latest_prov_num(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> float | None:
    row = cur.execute(
        """
        SELECT field_value_num
        FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = ?
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def latest_contact_ready(cur: sqlite3.Cursor, lead_key: str) -> str | None:
    return latest_prov_text(cur, lead_key, "contact_ready")


def latest_attom_snapshot(cur: sqlite3.Cursor, lead_key: str) -> dict[str, object | None]:
    row = cur.execute(
        """
        SELECT attom_raw_json, avm_value, avm_low, avm_high
        FROM attom_enrichments
        WHERE lead_key = ?
        ORDER BY enriched_at DESC, id DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    return {
        "attom_raw_json": row[0] if row and row[0] is not None else None,
        "value_anchor_mid": float(row[1]) if row and row[1] is not None else None,
        "value_anchor_low": float(row[2]) if row and row[2] is not None else None,
        "value_anchor_high": float(row[3]) if row and row[3] is not None else None,
    }


def derive_status(existing_row: dict, dts_days) -> str:
    if existing_row.get("status") == "claimed":
        return "claimed"

    if dts_days is not None:
        try:
            if float(dts_days) < 0:
                return "expired"
        except Exception:
            pass

    return "active"


def masked_title(county: str, distress_type: str) -> str:
    c = county or "Target County"
    d = distress_type or "Distress Opportunity"
    return f"{c} {d}"


def build_summary(county: str, distress_type: str, dts_days, readiness: str, falco_score, contact_ready: bool) -> str:
    dts_txt = f"{int(dts_days)} days" if dts_days is not None else "unknown timing"
    score_txt = f"FALCO score {int(falco_score)}" if falco_score is not None else "score pending"
    contact_txt = "contact ready" if contact_ready else "contact pending"
    return (
        f"{distress_type or 'Distress'} opportunity in {county or 'target market'} with "
        f"{readiness or 'unknown'} readiness, {score_txt}, {contact_txt}, and auction timing of {dts_txt}."
    )


def build_teaser(county: str, readiness: str, falco_score, dts_days) -> str:
    parts = [
        f"County: {county or 'Unknown'}",
        f"Readiness: {readiness or 'Unknown'}",
    ]
    if falco_score is not None:
        parts.append(f"FALCO Score: {int(falco_score)}")
    if dts_days is not None:
        parts.append(f"Auction In: {int(dts_days)} days")
    return " • ".join(parts)


def main() -> None:
    ensure_dirs()
    existing = load_existing_listings()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT
            lead_key,
            address,
            county,
            state,
            auction_readiness,
            dts_days,
            distress_type,
            falco_score_internal,
            equity_band
        FROM leads
        WHERE COALESCE(auction_readiness, '') IN ('GREEN', 'YELLOW', 'PARTIAL')
        ORDER BY
            CASE auction_readiness
                WHEN 'GREEN' THEN 1
                WHEN 'YELLOW' THEN 2
                WHEN 'PARTIAL' THEN 3
                ELSE 4
            END,
            COALESCE(dts_days, 9999) ASC,
            COALESCE(first_seen_at, '') DESC
        LIMIT ?
        """,
        (MAX_LISTINGS,),
    ).fetchall()

    out_rows: list[dict] = []
    copied = 0
    skipped_no_packet = 0

    for (
        lead_key,
        address,
        county,
        state,
        readiness,
        dts_days,
        distress_type,
        falco_score,
        equity_band,
    ) in rows:
        packet_path = packet_for_lead(lead_key)
        if not packet_path:
            skipped_no_packet += 1
            continue

        contact_ready = latest_contact_ready(cur, lead_key) == "1"
        attom = latest_attom_snapshot(cur, lead_key)
        title = masked_title(county or "", distress_type or "")
        slug = f"{slugify(title)}-{lead_key[:8]}"
        base = existing.get(slug, {})

        quality = assess_packet_data(
            {
                "address": address or "",
                "county": county,
                "distress_type": distress_type,
                "falco_score_internal": falco_score,
                "auction_readiness": readiness,
                "equity_band": equity_band,
                "dts_days": dts_days,
                "contact_ready": contact_ready,
                "attom_raw_json": attom["attom_raw_json"],
                "value_anchor_mid": attom["value_anchor_mid"],
                "value_anchor_low": attom["value_anchor_low"],
                "value_anchor_high": attom["value_anchor_high"],
                "property_identifier": latest_prov_text(cur, lead_key, "property_identifier"),
                "owner_name": latest_prov_text(cur, lead_key, "owner_name"),
                "owner_mail": latest_prov_text(cur, lead_key, "owner_mail"),
                "last_sale_date": latest_prov_text(cur, lead_key, "last_sale_date"),
                "mortgage_lender": latest_prov_text(cur, lead_key, "mortgage_lender"),
                "year_built": latest_prov_num(cur, lead_key, "year_built"),
                "building_area_sqft": latest_prov_num(cur, lead_key, "building_area_sqft"),
                "beds": latest_prov_num(cur, lead_key, "beds"),
                "baths": latest_prov_num(cur, lead_key, "baths"),
                "trustee_phone_public": latest_prov_text(cur, lead_key, "trustee_phone_public"),
                "owner_phone_primary": latest_prov_text(cur, lead_key, "owner_phone_primary"),
                "owner_phone_secondary": latest_prov_text(cur, lead_key, "owner_phone_secondary"),
                "notice_phone": latest_prov_text(cur, lead_key, "notice_phone"),
            }
        )
        if not quality["vault_publish_ready"] and not base:
            continue
        enriched_fields = quality.get("enriched_fields", {})
        published_readiness = readiness
        if readiness == "GREEN" and not quality["top_tier_ready"]:
            published_readiness = "YELLOW"

        packet_file_name = f"{slug}.pdf"
        site_packet_path = SITE_PACKET_DIR / packet_file_name
        shutil.copy2(packet_path, site_packet_path)
        copied += 1

        status = derive_status(base, dts_days)
        market = f"{county or 'Unknown County'}, {state or 'TN'}"
        auction_window = f"{int(dts_days)} Days" if dts_days is not None else "Confidential"

        created_at = base.get("createdAt")
        if not created_at:
            created_at = datetime.fromtimestamp(packet_path.stat().st_mtime, UTC).isoformat().replace("+00:00", "Z")

        row = {
            "slug": slug,
            "title": title,
            "market": market,
            "county": county or "",
            "status": status,
            "distressType": distress_type or "Distress Opportunity",
            "auctionWindow": auction_window,
            "summary": build_summary(
                county or "",
                distress_type or "Distress",
                dts_days,
                published_readiness or "",
                falco_score,
                contact_ready,
            ),
            "publicTeaser": build_teaser(county or "", published_readiness or "", falco_score, dts_days),
            "packetUrl": f"/api/vault/packet?slug={slug}",
            "packetLabel": "Auction Opportunity Brief",
            "packetFileName": packet_file_name,
            "sourceLeadKey": lead_key,
            "createdAt": created_at,
            "expiresAt": base.get("expiresAt", ""),
            "claimedAt": base.get("claimedAt", "") if status == "claimed" else "",
            "claimedBy": base.get("claimedBy", "") if status == "claimed" else "",
            "falcoScore": float(falco_score) if falco_score is not None else None,
            "auctionReadiness": published_readiness or "",
            "equityBand": equity_band or "",
            "dtsDays": int(dts_days) if dts_days is not None else None,
            "contactReady": contact_ready,
            "propertyIdentifier": enriched_fields.get("property_identifier"),
            "ownerName": enriched_fields.get("owner_name"),
            "ownerMail": enriched_fields.get("owner_mail"),
            "lastSaleDate": enriched_fields.get("last_sale_date"),
            "mortgageLender": enriched_fields.get("mortgage_lender"),
            "yearBuilt": enriched_fields.get("year_built"),
            "buildingAreaSqft": enriched_fields.get("building_area_sqft"),
            "beds": enriched_fields.get("beds"),
            "baths": enriched_fields.get("baths"),
            "topTierReady": bool(quality["top_tier_ready"]),
            "vaultPublishReady": bool(quality["vault_publish_ready"]),
            "dataNotes": quality["vault_publish_blockers"][:4],
        }
        out_rows.append(row)

    con.close()
    write_listings(out_rows)

    print(f"synced_listings={len(out_rows)}")
    print(f"copied_packets={copied}")
    print(f"skipped_no_packet={skipped_no_packet}")
    print(f"vault_registry={SITE_LISTINGS_FILE}")
    print(f"private_site_packets={SITE_PACKET_DIR}")


if __name__ == "__main__":
    main()
