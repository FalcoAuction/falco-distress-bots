"""Light columns first, the heavy JSON only for the rows that need it.

The staging table is about 50,000 rows at roughly 1 KB of raw_payload and
1 KB of phone_metadata each. A bot that selects both for every row downloads
over 100 MB to act on a handful. The pattern here: scan the same window with
plain columns and the few JSON paths the decision reads, then fetch the full
object by id for the rows that will actually be worked.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

STAGING = "homeowner_requests_staging"


def in_play(row: Dict[str, Any]) -> bool:
    """False for a rejected staging row. Live rows carry no staging_status."""
    return row.get("staging_status") != "rejected"


def full_rows(client, table: str, columns: str, ids: Iterable[str], batch: int = 100) -> Dict[str, Dict[str, Any]]:
    """{id: row} with `columns`, read in small batches by primary key."""
    ids = list(ids)
    out: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(ids), batch):
        page = client.table(table).select(columns).in_("id", ids[start:start + batch]).execute().data or []
        out.update((r["id"], r) for r in page)
    return out


def phone_metadata_now(client, table: str, row_id: str) -> Optional[Dict[str, Any]]:
    """The whole phone_metadata of one row as it stands now, for a bot whose
    scan carried only its own key and that is about to merge and write.
    {} when the column is empty, None when the row no longer exists."""
    row = full_rows(client, table, "id,phone_metadata", [row_id]).get(row_id)
    if row is None:
        return None
    meta = row.get("phone_metadata")
    return meta if isinstance(meta, dict) else {}


def paths(column: str, keys: Iterable[str], prefix: str) -> str:
    """Select-list entries "<prefix><key>:<column>-><key>" for each key."""
    return ",".join(f"{prefix}{k}:{column}->{k}" for k in keys)


def unpack(row: Dict[str, Any], keys: Iterable[str], prefix: str) -> Dict[str, Any]:
    """Pop the aliased paths off `row` and return them as the object they
    came from. A key that is absent or JSON null in the database is absent
    here, which is what `obj.get(key)` returned for it before."""
    out: Dict[str, Any] = {}
    for k in keys:
        value = row.pop(prefix + k, None)
        if value is not None:
            out[k] = value
    return out
