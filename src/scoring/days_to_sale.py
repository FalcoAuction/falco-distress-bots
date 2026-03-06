from __future__ import annotations
from datetime import date
from typing import Optional

def days_to_sale(sale_date_iso: Optional[str], *, today: Optional[date] = None) -> Optional[int]:
    """
    Return integer days until sale_date_iso (YYYY-MM-DD). If invalid/missing, return None.
    """
    if not sale_date_iso:
        return None
    try:
        sale_dt = date.fromisoformat(str(sale_date_iso).strip())
    except Exception:
        return None
    if today is None:
        today = date.today()
    return (sale_dt - today).days

def is_within_window(dts: Optional[int], dts_min: int, dts_max: int) -> bool:
    if dts is None:
        return False
    return dts_min <= dts <= dts_max
