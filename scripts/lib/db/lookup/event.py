"""Event master lookups shared by ETL scripts."""

from __future__ import annotations

from typing import Any

from scripts.lib.examination.lookup import qname


def get_event_insurer_number(
    cursor: Any,
    *,
    event_id: int,
    dev_db: str = "dev_phr",
) -> str | None:
    cursor.execute(
        f"""
        SELECT insurer_number
        FROM {qname(dev_db)}.event
        WHERE event_id = %s
        LIMIT 1
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if not row or row.get("insurer_number") is None:
        return None
    value = str(row["insurer_number"]).strip()
    return value or None
