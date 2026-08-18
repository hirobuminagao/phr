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


def get_event_age_rule(
    cursor: Any,
    *,
    event_id: int,
    dev_db: str = "dev_phr",
) -> dict[str, Any] | None:
    cursor.execute(
        f"""
        SELECT age_rule_type, age_reference_date
        FROM {qname(dev_db)}.event
        WHERE event_id = %s
        LIMIT 1
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def get_event_year(
    cursor: Any,
    *,
    event_id: int,
    dev_db: str = "dev_phr",
) -> int | None:
    cursor.execute(
        f"""
        SELECT event_year
        FROM {qname(dev_db)}.event
        WHERE event_id = %s
        LIMIT 1
        """,
        (event_id,),
    )
    row = cursor.fetchone()
    if not row or row.get("event_year") is None:
        return None
    try:
        return int(row["event_year"])
    except (TypeError, ValueError):
        return None
