"""Resolve one export insurer number without changing received ledger values."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Mapping

from scripts.lib.examination.lookup import qname


class InsurerResolutionError(ValueError):
    """Raised when an insurer number cannot be resolved safely."""


@dataclass(frozen=True)
class EventInsurerContext:
    event_insurer_number: str
    fund_id: int | None
    allowed_insurer_numbers: frozenset[str]


def canonical_insurer_number(value: Any) -> str | None:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits or len(digits) > 8 or set(digits) == {"0"}:
        return None
    return digits.zfill(8)


def load_event_insurer_context(
    cursor: Any,
    *,
    event_id: int,
    exam_date: date | str,
    dev_db: str = "dev_phr",
) -> EventInsurerContext:
    cursor.execute(
        f"SELECT insurer_number FROM {qname(dev_db)}.event WHERE event_id = %s LIMIT 1",
        (event_id,),
    )
    event_row = cursor.fetchone()
    event_number = canonical_insurer_number(event_row.get("insurer_number") if event_row else None)
    if event_number is None:
        raise InsurerResolutionError(f"EVENT_INSURER_NUMBER_INVALID: event_id={event_id}")

    cursor.execute(
        """
        SELECT COUNT(*) AS table_count
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = 'fund_insurer_numbers'
        """,
        (dev_db,),
    )
    table_row = cursor.fetchone() or {}
    if int(table_row.get("table_count") or 0) == 0:
        return EventInsurerContext(event_number, None, frozenset({event_number}))

    cursor.execute(
        f"""
        SELECT DISTINCT fund_id
        FROM {qname(dev_db)}.fund_insurer_numbers
        WHERE insurer_number = %s
          AND valid_from <= %s
          AND (valid_to IS NULL OR valid_to >= %s)
        """,
        (event_number, exam_date, exam_date),
    )
    fund_ids = {int(row["fund_id"]) for row in cursor.fetchall()}
    if len(fund_ids) > 1:
        raise InsurerResolutionError(
            f"EVENT_INSURER_FUND_AMBIGUOUS: event_id={event_id} fund_ids={sorted(fund_ids)}"
        )
    if not fund_ids:
        return EventInsurerContext(event_number, None, frozenset({event_number}))

    fund_id = next(iter(fund_ids))
    cursor.execute(
        f"""
        SELECT DISTINCT insurer_number
        FROM {qname(dev_db)}.fund_insurer_numbers
        WHERE fund_id = %s
          AND valid_from <= %s
          AND (valid_to IS NULL OR valid_to >= %s)
        """,
        (fund_id, exam_date, exam_date),
    )
    allowed = {
        number
        for row in cursor.fetchall()
        if (number := canonical_insurer_number(row.get("insurer_number"))) is not None
    }
    allowed.add(event_number)
    return EventInsurerContext(event_number, fund_id, frozenset(allowed))


def _unique_allowed(values: Iterable[Any], allowed: frozenset[str]) -> set[str]:
    return {
        number
        for value in values
        if (number := canonical_insurer_number(value)) is not None and number in allowed
    }


def resolve_case_insurer_number(
    *,
    context: EventInsurerContext,
    subscriber_insurer_number: Any,
    ledgers: Iterable[Mapping[str, Any]],
) -> str:
    ledger_rows = list(ledgers)
    subscriber_number = canonical_insurer_number(subscriber_insurer_number)
    if subscriber_number is not None:
        if subscriber_number not in context.allowed_insurer_numbers:
            raise InsurerResolutionError(
                f"SUBSCRIBER_INSURER_NOT_ALLOWED: insurer_number={subscriber_number}"
            )
        return subscriber_number

    corrected = _unique_allowed(
        (row.get("insurer_number_export_value") for row in ledger_rows),
        context.allowed_insurer_numbers,
    )
    if len(corrected) == 1:
        return next(iter(corrected))
    if len(corrected) > 1:
        raise InsurerResolutionError(f"CORRECTED_INSURER_AMBIGUOUS: values={sorted(corrected)}")

    received = _unique_allowed(
        (row.get("insurer_number") for row in ledger_rows),
        context.allowed_insurer_numbers,
    )
    if len(received) == 1:
        return next(iter(received))
    if len(received) > 1:
        raise InsurerResolutionError(f"RECEIVED_INSURER_AMBIGUOUS: values={sorted(received)}")

    if len(context.allowed_insurer_numbers) == 1:
        return next(iter(context.allowed_insurer_numbers))
    raise InsurerResolutionError(
        "INSURER_NUMBER_UNRESOLVED: "
        f"allowed={sorted(context.allowed_insurer_numbers)}"
    )
