from __future__ import annotations

from typing import Any, Mapping, Sequence


from scripts.lib.db.schemas import DEV_PHR

LAST_CHANGE_RUN_FIELD = "last_change_run_id"

_REQUIRED_FIELDS = ("subscriber_id", "field")
_AUDIT_COLUMNS = (
    "subscriber_id",
    "field",
    "old_value",
    "new_value",
    "source",
    "note",
    "change_run_id",
)



def audit_value(value: Any) -> str | None:
    """subscriber_audit 保存用に値を文字列化する。"""
    if value is None:
        return None
    return str(value)



def validate_subscriber_audit_row(row: Mapping[str, Any]) -> None:
    """subscriber_audit の 1 行分 dict を検証する。"""
    for field in _REQUIRED_FIELDS:
        if field not in row:
            raise ValueError(f"missing required audit field: {field}")

    subscriber_id = row.get("subscriber_id")
    if subscriber_id is None or str(subscriber_id).strip() == "":
        raise ValueError("subscriber_id is required")

    field_name = row.get("field")
    if field_name is None or str(field_name).strip() == "":
        raise ValueError("field is required")



def build_subscriber_audit_params(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """subscriber_audit INSERT 用パラメータへ変換する。"""
    validate_subscriber_audit_row(row)

    subscriber_id = int(row["subscriber_id"])
    field_name = str(row["field"]).strip()

    return (
        subscriber_id,
        field_name,
        audit_value(row.get("old_value")),
        audit_value(row.get("new_value")),
        audit_value(row.get("source")),
        audit_value(row.get("note")),
        row.get("change_run_id"),
    )



def insert_subscriber_audit_row(cur: Any, row: Mapping[str, Any]) -> None:
    """subscriber_audit に 1 行 INSERT する。"""
    insert_subscriber_audit_rows(cur, [row])



def insert_subscriber_audit_rows(cur: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    """subscriber_audit に複数行 INSERT する。"""
    if not rows:
        return

    params = [build_subscriber_audit_params(row) for row in rows]
    columns_sql = ", ".join(_AUDIT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_AUDIT_COLUMNS))

    cur.executemany(
        f"""
        INSERT INTO {DEV_PHR}.subscriber_audit (
            {columns_sql}
        )
        VALUES (
            {placeholders}
        )
        """,
        params,
    )



def insert_subscriber_audit_rows_and_touch_last_change_run(
    cur: Any,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    """subscriber_audit を INSERT し、subscribers.last_change_run_id も更新する。"""
    if not rows:
        return

    subscriber_id, change_run_id = validate_touch_last_change_rows(rows)
    insert_subscriber_audit_rows(cur, rows)
    touch_subscriber_last_change_run_id(cur, subscriber_id, change_run_id)



def validate_touch_last_change_rows(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[int, int]:
    """last_change_run_id 更新前提の rows を検証する。"""
    first_subscriber_id: int | None = None
    first_change_run_id: int | None = None

    for row in rows:
        validate_subscriber_audit_row(row)

        subscriber_id = int(row["subscriber_id"])
        change_run_id = row.get("change_run_id")
        if change_run_id is None or str(change_run_id).strip() == "":
            raise ValueError("change_run_id is required for touch_last_change_run")
        change_run_id_int = int(change_run_id)

        if first_subscriber_id is None:
            first_subscriber_id = subscriber_id
        elif first_subscriber_id != subscriber_id:
            raise ValueError("all audit rows must have the same subscriber_id")

        if first_change_run_id is None:
            first_change_run_id = change_run_id_int
        elif first_change_run_id != change_run_id_int:
            raise ValueError("all audit rows must have the same change_run_id")

    if first_subscriber_id is None or first_change_run_id is None:
        raise ValueError("rows must not be empty")

    return first_subscriber_id, first_change_run_id



def touch_subscriber_last_change_run_id(
    cur: Any,
    subscriber_id: int,
    change_run_id: int,
) -> None:
    """subscribers.last_change_run_id を更新する。"""
    cur.execute(
        f"""
        UPDATE {DEV_PHR}.subscribers
        SET {LAST_CHANGE_RUN_FIELD} = %s
        WHERE id = %s
        """,
        (change_run_id, subscriber_id),
    )
