"""Lookup helpers for phr_master exam facilities.

The caller owns the DB connection and transaction. This module only performs
SELECTs and returns small facility handles that scan/import code can pass
around safely.
"""

from __future__ import annotations

from typing import Any

from scripts.lib.db.schemas import PHR_MASTER


EXAM_FACILITY_HANDLE_COLUMNS = """
    ef.exam_facility_id,
    ef.exam_facility_code,
    ef.exam_facility_name,
    ef.exam_facility_display_name,
    ef.medical_institution_code
"""


def _compact_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_handle(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    handle = dict(row)
    handle["exam_facility_id"] = int(handle["exam_facility_id"])
    return handle


def get_exam_facility_by_id(
    cur: Any,
    exam_facility_id: int | str | None,
    *,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    """Return an active exam facility handle by `exam_facility_id`."""

    if exam_facility_id is None:
        return None

    cur.execute(
        f"""
        SELECT
            {EXAM_FACILITY_HANDLE_COLUMNS}
        FROM `{master_db}`.`exam_facilities` ef
        WHERE ef.exam_facility_id = %s
          AND ef.is_active = 1
        LIMIT 1
        """,
        (int(exam_facility_id),),
    )
    return _to_handle(cur.fetchone())


def get_exam_facility_by_code(
    cur: Any,
    exam_facility_code: str | None,
    *,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    """Return an active exam facility handle by `exam_facility_code`."""

    code = _compact_text(exam_facility_code)
    if code is None:
        return None

    cur.execute(
        f"""
        SELECT
            {EXAM_FACILITY_HANDLE_COLUMNS}
        FROM `{master_db}`.`exam_facilities` ef
        WHERE ef.exam_facility_code = %s
          AND ef.is_active = 1
        LIMIT 1
        """,
        (code,),
    )
    return _to_handle(cur.fetchone())


def get_exam_facility_by_medical_institution_code(
    cur: Any,
    medical_institution_code: str | None,
    *,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    """Return an active exam facility handle by medical institution code."""

    code = _compact_text(medical_institution_code)
    if code is None:
        return None

    cur.execute(
        f"""
        SELECT
            {EXAM_FACILITY_HANDLE_COLUMNS}
        FROM `{master_db}`.`exam_facilities` ef
        WHERE ef.medical_institution_code = %s
          AND ef.is_active = 1
        ORDER BY ef.exam_facility_id
        LIMIT 1
        """,
        (code,),
    )
    return _to_handle(cur.fetchone())


def get_exam_facility_by_folder_alias(
    cur: Any,
    *,
    event_id: int,
    folder_name: str | None,
    master_db: str = PHR_MASTER,
) -> dict[str, Any] | None:
    """Return an active facility handle by event folder alias."""

    src_folder_raw = _compact_text(folder_name)
    if src_folder_raw is None:
        return None

    cur.execute(
        f"""
        SELECT
            {EXAM_FACILITY_HANDLE_COLUMNS}
        FROM `{master_db}`.`medical_folder_aliases` mfa
        INNER JOIN `{master_db}`.`exam_facilities` ef
          ON ef.exam_facility_id = mfa.exam_facility_id
         AND ef.is_active = 1
        WHERE mfa.event_id = %s
          AND mfa.src_folder_raw = %s
          AND mfa.is_active = 1
        LIMIT 1
        """,
        (event_id, src_folder_raw),
    )
    return _to_handle(cur.fetchone())
