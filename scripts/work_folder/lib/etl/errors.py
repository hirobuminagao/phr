# -*- coding: utf-8 -*-
r"""
Path: work_folder/phr/lib/etl/errors.py
"""

from __future__ import annotations

from typing import Any, Optional

from phr.lib.errors import NormalizeError
from .ddl import ensure_tables

Cursor = Any

def _bump_error_count(cur: Cursor, run_id: int) -> None:
    cur.execute(
        "UPDATE etl_runs SET errors = errors + 1 WHERE run_id = %s",
        (run_id,),
    )

def log_error(
    cur: Cursor,
    run_id: int,
    *,
    phase: str,
    source: str,
    insurer_number: Optional[str],
    src_file: Optional[str],
    row_no: Optional[int],
    line_no: Optional[int],
    field: Optional[str],
    field_value: Optional[str],
    error_code: str,
    message: str,
    staging_rowid: Optional[int] = None,
    person_id_custom: Optional[str] = None,
) -> None:
    ensure_tables(cur)

    cur.execute(
        """
        INSERT INTO etl_errors (
            run_id,
            phase, source,
            insurer_number,
            src_file, src_row_no, src_line_no,
            staging_rowid, person_id_custom,
            field, field_value,
            error_code, message
        )
        VALUES (
            %s,
            %s, %s,
            %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
        """,
        (
            run_id,
            phase,
            source,
            insurer_number,
            src_file,
            row_no,
            line_no,
            staging_rowid,
            person_id_custom,
            field,
            field_value,
            error_code,
            message,
        ),
    )
    _bump_error_count(cur, run_id)

def log_normalize_error(
    cur: Cursor,
    run_id: int,
    *,
    phase: str,
    source: str,
    insurer_number: Optional[str],
    src_file: Optional[str],
    row_no: Optional[int],
    line_no: Optional[int],
    err: NormalizeError,
    staging_rowid: Optional[int] = None,
    person_id_custom: Optional[str] = None,
) -> None:
    ensure_tables(cur)

    cur.execute(
        """
        INSERT INTO etl_errors (
            run_id,
            phase, source,
            insurer_number,
            src_file, src_row_no, src_line_no,
            staging_rowid, person_id_custom,
            field, field_value,
            error_code, message
        )
        VALUES (
            %s,
            %s, %s,
            %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
        """,
        (
            run_id,
            phase,
            source,
            insurer_number,
            src_file,
            row_no,
            line_no,
            staging_rowid,
            person_id_custom,
            err.field,
            err.raw_value,
            err.code,
            str(err),
        ),
    )
    _bump_error_count(cur, run_id)
