# -*- coding: utf-8 -*-
r"""
Path: work_folder/phr/lib/etl/runs.py
"""

from __future__ import annotations

from typing import Any, Optional

from .ddl import ensure_tables
from .metrics import RunMetrics

Cursor = Any

def start_run(
    cur: Cursor,
    *,
    phase: str,
    source: str,
    db_schema: Optional[str],
    db_path: Optional[str],
    input_base: Optional[str],
    input_file: Optional[str],
    insurer_number: Optional[str],
    dry_run: bool,
    limit_rows: Optional[int],
) -> int:
    ensure_tables(cur)
    cur.execute(
        """
        INSERT INTO etl_runs (
            phase, source, db_schema, status,
            db_path, input_base, input_file, insurer_number,
            dry_run, limit_rows
        )
        VALUES (
            %s, %s, %s, 'running',
            %s, %s, %s, %s,
            %s, %s
        )
        """,
        (
            phase,
            source,
            db_schema,
            db_path,
            input_base,
            input_file,
            insurer_number,
            1 if dry_run else 0,
            limit_rows if limit_rows else None,
        ),
    )
    return int(cur.lastrowid)

def _decide_status(metrics: RunMetrics) -> str:
    changed = metrics.rows_inserted + metrics.rows_updated

    if metrics.errors > 0:
        return "partial" if changed > 0 else "failed"
    if changed > 0:
        return "success"
    return "failed"

def finish_run(
    cur: Cursor,
    run_id: int,
    metrics: RunMetrics,
    *,
    status_override: Optional[str] = None,
    extra_notes: Optional[str] = None,
) -> None:
    status = status_override or _decide_status(metrics)

    cur.execute(
        """
        UPDATE etl_runs
        SET
            status         = %s,
            finished_at    = CURRENT_TIMESTAMP(3),
            files          = %s,
            rows_seen      = %s,
            rows_inserted  = %s,
            rows_updated   = %s,
            rows_unchanged = %s,
            rows_skipped   = %s,
            errors         = %s,
            notes          = CASE
                                WHEN %s IS NULL OR %s = '' THEN notes
                                WHEN notes IS NULL OR notes = '' THEN %s
                                ELSE CONCAT(notes, '\n', %s)
                             END
        WHERE run_id = %s
        """,
        (
            status,
            metrics.files,
            metrics.rows_seen,
            metrics.rows_inserted,
            metrics.rows_updated,
            metrics.rows_unchanged,
            metrics.rows_skipped,
            metrics.errors,
            extra_notes,
            extra_notes,
            extra_notes,
            extra_notes,
            run_id,
        ),
    )
