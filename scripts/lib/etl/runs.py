# -*- coding: utf-8 -*-
r"""
etl/runs.py — ETL実行ライフサイクル管理（start_run / finish_run）

Path   : scripts/lib/etl/runs.py
Project: PHR

Notes:
    - scripts/work_folder/lib/etl/runs.py から scripts/lib/etl/runs.py へコピーして共通化した版
    - import パスは scripts.lib.etl.* を正とする

Purpose:
    - etl_runs テーブルに対する「実行開始」「実行終了」操作を一元管理する。
    - import / apply などの上位スクリプトから呼び出される基盤層。

Design (v1.0 as-is):
    - start_run は必ず status='running' で1行INSERTする
    - finish_run は RunMetrics を元に status / rows_* を確定する
    - status 判定ロジックは _decide_status に集約

V1.0 Freeze (Scope / Contract):
    - start_run:
        - ensure_tables() を必ず呼び、DDL存在保証後にINSERT
        - run_id（AUTO_INCREMENT）を返す
        - 呼び出し側が commit する前提（本関数では commit しない）
    - finish_run:
        - run_id に対して UPDATE を実行
        - finished_at は CURRENT_TIMESTAMP(3) で確定
        - notes は追記方式（既存notesがあれば改行連結）
    - Status policy:
        - errors > 0 かつ changed=0 → failed
        - errors > 0 かつ changed>0 → partial
        - errors=0 かつ changed>0 → success
        - それ以外 → failed
    - Non-goals:
        - commit/rollback 制御（呼び出し側の責務）
        - etl_errors への記録（errors.py 側の責務）
"""

from __future__ import annotations

from typing import Any, Optional

from .ddl import ensure_tables
from .metrics import RunMetrics

Cursor = Any

# v1.0: 実行開始
# - etl_runs に 1 行 INSERT（status='running'）
# - commit は呼び出し側で行う
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

# v1.0: ステータス判定ロジック（RunMetrics → status文字列）
# - changed = rows_inserted + rows_updated
def _decide_status(metrics: RunMetrics) -> str:
    changed = metrics.rows_inserted + metrics.rows_updated

    if metrics.errors > 0:
        return "partial" if changed > 0 else "failed"
    if changed > 0:
        return "success"
    return "failed"

# v1.0: 実行終了処理
# - RunMetrics の最終値を書き戻す
# - finished_at を確定し、status を更新する
# - notes は追記方式（NULL/空文字を考慮）
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
