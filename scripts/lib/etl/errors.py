# -*- coding: utf-8 -*-
r"""
etl/errors.py — ETL行エラー記録ユーティリティ

Path   : scripts/lib/etl/errors.py
Project: PHR

Notes:
    - scripts/work_folder/lib/etl/errors.py から scripts/lib/etl/errors.py へコピーして共通化した版
    - import パスは scripts.lib.etl.* を正とする

Purpose:
    - ETL処理中に発生した「行単位の失敗」を etl_errors テーブルへ記録する。
    - 呼び出し側が field / error_code / message を明示して構造化保存する。

Design (v1.0 as-is):
    - ensure_tables() を呼び、DDL存在を保証してから INSERT する
    - INSERT 後に etl_runs.errors を +1 する
    - commit/rollback は呼び出し側の責務

V1.0 Freeze (Scope / Contract):
    - 1 error = 1 etl_errors レコード
    - run_id は基本的に start_run 済みの値を前提
    - phase/source は呼び出し側が定義する文字列（import/apply 等）
    - field / field_value / error_code / message は呼び出し側が明示する
    - person_id_custom は突合キー補助情報として任意で保存
    - 本モジュールはステータス判定を行わない（runs.py 側の責務）

Non-goals:
    - エラーの重複排除
    - ログ出力（logging）
    - 実行ステータス更新（finish_run の責務）
"""

from __future__ import annotations

from typing import Any, Optional

from .ddl import ensure_tables

# v1.0: etl_runs.errors を +1 する内部関数
# - 1 etl_errors INSERT ごとに必ず呼ばれる
Cursor = Any

def _bump_error_count(cur: Cursor, run_id: int) -> None:
    cur.execute(
        "UPDATE etl_runs SET errors = errors + 1 WHERE run_id = %s",
        (run_id,),
    )

# v1.0: 汎用エラー記録
# - 呼び出し側が field / error_code / message を明示指定するケース
# - INSERT 後に errors カウンタを増やす
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
