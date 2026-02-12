# -*- coding: utf-8 -*-
r"""
etl/ddl.py — ETL基盤テーブル（etl_runs / etl_errors）DDL定義

Path   : scripts/work_folder/lib/etl/ddl.py
Project: PHR / work_folder/phr

Purpose:
    - import / apply 共通で使用する実行台帳（etl_runs）と行エラー台帳（etl_errors）のDDLを定義する。
    - アプリ起動時に ensure_tables() で存在保証を行う。

Design (v1.0 as-is):
    - etl_runs は「実行単位」の事実を記録する（1 run = 1 import/apply 実行）
    - etl_errors は「行単位の失敗」を記録する（run_id と紐付く）
    - DDLは IF NOT EXISTS により冪等（複数回実行しても安全）

V1.0 Freeze (Scope / Contract):
    - etl_runs:
        - status は running/success/partial/failed の4値
        - rows_* カラムは実行終了時点の集計値（ProgressLogger/RunMetrics 由来）
        - started_at は start_run 時に確定、finished_at は finish_run 時に確定
    - etl_errors:
        - run_id は NULL 可（start_run 前の致命などを将来許容）
        - src_file/src_row_no/src_line_no は入力由来の証跡
        - person_id_custom は突合キー補助情報
    - Non-goals:
        - 業務テーブル（subscribers 等）のDDL管理
        - マイグレーション管理（ALTERは本モジュールの責務外）
"""

from __future__ import annotations
from typing import Any

Cursor = Any

# v1.0: 実行単位の台帳
# - 1 run = 1 import / apply 実行
# - started_at は start_run、finished_at は finish_run で更新
# - rows_* は RunMetrics の最終値を書き戻す前提
_ETL_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

    phase          ENUM('import', 'apply') NOT NULL,
    source         VARCHAR(190) NOT NULL,
    db_schema      VARCHAR(64) NULL,

    status         ENUM('running', 'success', 'partial', 'failed')
                       NOT NULL
                       DEFAULT 'running',

    started_at     DATETIME(3) NOT NULL
                       DEFAULT CURRENT_TIMESTAMP(3),
    finished_at    DATETIME(3) NULL,

    db_path        VARCHAR(190) NULL,
    input_base     VARCHAR(190) NULL,
    input_file     VARCHAR(190) NULL,
    insurer_number VARCHAR(20) NULL,

    dry_run        TINYINT(1) NULL,
    limit_rows     INT NULL,

    files          INT NOT NULL DEFAULT 0,
    rows_seen      INT NOT NULL DEFAULT 0,
    rows_inserted  INT NOT NULL DEFAULT 0,
    rows_updated   INT NOT NULL DEFAULT 0,
    rows_unchanged INT NOT NULL DEFAULT 0,
    rows_skipped   INT NOT NULL DEFAULT 0,
    errors         INT NOT NULL DEFAULT 0,

    notes          TEXT NULL,
    admin_note     TEXT NULL,

    KEY idx_etl_runs_insurer_started (insurer_number, started_at),
    KEY idx_etl_runs_phase_started   (phase, started_at)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;
"""

# v1.0: 行単位エラー台帳
# - 1 error = 1 行の失敗
# - NormalizeError や想定内例外はここに記録
# - run_id は基本的に etl_runs と紐付く
_ETL_ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS etl_errors (
    error_id        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_id          BIGINT UNSIGNED NULL,

    phase           ENUM('import', 'apply') NOT NULL,
    source          VARCHAR(190) NOT NULL,

    insurer_number  VARCHAR(20) NULL,
    src_file        VARCHAR(190) NULL,
    src_row_no      INT NULL,
    src_line_no     INT NULL,

    staging_rowid   BIGINT NULL,
    person_id_custom VARCHAR(190) NULL,

    field           VARCHAR(190) NULL,
    field_value     TEXT NULL,

    error_code      VARCHAR(190) NULL,
    message         TEXT NULL,

    created_at      DATETIME(3) NOT NULL
                       DEFAULT CURRENT_TIMESTAMP(3),

    KEY idx_etl_errors_run_created (run_id, created_at),
    KEY idx_etl_errors_src         (src_file, src_line_no)
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;
"""

# v1.0: DDL存在保証
# - アプリ起動時に呼び出し、ETL基盤テーブルが無い場合のみ作成する
# - ALTERやバージョン管理はここでは行わない
def ensure_tables(cur: Cursor) -> None:
    cur.execute(_ETL_RUNS_DDL)
    cur.execute(_ETL_ERRORS_DDL)
