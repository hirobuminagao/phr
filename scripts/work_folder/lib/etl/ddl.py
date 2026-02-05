# -*- coding: utf-8 -*-
r"""
Path: work_folder/phr/lib/etl/ddl.py
"""

from __future__ import annotations
from typing import Any

Cursor = Any

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

def ensure_tables(cur: Cursor) -> None:
    cur.execute(_ETL_RUNS_DDL)
    cur.execute(_ETL_ERRORS_DDL)
