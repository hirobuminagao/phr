# -*- coding: utf-8 -*-
r"""
ETL 共通ユーティリティ（MySQL 版）
Path: work_folder/phr/lib/etl.py

役割:
    - etl_runs / etl_errors テーブルの存在保証（ensure_tables）
    - ETL 実行単位の開始・終了ログ（start_run / finish_run）
    - 行単位エラーログ（log_error / log_normalize_error）
    - 集計用 RunMetrics dataclass
    - 実行中進捗ログ（ProgressLogger：RunMetricsを参照して表示のみ）

前提:
    - DB は MySQL 8 系
    - 接続は mysql.connector を使用
    - カーソルは dict でも tuple でも動くように、型は緩め (Any) にしている

設計方針（重要）:
    - RunMetrics を「唯一の集計ソース」とする（＝真実は RunMetrics）
    - ProgressLogger は RunMetrics を参照して表示するだけ（自分ではカウントしない）
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Optional

from phr.lib.errors import NormalizeError


# ============================================================
# 型・集計構造
# ============================================================

Cursor = Any  # mysql.connector.cursor.MySQLCursor / MySQLCursorDict 相当


@dataclass
class RunMetrics:
    """1 run 全体の集計値"""
    files: int = 0
    rows_seen: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0  # 変更なし（INSERT/UPDATE されなかったが処理した行）
    rows_skipped: int = 0    # ビジネスルール等で「適用しない」と判断した行
    errors: int = 0


# ============================================================
# 進捗ログ（RunMetricsを参照して表示するだけ）
# ============================================================

class ProgressLogger:
    """
    ETL 進捗を N 件ごとに出す簡易ロガー。

    - RunMetrics を参照して表示するだけ（自分ではカウントしない）
    - interval=0 の場合は無効
    - total は「総件数（分母）」、不明なら 0 でもOK
    """

    def __init__(
        self,
        *,
        total: int,
        metrics: RunMetrics,
        interval: int = 1000,
        logger: Optional[logging.Logger] = None,
        label: str = "ETL",
    ) -> None:
        self.total = int(total) if total is not None else 0
        self.metrics = metrics
        self.interval = int(interval) if interval is not None else 0
        self.logger = logger or logging.getLogger(__name__)
        self.label = label

        self._enabled = self.interval > 0
        self._started_at = time.time()
        self._last_logged_seen = metrics.rows_seen

    def tick(self) -> None:
        """
        ループ内で呼ぶだけ。
        metrics.rows_seen が interval 以上進んでいたらログを出す。
        """
        if not self._enabled:
            return

        seen = self.metrics.rows_seen
        if (seen - self._last_logged_seen) < self.interval:
            return

        self._log()
        self._last_logged_seen = seen

    def finalize(self) -> None:
        """最後に 1 回だけ最終表示を出したい場合に呼ぶ。"""
        if not self._enabled:
            return
        self._log()
        self._last_logged_seen = self.metrics.rows_seen

    def _log(self) -> None:
        elapsed = time.time() - self._started_at
        seen = self.metrics.rows_seen

        rate = (seen / elapsed) if elapsed > 0 else 0.0
        if self.total > 0:
            percent = (seen / self.total) * 100.0
        else:
            percent = 100.0  # 分母不明なら便宜上100%

        msg = (
            f"[{self.label}] "
            f"{seen}/{self.total} ({percent:.2f}%) "
            f"ins={self.metrics.rows_inserted} "
            f"upd={self.metrics.rows_updated} "
            f"unchg={self.metrics.rows_unchanged} "
            f"skp={self.metrics.rows_skipped} "
            f"err={self.metrics.errors} "
            f"rate={rate:.1f}/s"
        )
        self.logger.info(msg)


# ============================================================
# DDL (MySQL 用) - 可能な限り Navicat DDL と一致させる
# ============================================================

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
    """
    etl_runs / etl_errors テーブルが無ければ作成する。
    （Navicat で事前に作ってあっても IF NOT EXISTS なので安全）
    """
    cur.execute(_ETL_RUNS_DDL)
    cur.execute(_ETL_ERRORS_DDL)


# ============================================================
# etl_runs: start / finish
# ============================================================

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
    """
    1 回の ETL 実行の「開始」レコードを etl_runs に登録し、
    run_id を返す。
    """
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
    run_id = int(cur.lastrowid)
    return run_id


def _decide_status(metrics: RunMetrics) -> str:
    """
    metrics から run のステータスを素直に決定する。

    ポリシー:
        - errors > 0 かつ (inserted+updated) > 0 → partial
        - errors > 0 かつ (inserted+updated) == 0 → failed
        - errors == 0 かつ (inserted+updated) > 0 → success
        - それ以外（何も反映されていない） → failed
    """
    changed = metrics.rows_inserted + metrics.rows_updated

    if metrics.errors > 0:
        if changed > 0:
            return "partial"
        return "failed"

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
    """
    ETL 実行終了時に etl_runs を更新する。
    - metrics を反映
    - finished_at を NOW() に更新
    - status は metrics から自動判定（または status_override 優先）
    """
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


def _bump_error_count(cur: Cursor, run_id: int) -> None:
    """
    etl_runs.errors を +1 する。
    """
    cur.execute(
        "UPDATE etl_runs SET errors = errors + 1 WHERE run_id = %s",
        (run_id,),
    )


# ============================================================
# etl_errors: ログ出力
# ============================================================

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
    """
    想定外エラー or 正規化以外のエラーを etl_errors に 1 行出力。
    """
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
    """
    NormalizeError 専用のショートカット。
    NormalizeError の中身（field, code, raw_value, message）を etl_errors に展開する。
    """
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
