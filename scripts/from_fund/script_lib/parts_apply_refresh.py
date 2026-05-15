#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parts_apply_refresh.py

staging_subscribers_fund の parts_apply_* 再確認処理。

責務:
- import_run_id 単位で対象 staging 行を取得する
- parts_apply_* を初期化する
- matched_subscriber_id と identity_hash を再確認する
- parts_apply_subscriber_id / status / reason を更新する
- dry_run 時はDB更新を行わず、判定metricsのみ返す

非責務:
- subscribers の name parts 更新
- import 時点の matched_subscriber_id 判定
- identity_hash 生成
- 新規 subscribers 作成
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from scripts.lib.db.mysql import dict_cursor


STATUS_IDENTITY_MATCHED = "IDENTITY_MATCHED"
STATUS_IDENTITY_CHANGED = "IDENTITY_CHANGED"
STATUS_SUBSCRIBER_NOT_FOUND = "SUBSCRIBER_NOT_FOUND"
STATUS_MISSING_IDENTITY_HASH = "MISSING_IDENTITY_HASH"
STATUS_MISSING_MATCHED_SUBSCRIBER = "MISSING_MATCHED_SUBSCRIBER"


def clear_parts_apply_columns(
    *,
    cur: Any,
    import_run_id: int,
) -> int:
    """対象 run の parts_apply_* を初期化する。"""
    sql = """
    UPDATE staging_subscribers_fund
    SET
      parts_apply_subscriber_id = NULL,
      parts_apply_status = NULL,
      parts_apply_reason = NULL,
      parts_apply_checked_at = NULL
    WHERE import_run_id = %s
    """
    cur.execute(sql, (import_run_id,))
    return int(cur.rowcount)


def fetch_target_rows(
    *,
    cur: Any,
    import_run_id: int,
) -> list[dict[str, Any]]:
    """parts apply 再確認対象を取得する。"""
    sql = """
    SELECT
      id,
      identity_hash,
      matched_subscriber_id
    FROM staging_subscribers_fund
    WHERE import_run_id = %s
    ORDER BY id
    """
    cur.execute(sql, (import_run_id,))
    return list(cur.fetchall())


def fetch_subscriber_identity(
    *,
    cur: Any,
    subscriber_id: int,
) -> dict[str, Any] | None:
    """subscribers identity 情報を取得する。"""
    sql = """
    SELECT
      id,
      identity_hash
    FROM subscribers
    WHERE id = %s
    LIMIT 1
    """
    cur.execute(sql, (subscriber_id,))
    row = cur.fetchone()
    return dict(row) if row else None


def build_parts_apply_result(
    *,
    row: dict[str, Any],
    subscriber_row: dict[str, Any] | None,
) -> dict[str, Any]:
    """parts apply 再確認結果を構築する。"""
    matched_subscriber_id = row.get("matched_subscriber_id")
    identity_hash = str(row.get("identity_hash") or "").strip()

    if not matched_subscriber_id:
        return {
            "parts_apply_subscriber_id": None,
            "parts_apply_status": STATUS_MISSING_MATCHED_SUBSCRIBER,
            "parts_apply_reason": "matched_subscriber_id is null",
        }

    if not identity_hash:
        return {
            "parts_apply_subscriber_id": None,
            "parts_apply_status": STATUS_MISSING_IDENTITY_HASH,
            "parts_apply_reason": "staging identity_hash is empty",
        }

    if not subscriber_row:
        return {
            "parts_apply_subscriber_id": None,
            "parts_apply_status": STATUS_SUBSCRIBER_NOT_FOUND,
            "parts_apply_reason": "subscriber not found",
        }

    subscriber_identity_hash = str(
        subscriber_row.get("identity_hash") or ""
    ).strip()

    if subscriber_identity_hash != identity_hash:
        return {
            "parts_apply_subscriber_id": None,
            "parts_apply_status": STATUS_IDENTITY_CHANGED,
            "parts_apply_reason": "identity_hash changed",
        }

    return {
        "parts_apply_subscriber_id": int(subscriber_row["id"]),
        "parts_apply_status": STATUS_IDENTITY_MATCHED,
        "parts_apply_reason": "identity confirmed",
    }


def update_parts_apply_result(
    *,
    cur: Any,
    staging_id: int,
    result: dict[str, Any],
) -> int:
    """parts apply 再確認結果を更新する。"""
    sql = """
    UPDATE staging_subscribers_fund
    SET
      parts_apply_subscriber_id = %s,
      parts_apply_status = %s,
      parts_apply_reason = %s,
      parts_apply_checked_at = %s
    WHERE id = %s
    """

    cur.execute(
        sql,
        (
            result.get("parts_apply_subscriber_id"),
            result.get("parts_apply_status"),
            result.get("parts_apply_reason"),
            datetime.now(),
            staging_id,
        ),
    )

    return int(cur.rowcount)


def refresh_parts_apply_targets(
    *,
    conn: Any,
    import_run_id: int,
    dry_run: bool,
) -> dict[str, Any]:
    """parts apply 再確認を実行する。"""
    metrics = {
        "import_run_id": import_run_id,
        "cleared_rows": 0,
        "target_rows": 0,
        "identity_matched": 0,
        "identity_changed": 0,
        "subscriber_not_found": 0,
        "missing_identity_hash": 0,
        "missing_matched_subscriber": 0,
        "updated_rows": 0,
        "dry_run": dry_run,
    }

    cur = dict_cursor(conn)
    try:
        if not dry_run:
            metrics["cleared_rows"] = clear_parts_apply_columns(
                cur=cur,
                import_run_id=import_run_id,
            )

        rows = fetch_target_rows(
            cur=cur,
            import_run_id=import_run_id,
        )

        metrics["target_rows"] = len(rows)

        for row in rows:
            matched_subscriber_id = row.get("matched_subscriber_id")

            subscriber_row = None
            if matched_subscriber_id:
                subscriber_row = fetch_subscriber_identity(
                    cur=cur,
                    subscriber_id=int(matched_subscriber_id),
                )

            result = build_parts_apply_result(
                row=row,
                subscriber_row=subscriber_row,
            )

            status = result["parts_apply_status"]

            if status == STATUS_IDENTITY_MATCHED:
                metrics["identity_matched"] += 1
            elif status == STATUS_IDENTITY_CHANGED:
                metrics["identity_changed"] += 1
            elif status == STATUS_SUBSCRIBER_NOT_FOUND:
                metrics["subscriber_not_found"] += 1
            elif status == STATUS_MISSING_IDENTITY_HASH:
                metrics["missing_identity_hash"] += 1
            elif status == STATUS_MISSING_MATCHED_SUBSCRIBER:
                metrics["missing_matched_subscriber"] += 1

            if not dry_run:
                metrics["updated_rows"] += update_parts_apply_result(
                    cur=cur,
                    staging_id=int(row["id"]),
                    result=result,
                )
    finally:
        cur.close()

    return metrics