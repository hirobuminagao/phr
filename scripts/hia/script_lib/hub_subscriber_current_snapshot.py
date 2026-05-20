

# -*- coding: utf-8 -*-
"""
============================================================
Module : hub_subscriber_current_snapshot.py
Path   : scripts/hia/script_lib/hub_subscriber_current_snapshot.py
Project: PHR

Purpose:
    staging_subscribers_hub に対して
    current subscriber snapshot を付与する。

Responsibility:
    - import_run_id 単位で staging 行を取得
    - HIA subscriber ID lookup
    - identity_hash lookup
    - current subscriber snapshot 更新
    - current lookup status 更新

Non-goals:
    - apply_action decision
    - subscribers 更新
    - compare diff 判定
    - audit 永続化
============================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from scripts.work_folder.lib.etl import (
    RunMetrics,
    ProgressLogger,
    log_error,
)


# ============================================================
# metrics
# ============================================================

@dataclass
class CurrentSnapshotMetrics:
    rows_seen: int = 0
    hia_id_matched: int = 0
    identity_hash_matched: int = 0
    not_found: int = 0
    multiple_match: int = 0
    review: int = 0
    updated: int = 0
    errors: int = 0


# ============================================================
# lookup helpers
# ============================================================


def lookup_subscriber_by_hia_subscriber_id(
    cur,
    *,
    hia_subscriber_id: str,
):
    """
    HIA subscriber ID で current subscriber を検索する。

    TODO:
        共通lib lookup helper へ寄せる。
    """
    raise NotImplementedError



def lookup_subscriber_by_identity_hash(
    cur,
    *,
    identity_hash: str,
):
    """
    identity_hash で current subscriber を検索する。

    TODO:
        共通lib lookup helper へ寄せる。
    """
    raise NotImplementedError



def lookup_current_address(
    cur,
    *,
    subscriber_id: int,
):
    """
    current subscriber_addresses を取得する。

    TODO:
        共通lib lookup helper へ寄せる。
    """
    raise NotImplementedError



def lookup_current_contact(
    cur,
    *,
    subscriber_id: int,
):
    """
    current subscriber_contacts を取得する。

    TODO:
        共通lib lookup helper へ寄せる。
    """
    raise NotImplementedError


# ============================================================
# staging update
# ============================================================


def update_staging_current_snapshot(
    cur,
    *,
    staging_id: int,
    current_subscriber_id: Optional[int],
    current_identity_hash: Optional[str],
    current_name_kana_full_match: Optional[str],
    current_address_id: Optional[int],
    current_contact_id: Optional[int],
    current_lookup_status: str,
):
    """
    staging_subscribers_hub.current_* を更新する。
    """
    cur.execute(
        """
        UPDATE staging_subscribers_hub
        SET
            current_subscriber_id = %(current_subscriber_id)s,
            current_identity_hash = %(current_identity_hash)s,
            current_name_kana_full_match = %(current_name_kana_full_match)s,
            current_address_id = %(current_address_id)s,
            current_contact_id = %(current_contact_id)s,
            current_lookup_status = %(current_lookup_status)s,
            current_lookup_checked_at = NOW()
        WHERE staging_subscriber_hub_id = %(staging_id)s
        """,
        {
            "staging_id": staging_id,
            "current_subscriber_id": current_subscriber_id,
            "current_identity_hash": current_identity_hash,
            "current_name_kana_full_match": current_name_kana_full_match,
            "current_address_id": current_address_id,
            "current_contact_id": current_contact_id,
            "current_lookup_status": current_lookup_status,
        },
    )


# ============================================================
# main process
# ============================================================


def update_current_snapshot(
    cur,
    *,
    import_run_id: int,
    metrics: CurrentSnapshotMetrics,
    plog: Optional[ProgressLogger] = None,
):
    """
    import_run_id 単位で current snapshot を付与する。

    Flow:
        staging_subscribers_hub
            ↓
        HIA subscriber ID lookup
            ↓
        identity_hash lookup
            ↓
        current address lookup
            ↓
        current contact lookup
            ↓
        staging current_* update
    """

    cur.execute(
        """
        SELECT
            staging_subscriber_hub_id,
            hia_subscriber_id,
            identity_hash
        FROM staging_subscribers_hub
        WHERE import_run_id = %s
        ORDER BY staging_subscriber_hub_id
        """,
        (import_run_id,),
    )

    rows = cur.fetchall()

    for row in rows:
        metrics.rows_seen += 1

        try:
            staging_id = row["staging_subscriber_hub_id"]
            hia_subscriber_id = row["hia_subscriber_id"]
            identity_hash = row["identity_hash"]

            # ----------------------------------------------------
            # 1. HIA subscriber ID lookup
            # ----------------------------------------------------

            # TODO

            # ----------------------------------------------------
            # 2. identity_hash lookup
            # ----------------------------------------------------

            # TODO

            # ----------------------------------------------------
            # 3. current address/contact lookup
            # ----------------------------------------------------

            # TODO

            # ----------------------------------------------------
            # 4. staging update
            # ----------------------------------------------------

            # TODO

            metrics.updated += 1

        except Exception as e:
            metrics.errors += 1

            log_error(
                cur,
                run_id=import_run_id,
                phase="current_snapshot",
                source="hub_subscriber_current_snapshot",
                insurer_number=None,
                src_file=None,
                row_no=None,
                line_no=None,
                field=None,
                field_value=None,
                error_code=type(e).__name__,
                message=str(e),
            )

        if plog:
            plog.tick()