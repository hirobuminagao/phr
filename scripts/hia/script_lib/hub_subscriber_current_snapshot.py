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

from scripts.lib.db.lookup.subscriber_identity import resolve_subscriber_identity
from scripts.lib.db.lookup.subscriber_projection import (
    load_subscriber_rows_for_hia_current_snapshot,
    load_current_address_rows_for_hia_current_snapshot,
    load_current_contact_rows_for_hia_current_snapshot,
)


# ============================================================
# metrics
# ============================================================

@dataclass
class CurrentSnapshotMetrics:
    rows_seen: int = 0
    hia_id_matched: int = 0
    identity_hash_matched: int = 0
    person_id_custom_matched: int = 0
    not_found: int = 0
    multiple_match: int = 0
    review: int = 0
    updated: int = 0
    errors: int = 0


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
        staging_subscribers_hub(import_run_id)
            ↓
        subscriber_identity resolver
            ↓
        subscriber_id list
            ↓
        subscriber_projection
          ├─ subscriber row
          ├─ current address row
          └─ current contact row
            ↓
        staging current_* update
    """

    cur.execute(
        """
        SELECT
            staging_subscriber_hub_id,
            hia_subscriber_id,
            identity_hash,
            person_id_custom
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
            person_id_custom = row["person_id_custom"]

            # ----------------------------------------------------
            # 1. subscriber identity resolve
            # ----------------------------------------------------
            # このスクリプトでは複数候補は特定不能として staging current_* には採用しない。
            # not_found も採用しない。
            # 単一候補のみ projection に渡して current_* 更新対象にする。
            resolve_result = resolve_subscriber_identity(
                cur,
                hia_subscriber_id=hia_subscriber_id,
                identity_hash=identity_hash,
                person_id_custom=person_id_custom,
            )

            if resolve_result.status == "multiple_match":
                update_staging_current_snapshot(
                    cur,
                    staging_id=staging_id,
                    current_subscriber_id=None,
                    current_identity_hash=None,
                    current_name_kana_full_match=None,
                    current_address_id=None,
                    current_contact_id=None,
                    current_lookup_status="multiple_match",
                )
                metrics.multiple_match += 1
                metrics.updated += 1
                continue

            if resolve_result.status == "not_found":
                update_staging_current_snapshot(
                    cur,
                    staging_id=staging_id,
                    current_subscriber_id=None,
                    current_identity_hash=None,
                    current_name_kana_full_match=None,
                    current_address_id=None,
                    current_contact_id=None,
                    current_lookup_status="not_found",
                )
                metrics.not_found += 1
                metrics.updated += 1
                continue

            if not resolve_result.is_single_match or resolve_result.subscriber_id is None:
                update_staging_current_snapshot(
                    cur,
                    staging_id=staging_id,
                    current_subscriber_id=None,
                    current_identity_hash=None,
                    current_name_kana_full_match=None,
                    current_address_id=None,
                    current_contact_id=None,
                    current_lookup_status="review",
                )
                metrics.review += 1
                metrics.updated += 1
                continue

            if resolve_result.matched_by == "hia_subscriber_id":
                current_lookup_status = "hia_id_matched"
                metrics.hia_id_matched += 1
            elif resolve_result.matched_by == "identity_hash":
                current_lookup_status = "identity_hash_matched"
                metrics.identity_hash_matched += 1
            elif resolve_result.matched_by == "person_id_custom":
                current_lookup_status = "person_id_custom_matched"
                metrics.person_id_custom_matched += 1
            else:
                current_lookup_status = "review"
                metrics.review += 1

            # ----------------------------------------------------
            # 2. projection
            # ----------------------------------------------------
            projection_rows = load_subscriber_rows_for_hia_current_snapshot(
                cur,
                subscriber_ids=[resolve_result.subscriber_id],
            )

            if len(projection_rows) != 1:
                update_staging_current_snapshot(
                    cur,
                    staging_id=staging_id,
                    current_subscriber_id=None,
                    current_identity_hash=None,
                    current_name_kana_full_match=None,
                    current_address_id=None,
                    current_contact_id=None,
                    current_lookup_status="review",
                )
                metrics.review += 1
                metrics.updated += 1
                continue

            current_row = projection_rows[0]

            # ----------------------------------------------------
            # 2-1. current address projection
            # ----------------------------------------------------
            address_rows = load_current_address_rows_for_hia_current_snapshot(
                cur,
                subscriber_ids=[resolve_result.subscriber_id],
            )

            current_address_id = None
            if len(address_rows) == 1:
                current_address_id = address_rows[0].get("current_address_id")

            # ----------------------------------------------------
            # 2-2. current contact projection
            # ----------------------------------------------------
            contact_rows = load_current_contact_rows_for_hia_current_snapshot(
                cur,
                subscriber_ids=[resolve_result.subscriber_id],
            )

            current_contact_id = None
            if len(contact_rows) == 1:
                current_contact_id = contact_rows[0].get("current_contact_id")

            # ----------------------------------------------------
            # 3. staging update
            # ----------------------------------------------------
            update_staging_current_snapshot(
                cur,
                staging_id=staging_id,
                current_subscriber_id=current_row["subscriber_id"],
                current_identity_hash=current_row.get("identity_hash"),
                current_name_kana_full_match=current_row.get("name_kana_full_match"),
                current_address_id=current_address_id,
                current_contact_id=current_contact_id,
                current_lookup_status=current_lookup_status,
            )

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