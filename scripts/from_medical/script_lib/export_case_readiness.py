"""Resolve operator-facing readiness status for exam_export_cases."""

from __future__ import annotations

from typing import Any

from scripts.lib.examination.lookup import qname


def refresh_export_case_readiness(
    cur: Any,
    *,
    health_db: str,
    event_id: int,
) -> int:
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.`exam_export_cases` AS eec
        SET
          `manual_export_approved` = CASE
            WHEN `check_status` = 'NG'
             AND EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
             )
             AND NOT EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
                 AND (
                   cri.`review_status` <> 'APPROVED_WITH_REASON'
                   OR cri.`reviewed_at` IS NULL
                   OR cri.`reviewed_by_app_user_id` IS NULL
                 )
             )
            THEN 1
            ELSE 0
          END,
          `manual_export_reason` = CASE
            WHEN `check_status` = 'NG'
             AND EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
             )
             AND NOT EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
                 AND (
                   cri.`review_status` <> 'APPROVED_WITH_REASON'
                   OR cri.`reviewed_at` IS NULL
                   OR cri.`reviewed_by_app_user_id` IS NULL
                 )
             )
            THEN (
              SELECT GROUP_CONCAT(
                CONCAT(cri.`check_item_code`, ':', COALESCE(cri.`check_item_name`, ''), ':', COALESCE(cri.`validation_reason`, ''))
                ORDER BY cri.`check_scope`, cri.`check_item_code`
                SEPARATOR ' | '
              )
              FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
              WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
            )
            ELSE NULL
          END,
          `manual_export_approved_at` = CASE
            WHEN `check_status` = 'NG'
             AND EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
             )
             AND NOT EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
                 AND (
                   cri.`review_status` <> 'APPROVED_WITH_REASON'
                   OR cri.`reviewed_at` IS NULL
                   OR cri.`reviewed_by_app_user_id` IS NULL
                 )
             )
            THEN (
              SELECT MAX(cri.`reviewed_at`)
              FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
              WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
            )
            ELSE NULL
          END,
          `manual_export_approved_by` = CASE
            WHEN `check_status` = 'NG'
             AND EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
             )
             AND NOT EXISTS (
               SELECT 1
               FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
               WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                 AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
                 AND (
                   cri.`review_status` <> 'APPROVED_WITH_REASON'
                   OR cri.`reviewed_at` IS NULL
                   OR cri.`reviewed_by_app_user_id` IS NULL
                 )
             )
            THEN (
              SELECT GROUP_CONCAT(DISTINCT CAST(cri.`reviewed_by_app_user_id` AS CHAR) ORDER BY cri.`reviewed_by_app_user_id` SEPARATOR ',')
              FROM {qname(health_db)}.`exam_case_check_review_items` AS cri
              WHERE cri.`exam_export_case_id` = eec.`exam_export_case_id`
                AND cri.`review_status` <> 'RESOLVED_BY_SOURCE_VALUE'
            )
            ELSE NULL
          END,
          `updated_at` = CURRENT_TIMESTAMP(3)
        WHERE eec.`event_id` = %s
        """,
        (event_id,),
    )
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.`exam_export_cases`
        SET
          `export_readiness_status` = CASE
            WHEN `xml_export_status` = 'EXPORTED' THEN 'EXPORTED'
            WHEN `xml_export_status` = 'ERROR' THEN 'EXPORT_ERROR'
            WHEN `subscriber_match_status` <> 'MATCHED' THEN 'BLOCKED'
            WHEN `merge_status` = 'REVIEW_REQUIRED' THEN 'BLOCKED'
            WHEN `case_status` <> 'READY' THEN 'BLOCKED'
            WHEN `value_build_status` = 'PENDING' THEN 'WAITING_VALUES'
            WHEN `value_build_status` <> 'READY' THEN 'BLOCKED'
            WHEN `check_status` = 'PENDING' THEN 'WAITING_CHECK'
            WHEN `check_status` = 'OK' THEN 'EXPORT_READY'
            WHEN `check_status` = 'NG' AND `manual_export_approved` = 1 THEN 'APPROVED_WITH_REASON'
            WHEN `check_status` = 'NG' THEN 'BLOCKED'
            ELSE 'WAITING_CHECK'
          END,
          `export_readiness_reason` = CASE
            WHEN `xml_export_status` = 'EXPORTED'
              THEN CONCAT('XML出力済み: ', COALESCE(`output_zip_file_name`, 'ZIP名未記録'))
            WHEN `xml_export_status` = 'ERROR'
              THEN CONCAT('XML出力エラー: ', COALESCE(`output_zip_file_name`, `value_build_reason`, `check_reason`, '理由未入力'))
            WHEN `subscriber_match_status` <> 'MATCHED'
              THEN CONCAT('加入者突合: ', COALESCE(`subscriber_match_reason`, `subscriber_match_status`))
            WHEN `merge_status` = 'REVIEW_REQUIRED'
              THEN CONCAT('結合確認: ', COALESCE(`merge_reason`, `merge_status`))
            WHEN `case_status` <> 'READY'
              THEN CONCAT('case作成: ', COALESCE(`case_reason`, `case_status`))
            WHEN `value_build_status` = 'PENDING' THEN '出力値作成待ち'
            WHEN `value_build_status` <> 'READY'
              THEN CONCAT('出力値作成: ', COALESCE(`value_build_reason`, `value_build_status`))
            WHEN `check_status` = 'PENDING' THEN '法定チェック待ち'
            WHEN `check_status` = 'OK' THEN '出力可能'
            WHEN `check_status` = 'NG' AND `manual_export_approved` = 1
              THEN CONCAT('理由あり出力許可: ', COALESCE(`manual_export_reason`, `check_reason`, '理由未入力'))
            WHEN `check_status` = 'NG'
              THEN CONCAT('法定チェックNG: ', COALESCE(`check_reason`, '理由未入力'))
            ELSE CONCAT_WS(
              ' | ',
              CONCAT('case=', `case_status`),
              CONCAT('merge=', `merge_status`),
              CONCAT('values=', `value_build_status`),
              CONCAT('check=', `check_status`),
              CONCAT('export=', `xml_export_status`)
            )
          END,
          `updated_at` = CURRENT_TIMESTAMP(3)
        WHERE `event_id` = %s
        """,
        (event_id,),
    )
    return int(cur.rowcount or 0)


def mark_export_case_exported(
    cur: Any,
    *,
    health_db: str,
    exam_export_case_id: int,
    output_zip_path: str,
    output_zip_file_name: str,
    output_xml_file_name: str,
    etl_run_id: int,
) -> int:
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.`exam_export_cases`
        SET `xml_export_status` = 'EXPORTED',
            `output_zip_path` = %s,
            `output_zip_file_name` = %s,
            `output_xml_file_name` = %s,
            `xml_exported_at` = CURRENT_TIMESTAMP(3),
            `xml_export_etl_run_id` = %s,
            `updated_at` = CURRENT_TIMESTAMP(3)
        WHERE `exam_export_case_id` = %s
        """,
        (output_zip_path, output_zip_file_name, output_xml_file_name, etl_run_id, exam_export_case_id),
    )
    return int(cur.rowcount or 0)


def mark_export_case_export_error(
    cur: Any,
    *,
    health_db: str,
    exam_export_case_id: int,
    reason: str,
    etl_run_id: int,
) -> int:
    cur.execute(
        f"""
        UPDATE {qname(health_db)}.`exam_export_cases`
        SET `xml_export_status` = 'ERROR',
            `value_build_reason` = COALESCE(`value_build_reason`, %s),
            `xml_export_etl_run_id` = %s,
            `updated_at` = CURRENT_TIMESTAMP(3)
        WHERE `exam_export_case_id` = %s
        """,
        (reason, etl_run_id, exam_export_case_id),
    )
    return int(cur.rowcount or 0)
