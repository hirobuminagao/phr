-- Revert HIA XML official export state for a specific EXPORT_HIA_XML etl run.
-- ETL runs/errors are intentionally kept as execution evidence.
--
-- Use this when an official export was produced but needs to be regenerated
-- because of output content fixes, such as XML unit correction.
--
-- Usage:
--   1. Set @export_run_id to the EXPORT_HIA_XML etl_runs.run_id to revert.
--   2. Adjust @event_id if needed.
--   3. Run this SQL in the execution environment.
--   4. Re-run scripts/from_medical/03_04_check_exam_export_cases.py before exporting again.

SET @event_id := 2;
SET @export_run_id := NULL; -- REQUIRED: set to the official EXPORT_HIA_XML run_id.

DROP TEMPORARY TABLE IF EXISTS tmp_required_export_run_id;
CREATE TEMPORARY TABLE tmp_required_export_run_id (
  export_run_id bigint unsigned NOT NULL
);
INSERT INTO tmp_required_export_run_id (export_run_id)
VALUES (@export_run_id);

DROP TEMPORARY TABLE IF EXISTS tmp_xml_export_zips;
CREATE TEMPORARY TABLE tmp_xml_export_zips AS
SELECT
  zez.xml_export_zip_id,
  zez.xml_export_list_id
FROM health_exam_result.xml_export_zips AS zez
WHERE zez.event_id = @event_id
  AND zez.etl_run_id = @export_run_id;

DROP TEMPORARY TABLE IF EXISTS tmp_xml_export_members;
CREATE TEMPORARY TABLE tmp_xml_export_members AS
SELECT
  zem.xml_export_member_id,
  zem.ledger_id AS exam_export_case_id
FROM health_exam_result.xml_export_members AS zem
INNER JOIN tmp_xml_export_zips AS tz
  ON tz.xml_export_zip_id = zem.xml_export_zip_id
WHERE zem.event_id = @event_id
  AND zem.ledger_type = 'CASE';

UPDATE health_exam_result.ops_xml_export_list_cases AS xelc
INNER JOIN tmp_xml_export_members AS tm
  ON tm.xml_export_member_id = xelc.exported_xml_export_member_id
SET
  xelc.list_case_status = CASE
    WHEN xelc.removed_at IS NOT NULL THEN 'REMOVED'
    ELSE 'READY'
  END,
  xelc.exported_xml_export_member_id = NULL,
  xelc.exported_at = NULL,
  xelc.export_error_reason = NULL,
  xelc.updated_at = CURRENT_TIMESTAMP(3);

UPDATE health_exam_result.exam_export_cases AS eec
INNER JOIN tmp_xml_export_members AS tm
  ON tm.exam_export_case_id = eec.exam_export_case_id
SET
  eec.xml_export_status = 'PENDING',
  eec.output_zip_path = NULL,
  eec.output_zip_file_name = NULL,
  eec.output_xml_file_name = NULL,
  eec.xml_exported_at = NULL,
  eec.xml_export_etl_run_id = NULL,
  eec.updated_at = CURRENT_TIMESTAMP(3);

UPDATE health_exam_result.exam_export_cases AS eec
INNER JOIN tmp_xml_export_members AS tm
  ON tm.exam_export_case_id = eec.exam_export_case_id
SET
  eec.export_readiness_status = CASE
    WHEN eec.xml_export_status = 'EXPORTED' THEN 'EXPORTED'
    WHEN eec.xml_export_status = 'ERROR' THEN 'EXPORT_ERROR'
    WHEN eec.subscriber_match_status <> 'MATCHED' THEN 'BLOCKED'
    WHEN eec.merge_status = 'REVIEW_REQUIRED' THEN 'BLOCKED'
    WHEN eec.case_status <> 'READY' THEN 'BLOCKED'
    WHEN eec.value_build_status = 'PENDING' THEN 'WAITING_VALUES'
    WHEN eec.value_build_status <> 'READY' THEN 'BLOCKED'
    WHEN eec.check_status = 'PENDING' THEN 'WAITING_CHECK'
    WHEN eec.check_status = 'OK' THEN 'EXPORT_READY'
    WHEN eec.check_status = 'NG' AND eec.manual_export_approved = 1 THEN 'APPROVED_WITH_REASON'
    WHEN eec.check_status = 'NG' THEN 'BLOCKED'
    ELSE 'WAITING_CHECK'
  END,
  eec.export_readiness_reason = CASE
    WHEN eec.xml_export_status = 'EXPORTED'
      THEN CONCAT('XML出力済み: ', COALESCE(eec.output_zip_file_name, 'ZIP名未記録'))
    WHEN eec.xml_export_status = 'ERROR'
      THEN CONCAT('XML出力エラー: ', COALESCE(eec.output_zip_file_name, eec.value_build_reason, eec.check_reason, '理由未入力'))
    WHEN eec.subscriber_match_status <> 'MATCHED'
      THEN CONCAT('加入者突合: ', COALESCE(eec.subscriber_match_reason, eec.subscriber_match_status))
    WHEN eec.merge_status = 'REVIEW_REQUIRED'
      THEN CONCAT('結合確認: ', COALESCE(eec.merge_reason, eec.merge_status))
    WHEN eec.case_status <> 'READY'
      THEN CONCAT('case作成: ', COALESCE(eec.case_reason, eec.case_status))
    WHEN eec.value_build_status = 'PENDING' THEN '出力値作成待ち'
    WHEN eec.value_build_status <> 'READY'
      THEN CONCAT('出力値作成: ', COALESCE(eec.value_build_reason, eec.value_build_status))
    WHEN eec.check_status = 'PENDING' THEN '法定チェック待ち'
    WHEN eec.check_status = 'OK' THEN '出力可能'
    WHEN eec.check_status = 'NG' AND eec.manual_export_approved = 1
      THEN CONCAT('理由あり出力許可: ', COALESCE(eec.manual_export_reason, eec.check_reason, '理由未入力'))
    WHEN eec.check_status = 'NG'
      THEN CONCAT('法定チェックNG: ', COALESCE(eec.check_reason, '理由未入力'))
    ELSE CONCAT_WS(
      ' | ',
      CONCAT('case=', eec.case_status),
      CONCAT('merge=', eec.merge_status),
      CONCAT('values=', eec.value_build_status),
      CONCAT('check=', eec.check_status),
      CONCAT('export=', eec.xml_export_status)
    )
  END,
  eec.updated_at = CURRENT_TIMESTAMP(3);

UPDATE health_exam_result.ops_xml_export_list_cases AS xelc
INNER JOIN tmp_xml_export_members AS tm
  ON tm.exam_export_case_id = xelc.exam_export_case_id
INNER JOIN health_exam_result.exam_export_cases AS eec
  ON eec.exam_export_case_id = xelc.exam_export_case_id
SET
  xelc.export_readiness_status_snapshot = eec.export_readiness_status,
  xelc.export_readiness_reason_snapshot = eec.export_readiness_reason,
  xelc.updated_at = CURRENT_TIMESTAMP(3)
WHERE xelc.removed_at IS NULL;

DELETE zem
FROM health_exam_result.xml_export_members AS zem
INNER JOIN tmp_xml_export_zips AS tz
  ON tz.xml_export_zip_id = zem.xml_export_zip_id;

DELETE zez
FROM health_exam_result.xml_export_zips AS zez
INNER JOIN tmp_xml_export_zips AS tz
  ON tz.xml_export_zip_id = zez.xml_export_zip_id;

UPDATE health_exam_result.ops_xml_export_lists AS xel
INNER JOIN (
  SELECT DISTINCT xml_export_list_id
  FROM tmp_xml_export_zips
  WHERE xml_export_list_id IS NOT NULL
) AS target
  ON target.xml_export_list_id = xel.xml_export_list_id
SET
  xel.list_status = CASE
    WHEN EXISTS (
      SELECT 1
      FROM health_exam_result.xml_export_zips AS zez
      WHERE zez.xml_export_list_id = xel.xml_export_list_id
    ) THEN xel.list_status
    ELSE 'READY'
  END,
  xel.export_etl_run_id = CASE
    WHEN EXISTS (
      SELECT 1
      FROM health_exam_result.xml_export_zips AS zez
      WHERE zez.xml_export_list_id = xel.xml_export_list_id
    ) THEN xel.export_etl_run_id
    ELSE NULL
  END,
  xel.export_started_at = CASE
    WHEN EXISTS (
      SELECT 1
      FROM health_exam_result.xml_export_zips AS zez
      WHERE zez.xml_export_list_id = xel.xml_export_list_id
    ) THEN xel.export_started_at
    ELSE NULL
  END,
  xel.exported_zip_count = (
    SELECT COUNT(*)
    FROM health_exam_result.xml_export_zips AS zez
    WHERE zez.xml_export_list_id = xel.xml_export_list_id
  ),
  xel.exported_member_count = (
    SELECT COUNT(*)
    FROM health_exam_result.xml_export_members AS zem
    INNER JOIN health_exam_result.xml_export_zips AS zez
      ON zez.xml_export_zip_id = zem.xml_export_zip_id
    WHERE zez.xml_export_list_id = xel.xml_export_list_id
  ),
  xel.export_finished_at = CASE
    WHEN EXISTS (
      SELECT 1
      FROM health_exam_result.xml_export_zips AS zez
      WHERE zez.xml_export_list_id = xel.xml_export_list_id
    ) THEN xel.export_finished_at
    ELSE NULL
  END,
  xel.updated_at = CURRENT_TIMESTAMP(3);

SELECT
  @export_run_id AS reverted_export_run_id,
  (SELECT COUNT(*) FROM tmp_xml_export_zips) AS reverted_zip_rows,
  (SELECT COUNT(*) FROM tmp_xml_export_members) AS reverted_member_rows;
