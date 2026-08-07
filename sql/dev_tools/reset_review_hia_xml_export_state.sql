-- Revert HIA XML review-mode exports that were accidentally recorded as official exports.
-- ETL runs/errors are intentionally kept as execution evidence.
--
-- Usage:
--   1. Adjust @event_id and @review_path_token if needed.
--   2. Run this SQL in the execution environment.
--   3. Re-run scripts/from_medical/03_04_check_exam_export_cases.py before exporting again.

SET @event_id := 2;
SET @review_path_token := 'data/hia_xml_review_exports';

DROP TEMPORARY TABLE IF EXISTS tmp_review_xml_export_zips;
CREATE TEMPORARY TABLE tmp_review_xml_export_zips AS
SELECT
  zez.xml_export_zip_id,
  zez.xml_export_list_id
FROM health_exam_result.xml_export_zips AS zez
WHERE zez.event_id = @event_id
  AND REPLACE(zez.zip_path, '\\', '/') LIKE CONCAT('%', @review_path_token, '%');

DROP TEMPORARY TABLE IF EXISTS tmp_review_xml_export_members;
CREATE TEMPORARY TABLE tmp_review_xml_export_members AS
SELECT
  zem.xml_export_member_id,
  zem.ledger_id AS exam_export_case_id
FROM health_exam_result.xml_export_members AS zem
INNER JOIN tmp_review_xml_export_zips AS trz
  ON trz.xml_export_zip_id = zem.xml_export_zip_id
WHERE zem.event_id = @event_id
  AND zem.ledger_type = 'CASE';

UPDATE health_exam_result.xml_export_list_cases AS xelc
INNER JOIN tmp_review_xml_export_members AS trm
  ON trm.xml_export_member_id = xelc.exported_xml_export_member_id
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
INNER JOIN tmp_review_xml_export_members AS trm
  ON trm.exam_export_case_id = eec.exam_export_case_id
SET
  eec.xml_export_status = 'PENDING',
  eec.output_zip_path = NULL,
  eec.output_zip_file_name = NULL,
  eec.output_xml_file_name = NULL,
  eec.xml_exported_at = NULL,
  eec.xml_export_etl_run_id = NULL,
  eec.updated_at = CURRENT_TIMESTAMP(3);

DELETE zem
FROM health_exam_result.xml_export_members AS zem
INNER JOIN tmp_review_xml_export_zips AS trz
  ON trz.xml_export_zip_id = zem.xml_export_zip_id;

DELETE zez
FROM health_exam_result.xml_export_zips AS zez
INNER JOIN tmp_review_xml_export_zips AS trz
  ON trz.xml_export_zip_id = zez.xml_export_zip_id;

UPDATE health_exam_result.xml_export_lists AS xel
INNER JOIN (
  SELECT DISTINCT xml_export_list_id
  FROM tmp_review_xml_export_zips
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
  (SELECT COUNT(*) FROM tmp_review_xml_export_zips) AS reverted_zip_rows,
  (SELECT COUNT(*) FROM tmp_review_xml_export_members) AS reverted_member_rows;
