WITH latest_before AS (
  SELECT ranked.*
  FROM (
    SELECT
      before_target.*,
      ROW_NUMBER() OVER (
        PARTITION BY before_target.incident_id, before_target.target_type, before_target.target_id
        ORDER BY before_target.captured_at DESC, before_target.snapshot_target_id DESC
      ) AS snapshot_rank
    FROM phr_system_support.support_snapshot_targets AS before_target
    WHERE before_target.incident_id = %(incident_id)s
      AND before_target.snapshot_phase = 'BEFORE'
  ) AS ranked
  WHERE ranked.snapshot_rank = 1
), expected_author_items AS (
  SELECT
    before_target.snapshot_target_id,
    before_target.exam_export_case_id,
    author_map.author_namecode
  FROM latest_before AS before_target
  INNER JOIN (
    SELECT '9N516000000000049' AS author_namecode
    UNION ALL SELECT '9N526000000000049'
    UNION ALL SELECT '9N536000000000049'
    UNION ALL SELECT '9N546000000000049'
    UNION ALL SELECT '9N576000000000049'
    UNION ALL SELECT '9N586000000000049'
    UNION ALL SELECT '9N596000000000049'
    UNION ALL SELECT '9N606000000000049'
    UNION ALL SELECT '9N616000000000049'
    UNION ALL SELECT '9N626000000000049'
    UNION ALL SELECT '9N636000000000049'
    UNION ALL SELECT '9N646000000000049'
  ) AS author_map
    ON FIND_IN_SET(
      author_map.author_namecode,
      JSON_UNQUOTE(JSON_EXTRACT(before_target.snapshot_data, '$.missing_author_namecodes'))
    ) > 0
), comparison AS (
  SELECT
    before_target.snapshot_target_id,
    before_target.target_type,
    before_target.target_schema,
    before_target.target_table,
    before_target.target_id,
    before_target.event_id,
    before_target.exam_export_case_id,
    before_target.source_exam_ledger_id,
    before_target.reexport_required AS before_reexport_required,
    before_target.snapshot_data AS before_snapshot_data,
    COUNT(DISTINCT expected.author_namecode) AS expected_author_count,
    COUNT(DISTINCT current_author.namecode) AS current_author_count,
    GROUP_CONCAT(DISTINCT expected.author_namecode ORDER BY expected.author_namecode SEPARATOR ',') AS expected_author_namecodes,
    GROUP_CONCAT(DISTINCT current_author.namecode ORDER BY current_author.namecode SEPARATOR ',') AS recovered_author_namecodes,
    eec.exam_export_case_id AS current_case_id,
    eec.export_readiness_status AS current_export_readiness_status,
    eec.export_readiness_reason AS current_export_readiness_reason,
    eec.xml_export_status AS current_xml_export_status
  FROM latest_before AS before_target
  LEFT JOIN expected_author_items AS expected
    ON expected.snapshot_target_id = before_target.snapshot_target_id
  LEFT JOIN health_exam_result.exam_export_cases AS eec
    ON eec.exam_export_case_id = before_target.exam_export_case_id
  LEFT JOIN health_exam_result.exam_export_case_values AS current_author
    ON current_author.exam_export_case_id = before_target.exam_export_case_id
   AND current_author.namecode = expected.author_namecode
  GROUP BY
    before_target.snapshot_target_id,
    before_target.target_type,
    before_target.target_schema,
    before_target.target_table,
    before_target.target_id,
    before_target.event_id,
    before_target.exam_export_case_id,
    before_target.source_exam_ledger_id,
    before_target.reexport_required,
    before_target.snapshot_data,
    eec.exam_export_case_id,
    eec.export_readiness_status,
    eec.export_readiness_reason,
    eec.xml_export_status
)
SELECT
  target_type,
  target_schema,
  target_table,
  target_id,
  event_id,
  exam_export_case_id,
  source_exam_ledger_id,
  CASE
    WHEN current_case_id IS NULL THEN 1
    WHEN expected_author_count = 0 THEN 1
    WHEN current_author_count < expected_author_count THEN 1
    ELSE 0
  END AS reprocess_required,
  CASE
    WHEN current_case_id IS NOT NULL
     AND expected_author_count > 0
     AND current_author_count = expected_author_count
     AND before_reexport_required = 1
    THEN 1 ELSE 0
  END AS reexport_required,
  CASE
    WHEN current_case_id IS NULL THEN 'CASE_NOT_FOUND'
    WHEN expected_author_count = 0 THEN 'SNAPSHOT_DATA_INVALID'
    WHEN current_author_count = expected_author_count THEN 'RECOVERED'
    ELSE 'STILL_MISSING'
  END AS comparison_status,
  expected_author_count,
  current_author_count,
  expected_author_namecodes,
  recovered_author_namecodes,
  before_reexport_required,
  current_export_readiness_status,
  current_export_readiness_reason,
  current_xml_export_status,
  before_snapshot_data
FROM comparison
ORDER BY exam_export_case_id;
