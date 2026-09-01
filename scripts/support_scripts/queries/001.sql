SELECT
  'CASE' AS target_type,
  'health_exam_result' AS target_schema,
  'exam_export_cases' AS target_table,
  CAST(eec.exam_export_case_id AS CHAR) AS target_id,
  eec.event_id,
  eec.exam_export_case_id,
  NULL AS source_exam_ledger_id,
  1 AS reprocess_required,
  CASE WHEN eec.xml_export_status = 'EXPORTED' THEN 1 ELSE 0 END AS reexport_required,
  eec.hia_subscriber_id,
  eec.exam_date,
  eec.facility_code,
  eec.facility_name,
  eec.check_status,
  eec.check_reason,
  eec.export_readiness_status,
  eec.xml_export_status,
  eec.output_zip_file_name,
  eec.output_xml_file_name,
  GROUP_CONCAT(DISTINCT author_map.parent_namecode ORDER BY author_map.parent_namecode SEPARATOR ',') AS present_author_parent_namecodes,
  GROUP_CONCAT(DISTINCT author_map.author_namecode ORDER BY author_map.author_namecode SEPARATOR ',') AS missing_author_namecodes,
  GROUP_CONCAT(DISTINCT oelc.xml_export_list_id ORDER BY oelc.xml_export_list_id SEPARATOR ',') AS output_list_ids
FROM health_exam_result.exam_export_cases AS eec
INNER JOIN health_exam_result.exam_export_case_values AS parent_value
  ON parent_value.exam_export_case_id = eec.exam_export_case_id
INNER JOIN (
  SELECT '9N511000000000049' AS parent_namecode, '9N516000000000049' AS author_namecode
  UNION ALL SELECT '9N521000000000049', '9N526000000000049'
  UNION ALL SELECT '9N531000000000049', '9N536000000000049'
  UNION ALL SELECT '9N541000000000049', '9N546000000000049'
  UNION ALL SELECT '9N571000000000049', '9N576000000000049'
  UNION ALL SELECT '9N581161300000011', '9N586000000000049'
  UNION ALL SELECT '9N581161400000049', '9N586000000000049'
  UNION ALL SELECT '9N591161300000011', '9N596000000000049'
  UNION ALL SELECT '9N591161400000049', '9N596000000000049'
  UNION ALL SELECT '9N601161300000011', '9N606000000000049'
  UNION ALL SELECT '9N601161400000049', '9N606000000000049'
  UNION ALL SELECT '9N611161300000011', '9N616000000000049'
  UNION ALL SELECT '9N611161400000049', '9N616000000000049'
  UNION ALL SELECT '9N621161300000011', '9N626000000000049'
  UNION ALL SELECT '9N621161400000049', '9N626000000000049'
  UNION ALL SELECT '9N631161300000011', '9N636000000000049'
  UNION ALL SELECT '9N631161400000049', '9N636000000000049'
  UNION ALL SELECT '9N641000000000049', '9N646000000000049'
) AS author_map
  ON author_map.parent_namecode = parent_value.namecode
LEFT JOIN health_exam_result.exam_export_case_values AS author_value
  ON author_value.exam_export_case_id = eec.exam_export_case_id
 AND author_value.namecode = author_map.author_namecode
LEFT JOIN health_exam_result.ops_xml_export_list_cases AS oelc
  ON oelc.exam_export_case_id = eec.exam_export_case_id
WHERE (%(event_id)s IS NULL OR eec.event_id = %(event_id)s)
  AND author_value.exam_export_case_value_id IS NULL
GROUP BY
  eec.exam_export_case_id,
  eec.event_id,
  eec.hia_subscriber_id,
  eec.exam_date,
  eec.facility_code,
  eec.facility_name,
  eec.check_status,
  eec.check_reason,
  eec.export_readiness_status,
  eec.xml_export_status,
  eec.output_zip_file_name,
  eec.output_xml_file_name;
