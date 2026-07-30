CREATE OR REPLACE SQL SECURITY INVOKER VIEW `health_exam_result`.`exam_result_facility_error_rate` AS
SELECT
  person_rows.`event_id`,
  person_rows.`facility_code`,
  person_rows.`facility_name`,
  SUM(person_rows.`source_result_count`) AS `source_result_count`,
  COUNT(*) AS `total_person_count`,
  SUM(person_rows.`has_xml`) AS `xml_person_count`,
  SUM(person_rows.`has_csv`) AS `csv_person_count`,
  SUM(
    CASE
      WHEN person_rows.`has_ng` = 0 AND person_rows.`has_ok` = 1 THEN 1
      ELSE 0
    END
  ) AS `ok_person_count`,
  SUM(person_rows.`has_ng`) AS `error_person_count`,
  SUM(
    CASE
      WHEN person_rows.`has_ng` = 0
       AND person_rows.`has_ok` = 0
       AND person_rows.`has_warning` = 1
      THEN 1
      ELSE 0
    END
  ) AS `warning_person_count`,
  SUM(
    CASE
      WHEN person_rows.`has_ng` = 0
       AND person_rows.`has_ok` = 0
       AND person_rows.`has_warning` = 0
      THEN 1
      ELSE 0
    END
  ) AS `pending_person_count`,
  ROUND(
    100.0 * SUM(person_rows.`has_ng`) / NULLIF(COUNT(*), 0),
    2
  ) AS `error_rate_percent`,
  ROUND(
    100.0 * SUM(person_rows.`has_ng`)
      / NULLIF(
          SUM(
            CASE
              WHEN person_rows.`has_ng` = 1
                OR person_rows.`has_ok` = 1
                OR person_rows.`has_warning` = 1
              THEN 1
              ELSE 0
            END
          ),
          0
        ),
    2
  ) AS `checked_error_rate_percent`,
  MAX(person_rows.`last_refreshed_at`) AS `last_refreshed_at`
FROM (
  SELECT
    r.`event_id`,
    r.`facility_code`,
    r.`facility_name`,
    COALESCE(
      CONCAT('SUBSCRIBER:', r.`subscriber_id`),
      CONCAT('IDENTITY:', r.`identity_hash`),
      CONCAT('LEDGER:', r.`ledger_type`, ':', r.`ledger_id`)
    ) AS `person_key`,
    COUNT(*) AS `source_result_count`,
    MAX(CASE WHEN r.`ledger_type` = 'XML' THEN 1 ELSE 0 END) AS `has_xml`,
    MAX(CASE WHEN r.`ledger_type` = 'CSV' THEN 1 ELSE 0 END) AS `has_csv`,
    MAX(CASE WHEN r.`check_status` = 'NG' THEN 1 ELSE 0 END) AS `has_ng`,
    MAX(CASE WHEN r.`check_status` = 'OK' THEN 1 ELSE 0 END) AS `has_ok`,
    MAX(CASE WHEN r.`check_status` = 'WARNING' THEN 1 ELSE 0 END) AS `has_warning`,
    MAX(r.`refreshed_at`) AS `last_refreshed_at`
  FROM `health_exam_result`.`exam_result_ledger_report` AS r
  GROUP BY
    r.`event_id`,
    r.`facility_code`,
    r.`facility_name`,
    COALESCE(
      CONCAT('SUBSCRIBER:', r.`subscriber_id`),
      CONCAT('IDENTITY:', r.`identity_hash`),
      CONCAT('LEDGER:', r.`ledger_type`, ':', r.`ledger_id`)
    )
) AS person_rows
GROUP BY
  person_rows.`event_id`,
  person_rows.`facility_code`,
  person_rows.`facility_name`;

