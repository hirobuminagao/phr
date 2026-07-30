-- 小禄病院CSVの「医療機関コード」は施設内コードであり、
-- 健診機関を識別するコードとしては使用しない。
-- CSV取込では、scan時にexam_facilitiesからfile_receiptsへ保存した
-- facility_code / facility_nameを使用する。

SET @oroku_csv_format_version_id := (
  SELECT cfv.`csv_format_version_id`
  FROM `phr_master`.`csv_format_versions` cfv
  JOIN `phr_master`.`exam_facilities` ef
    ON ef.`exam_facility_id` = cfv.`exam_facility_id`
  WHERE ef.`medical_institution_code` = '4710114044'
    AND cfv.`mapping_version` = 'OROKU_2026_05_JOINED_PATTERN_C_V1'
  LIMIT 1
);

UPDATE `phr_master`.`csv_exam_result_mapping_conditions` c
JOIN `phr_master`.`csv_exam_result_mapping_rules` r
  ON r.`csv_exam_result_mapping_rule_id` = c.`csv_exam_result_mapping_rule_id`
SET c.`is_active` = 0,
    c.`note` = 'disabled: use exam_facilities snapshot from file_receipts',
    c.`updated_at` = CURRENT_TIMESTAMP(3)
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id
  AND r.`rule_key` = 'oroku.basic.facility_code';

UPDATE `phr_master`.`csv_exam_result_mapping_rules`
SET `is_active` = 0,
    `note` = 'disabled: CSV medical institution code is facility-local; use exam_facilities snapshot from file_receipts',
    `updated_at` = CURRENT_TIMESTAMP(3)
WHERE `csv_format_version_id` = @oroku_csv_format_version_id
  AND `rule_key` = 'oroku.basic.facility_code';

SELECT
  @oroku_csv_format_version_id AS `csv_format_version_id`,
  r.`rule_key`,
  r.`is_active`,
  r.`note`
FROM `phr_master`.`csv_exam_result_mapping_rules` r
WHERE r.`csv_format_version_id` = @oroku_csv_format_version_id
  AND r.`rule_key` = 'oroku.basic.facility_code';
