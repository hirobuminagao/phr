CREATE TABLE `health_exam_result`.`xml_export_lists` (
  `xml_export_list_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `list_name` varchar(255) NOT NULL COMMENT '人が識別する出力リスト名',
  `list_status` varchar(32) NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT/READY/EXPORTING/EXPORTED/PARTIAL/ERROR/CANCELLED',
  `selector_summary` text DEFAULT NULL COMMENT '作成時の検索条件メモ',
  `requested_exam_month` char(7) DEFAULT NULL COMMENT '検索条件の受診月 YYYY-MM',
  `requested_facility_codes` text DEFAULT NULL COMMENT '検索条件の健診機関コード。複数時は改行区切り',
  `include_exported` tinyint(1) NOT NULL DEFAULT 0 COMMENT '出力済みcaseを候補に含める指定',
  `requested_file_date` date DEFAULT NULL COMMENT '提出日/作成日指定',
  `requested_split_no` tinyint unsigned DEFAULT NULL COMMENT '同日分割送信回数の明示指定。NULLは自動',
  `created_by` varchar(190) DEFAULT NULL,
  `confirmed_by` varchar(190) DEFAULT NULL,
  `confirmed_at` datetime(3) DEFAULT NULL,
  `export_etl_run_id` bigint unsigned DEFAULT NULL,
  `export_started_at` datetime(3) DEFAULT NULL,
  `export_finished_at` datetime(3) DEFAULT NULL,
  `exported_zip_count` int unsigned NOT NULL DEFAULT 0,
  `exported_member_count` int unsigned NOT NULL DEFAULT 0,
  `list_note` text DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`xml_export_list_id`),
  KEY `idx_xml_export_lists_event` (`event_id`),
  KEY `idx_xml_export_lists_status` (`list_status`),
  KEY `idx_xml_export_lists_exam_month` (`requested_exam_month`),
  KEY `idx_xml_export_lists_export_run` (`export_etl_run_id`),
  CONSTRAINT `fk_xml_export_lists_export_run`
    FOREIGN KEY (`export_etl_run_id`) REFERENCES `health_exam_result`.`etl_runs` (`run_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`xml_export_list_cases` (
  `xml_export_list_case_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `xml_export_list_id` bigint unsigned NOT NULL,
  `exam_export_case_id` bigint unsigned NOT NULL,
  `list_case_status` varchar(32) NOT NULL DEFAULT 'SELECTED' COMMENT 'SELECTED/READY/REMOVED/EXPORTED/EXPORT_ERROR',
  `export_readiness_status_snapshot` varchar(32) DEFAULT NULL,
  `export_readiness_reason_snapshot` text DEFAULT NULL,
  `added_by` varchar(190) DEFAULT NULL,
  `added_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `removed_by` varchar(190) DEFAULT NULL,
  `removed_at` datetime(3) DEFAULT NULL,
  `remove_reason` text DEFAULT NULL,
  `exported_xml_export_member_id` bigint unsigned DEFAULT NULL,
  `exported_at` datetime(3) DEFAULT NULL,
  `export_error_reason` text DEFAULT NULL,
  `list_case_note` text DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`xml_export_list_case_id`),
  UNIQUE KEY `uq_xml_export_list_cases_list_case` (`xml_export_list_id`, `exam_export_case_id`),
  KEY `idx_xml_export_list_cases_case` (`exam_export_case_id`),
  KEY `idx_xml_export_list_cases_status` (`list_case_status`),
  KEY `idx_xml_export_list_cases_export_member` (`exported_xml_export_member_id`),
  CONSTRAINT `fk_xml_export_list_cases_list`
    FOREIGN KEY (`xml_export_list_id`) REFERENCES `health_exam_result`.`xml_export_lists` (`xml_export_list_id`),
  CONSTRAINT `fk_xml_export_list_cases_case`
    FOREIGN KEY (`exam_export_case_id`) REFERENCES `health_exam_result`.`exam_export_cases` (`exam_export_case_id`),
  CONSTRAINT `fk_xml_export_list_cases_export_member`
    FOREIGN KEY (`exported_xml_export_member_id`) REFERENCES `health_exam_result`.`xml_export_members` (`xml_export_member_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

ALTER TABLE `health_exam_result`.`xml_export_zips`
  ADD COLUMN `xml_export_list_id` bigint unsigned DEFAULT NULL
    COMMENT 'このZIPを作成した出力リスト。CLI直接出力時はNULL'
    AFTER `etl_run_id`,
  ADD KEY `idx_xml_export_zips_export_list` (`xml_export_list_id`),
  ADD CONSTRAINT `fk_xml_export_zips_export_list`
    FOREIGN KEY (`xml_export_list_id`) REFERENCES `health_exam_result`.`xml_export_lists` (`xml_export_list_id`);

CREATE OR REPLACE VIEW `health_exam_result`.`v_xml_export_hia_upload_worklist` AS
SELECT
  zez.`xml_export_zip_id`,
  zem.`xml_export_member_id`,
  zez.`xml_export_list_id`,
  xel.`list_name` AS `xml_export_list_name`,
  xel.`list_status` AS `xml_export_list_status`,
  zez.`etl_run_id`,
  zez.`event_id`,
  zez.`exam_facility_id`,
  zez.`facility_code`,
  zez.`facility_name`,
  zez.`facility_folder_name`,
  zez.`insurer_number`,
  zez.`file_date`,
  zez.`split_no`,
  zez.`root_dir_name`,
  zez.`zip_file_name`,
  zez.`zip_path`,
  zez.`member_count`,
  zez.`xsd_bundle_id`,
  zez.`hia_upload_status` AS `zip_hia_upload_status`,
  zez.`hia_uploaded_at` AS `zip_hia_uploaded_at`,
  zez.`hia_uploaded_by` AS `zip_hia_uploaded_by`,
  zez.`hia_upload_checked_at` AS `zip_hia_upload_checked_at`,
  zez.`hia_upload_checked_by` AS `zip_hia_upload_checked_by`,
  zez.`hia_upload_error_summary` AS `zip_hia_upload_error_summary`,
  zez.`hia_upload_note` AS `zip_hia_upload_note`,
  zem.`ledger_type`,
  zem.`ledger_id`,
  zem.`source_file_receipt_id`,
  zem.`subscriber_id`,
  zem.`hia_subscriber_id`,
  zem.`person_xml_file_name`,
  zem.`report_category_code`,
  zem.`program_type_code`,
  zem.`manual_export_approved`,
  zem.`manual_export_reason`,
  zem.`hia_upload_status` AS `member_hia_upload_status`,
  zem.`hia_upload_error_code` AS `member_hia_upload_error_code`,
  zem.`hia_upload_error_message` AS `member_hia_upload_error_message`,
  zem.`hia_upload_note` AS `member_hia_upload_note`,
  zem.`hia_uploaded_at` AS `member_hia_uploaded_at`,
  zem.`hia_uploaded_by` AS `member_hia_uploaded_by`,
  eec.`exam_date`,
  eec.`name_full_raw`,
  eec.`name_kana_export_value`,
  eec.`insurance_symbol_export_value`,
  eec.`insurance_number_export_value`,
  eec.`health_exam_report_category`,
  eec.`program_code`,
  eec.`export_readiness_status`,
  eec.`export_readiness_reason`,
  fr.`file_name` AS `source_file_name`,
  fr.`relative_path` AS `source_relative_path`,
  zez.`created_at` AS `zip_created_at`,
  zem.`created_at` AS `member_created_at`
FROM `health_exam_result`.`xml_export_zips` AS zez
INNER JOIN `health_exam_result`.`xml_export_members` AS zem
  ON zem.`xml_export_zip_id` = zez.`xml_export_zip_id`
LEFT JOIN `health_exam_result`.`xml_export_lists` AS xel
  ON xel.`xml_export_list_id` = zez.`xml_export_list_id`
LEFT JOIN `health_exam_result`.`exam_export_cases` AS eec
  ON zem.`ledger_type` = 'CASE'
 AND zem.`ledger_id` = eec.`exam_export_case_id`
LEFT JOIN `health_exam_result`.`file_receipts` AS fr
  ON fr.`id` = zem.`source_file_receipt_id`;
