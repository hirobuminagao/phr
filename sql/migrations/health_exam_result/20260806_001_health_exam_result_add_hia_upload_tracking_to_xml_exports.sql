ALTER TABLE `health_exam_result`.`xml_export_zips`
  ADD COLUMN `hia_upload_status` varchar(32) NOT NULL DEFAULT 'PENDING'
    COMMENT 'HIAアップロード作業状態。PENDING/UPLOADED/UPLOAD_ERROR/PARTIAL/CONFIRMED等'
    AFTER `xsd_bundle_id`,
  ADD COLUMN `hia_uploaded_at` datetime(3) DEFAULT NULL
    COMMENT 'HIAアップロード実施日時'
    AFTER `hia_upload_status`,
  ADD COLUMN `hia_uploaded_by` varchar(190) DEFAULT NULL
    COMMENT 'HIAアップロード実施者'
    AFTER `hia_uploaded_at`,
  ADD COLUMN `hia_upload_checked_at` datetime(3) DEFAULT NULL
    COMMENT 'HIAアップロード結果確認日時'
    AFTER `hia_uploaded_by`,
  ADD COLUMN `hia_upload_checked_by` varchar(190) DEFAULT NULL
    COMMENT 'HIAアップロード結果確認者'
    AFTER `hia_upload_checked_at`,
  ADD COLUMN `hia_upload_error_summary` text DEFAULT NULL
    COMMENT 'ZIP単位のHIAアップロードエラー要約'
    AFTER `hia_upload_checked_by`,
  ADD COLUMN `hia_upload_note` text DEFAULT NULL
    COMMENT 'HIAアップロード作業メモ'
    AFTER `hia_upload_error_summary`,
  ADD KEY `idx_xml_export_zips_hia_upload_status` (`hia_upload_status`);

ALTER TABLE `health_exam_result`.`xml_export_members`
  ADD COLUMN `hia_upload_status` varchar(32) NOT NULL DEFAULT 'PENDING'
    COMMENT '個人XML単位のHIAアップロード結果。PENDING/UPLOADED/UPLOAD_ERROR/EXCLUDED等'
    AFTER `manual_export_approved_by`,
  ADD COLUMN `hia_upload_error_code` varchar(64) DEFAULT NULL
    COMMENT 'HIAアップロード時の個人単位エラーコードまたは分類'
    AFTER `hia_upload_status`,
  ADD COLUMN `hia_upload_error_message` text DEFAULT NULL
    COMMENT 'HIAアップロード時の個人単位エラー内容'
    AFTER `hia_upload_error_code`,
  ADD COLUMN `hia_upload_note` text DEFAULT NULL
    COMMENT 'HIAアップロード個人単位メモ'
    AFTER `hia_upload_error_message`,
  ADD COLUMN `hia_uploaded_at` datetime(3) DEFAULT NULL
    COMMENT '個人XMLのHIAアップロード完了確認日時'
    AFTER `hia_upload_note`,
  ADD COLUMN `hia_uploaded_by` varchar(190) DEFAULT NULL
    COMMENT '個人XMLのHIAアップロード完了確認者'
    AFTER `hia_uploaded_at`,
  ADD KEY `idx_xml_export_members_hia_upload_status` (`hia_upload_status`);

CREATE OR REPLACE VIEW `health_exam_result`.`v_xml_export_hia_upload_worklist` AS
SELECT
  zez.`xml_export_zip_id`,
  zem.`xml_export_member_id`,
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
LEFT JOIN `health_exam_result`.`exam_export_cases` AS eec
  ON zem.`ledger_type` = 'CASE'
 AND zem.`ledger_id` = eec.`exam_export_case_id`
LEFT JOIN `health_exam_result`.`file_receipts` AS fr
  ON fr.`id` = zem.`source_file_receipt_id`;
