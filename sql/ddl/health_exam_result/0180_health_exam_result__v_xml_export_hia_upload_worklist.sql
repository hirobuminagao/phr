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
