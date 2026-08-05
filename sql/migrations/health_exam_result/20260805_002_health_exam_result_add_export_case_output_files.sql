ALTER TABLE `health_exam_result`.`exam_export_cases`
  ADD COLUMN `output_zip_path` varchar(1024) DEFAULT NULL
    COMMENT '出力ZIPフルパス'
    AFTER `xml_export_status`,
  ADD COLUMN `output_zip_file_name` varchar(255) DEFAULT NULL
    COMMENT '出力ZIPファイル名'
    AFTER `output_zip_path`,
  ADD COLUMN `output_xml_file_name` varchar(255) DEFAULT NULL
    COMMENT 'ZIP内の個人XMLファイル名'
    AFTER `output_zip_file_name`,
  ADD COLUMN `xml_exported_at` datetime(3) DEFAULT NULL
    COMMENT 'XML出力完了日時'
    AFTER `output_xml_file_name`,
  ADD COLUMN `xml_export_etl_run_id` bigint unsigned DEFAULT NULL
    COMMENT 'XML出力ETL run ID'
    AFTER `xml_exported_at`,
  ADD KEY `idx_exam_export_cases_exported_at` (`xml_exported_at`);
