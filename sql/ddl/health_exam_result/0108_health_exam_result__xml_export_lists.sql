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
