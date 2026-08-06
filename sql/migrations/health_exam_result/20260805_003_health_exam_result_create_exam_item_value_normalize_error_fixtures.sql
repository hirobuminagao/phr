CREATE TABLE `health_exam_result`.`exam_item_value_normalize_error_fixtures` (
  `exam_item_value_normalize_error_fixture_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `source_label` varchar(190) NOT NULL COMMENT '取込元を識別する任意ラベル',
  `source_file_name` varchar(255) NOT NULL,
  `source_file_sha256` char(64) NOT NULL,
  `source_row_no` int NOT NULL COMMENT 'CSV内のデータ行番号。ヘッダーを1行目として数える',
  `source_row_sha256` char(64) NOT NULL,
  `namecode` char(17) NOT NULL,
  `namecode_display_name` varchar(255) DEFAULT NULL,
  `raw_value` text,
  `raw_value_type` varchar(16) DEFAULT NULL,
  `raw_unit` varchar(64) DEFAULT NULL,
  `normalized_unit` varchar(64) DEFAULT NULL,
  `master_display_unit` varchar(190) DEFAULT NULL,
  `master_ucum_unit` varchar(190) DEFAULT NULL,
  `code_system` varchar(190) DEFAULT NULL,
  `normalize_status` varchar(32) NOT NULL,
  `normalize_reason` varchar(190) DEFAULT NULL,
  `validation_status` varchar(32) NOT NULL,
  `validation_reason` varchar(190) DEFAULT NULL,
  `sample_count` int NOT NULL DEFAULT 0 COMMENT '実行環境側でGROUP BYした件数',
  `review_status` varchar(32) NOT NULL DEFAULT 'UNREVIEWED' COMMENT 'UNREVIEWED/ADD_VARIANT/KEEP_ERROR/IGNORE等',
  `review_note` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_item_value_normalize_error_fixture_id`),
  UNIQUE KEY `uq_exam_item_value_norm_error_fixture_row` (`source_label`, `source_row_sha256`),
  KEY `idx_exam_item_value_norm_error_fixture_namecode` (`namecode`),
  KEY `idx_exam_item_value_norm_error_fixture_status` (`normalize_status`, `validation_status`),
  KEY `idx_exam_item_value_norm_error_fixture_reason` (`normalize_reason`, `validation_reason`),
  KEY `idx_exam_item_value_norm_error_fixture_review` (`review_status`),
  KEY `idx_exam_item_value_norm_error_fixture_source` (`source_label`, `source_file_name`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='実行環境から持ち帰った匿名化済みexam_item_values normalizeエラー集計fixture。正式seedではなく辞書検討・回帰確認用。';
