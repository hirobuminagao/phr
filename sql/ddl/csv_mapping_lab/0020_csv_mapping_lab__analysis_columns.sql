CREATE DATABASE IF NOT EXISTS `csv_mapping_lab`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_ja_0900_as_cs;

CREATE TABLE `csv_mapping_lab`.`analysis_columns` (
  `analysis_column_id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'CSV解析列ID',
  `analysis_file_id` bigint unsigned NOT NULL COMMENT 'CSV解析ファイルID',
  `column_no` int NOT NULL COMMENT 'CSV列番。1始まり',
  `header_occurrence` int NOT NULL DEFAULT 1 COMMENT '同一ヘッダー名の出現順',
  `header_name` varchar(255) DEFAULT NULL COMMENT '元ヘッダー名',
  `normalized_header_name` varchar(255) DEFAULT NULL COMMENT '比較用の正規化ヘッダー名',
  `analysis_note` text DEFAULT NULL COMMENT '解析内容、候補理由、目検メモ',
  `sample_values_json` json DEFAULT NULL COMMENT 'サンプル値配列',
  `sample_value_counts_json` json DEFAULT NULL COMMENT '出現数上位の値',
  `distinct_value_count` int DEFAULT NULL COMMENT '値種類数。多すぎる場合は概算可',
  `blank_count` int NOT NULL DEFAULT 0 COMMENT '空欄件数',
  `non_blank_count` int NOT NULL DEFAULT 0 COMMENT '非空欄件数',
  `blank_rate` decimal(7,4) DEFAULT NULL COMMENT '空欄率。0.0000から1.0000',
  `min_numeric_value` decimal(20,6) DEFAULT NULL COMMENT '数値推定時の最小値',
  `max_numeric_value` decimal(20,6) DEFAULT NULL COMMENT '数値推定時の最大値',
  `min_text_length` int DEFAULT NULL COMMENT '非空値の最小文字数',
  `max_text_length` int DEFAULT NULL COMMENT '非空値の最大文字数',
  `first_non_blank_row_no` int DEFAULT NULL COMMENT '最初に値が入っているCSV行番号',
  `last_non_blank_row_no` int DEFAULT NULL COMMENT '最後に値が入っているCSV行番号',
  `inferred_value_type` varchar(32) NOT NULL DEFAULT 'UNKNOWN' COMMENT 'EMPTY/NUMERIC/DATE/CODE/TEXT/MIXED/UNKNOWN',
  `inferred_format` varchar(64) DEFAULT NULL COMMENT 'YYYYMMDD/YYYY-MM-DD/decimal/integerなどの補助推定',
  `sensitive_hint` tinyint(1) NOT NULL DEFAULT 0 COMMENT '個人特定情報に近い列の可能性',
  `value_profile_json` json DEFAULT NULL COMMENT '型推定、正規化、LLM投入用の補助プロファイル',
  `related_column_nos_json` json DEFAULT NULL COMMENT '関連列番の配列',
  `candidate_target_kind` varchar(64) DEFAULT NULL COMMENT 'LEDGER_FIELD/EXAM_ITEM_VALUE/IGNORE/REVIEW等',
  `candidate_namecode` char(17) DEFAULT NULL COMMENT '候補namecode',
  `candidate_ledger_field` varchar(64) DEFAULT NULL COMMENT '候補ledger field',
  `candidate_confidence` decimal(5,4) DEFAULT NULL COMMENT '機械候補またはLLM候補の信頼度。0.0000から1.0000',
  `decision_status` varchar(32) NOT NULL DEFAULT 'UNREVIEWED' COMMENT 'UNREVIEWED/ADOPT/IGNORE/NEEDS_CONFIRMATION/DEFERRED',
  `decision_note` text DEFAULT NULL COMMENT '最終判断メモ',
  `seed_target` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'seed化対象',
  `seed_exported` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'seed反映済み',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`analysis_column_id`),
  UNIQUE KEY `uq_analysis_columns_file_column` (`analysis_file_id`, `column_no`),
  KEY `idx_analysis_columns_file` (`analysis_file_id`),
  KEY `idx_analysis_columns_header` (`normalized_header_name`),
  KEY `idx_analysis_columns_type` (`inferred_value_type`, `inferred_format`),
  KEY `idx_analysis_columns_candidate_namecode` (`candidate_namecode`),
  KEY `idx_analysis_columns_candidate_ledger_field` (`candidate_ledger_field`),
  KEY `idx_analysis_columns_decision` (`decision_status`, `seed_target`, `seed_exported`),
  KEY `idx_analysis_columns_sensitive_hint` (`sensitive_hint`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
