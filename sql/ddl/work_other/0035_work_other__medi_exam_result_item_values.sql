CREATE TABLE `work_other`.`medi_exam_result_item_values` (
  `item_value_id` bigint NOT NULL AUTO_INCREMENT COMMENT '健診項目値ID',

  `ledger_id` bigint NOT NULL COMMENT '健診結果台帳ID',
  `namecode` varchar(32) NOT NULL COMMENT 'namecode',

  `raw_value` text COMMENT '入力値（未正規化の原文）',
  `value` text COMMENT '正規化後の値（XML生成に使用）',
  `nullflavor` varchar(32) DEFAULT NULL COMMENT 'nullFlavor',

  `normalize_status` enum('RAW','OK','ERROR') NOT NULL DEFAULT 'RAW' COMMENT 'RAW=未正規化, OK=正規化済, ERROR=正規化失敗',
  `normalized_at` datetime(6) DEFAULT NULL COMMENT '正規化実行日時',
  `normalize_error` text COMMENT '正規化エラー詳細（TEXT）',

  `identity_item_code` varchar(16) DEFAULT NULL COMMENT 'identity_item_code（参照用）',
  `value_seq` smallint NOT NULL DEFAULT 1 COMMENT '同一namecode内の連番',
  `jun_no` int DEFAULT NULL COMMENT 'jun_no（参照用）',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`item_value_id`),

  UNIQUE KEY `uq_ledger_namecode_seq` (`ledger_id`, `namecode`, `value_seq`),

  KEY `idx_item_values_namecode` (`namecode`),
  KEY `idx_item_values_ledger` (`ledger_id`),
  KEY `idx_item_values_norm` (`normalize_status`, `normalized_at`)
)
ENGINE=InnoDB
AUTO_INCREMENT=500
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目値（raw保持→正規化→valueでXML生成。エラーはTEXTで記録）';
