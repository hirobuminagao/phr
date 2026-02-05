CREATE TABLE `work_other`.`medi_xml_item_values` (
  `xml_item_value_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'PK',

  `xml_sha256` char(64) NOT NULL COMMENT 'XML sha256（medi_xml_receipts.xml_sha256）',
  `zip_sha256` char(64) NOT NULL COMMENT '親ZIP sha256',
  `zip_inner_path` varchar(512) NOT NULL COMMENT 'ZIP内相対パス（正規化済み / 区切りは / ）',
  `zip_inner_path_sha256` char(64) NOT NULL COMMENT 'ZIP内パスsha256（inner_pathから算出）',

  `namecode` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `occurrence_no` int NOT NULL DEFAULT 1 COMMENT '同一namecodeの出現順（1始まり）',

  `value_raw` text COMMENT '抽出した生値',
  `value_type` varchar(16) DEFAULT NULL COMMENT 'string/numeric/coded 等（任意）',
  `unit` varchar(32) DEFAULT NULL COMMENT '単位（任意）',

  `code_system` varchar(128) DEFAULT NULL COMMENT 'コード体系OID等（任意）',
  `code_value` varchar(64) DEFAULT NULL COMMENT 'コード値（任意）',
  `code_display` varchar(255) DEFAULT NULL COMMENT '表示名（任意）',

  `extracted_run_id` bigint DEFAULT NULL COMMENT '抽出run（medi_xml_item_extract_runs.extract_run_id）',
  `extracted_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`xml_item_value_id`),

  UNIQUE KEY `uq_xml_namecode_occ` (`xml_sha256`, `namecode`, `occurrence_no`),

  KEY `idx_xml_sha` (`xml_sha256`),
  KEY `idx_namecode` (`namecode`),
  KEY `idx_zip_inner_sha` (`zip_inner_path_sha256`),
  KEY `idx_extract_run` (`extracted_run_id`),
  KEY `idx_item_values_zip_inner` (`zip_sha256`, `zip_inner_path`),
  KEY `idx_item_values_namecode` (`namecode`)
)
ENGINE=InnoDB
AUTO_INCREMENT=2633199
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='【medi/work_other】XML項目値（生抽出のみ。法定判定マスタは持たない）';
