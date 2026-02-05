CREATE TABLE `work_other`.`medi_xml_ledger` (
  `xml_ledger_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'XML台帳ID',
  `run_id` bigint NOT NULL COMMENT '実行ID',
  `zip_receipt_id` bigint NOT NULL COMMENT 'ZIP受領ログID',

  `facility_folder_name` varchar(255) DEFAULT NULL COMMENT '施設フォルダ名（フォルダ由来）',
  `facility_code` varchar(64) DEFAULT NULL COMMENT '健診機関コード（フォルダ由来）',
  `facility_name` varchar(255) DEFAULT NULL COMMENT '健診機関名（フォルダ由来）',

  `zip_name` varchar(255) NOT NULL COMMENT 'ZIPファイル名',
  `zip_sha256` char(64) NOT NULL COMMENT 'ZIPのSHA256',
  `xml_filename` varchar(255) NOT NULL COMMENT 'XMLファイル名',
  `zip_inner_path` varchar(1024) NOT NULL COMMENT 'ZIP内パス',
  `zip_inner_path_sha256` char(64) NOT NULL COMMENT 'zip_inner_pathのSHA256（インデックス用）',

  `insurer_number` varchar(8) DEFAULT NULL COMMENT '保険者番号',
  `insurance_symbol` varchar(190) DEFAULT NULL COMMENT '保険証記号（正規化後）',
  `insurance_symbol_match` varchar(190) DEFAULT NULL COMMENT '保険証記号（照合用・全角正規化）',
  `insurance_number` varchar(20) DEFAULT NULL COMMENT '保険証番号（正規化後）',
  `insurance_number_match` varchar(20) DEFAULT NULL COMMENT '保険証番号（照合用・半角数字）',
  `insurance_branch_number` varchar(20) DEFAULT NULL COMMENT '枝番',

  `birth_date` date DEFAULT NULL COMMENT '生年月日（正規化後）',
  `kenshin_date` date DEFAULT NULL COMMENT '健診実施日（正規化後）',

  `gender_code` varchar(10) DEFAULT NULL COMMENT '性別コード',
  `name_kana_full` varchar(190) DEFAULT NULL COMMENT '対象者氏名カナ',
  `name_kana_match` varchar(190) DEFAULT NULL COMMENT '照合用氏名カナ（name_kana_fullを正規化）',

  `postal_code` varchar(10) DEFAULT NULL COMMENT '郵便番号',
  `address` text COMMENT '住所（正規化後）',

  `org_name_in_xml` varchar(255) DEFAULT NULL COMMENT '健診実施医療機関名（XML内）',
  `org_code_in_xml` varchar(64) DEFAULT NULL COMMENT '健診実施機関番号（XML内）',

  `report_category_code` varchar(32) DEFAULT NULL COMMENT '報告区分コード',
  `program_type_code` varchar(32) DEFAULT NULL COMMENT '健診実施プログラム種別コード',
  `guidance_level_code` varchar(32) DEFAULT NULL COMMENT '保健指導レベルコード',
  `metabo_code` varchar(32) DEFAULT NULL COMMENT 'メタボ判定コード',

  `xsd_valid` tinyint(1) DEFAULT NULL COMMENT 'XSD検証結果（NULL/1/0）',
  `error_content` text COMMENT 'XSDエラー・未検出・例外など',

  `judge_status` enum('OK','ERROR','SKIP') DEFAULT NULL COMMENT '【未使用】判定結果ステータス（OK=健診結果として有効, ERROR=判定失敗, SKIP=健診結果ではない）',
  `is_exam_result` tinyint(1) DEFAULT NULL COMMENT '【未使用】健診結果XMLか（1=候補, 0=非健診, NULL=未判定）',
  `is_legal_exam` tinyint(1) DEFAULT NULL COMMENT '【未使用】法定健診として成立するか（1=成立, 0=不足, NULL=未判定）',
  `judge_score` int DEFAULT NULL COMMENT '【未使用】判定スコア（必須項目充足数など、任意ロジック用）',
  `judge_note` text COMMENT '【未使用】判定メモ（例: 健診結果ではない / 必須項目不足: 血圧）',
  `judged_run_id` bigint DEFAULT NULL COMMENT '【未使用】判定を行った run_id',
  `judged_at` datetime(6) DEFAULT NULL COMMENT '【未使用】判定日時',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',

  `lsio_legal_required_count` int NOT NULL DEFAULT 0 COMMENT 'LSIO required method count',
  `lsio_legal_present_count` int NOT NULL DEFAULT 0 COMMENT 'LSIO present method count (found in xml)',
  `lsio_legal_is_complete` tinyint NOT NULL DEFAULT 0 COMMENT '1=all required methods present',
  `lsio_legal_missing_methods` text COMMENT 'missing xml_method_code list (comma)',
  `lsio_legal_judged_run_id` bigint DEFAULT NULL COMMENT 'judge run id',
  `lsio_legal_judged_at` datetime(6) DEFAULT NULL COMMENT 'judged at',

  `xml_sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL COMMENT 'work_other.medi_xml_receiptsのxml_sha256からクエリーで埋めてる',

  PRIMARY KEY (`xml_ledger_id`),

  UNIQUE KEY `uq_medi_xml_ledger_zip_member` (`zip_sha256`, `zip_inner_path_sha256`),

  KEY `idx_medi_xml_ledger_run` (`run_id`),
  KEY `idx_medi_xml_ledger_zip_sha256` (`zip_sha256`),
  KEY `idx_medi_xml_ledger_facility_code` (`facility_code`),
  KEY `idx_medi_xml_ledger_person_hint` (`insurer_number`, `insurance_symbol`, `insurance_number`, `insurance_branch_number`, `birth_date`),
  KEY `idx_medi_xml_ledger_kenshin_date` (`kenshin_date`),
  KEY `idx_medi_xml_ledger_xml_sha256` (`xml_sha256`),
  KEY `idx_medi_xml_ledger_zip_receipt_id` (`zip_receipt_id`)
)
ENGINE=InnoDB
AUTO_INCREMENT=318519
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='【medi】健診XML台帳（ZIP構造OKのみ）';
