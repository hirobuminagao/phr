CREATE TABLE `work_other`.`medi_exam_result_ledger` (
  `ledger_id` bigint NOT NULL AUTO_INCREMENT COMMENT '健診結果台帳ID',

  `health_examination_date` date DEFAULT NULL COMMENT 'HEALTH_EXAMINATION_DATE',

  `insurer_number` varchar(8) DEFAULT NULL COMMENT '保険者番号',
  `insurance_card_symbol` varchar(64) DEFAULT NULL COMMENT 'INSURANCE_CARD_SYMBOL',
  `insurance_card_number` varchar(64) DEFAULT NULL COMMENT 'INSURANCE_CARD_NUMBER',

  `name_full` varchar(255) DEFAULT NULL COMMENT 'NAME_FULL',
  `name_kana` varchar(255) DEFAULT NULL COMMENT 'NAME_KANA',
  `gender_code` varchar(2) DEFAULT NULL COMMENT '性別コード（1=男性, 2=女性, 9=不明）',
  `birthday` date DEFAULT NULL COMMENT 'BIRTHDAY',

  `health_exam_report_category` varchar(64) DEFAULT NULL COMMENT '健診報告区分',
  `program_code` varchar(64) DEFAULT NULL COMMENT 'プログラムコード',

  `postalcode` varchar(16) DEFAULT NULL COMMENT 'POSTALCODE',
  `address` varchar(255) DEFAULT NULL COMMENT 'ADDRESS',

  `health_examination_organization_name` varchar(255) DEFAULT NULL COMMENT 'HEALTH_EXAMINATION_ORGANIZATION_NAME',
  `health_examination_organization_no` varchar(64) DEFAULT NULL COMMENT 'HEALTH_EXAMINATION_ORGANIZATION_NO',
  `health_examination_organization_postalcode` varchar(16) DEFAULT NULL COMMENT 'HEALTH_EXAMINATION_ORGANIZATION_POSTALCODE',
  `health_examination_organization_address` varchar(255) DEFAULT NULL COMMENT 'HEALTH_EXAMINATION_ORGANIZATION_ADDRESS',
  `health_examination_organization_tel` varchar(32) DEFAULT NULL COMMENT 'HEALTH_EXAMINATION_ORGANIZATION_TEL',

  `input_method` varchar(64) DEFAULT NULL COMMENT '入力方法（例: excel_copy, navicat_import など）',
  `source_note` text COMMENT 'ソース（ファイル名・元データ説明など）',

  `insurance_card_symbol_match` varchar(64) DEFAULT NULL COMMENT '照合用記号（運用/正規化後の想定）',
  `insurance_card_number_match` varchar(64) DEFAULT NULL COMMENT '照合用番号（運用/正規化後の想定）',
  `name_kana_match` varchar(255) DEFAULT NULL COMMENT '照合用カナ（運用/正規化後の想定）',

  `gender` varchar(16) DEFAULT NULL COMMENT 'GENDER（原文のまま保持）',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`ledger_id`),

  KEY `idx_ledger_exam_date` (`health_examination_date`),
  KEY `idx_ledger_name_kana` (`name_kana`),
  KEY `idx_ledger_insurance` (`insurance_card_symbol`, `insurance_card_number`),
  KEY `idx_ledger_insurance_match` (`insurance_card_symbol_match`, `insurance_card_number_match`)
)
ENGINE=InnoDB
AUTO_INCREMENT=9
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診結果台帳（1人=1件の基本情報。namecode以外を基本ここに保持）';
