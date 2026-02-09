CREATE TABLE `medi_xml_receipt_runs` (
  `xml_receipt_run_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'run×xml 実績ID',
  `run_id` bigint NOT NULL COMMENT '実行ID',

  `xml_sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'XML sha256(hex)',
  `xml_receipt_id` bigint DEFAULT NULL,

  `action` enum('NEW','SEEN') CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '検出結果',
  `message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_ja_0900_as_cs COMMENT '補足（例: エラー概要など）',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',

  PRIMARY KEY (`xml_receipt_run_id`),
  UNIQUE KEY `uq_medi_xml_receipt_runs_run_xml` (`run_id`,`xml_sha256`),
  KEY `idx_medi_xml_receipt_runs_run` (`run_id`),
  KEY `idx_medi_xml_receipt_runs_xml` (`xml_sha256`),
  KEY `idx_medi_xml_receipt_runs_receipt_id` (`xml_receipt_id`),

  CONSTRAINT `fk_medi_xml_receipt_runs_receipt`
    FOREIGN KEY (`xml_receipt_id`) REFERENCES `medi_xml_receipts` (`xml_receipt_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE,

  CONSTRAINT `fk_medi_xml_receipt_runs_run`
    FOREIGN KEY (`run_id`) REFERENCES `medi_import_runs` (`run_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs
  COMMENT='【medi】XML検出実績（run×xml：NEW/SEEN）';
