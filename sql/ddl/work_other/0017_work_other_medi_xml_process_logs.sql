CREATE TABLE `medi_xml_process_logs` (
  `xml_process_log_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'PK',
  `run_id` bigint NOT NULL COMMENT '実行(run)ID',

  `xml_sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'XML sha256(hex)',

  `step` enum('WELLFORMED','CDA_INDEX','XSD_VALIDATE','EXTRACT_ITEMS','LEDGER','OTHER')
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

  `result` enum('OK','ERROR','SKIP')
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '結果',

  `message` text CHARACTER SET utf8mb4 COLLATE utf8mb4_ja_0900_as_cs COMMENT '補足/エラー概要（短文化推奨）',
  `processed_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '処理日時',

  PRIMARY KEY (`xml_process_log_id`),

  UNIQUE KEY `uq_medi_xml_process_logs_run_xml_step` (`run_id`,`xml_sha256`,`step`),
  KEY `idx_medi_xml_process_logs_xml` (`xml_sha256`),
  KEY `idx_medi_xml_process_logs_run` (`run_id`),

  CONSTRAINT `fk_medi_xml_process_logs_run`
    FOREIGN KEY (`run_id`) REFERENCES `medi_import_runs` (`run_id`)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs
  COMMENT='【medi】XML処理ログ（パース/索引抽出/XSD検証などの結果履歴）';
