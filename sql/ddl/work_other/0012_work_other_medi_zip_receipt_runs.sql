CREATE TABLE `medi_zip_receipt_runs` (
  `zip_receipt_run_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'ZIP受領-実行の紐付けID',
  `run_id` bigint NOT NULL COMMENT '実行ID',
  `zip_receipt_id` bigint NOT NULL COMMENT 'ZIP受領ログID（medi_zip_receipts）',

  `zip_sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT 'ZIPのSHA256（冗長だが検索用）',

  `action` enum('NEW','SEEN','UPDATED') CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '今回runでの扱い',
  `seen_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '検出日時',

  PRIMARY KEY (`zip_receipt_run_id`),

  UNIQUE KEY `uq_medi_zip_receipt_runs_run_sha` (`run_id`,`zip_sha256`),
  KEY `idx_medi_zip_receipt_runs_run` (`run_id`),
  KEY `idx_medi_zip_receipt_runs_zip_receipt` (`zip_receipt_id`),

  CONSTRAINT `fk_medi_zip_receipt_runs_run`
    FOREIGN KEY (`run_id`) REFERENCES `medi_import_runs` (`run_id`),

  CONSTRAINT `fk_medi_zip_receipt_runs_zip_receipt`
    FOREIGN KEY (`zip_receipt_id`) REFERENCES `medi_zip_receipts` (`zip_receipt_id`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs
  COMMENT='【medi】ZIP受領ログ×実行ログ（今回runで見たもの）';
