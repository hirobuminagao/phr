CREATE TABLE `work_other`.`medi_import_runs` (
  `run_id` bigint NOT NULL AUTO_INCREMENT COMMENT '実行ID',
  `started_at` datetime(6) NOT NULL COMMENT '開始時刻',
  `finished_at` datetime(6) DEFAULT NULL COMMENT '終了時刻',
  `input_root` varchar(1024) DEFAULT NULL COMMENT '入力ルート（ZIP探索開始パス）',
  `note` varchar(1024) DEFAULT NULL COMMENT 'メモ',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '作成日時',

  PRIMARY KEY (`run_id`),
  KEY `idx_medi_import_runs_started_at` (`started_at`)
)
ENGINE=InnoDB
AUTO_INCREMENT=55
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='【medi】健診XML受領・記帳の実行ログ';
