CREATE TABLE `dev_phr`.`event_ledger` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'イベント台帳ID',
  `fund_id` bigint unsigned DEFAULT NULL COMMENT '健保ID（funds.id）',
  `insurer_number` char(8) DEFAULT NULL COMMENT '保険者番号（8桁）',
  `event_type` varchar(64) NOT NULL COMMENT 'イベント種別（MEDI_XML_RECEIVED / RESERVATION_CREATED 等）',
  `event_date` date DEFAULT NULL COMMENT 'イベント日（種別ごとに定義）',
  `event_ts` datetime(3) DEFAULT NULL COMMENT 'イベント時刻（必要時のみ）',
  `fiscal_year` smallint DEFAULT NULL COMMENT '年度（例：2025）',
  `person_key` varchar(190) DEFAULT NULL COMMENT '個人キー（正規化済・暫定）',
  `person_key_type` varchar(32) DEFAULT NULL COMMENT '個人キー種別（XML/HIA/RESERVE等）',
  `person_id_final` bigint unsigned DEFAULT NULL COMMENT '最終確定加入者ID（レビュー後に埋める）',
  `source_system` varchar(32) NOT NULL COMMENT 'ソース（MEDI/HIA/RESERVE等）',
  `source_table` varchar(64) NOT NULL COMMENT '元テーブル名',
  `source_record_id` bigint unsigned NOT NULL COMMENT '元テーブルのPK',
  `match_status` varchar(32) NOT NULL DEFAULT 'NEW' COMMENT 'NEW/AUTO_MATCH/NEEDS_REVIEW/CONFIRMED/OUT_OF_SCOPE/OVERRIDDEN 等',
  `match_reason` text COMMENT '根拠（JSONでも文字列でも）',
  `reviewed_at` datetime(3) DEFAULT NULL COMMENT 'レビュー確定日時',
  `reviewed_by` varchar(64) DEFAULT NULL COMMENT 'レビュー確定者',
  `review_note` text COMMENT 'レビュー備考',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_event_source` (`source_system`, `source_table`, `source_record_id`),
  KEY `idx_event_insurer_date` (`insurer_number`, `event_date`),
  KEY `idx_event_type_date` (`event_type`, `event_date`),
  KEY `idx_event_person_key` (`insurer_number`, `person_key`),
  KEY `idx_event_person_final` (`insurer_number`, `person_id_final`),
  KEY `idx_event_fund_id` (`fund_id`),

  CONSTRAINT `fk_event_fund`
    FOREIGN KEY (`fund_id`)
    REFERENCES `dev_phr`.`funds` (`id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='イベント台帳（各ソースから抽出したイベントを共通フォーマットで記帳）';
