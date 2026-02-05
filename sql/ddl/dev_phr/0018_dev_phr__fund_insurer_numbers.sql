CREATE TABLE `dev_phr`.`fund_insurer_numbers` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '健保保険者番号ID',
  `fund_id` bigint unsigned NOT NULL COMMENT '健保ID（funds.id）',
  `insurer_number` char(8) NOT NULL COMMENT '保険者番号（8桁）',
  `line_type_id` tinyint unsigned NOT NULL COMMENT '系統種別ID',
  `valid_from` date NOT NULL COMMENT '有効開始日',
  `valid_to` date DEFAULT NULL COMMENT '有効終了日',
  `is_current` tinyint(1) NOT NULL DEFAULT 1 COMMENT '現行フラグ',
  `notes` text COMMENT '備考',
  `insurer_no_id_custom` varchar(64) DEFAULT NULL COMMENT 'カスタム識別子',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_insurer_number_line_valid` (`insurer_number`, `line_type_id`, `valid_from`),
  KEY `idx_line_type_id` (`line_type_id`),
  KEY `idx_fund_id` (`fund_id`),
  KEY `idx_insurer_number` (`insurer_number`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
