CREATE TABLE `dev_phr`.`fund_line_types` (
  `id` tinyint unsigned NOT NULL AUTO_INCREMENT COMMENT '系統種別ID',
  `line_code` varchar(32) NOT NULL COMMENT '系統コード',
  `line_label` varchar(64) NOT NULL COMMENT '系統表示名',
  `display_order` int unsigned NOT NULL DEFAULT 0 COMMENT '表示順',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '有効フラグ',
  `description` varchar(190) DEFAULT NULL COMMENT '説明',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_line_code` (`line_code`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
