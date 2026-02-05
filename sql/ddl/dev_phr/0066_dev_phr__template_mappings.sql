CREATE TABLE `dev_phr`.`template_mappings` (
  `fund_id` bigint unsigned NOT NULL,
  `version` int NOT NULL,
  `col_order` int NOT NULL,
  `csv_header` varchar(190) NOT NULL,
  `target_column` varchar(190) NOT NULL,
  `rule` varchar(190) NOT NULL,
  `required` tinyint(1) NOT NULL DEFAULT 0,
  `notes` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  UNIQUE KEY `uq_template_mapping` (`fund_id`, `version`, `col_order`, `target_column`),
  KEY `idx_tmplate_template` (`fund_id`, `version`),

  CONSTRAINT `chk_template_required`
    CHECK ((`required` IN (0, 1)))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
