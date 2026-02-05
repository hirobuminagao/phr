CREATE TABLE `dev_phr`.`templates` (
  `fund_id` bigint unsigned NOT NULL,
  `version` int NOT NULL,
  `name` varchar(190) DEFAULT NULL,
  `template_type` varchar(190) NOT NULL DEFAULT 'fund_to_staging',
  `target_table` varchar(190) NOT NULL DEFAULT 'staging_subscribers_fund',
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `configured_on` datetime(3) DEFAULT NULL,
  `version_label` varchar(190) DEFAULT NULL,
  `created_by` varchar(190) DEFAULT NULL,
  `notes` text,

  PRIMARY KEY (`fund_id`, `version`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
