-- Create the subscriber address history table in environments built before
-- the current dev_phr DDL set. Parent consistency is managed by the apply
-- pipeline, so this table intentionally has no foreign key.

CREATE TABLE IF NOT EXISTS `subscriber_addresses` (
  `address_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `subscriber_id` bigint unsigned NOT NULL,
  `postal_code` varchar(10) DEFAULT NULL,
  `address_line` varchar(190) DEFAULT NULL,
  `building` varchar(190) DEFAULT NULL,
  `address_hash` char(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL
    COMMENT '住所値差分検知用 compare hash。apply時はstaging_subscribers_hubから反映。対象値更新時は再生成必須',
  `valid_from` datetime(3) DEFAULT NULL,
  `valid_to` datetime(3) DEFAULT NULL,
  `is_current` tinyint(1) NOT NULL DEFAULT 1,
  `source` varchar(50) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `prefecture` varchar(50) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `prefecture_code` tinyint unsigned DEFAULT NULL,
  PRIMARY KEY (`address_id`),
  KEY `idx_addresses_subscriber` (`subscriber_id`),
  KEY `idx_addresses_subscriber_current` (`subscriber_id`, `is_current`),
  KEY `idx_addresses_address_hash` (`address_hash`),
  KEY `idx_addresses_subscriber_address_hash` (`subscriber_id`, `address_hash`),
  CONSTRAINT `chk_addresses_is_current` CHECK ((`is_current` IN (0, 1)))
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs;
