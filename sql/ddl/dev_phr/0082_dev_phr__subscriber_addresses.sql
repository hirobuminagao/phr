CREATE TABLE `dev_phr`.`subscriber_addresses` (
  `address_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `subscriber_id` bigint unsigned NOT NULL,
  `postal_code` varchar(10) DEFAULT NULL,
  `address_line` varchar(190) DEFAULT NULL,
  `building` varchar(190) DEFAULT NULL,
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

  CONSTRAINT `chk_addresses_is_current`
    CHECK ((`is_current` IN (0, 1)))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
