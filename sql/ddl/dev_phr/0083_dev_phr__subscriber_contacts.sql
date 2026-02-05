CREATE TABLE `subscriber_contacts` (
  `contact_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `subscriber_id` bigint unsigned NOT NULL,
  `phone` varchar(50) DEFAULT NULL,
  `email` varchar(190) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `valid_from` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `valid_to` datetime(3) DEFAULT NULL,
  `is_current` tinyint(1) NOT NULL DEFAULT '1',
  `source` varchar(50) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`contact_id`),
  KEY `idx_contacts_subscriber` (`subscriber_id`),
  KEY `idx_contacts_subscriber_current` (`subscriber_id`,`is_current`),
  CONSTRAINT `fk_contacts_subscriber` FOREIGN KEY (`subscriber_id`) REFERENCES `subscribers` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT,
  CONSTRAINT `chk_contacts_is_current` CHECK ((`is_current` in (0,1)))
) ENGINE=InnoDB AUTO_INCREMENT=154 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;