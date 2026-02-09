CREATE TABLE `shg_result` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',

  `person_id_custom` varchar(190)
    CHARACTER SET ascii COLLATE ascii_bin
    DEFAULT NULL COMMENT '照合用 person_id_custom',

  `shg_year_of_health_exam` int DEFAULT NULL COMMENT '健診実施年度',
  `usage_ticket_number` bigint DEFAULT NULL COMMENT '利用券番号',
  `expiration_date` date DEFAULT NULL COMMENT '利用券有効期限',

  PRIMARY KEY (`id`),
  KEY `idx_shg_person` (`person_id_custom`),
  KEY `idx_shg_year` (`shg_year_of_health_exam`)
) ENGINE=InnoDB
  AUTO_INCREMENT=5767
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs
  COMMENT='【shg】特定保健指導 利用券・年度管理（person_id_custom基準）';
