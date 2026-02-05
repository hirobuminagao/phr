CREATE TABLE `dev_phr`.`subscribers_exclusions` (
  `subscriber_id` bigint unsigned NOT NULL,
  `kind` varchar(20) NOT NULL,
  `reason` text,
  `source` varchar(50) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`subscriber_id`),

  CONSTRAINT `chk_exclusions_kind`
    CHECK ((`kind` IN ('demo', 'test', 'internal', 'other')))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
