

CREATE TABLE `subscriber_contact_points` (
  `contact_point_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `subscriber_id` BIGINT UNSIGNED NOT NULL,

  `contact_type` VARCHAR(50)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs
    NOT NULL COMMENT 'phone / email 等の連絡先種別',

  `contact_value` VARCHAR(255)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs
    NOT NULL COMMENT '連絡先値',

  `is_current` TINYINT(1)
    NOT NULL DEFAULT 1 COMMENT 'current contact flag',

  `valid_from` DATETIME
    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'current開始日時',

  `valid_to` DATETIME
    NULL COMMENT 'current終了日時',

  `source` VARCHAR(100)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs
    NULL COMMENT 'データ由来',

  `created_at` DATETIME
    NOT NULL DEFAULT CURRENT_TIMESTAMP,

  `updated_at` DATETIME
    NOT NULL DEFAULT CURRENT_TIMESTAMP
    ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (`contact_point_id`),

  KEY `idx_scp_subscriber_id` (`subscriber_id`),
  KEY `idx_scp_contact_type` (`contact_type`),
  KEY `idx_scp_is_current` (`is_current`),

  KEY `idx_scp_subscriber_type_current` (
    `subscriber_id`,
    `contact_type`,
    `is_current`
  ),

  KEY `idx_scp_subscriber_type_value` (
    `subscriber_id`,
    `contact_type`,
    `contact_value`
  ),

  CONSTRAINT `fk_scp_subscriber_id`
    FOREIGN KEY (`subscriber_id`)
    REFERENCES `subscribers` (`subscriber_id`)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='加入者連絡先履歴（Hub apply contact point 正本構造）';