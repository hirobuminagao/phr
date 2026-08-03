CREATE TABLE IF NOT EXISTS `dev_phr`.`person_event_status_items` (
  `person_event_status_item_id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '人×イベント状態項目ID',
  `person_event_id` bigint NOT NULL COMMENT '人×イベント単位ID',
  `event_id` bigint NOT NULL COMMENT 'イベントID',
  `subscriber_id` bigint unsigned NOT NULL COMMENT '加入者ID',
  `item_code` varchar(64) NOT NULL COMMENT '状態項目コード',
  `value_type` varchar(16) NOT NULL COMMENT '値型 BOOL/NUMBER/TEXT/CODE/DATE/DATETIME/REF',
  `value_bool` tinyint(1) DEFAULT NULL COMMENT '真偽値',
  `value_number` decimal(20,6) DEFAULT NULL COMMENT '数値',
  `value_text` text DEFAULT NULL COMMENT '文字列',
  `value_code` varchar(128) DEFAULT NULL COMMENT 'コード値',
  `value_date` date DEFAULT NULL COMMENT '日付',
  `value_datetime` datetime DEFAULT NULL COMMENT '日時',
  `value_ref_type` varchar(64) DEFAULT NULL COMMENT '参照先種別',
  `value_ref_id` bigint unsigned DEFAULT NULL COMMENT '参照先ID',
  `reason` text DEFAULT NULL COMMENT '理由・補足',
  `source_system` varchar(64) NOT NULL DEFAULT 'FROM_MEDICAL' COMMENT '更新元システム',
  `source_run_id` bigint unsigned DEFAULT NULL COMMENT '更新元ETL run ID',
  `refreshed_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最終反映日時',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`person_event_status_item_id`),
  UNIQUE KEY `uq_person_event_status_items_code` (`person_event_id`, `item_code`),
  KEY `idx_person_event_status_items_event` (`event_id`, `item_code`),
  KEY `idx_person_event_status_items_subscriber` (`subscriber_id`),
  KEY `idx_person_event_status_items_ref` (`value_ref_type`, `value_ref_id`),
  CONSTRAINT `fk_person_event_status_items_person_event`
    FOREIGN KEY (`person_event_id`) REFERENCES `dev_phr`.`person_event` (`person_event_id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_person_event_status_items_event`
    FOREIGN KEY (`event_id`) REFERENCES `dev_phr`.`event` (`event_id`),
  CONSTRAINT `fk_person_event_status_items_subscriber`
    FOREIGN KEY (`subscriber_id`) REFERENCES `dev_phr`.`subscribers` (`id`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='人×イベント単位の可変状態項目';
