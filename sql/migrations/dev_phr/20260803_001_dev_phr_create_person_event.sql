CREATE TABLE IF NOT EXISTS `dev_phr`.`person_event` (
    `person_event_id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '人×イベント単位ID',

    `event_id` BIGINT NOT NULL COMMENT 'イベントID',
    `subscriber_id` BIGINT UNSIGNED NOT NULL COMMENT '加入者ID（subscribers.id参照）',
    `person_id_custom` VARCHAR(64) NOT NULL COMMENT '個人ID（簡易キー）',
    `identity_hash` CHAR(64) NOT NULL COMMENT '識別ハッシュ',
    `result_received_count` INT NOT NULL DEFAULT 0 COMMENT '結果受領回数',
    `delivery_detected_count` INT NOT NULL DEFAULT 0 COMMENT '納品対象として検出された回数',
    `last_result_received_at` DATETIME NULL COMMENT '最終結果受領日時',
    `last_delivery_detected_at` DATETIME NULL COMMENT '最終納品検出日時',

    `is_eligible` TINYINT(1) NULL COMMENT '対象者フラグ',
    `result_received_flag` TINYINT(1) NULL COMMENT '結果受領フラグ',

    `hia_status_code` VARCHAR(50) NULL COMMENT 'HIA状態コード',

    `delivery_target_flag` TINYINT(1) NULL COMMENT '納品対象フラグ',
    `delivery_exported_flag` TINYINT(1) NULL COMMENT '納品済フラグ',
    `delivery_exported_at` DATETIME NULL COMMENT '納品日時',

    `gap_flag` TINYINT(1) NULL COMMENT 'ギャップフラグ',
    `gap_reason` VARCHAR(255) NULL COMMENT 'ギャップ理由',

    `last_observed_at` DATETIME NULL COMMENT '最終観測日時',

    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`person_event_id`),

    KEY `idx_person_event_01` (`event_id`, `subscriber_id`),
    KEY `idx_person_event_02` (`identity_hash`),
    KEY `idx_person_event_03` (`person_id_custom`),
    UNIQUE KEY `uk_person_event_01` (`event_id`, `subscriber_id`),
    CONSTRAINT `fk_person_event_01` FOREIGN KEY (`event_id`) REFERENCES `dev_phr`.`event` (`event_id`),
    CONSTRAINT `fk_person_event_02` FOREIGN KEY (`subscriber_id`) REFERENCES `dev_phr`.`subscribers` (`id`)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='人×イベント単位の状態管理';
