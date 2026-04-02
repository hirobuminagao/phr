CREATE TABLE dev_phr.event_instance (
    event_instance_id BIGINT NOT NULL AUTO_INCREMENT COMMENT 'イベント実体ID',

    event_id BIGINT NOT NULL COMMENT 'イベントID',
    subscriber_id BIGINT UNSIGNED NOT NULL COMMENT '加入者ID（subscribers.id参照）',
    person_id_custom VARCHAR(64) NOT NULL COMMENT '個人ID',
    identity_hash CHAR(64) NOT NULL COMMENT '識別ハッシュ',

    instance_type VARCHAR(50) NOT NULL COMMENT 'イベント実体種別（受領/観測/納品検出など）',
    instance_status VARCHAR(50) NULL COMMENT '状態',

    occurred_at DATETIME NULL COMMENT '発生日時',
    observed_at DATETIME NULL COMMENT '観測日時',

    source_system VARCHAR(100) NULL COMMENT 'データソース',
    source_key VARCHAR(255) NULL COMMENT 'ソース内キー',

    payload_json JSON NULL COMMENT '元データ',

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (event_instance_id),

    KEY idx_event_instance_01 (event_id, subscriber_id),
    KEY idx_event_instance_02 (identity_hash),
    KEY idx_event_instance_03 (instance_type, occurred_at),
    KEY idx_event_instance_04 (source_system, source_key),
    KEY idx_event_instance_05 (person_id_custom),
    KEY idx_event_instance_06 (subscriber_id),
    CONSTRAINT fk_event_instance_01 FOREIGN KEY (event_id) REFERENCES dev_phr.event (event_id),
    CONSTRAINT fk_event_instance_02 FOREIGN KEY (subscriber_id) REFERENCES dev_phr.subscribers (id)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='実施・結果・請求などのイベント実体';