CREATE TABLE dev_phr.event (
    event_id BIGINT NOT NULL AUTO_INCREMENT COMMENT 'イベントID（内部識別子）',

    insurer_number VARCHAR(20) NOT NULL COMMENT '保険者番号',
    event_year INT NOT NULL COMMENT '年度',
    event_type VARCHAR(50) NOT NULL COMMENT 'イベント種別（健診/保健指導など）',
    event_name VARCHAR(255) NULL COMMENT 'イベント名称',

    start_date DATE NULL COMMENT '開始日',
    end_date DATE NULL COMMENT '終了日',
    submission_deadline DATE NULL COMMENT '提出期限',
    eligibility_reference_date DATE NULL COMMENT '対象判定基準日',
    qualification_lost_reference_date DATE NULL COMMENT '資格喪失判定基準日',
    eligibility_rule_type VARCHAR(20) NOT NULL DEFAULT 'FIXED_DATE' COMMENT '資格判定ルール種別（NONE/FIXED_DATE）',
    qualification_lost_rule_type VARCHAR(20) NOT NULL DEFAULT 'NONE' COMMENT '資格喪失判定ルール種別（NONE/FIXED_DATE）',
    age_rule_type VARCHAR(20) NOT NULL DEFAULT 'FIXED_DATE' COMMENT '年齢計算ルール種別（FIXED_DATE/EXAM_DATE）',
    age_reference_date DATE NULL COMMENT '年齢計算基準日',

    is_active TINYINT(1) NOT NULL DEFAULT 1 COMMENT '有効フラグ',

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (event_id),

    KEY idx_event_01 (insurer_number, event_year, event_type)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci
COMMENT='イベント枠（制度・年度単位の定義）';