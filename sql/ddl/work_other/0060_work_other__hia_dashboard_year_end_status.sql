-- 0060_work_other__hia_dashboard_year_end_status.sql

CREATE TABLE `work_other`.`hia_dashboard_year_end_status` (

  `hia_dashboard_year_end_status_sid` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'surrogate key',

  -- =============================
  -- キー
  -- =============================
  `identity_hash` VARCHAR(190) NOT NULL COMMENT '人物識別ハッシュ',
  `fiscal_year` INT NOT NULL COMMENT '年度（例: 2025）',
  `insurer_number` CHAR(8) NOT NULL COMMENT '保険者番号',

  -- =============================
  -- ID系
  -- =============================
  `person_id_custom` VARCHAR(190) NULL COMMENT '内部人物ID',
  `subscribers_id` BIGINT UNSIGNED NULL COMMENT 'subscribers.id（参照用）',
  `hia_subscriber_id` VARCHAR(190) NULL COMMENT 'HIA加入者ID（取得できる場合）',

  -- =============================
  -- ダッシュボード状態
  -- =============================
  `status` VARCHAR(50) NULL COMMENT '年度末時点のダッシュボードステータス',

  -- =============================
  -- 健診イベント
  -- =============================
  `reservation_date` DATE NULL COMMENT '健診予約日',
  `exam_date` DATE NULL COMMENT '健診受診日',

  -- =============================
  -- 医療機関
  -- =============================
  `medical_institution_code` VARCHAR(50) NULL COMMENT '医療機関コード',
  `medical_institution_name` VARCHAR(255) NULL COMMENT '医療機関名',

  -- =============================
  -- 記帳管理
  -- =============================
  `snapshot_at` DATETIME NOT NULL COMMENT 'スナップショット記帳日時',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '作成日時',

  PRIMARY KEY (`hia_dashboard_year_end_status_sid`),

  UNIQUE KEY `uk_identity_hash_fiscal_year`
    (`identity_hash`, `fiscal_year`, `insurer_number`)

) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='HIAダッシュボード年度末状態スナップショット';