-- =====================================================================
-- Migration: 20260709_001_dev_phr_add_v2_rule_columns_to_exam_item_group_method_members
-- Target   : dev_phr.exam_item_group_method_members
-- Purpose  : v2制度チェック用のmethod単位ルール管理カラムを追加
-- Notes    : 既存LSIO行へ意味を付与しないため、rule系カラムはNULL許容で追加する。
-- =====================================================================

-- =============================
-- 0) Pre-check
-- =============================
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dev_phr'
  AND TABLE_NAME   = 'exam_item_group_method_members'
  AND COLUMN_NAME IN (
    'presence_value_mode',
    'required_flag',
    'rule_code',
    'rule_source_identity_codes',
    'rule_source_method_codes',
    'rule_source_namecodes',
    'is_active',
    'updated_at'
  );


-- =============================
-- 1) ALTER TABLE
-- =============================
ALTER TABLE `dev_phr`.`exam_item_group_method_members`
  ADD COLUMN `presence_value_mode` varchar(32)
    DEFAULT NULL
    COMMENT '存在判定方式'
    AFTER `priority`,

  ADD COLUMN `required_flag` tinyint(1)
    DEFAULT NULL
    COMMENT '制度上の必須・任意判定'
    AFTER `presence_value_mode`,

  ADD COLUMN `rule_code` varchar(64)
    DEFAULT NULL
    COMMENT '共通Rule/Calculateライブラリ識別子'
    AFTER `required_flag`,

  ADD COLUMN `rule_source_identity_codes` varchar(255)
    DEFAULT NULL
    COMMENT 'Ruleが参照するidentity_code一覧'
    AFTER `rule_code`,

  ADD COLUMN `rule_source_method_codes` varchar(255)
    DEFAULT NULL
    COMMENT 'Ruleが参照するmethod_code一覧'
    AFTER `rule_source_identity_codes`,

  ADD COLUMN `rule_source_namecodes` text NULL
    COMMENT 'Ruleが参照するnamecode一覧'
    AFTER `rule_source_method_codes`,

  ADD COLUMN `is_active` tinyint(1)
    NOT NULL
    DEFAULT 1
    COMMENT '1=有効, 0=無効'
    AFTER `rule_source_namecodes`,

  ADD COLUMN `updated_at` datetime(6)
    DEFAULT NULL
    ON UPDATE CURRENT_TIMESTAMP(6)
    AFTER `created_at`;


-- =============================
-- 2) Post-check
-- =============================
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dev_phr'
  AND TABLE_NAME   = 'exam_item_group_method_members'
  AND COLUMN_NAME IN (
    'presence_value_mode',
    'required_flag',
    'rule_code',
    'rule_source_identity_codes',
    'rule_source_method_codes',
    'rule_source_namecodes',
    'is_active',
    'updated_at'
  )
ORDER BY ORDINAL_POSITION;


-- =============================
-- 3) Rollback (manual)
-- =============================
-- 必要時のみ手動実行
-- ALTER TABLE `dev_phr`.`exam_item_group_method_members`
--   DROP COLUMN `updated_at`,
--   DROP COLUMN `is_active`,
--   DROP COLUMN `rule_source_namecodes`,
--   DROP COLUMN `rule_source_method_codes`,
--   DROP COLUMN `rule_source_identity_codes`,
--   DROP COLUMN `rule_code`,
--   DROP COLUMN `required_flag`,
--   DROP COLUMN `presence_value_mode`;

-- =====================================================================
-- End of migration
-- =====================================================================
