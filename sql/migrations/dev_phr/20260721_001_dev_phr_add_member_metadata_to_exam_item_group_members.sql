-- =====================================================================
-- Migration: 20260721_001_dev_phr_add_member_metadata_to_exam_item_group_members
-- Target   : dev_phr.exam_item_group_members
-- Purpose  : 則44取得定義用のnamecodeメタ情報カラムを追加
-- Notes    : 既存group member行への即時バックフィルを必須にしないためNULL許容で追加する。
-- =====================================================================

-- =============================
-- 0) Pre-check
-- =============================
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dev_phr'
  AND TABLE_NAME   = 'exam_item_group_members'
  AND COLUMN_NAME IN (
    'value_type',
    'method',
    'identity_code'
  );


-- =============================
-- 1) ALTER TABLE
-- =============================
ALTER TABLE `dev_phr`.`exam_item_group_members`
  ADD COLUMN `value_type` varchar(8)
    DEFAULT NULL
    COMMENT '期待XML値型'
    AFTER `priority`,

  ADD COLUMN `method` varchar(32)
    DEFAULT NULL
    COMMENT '取得定義・分類用メタ情報'
    AFTER `value_type`,

  ADD COLUMN `identity_code` varchar(32)
    DEFAULT NULL
    COMMENT 'namecode由来の既存項目識別子（法令項目詳細Noではない）'
    AFTER `method`;


-- =============================
-- 2) Post-check
-- =============================
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dev_phr'
  AND TABLE_NAME   = 'exam_item_group_members'
  AND COLUMN_NAME IN (
    'value_type',
    'method',
    'identity_code'
  )
ORDER BY ORDINAL_POSITION;


-- =============================
-- 3) Rollback (manual)
-- =============================
-- 必要時のみ手動実行
-- ALTER TABLE `dev_phr`.`exam_item_group_members`
--   DROP COLUMN `identity_code`,
--   DROP COLUMN `method`,
--   DROP COLUMN `value_type`;

-- =====================================================================
-- End of migration
-- =====================================================================
