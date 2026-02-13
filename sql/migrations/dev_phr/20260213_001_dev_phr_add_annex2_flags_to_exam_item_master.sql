

-- =====================================================================
-- Migration: 20260213_001_dev_phr_add_annex2_flags_to_exam_item_master
-- Target   : dev_phr.exam_item_master
-- Purpose  : 付属2（A列/B列）対応カラムを追加（freeze v1.0 後の前提固定）
-- Author   : hiro
-- Notes    : v1.0 では未使用可。NULL許容で安全追加。
-- =====================================================================

-- =============================
-- 0) Pre-check
-- =============================
-- 既存カラム確認（実行前に結果を確認すること）
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dev_phr'
  AND TABLE_NAME   = 'exam_item_master'
  AND COLUMN_NAME IN (
    'annex2_exec_requirement',
    'annex2_legal_report_flag',
    'cda_section_code_default'
  );


-- =============================
-- 1) ALTER TABLE
-- =============================
ALTER TABLE dev_phr.exam_item_master
  ADD COLUMN annex2_exec_requirement varchar(32)
    DEFAULT NULL
    COMMENT '付属2 A列: 実施要件（MUST / OPTIONAL_BY_DOCTOR / EITHER_OK / REPORT_IF_AVAILABLE）'
    AFTER identity_item_name,

  ADD COLUMN annex2_legal_report_flag tinyint(1)
    DEFAULT NULL
    COMMENT '付属2 B列: 法定報告フラグ（1=法定, 0=任意）'
    AFTER annex2_exec_requirement,

  ADD COLUMN cda_section_code_default varchar(16)
    DEFAULT NULL
    COMMENT 'CDAデフォルトセクションコード（01010 / 01990）'
    AFTER annex2_legal_report_flag;


-- =============================
-- 2) Post-check
-- =============================
-- カラム追加確認
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dev_phr'
  AND TABLE_NAME   = 'exam_item_master'
  AND COLUMN_NAME IN (
    'annex2_exec_requirement',
    'annex2_legal_report_flag',
    'cda_section_code_default'
  );


-- =============================
-- 3) Rollback (manual)
-- =============================
-- 必要時のみ手動実行
-- ALTER TABLE dev_phr.exam_item_master
--   DROP COLUMN cda_section_code_default,
--   DROP COLUMN annex2_legal_report_flag,
--   DROP COLUMN annex2_exec_requirement;

-- =====================================================================
-- End of migration
-- =====================================================================