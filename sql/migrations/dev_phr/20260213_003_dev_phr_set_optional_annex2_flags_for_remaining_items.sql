

-- =====================================================================
-- Migration: 20260213_003_dev_phr_set_optional_annex2_flags_for_remaining_items
-- Target   : dev_phr.exam_item_master
-- Purpose  : 付属2掲載項目のうち、A/B列に印が無い項目を「任意(0)」として確定
-- Source   : docs/mhlw/phase4_v08/001082795.xlsx
-- Author   : hiro
-- Notes    : 20260213_002 の補完処理。jun_no IS NOT NULL を付属2掲載条件とする。
-- =====================================================================

-- =============================
-- 0) Pre-check
-- =============================
-- 対象件数確認（付属2掲載かつ未設定）
SELECT COUNT(*) AS target_rows
FROM dev_phr.exam_item_master
WHERE jun_no IS NOT NULL
  AND annex2_legal_report_flag IS NULL;

START TRANSACTION;

UPDATE dev_phr.exam_item_master
SET
  annex2_legal_report_flag = 0,
  cda_section_code_default = '01990'
WHERE jun_no IS NOT NULL
  AND annex2_legal_report_flag IS NULL;

COMMIT;

-- =============================
-- 1) Post-check
-- =============================
SELECT
  annex2_legal_report_flag,
  COUNT(*) AS cnt
FROM dev_phr.exam_item_master
WHERE jun_no IS NOT NULL
GROUP BY annex2_legal_report_flag
ORDER BY annex2_legal_report_flag;

-- =====================================================================
-- End of migration
-- =====================================================================