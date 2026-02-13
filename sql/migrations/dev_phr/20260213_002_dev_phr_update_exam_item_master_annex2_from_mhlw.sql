-- =====================================================================
-- Migration: 20260213_002_dev_phr_update_exam_item_master_annex2_from_mhlw
-- Target   : dev_phr.exam_item_master
-- Purpose  : 付属2（A列/B列）に基づき annex2 3カラムを初期反映（namecodeキー）
-- Source   : docs/mhlw/phase4_v08/001082795.xlsx
-- Author   : hiro
-- Notes    : freeze v1.0 中の前提固定。UPDATEは生成Excelを元に貼り付け。
-- =====================================================================

-- =============================
-- 0) Pre-check
-- =============================
-- 対象カラム存在確認
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dev_phr'
  AND TABLE_NAME   = 'exam_item_master'
  AND COLUMN_NAME IN (
    'annex2_exec_requirement',
    'annex2_legal_report_flag',
    'cda_section_code_default'
  );

-- 参考: 対象件数（namecodeが存在する前提）
SELECT COUNT(*) AS total_rows
FROM dev_phr.exam_item_master;

START TRANSACTION;

UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N001000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N006000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N011000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N021000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N016160100000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N016160200000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N016160300000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N056000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N056160400000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N061000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N061160800000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N066000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N066160800000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9A755000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9A752000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9A751000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9A765000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9A762000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9A761000000000001';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N141000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'EITHER_OK', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F015000002327101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'EITHER_OK', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F015000002327201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'EITHER_OK', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F015000002399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'EITHER_OK', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F015129902327101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'EITHER_OK', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F015129902327201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'EITHER_OK', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F015129902399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F070000002327101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F070000002327201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F070000002399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F077000002327101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F077000002327201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F077000002399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F077000002391901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3F069000002391901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3B035000002327201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3B035000002399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3B045000002327201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3B045000002399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3B090000002327101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '3B090000002399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3C015000002327101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3C015000002399901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3C015161602399911';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3C015161002399949';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '8A065000002391901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010000001926101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010000002227101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010000001927201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010000001999901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010129901926101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010129902227101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010129901927201';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D010129901999901';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D046000001906202';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D046000001920402';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D046000001927102';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'CONDITIONAL', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '3D046000001999902';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '1A020000000191111';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '1A020000000190111';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '1A010000000191111';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '1A010000000190111';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '2A040000001930102';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '2A030000001930101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '2A020000001930101';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '2A020161001930149';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9A110160700000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9A110160800000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9A110161600000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9A110161000000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100166000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100166100000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100166200000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100166300000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100166600000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100166500000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100160900000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100161600000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'OPTIONAL_BY_DOCTOR', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9E100161000000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N501000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N506000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N511000000000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'REPORT_IF_AVAILABLE', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9N512000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'MUST', annex2_legal_report_flag = 1, cda_section_code_default = '01010' WHERE namecode = '9N701000000000011';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'REPORT_IF_AVAILABLE', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9N701167000000049';
UPDATE dev_phr.exam_item_master SET annex2_exec_requirement = 'REPORT_IF_AVAILABLE', annex2_legal_report_flag = 2, cda_section_code_default = '01010' WHERE namecode = '9N701167100000049';

COMMIT;

-- =============================
-- 1) Post-check
-- =============================
-- 反映状況のざっくり集計（NULLも含めて確認）
SELECT
  COUNT(*) AS total,
  SUM(annex2_exec_requirement IS NOT NULL) AS filled_exec_requirement,
  SUM(annex2_legal_report_flag IS NOT NULL) AS filled_legal_flag,
  SUM(cda_section_code_default IS NOT NULL) AS filled_section
FROM dev_phr.exam_item_master;

-- flag別の分布
SELECT annex2_legal_report_flag, COUNT(*) AS cnt
FROM dev_phr.exam_item_master
GROUP BY annex2_legal_report_flag
ORDER BY annex2_legal_report_flag;

-- =====================================================================
-- End of migration
-- =====================================================================