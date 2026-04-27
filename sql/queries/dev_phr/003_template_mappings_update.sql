-- ============================================================
-- template_mappings 更新（会社・住所・連絡先の正規化強化）
-- 目的:
-- as_is になっているカラムを text_norm へ変更し、
-- *_norm カラムの実態を base_norm 済みに統一する
-- ============================================================

-- 会社コード / 会社名
UPDATE dev_phr.template_mappings
SET rule = 'text_norm'
WHERE fund_id = 2
  AND version = '20260416'
  AND target_column IN (
    'received_company_code_norm',
    'received_company_name_norm'
  );

-- 所属 / 配布先 / 社員コード
UPDATE dev_phr.template_mappings
SET rule = 'text_norm'
WHERE fund_id = 2
  AND version = '20260416'
  AND target_column IN (
    'received_department_code_norm',
    'received_distribution_code_norm',
    'received_employee_code_norm'
  );

-- 住所
UPDATE dev_phr.template_mappings
SET rule = 'text_norm'
WHERE fund_id = 2
  AND version = '20260416'
  AND target_column IN (
    'address_line_norm',
    'building_norm'
  );

-- 電話 / メール
UPDATE dev_phr.template_mappings
SET rule = 'text_norm'
WHERE fund_id = 2
  AND version = '20260416'
  AND target_column IN (
    'phone_norm',
    'email_norm'
  );

-- 続柄名称（任意）
UPDATE dev_phr.template_mappings
SET rule = 'text_norm'
WHERE fund_id = 2
  AND version = '20260416'
  AND target_column = 'relationship_name_norm';

-- 社員コード mapping 追加（存在しない場合のみ）
INSERT INTO dev_phr.template_mappings (
  fund_id,
  version,
  col_order,
  csv_header,
  target_column,
  rule,
  required,
  notes
)
SELECT
  2,
  '20260416',
  19,
  '社員コード',
  'received_employee_code_norm',
  'text_norm',
  0,
  '受領CSV由来の社員コードを正規化して保持'
WHERE NOT EXISTS (
  SELECT 1
  FROM dev_phr.template_mappings
  WHERE fund_id = 2
    AND version = '20260416'
    AND csv_header = '社員コード'
    AND target_column = 'received_employee_code_norm'
);