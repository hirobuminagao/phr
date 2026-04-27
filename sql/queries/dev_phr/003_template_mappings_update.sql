

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