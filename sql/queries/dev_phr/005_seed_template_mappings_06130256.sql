

-- ============================================================
-- seed: templates / template_mappings for insurer_number = 06130256
-- fund_id = 48
-- version = 20260428
--
-- 方針:
-- - 名前系は受取データ側の列を使う
--   - 氏名（カナ）
--   - 氏名（漢字）
-- - 保険証・性別・日付・会社部署コード等はHIAフォーマット計算結果列を使う
-- - 会社・部署は登録用列が正のため、以下を staging の received_* へ入れる
--   - 事業所（企業）コード -> received_company_code_norm
--   - 所属コード -> received_department_code_norm
--   - 配付先コード -> received_distribution_code_norm
--   - 社員コード -> received_employee_code_norm
-- ============================================================

START TRANSACTION;

-- template header
INSERT INTO dev_phr.templates (
  fund_id,
  version,
  name,
  template_type,
  target_table,
  configured_on,
  version_label,
  created_by
)
SELECT
  48,
  '20260428',
  '06130256 staging_subscribers_fund 2026受領データ',
  'subscribers_staging',
  'staging_subscribers_fund',
  CURRENT_DATE,
  '2026年度受領データ staging 取込用',
  'manual'
WHERE NOT EXISTS (
  SELECT 1
  FROM dev_phr.templates
  WHERE fund_id = 48
    AND version = '20260428'
);

-- 再実行しやすいように、同一 fund/version の mappings は入れ替える
DELETE FROM dev_phr.template_mappings
WHERE fund_id = 48
  AND version = '20260428';

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
VALUES
-- ------------------------------------------------------------
-- 保険情報（HIAフォーマット計算結果列）
-- ------------------------------------------------------------
(48,'20260428',1,'被保険者証記号','insurance_symbol_digits','symbol_digits',0,'記号中の数字部分を補助値として保持'),
(48,'20260428',1,'被保険者証記号','insurance_symbol_norm','symbol_norm',0,'記号を正規化'),
(48,'20260428',1,'被保険者証記号','insurance_symbol_match','symbol_match',0,'記号から保険証記号の照合用を生成'),

(48,'20260428',2,'被保険者証番号','insurance_number_norm','digits_required',1,'identity構成要素'),
(48,'20260428',2,'被保険者証番号','insurance_number_match','number_match',0,'番号から保険証番号の照合用を生成'),

(48,'20260428',3,'被保険者証枝番','insurance_branchnumber_norm','digits_or_null',0,'枝番がある場合のみ保持'),

-- ------------------------------------------------------------
-- 氏名（受取データ側）
-- ------------------------------------------------------------
(48,'20260428',4,'氏名（カナ）','name_kana_full_norm','kana_full_no_space',1,'受取側カナ氏名をidentity用に保持'),
(48,'20260428',4,'氏名（カナ）','name_kana_full_match','name_kana_full_match',0,'受取側カナ氏名から照合用を生成'),
(48,'20260428',4,'氏名（カナ）','name_kana_family_norm','split_family_kana',0,'受取側カナ氏名から姓カナを生成'),
(48,'20260428',4,'氏名（カナ）','name_kana_middle_norm','split_middle_kana',0,'受取側カナ氏名から中間名カナを生成'),
(48,'20260428',4,'氏名（カナ）','name_kana_given_norm','split_given_kana',0,'受取側カナ氏名から名カナを生成'),

(48,'20260428',5,'氏名（漢字）','name_kanji_full_norm','text_norm',0,'受取側漢字氏名を正規化して保持'),
(48,'20260428',5,'氏名（漢字）','name_kanji_full_match','name_kanji_full_match',0,'受取側漢字氏名から照合用を生成'),
(48,'20260428',5,'氏名（漢字）','name_kanji_family_norm','split_family',0,'受取側漢字氏名から姓を生成'),
(48,'20260428',5,'氏名（漢字）','name_kanji_middle_norm','split_middle',0,'受取側漢字氏名から中間名を生成'),
(48,'20260428',5,'氏名（漢字）','name_kanji_given_norm','split_given',0,'受取側漢字氏名から名を生成'),
(48,'20260428',5,'氏名（漢字）','name_kanji_family_match','split_family_match',0,'受取側漢字氏名から姓の照合用を生成'),
(48,'20260428',5,'氏名（漢字）','name_kanji_middle_match','split_middle_match',0,'受取側漢字氏名から中間名の照合用を生成'),
(48,'20260428',5,'氏名（漢字）','name_kanji_given_match','split_given_match',0,'受取側漢字氏名から名の照合用を生成'),

-- ------------------------------------------------------------
-- 基本属性・続柄
-- ------------------------------------------------------------
(48,'20260428',6,'性別','gender_code_norm','gender_code_norm',1,'性別を1/2/9へ正規化'),
(48,'20260428',7,'続柄名称','relationship_name_norm','text_norm',0,'HIAフォーマット計算結果列の続柄名称を保持'),

(48,'20260428',8,'生年月日','birth_norm','birth_norm',1,'identity構成要素'),
(48,'20260428',9,'資格取得日（家族認定日）','qualification_acquired_date_norm','date_or_null',0,'資格取得日または家族認定日'),
(48,'20260428',10,'資格喪失日（家族削除日）','qualification_lost_date_norm','date_or_null',0,'資格喪失日または家族削除日'),

-- ------------------------------------------------------------
-- 住所・連絡先（HIAフォーマット計算結果列）
-- ------------------------------------------------------------
(48,'20260428',11,'郵便番号','postal_code_norm','digits_or_null',0,'郵便番号'),
(48,'20260428',12,'住所','address_line_norm','text_norm',0,'住所'),
(48,'20260428',13,'住所（建物名）','building_norm','text_norm',0,'建物名'),
(48,'20260428',14,'電話番号','phone_norm','text_norm',0,'電話番号'),
(48,'20260428',15,'メールアドレス','email_norm','text_norm',0,'メールアドレス'),

-- ------------------------------------------------------------
-- 会社・部署・外部ID（HIAフォーマット計算結果列）
-- ------------------------------------------------------------
(48,'20260428',15,'キー','received_company_name_norm','text_norm',0,'受取CSVのキー列を会社名（補助）として保持'),
(48,'20260428',16,'事業所（企業）コード','received_company_code_norm','text_norm',0,'HIA登録用の事業所（企業）コードを保持'),
(48,'20260428',17,'所属コード','received_department_code_norm','text_norm',0,'HIA登録用の所属コードを保持'),
(48,'20260428',18,'配付先コード','received_distribution_code_norm','text_norm',0,'HIA登録用の配付先コードを保持'),
(48,'20260428',19,'社員コード','received_employee_code_norm','text_norm',0,'HIA登録用の社員コードを保持'),
(48,'20260428',20,'connectID','connect_id_norm','text_norm',0,'connectIDを保持');

COMMIT;

-- ============================================================
-- 確認用
-- ============================================================
SELECT
  fund_id,
  version,
  col_order,
  csv_header,
  target_column,
  rule,
  required,
  notes
FROM dev_phr.template_mappings
WHERE fund_id = 48
  AND version = '20260428'
ORDER BY col_order, csv_header, target_column;


-- ============================================================
-- seed: fund_company_mapping for insurer_number = 06130256
-- 方針:
-- - 06130256 はHIAフォーマット計算結果列の事業所（企業）コード / 所属コードが正
-- - HIA会社部署マスタは参照しない
-- - received_company_code_norm / received_department_code_norm を mapped_* へ直接反映する
-- - 部署コードが空の場合は mapped_department_code を NULL とする
-- ============================================================

-- 再実行しやすいように、06130256 の mapping は入れ替える
DELETE FROM dev_phr.fund_company_mapping
WHERE insurer_number = '06130256';

-- 登録用コード passthrough
INSERT INTO dev_phr.fund_company_mapping (
  insurer_number,
  match_style,
  mapping_type,
  source_target_columns,
  source_match_rule,
  priority,
  notes
) VALUES (
  '06130256',
  'department',
  'passthrough',
  'received_company_code_norm,received_department_code_norm',
  'as_is',
  1,
  'HIA登録用の企業コード＋所属コードをmapped_*へ直接反映'
);

-- ============================================================
-- mapping確認用
-- ============================================================
SELECT
  fund_company_mapping_id,
  insurer_number,
  match_style,
  mapping_type,
  source_target_columns,
  source_match_rule,
  source_match_key,
  source_match_conditions,
  company_lookup_columns,
  company_lookup_rule,
  fixed_employer_code,
  fixed_department_code,
  priority,
  notes
FROM dev_phr.fund_company_mapping
WHERE insurer_number = '06130256'
ORDER BY priority, fund_company_mapping_id;
