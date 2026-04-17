-- 20260417_001_add_match_mappings_to_staging_subscribers_fund.sql
-- 目的:
--   staging_subscribers_fund 向け template_mappings に match 系 target を追加する
-- 対象:
--   fund_id = 2 (06139463)
--   version = 20260416
--
-- 方針:
--   - 今回は import_staging_subscribers_fund.py で実装済みの rule のみ追加する
--   - staging_subscribers_fund に実在する target_column のみ追加する
--   - name_kana の parts match は target column 未作成のため今回は追加しない
--   - 既存行を削除せず、未登録の target_column のみ追加する

START TRANSACTION;

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
SELECT *
FROM (
  SELECT
    2 AS fund_id,
    20260416 AS version,
    8 AS col_order,
    '氏名（カナ）' AS csv_header,
    'name_kana_full_match' AS target_column,
    'name_kana_full_match' AS rule,
    0 AS required,
    '氏名（カナ）から氏名カナ照合用を生成（name_kana_full_norm 系から生成）' AS notes

  UNION ALL

  SELECT
    2,
    20260416,
    7,
    '氏名（漢字）',
    'name_kanji_full_match',
    'name_kanji_full_match',
    0,
    '氏名（漢字）から氏名漢字照合用を生成'

  UNION ALL

  SELECT
    2,
    20260416,
    7,
    '氏名（漢字）',
    'name_kanji_family_match',
    'split_family_match',
    0,
    '氏名（漢字）から姓の照合用を生成'

  UNION ALL

  SELECT
    2,
    20260416,
    7,
    '氏名（漢字）',
    'name_kanji_middle_match',
    'split_middle_match',
    0,
    '氏名（漢字）からミドルネームの照合用を生成'

  UNION ALL

  SELECT
    2,
    20260416,
    7,
    '氏名（漢字）',
    'name_kanji_given_match',
    'split_given_match',
    0,
    '氏名（漢字）から名の照合用を生成'

  UNION ALL

  SELECT
    2,
    20260416,
    2,
    '記号',
    'insurance_symbol_match',
    'symbol_match',
    0,
    '記号から保険証記号の照合用を生成'

  UNION ALL

  SELECT
    2,
    20260416,
    3,
    '番号',
    'insurance_number_match',
    'number_match',
    0,
    '番号から保険証番号の照合用を生成'
) AS new_rows
WHERE NOT EXISTS (
  SELECT 1
  FROM dev_phr.template_mappings tm
  WHERE tm.fund_id = new_rows.fund_id
    AND tm.version = new_rows.version
    AND tm.target_column = new_rows.target_column
);

COMMIT;