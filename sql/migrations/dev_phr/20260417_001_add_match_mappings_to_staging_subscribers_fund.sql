

-- 20260417_001_add_match_mappings_to_staging_subscribers_fund.sql
-- 目的:
--   staging_subscribers_fund 向け template_mappings に match 系 target を追加する
-- 対象:
--   fund_id = 2 (06139463)
--   version = 20250908
--
-- 方針:
--   - 今回は import_staging_subscribers_fund.py で実装済みの rule のみ追加する
--   - staging_subscribers_fund に実在する target_column のみ追加する
--   - name_kana の parts match は target column 未作成のため今回は追加しない

START TRANSACTION;

DELETE FROM dev_phr.template_mappings
WHERE fund_id = 2
  AND version = 20250908
  AND target_column IN (
    'name_kana_full_match',
    'name_kanji_full_match',
    'name_kanji_family_match',
    'name_kanji_middle_match',
    'name_kanji_given_match'
  );

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
  (
    2,
    20250908,
    8,
    '氏名（カナ）',
    'name_kana_full_match',
    'name_kana_full_match',
    0,
    '氏名（カナ）から氏名カナ照合用を生成（name_kana_full_norm 系から生成）'
  ),
  (
    2,
    20250908,
    7,
    '氏名（漢字）',
    'name_kanji_full_match',
    'name_kanji_full_match',
    0,
    '氏名（漢字）から氏名漢字照合用を生成'
  ),
  (
    2,
    20250908,
    7,
    '氏名（漢字）',
    'name_kanji_family_match',
    'split_family_match',
    0,
    '氏名（漢字）から姓の照合用を生成'
  ),
  (
    2,
    20250908,
    7,
    '氏名（漢字）',
    'name_kanji_middle_match',
    'split_middle_match',
    0,
    '氏名（漢字）からミドルネームの照合用を生成'
  ),
  (
    2,
    20250908,
    7,
    '氏名（漢字）',
    'name_kanji_given_match',
    'split_given_match',
    0,
    '氏名（漢字）から名の照合用を生成'
  );

COMMIT;