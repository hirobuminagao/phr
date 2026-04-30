-- =====================================================================
-- Seed: add kana parts match mappings
-- Purpose: カナparts match を staging_subscribers_fund に生成する
-- =====================================================================

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
    new_rows.fund_id,
    new_rows.version,
    new_rows.col_order,
    new_rows.csv_header,
    new_rows.target_column,
    new_rows.rule,
    new_rows.required,
    new_rows.notes
FROM (
    SELECT
        2 AS fund_id,
        20260416 AS version,
        3 AS col_order,
        '氏名（カナ）' AS csv_header,
        'name_kana_family_match' AS target_column,
        'split_family_kana_match' AS rule,
        0 AS required,
        '氏名（カナ）から姓カナ照合用を生成'

    UNION ALL

    SELECT
        2,
        20260416,
        3,
        '氏名（カナ）',
        'name_kana_middle_match',
        'split_middle_kana_match',
        0,
        '氏名（カナ）から中間名カナ照合用を生成'

    UNION ALL

    SELECT
        2,
        20260416,
        3,
        '氏名（カナ）',
        'name_kana_given_match',
        'split_given_kana_match',
        0,
        '氏名（カナ）から名カナ照合用を生成'

    UNION ALL

    SELECT
        48 AS fund_id,
        20260428 AS version,
        4 AS col_order,
        '氏名（カナ）' AS csv_header,
        'name_kana_family_match' AS target_column,
        'split_family_kana_match' AS rule,
        0 AS required,
        '氏名（カナ）から姓カナ照合用を生成'

    UNION ALL

    SELECT
        48,
        20260428,
        4,
        '氏名（カナ）',
        'name_kana_middle_match',
        'split_middle_kana_match',
        0,
        '氏名（カナ）から中間名カナ照合用を生成'

    UNION ALL

    SELECT
        48,
        20260428,
        4,
        '氏名（カナ）',
        'name_kana_given_match',
        'split_given_kana_match',
        0,
        '氏名（カナ）から名カナ照合用を生成'
) new_rows
LEFT JOIN dev_phr.template_mappings existing
  ON existing.fund_id = new_rows.fund_id
 AND existing.version = new_rows.version
 AND existing.target_column = new_rows.target_column
WHERE existing.target_column IS NULL;