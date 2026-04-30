


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
    fund_id,
    version,
    col_order,
    csv_header,
    target_column,
    rule,
    required,
    notes
FROM (
    SELECT
        t.fund_id,
        t.version,
        999 AS col_order,
        '氏名（カナ）' AS csv_header,
        'name_kana_family_match' AS target_column,
        'split_family_kana_match' AS rule,
        0 AS required,
        '氏名（カナ）から姓カナ照合用を生成'
    FROM (SELECT DISTINCT fund_id, version FROM dev_phr.template_mappings) t

    UNION ALL

    SELECT
        t.fund_id,
        t.version,
        999,
        '氏名（カナ）',
        'name_kana_middle_match',
        'split_middle_kana_match',
        0,
        '氏名（カナ）から中間名カナ照合用を生成'
    FROM (SELECT DISTINCT fund_id, version FROM dev_phr.template_mappings) t

    UNION ALL

    SELECT
        t.fund_id,
        t.version,
        999,
        '氏名（カナ）',
        'name_kana_given_match',
        'split_given_kana_match',
        0,
        '氏名（カナ）から名カナ照合用を生成'
    FROM (SELECT DISTINCT fund_id, version FROM dev_phr.template_mappings) t
) new_rows
LEFT JOIN dev_phr.template_mappings existing
  ON existing.fund_id = new_rows.fund_id
 AND existing.version = new_rows.version
 AND existing.target_column = new_rows.target_column
WHERE existing.target_column IS NULL;