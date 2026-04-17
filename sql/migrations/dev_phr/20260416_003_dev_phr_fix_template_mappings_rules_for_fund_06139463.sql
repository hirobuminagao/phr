START TRANSACTION;

-- 06139463（fund_id=2）向け templates / template_mappings 初期作成
-- ※ 本 migration は今回案件用のテンプレート土台を作る
-- ※ insurer_number_norm は CSV ではなく、input/<insurer_number>/ のフォルダ名と
--    fund_insurer_number(fund_id=2, insurer_number=6139463) の対応を用いて
--    import スクリプト側で注入する前提とする
-- ※ rule は import スクリプト側で実装済みのものだけを使う

SET @fund_id := 2;
SET @version := 20260416;
SET @template_name := 'トランス・コスモス_加入者_2026-04-16版';
SET @version_label := '2026-04-16';

-- templates: 存在しなければ作成、存在すれば名称等を同期
INSERT INTO `dev_phr`.`templates` (
  `fund_id`,
  `version`,
  `name`,
  `template_type`,
  `target_table`,
  `configured_on`,
  `version_label`,
  `notes`
)
VALUES (
  @fund_id,
  @version,
  @template_name,
  'fund_to_staging',
  'staging_subscribers_fund',
  CURRENT_TIMESTAMP(3),
  @version_label,
  '06139463案件用テンプレート。insurer_number_norm は CSV ではなくフォルダ名 / fund_insurer_number からスクリプト側で注入する。'
)
ON DUPLICATE KEY UPDATE
  `name` = VALUES(`name`),
  `template_type` = VALUES(`template_type`),
  `target_table` = VALUES(`target_table`),
  `configured_on` = VALUES(`configured_on`),
  `version_label` = VALUES(`version_label`),
  `notes` = VALUES(`notes`);

-- 再実行時に同versionを作り直せるよう、先に当該mappingを消してから作る
DELETE FROM `dev_phr`.`template_mappings`
 WHERE `fund_id` = @fund_id
   AND `version` = @version;

INSERT INTO `dev_phr`.`template_mappings` (
  `fund_id`,
  `version`,
  `col_order`,
  `csv_header`,
  `target_column`,
  `rule`,
  `required`,
  `notes`
)
VALUES
  -- 保険情報
  (@fund_id, @version,  1, '記号',               'insurance_symbol_norm',            'symbol_norm',         0, '記号を正規化'),
  (@fund_id, @version,  1, '記号',               'insurance_symbol_digits',          'symbol_digits',       0, '記号中の数字部分を補助値として保持'),
  (@fund_id, @version,  2, '番号',               'insurance_number_norm',            'digits_required',     1, 'identity構成要素'),

  -- 氏名（カナ）: match はスクリプト側で name_kana_full_norm から生成
  (@fund_id, @version,  3, '氏名（カナ）',       'name_kana_full_norm',              'kana_full_no_space',  1, 'full norm を生成。match はここからスクリプト側で生成'),
  (@fund_id, @version,  3, '氏名（カナ）',       'name_kana_family_norm',            'split_family_kana',   0, '姓カナ'),
  (@fund_id, @version,  3, '氏名（カナ）',       'name_kana_middle_norm',            'split_middle_kana',   0, '中間名カナ'),
  (@fund_id, @version,  3, '氏名（カナ）',       'name_kana_given_norm',             'split_given_kana',    0, '名カナ'),

  -- 氏名（漢字）
  (@fund_id, @version,  4, '氏名（漢字）',       'name_kanji_full_norm',             'as_is',               0, 'フル名称'),
  (@fund_id, @version,  4, '氏名（漢字）',       'name_kanji_family_norm',           'split_family',        0, '姓'),
  (@fund_id, @version,  4, '氏名（漢字）',       'name_kanji_middle_norm',           'split_middle',        0, '中間名'),
  (@fund_id, @version,  4, '氏名（漢字）',       'name_kanji_given_norm',            'split_given',         0, '名'),

  -- 基本属性
  (@fund_id, @version,  5, '性別コード',         'gender_code_norm',                 'gender_code_norm',    0, '1/2/9 等へ正規化'),
  (@fund_id, @version,  6, '続柄コード',         'relationship_code_norm',           'digits_or_null',      0, 'コード原値保持'),
  (@fund_id, @version,  7, '続柄',               'relationship_name_norm',           'as_is',               0, '名称がある場合はこちらを優先保持'),
  (@fund_id, @version,  8, '生年月日',           'birth_norm',                       'birth_norm',          1, 'identity構成要素'),

  -- 資格
  (@fund_id, @version,  9, '本人資格取得日',     'qualification_acquired_date_norm', 'date_or_null',        0, '本人の資格取得日'),
  (@fund_id, @version, 10, '家族認定日',         'qualification_acquired_date_norm', 'date_or_null',        0, '家族の場合は認定日を資格取得日相当として保持'),

  -- 住所・連絡先
  (@fund_id, @version, 11, '居所郵便番号',       'postal_code_norm',                 'digits_or_null',      0, '郵便番号'),
  (@fund_id, @version, 12, '居所住所１',         'address_line_norm',                'as_is',               0, '住所1'),
  (@fund_id, @version, 13, '居所住所２',         'building_norm',                    'as_is',               0, '住所2 / 建物'),
  (@fund_id, @version, 14, '個人電話番号',       'phone_norm',                       'as_is',               0, '電話番号。携帯との優先順位は別途実データ確認'),
  (@fund_id, @version, 15, '個人携帯電話番号',   'phone_norm',                       'as_is',               0, '携帯電話番号。電話番号との優先順位は別途実データ確認'),
  (@fund_id, @version, 16, '個人E_MAIL',         'email_norm',                       'as_is',               0, 'メールアドレス'),

  -- 会社・組織
  (@fund_id, @version, 17, '事業所コード',       'received_company_code_norm',       'as_is',               0, '受領会社コード'),
  (@fund_id, @version, 18, '事業所',             'received_company_name_norm',       'as_is',               0, '受領会社名');

COMMIT;
