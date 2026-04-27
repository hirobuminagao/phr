CREATE TABLE `dev_phr`.`staging_subscribers_fund` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',

  -- source / control
  `fund_id` bigint unsigned NOT NULL COMMENT '健保識別ID',
  `version` int NOT NULL COMMENT 'テンプレートバージョン',
  `import_run_id` bigint unsigned DEFAULT NULL COMMENT '取込実行ID',
  `src_file` varchar(190) DEFAULT NULL COMMENT '元CSVファイル名',
  `src_row_no` int DEFAULT NULL COMMENT 'CSV行番号（論理）',
  `src_line_no` int DEFAULT NULL COMMENT 'CSV行番号（物理）',

  -- raw values (原則は norm 格納。mapping上どうしても raw しか取れない項目のみ保持)

  -- normalized (norm)
  `name_kana_full_norm` varchar(190) DEFAULT NULL COMMENT '氏名カナ（正規化）',
  `name_kana_family_norm` varchar(190) DEFAULT NULL COMMENT '姓カナ（正規化）',
  `name_kana_middle_norm` varchar(190) DEFAULT NULL COMMENT 'ミドルネームカナ（正規化）',
  `name_kana_given_norm` varchar(190) DEFAULT NULL COMMENT '名カナ（正規化）',
  `name_kanji_full_norm` varchar(190) DEFAULT NULL COMMENT '氏名漢字（正規化）',
  `name_kanji_family_norm` varchar(190) DEFAULT NULL COMMENT '姓（正規化）',
  `name_kanji_middle_norm` varchar(190) DEFAULT NULL COMMENT 'ミドルネーム（正規化）',
  `name_kanji_given_norm` varchar(190) DEFAULT NULL COMMENT '名（正規化）',

  `gender_code_norm` tinyint unsigned DEFAULT NULL COMMENT '性別コード（正規化）',
  `birth_norm` date DEFAULT NULL COMMENT '生年月日（正規化）',

  `insurer_number_norm` char(8) DEFAULT NULL COMMENT '保険者番号（正規化）',
  `insurance_symbol_norm` varchar(190) DEFAULT NULL COMMENT '保険証記号（正規化）',
  `insurance_number_norm` varchar(190) DEFAULT NULL COMMENT '保険証番号（正規化）',
  `insurance_branchnumber_norm` varchar(190) DEFAULT NULL COMMENT '保険証枝番（正規化）',
  `qualification_acquired_date_norm` date DEFAULT NULL COMMENT '資格取得日（正規化）',
  `qualification_lost_date_norm` date DEFAULT NULL COMMENT '資格喪失日（正規化）',
  `insurance_symbol_digits` int unsigned DEFAULT NULL COMMENT '記号の数字部分（補助）',

  `relationship_code_norm` varchar(64) DEFAULT NULL COMMENT '続柄コード（正規化）',
  `relationship_name_norm` varchar(190) DEFAULT NULL COMMENT '続柄名称（正規化）',
  `postal_code_norm` varchar(190) DEFAULT NULL COMMENT '郵便番号（正規化）',
  `address_line_norm` varchar(190) DEFAULT NULL COMMENT '住所1（正規化）',
  `building_norm` varchar(190) DEFAULT NULL COMMENT '住所2（建物）（正規化）',
  `phone_norm` varchar(190) DEFAULT NULL COMMENT '電話番号（正規化）',
  `email_norm` varchar(190) DEFAULT NULL COMMENT 'メールアドレス（正規化）',

  -- exceptional raw-retained field
  `connect_id_raw` varchar(190) DEFAULT NULL COMMENT '連携ID（raw例外保持）',

  `connect_id_norm` varchar(190) DEFAULT NULL COMMENT '連携ID（正規化）',
  `received_company_code_norm` varchar(190) DEFAULT NULL COMMENT '受領企業コード（正規化）',
  `received_company_name_norm` varchar(190) DEFAULT NULL COMMENT '受領企業名（正規化）',
  `department_code_norm` varchar(190) DEFAULT NULL COMMENT '所属コード（正規化）',
  `distribution_code_norm` varchar(190) DEFAULT NULL COMMENT '配布先コード（正規化）',
  `employee_code_norm` varchar(190) DEFAULT NULL COMMENT '社員コード（正規化）',

  -- match values
  `name_kana_full_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '氏名カナ（照合用、必ず name_kana_full_norm から生成。分割カナから直接生成しない）',
  `name_kanji_full_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '氏名漢字（照合用）',
  `name_kanji_family_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '姓（照合用）',
  `name_kanji_middle_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT 'ミドルネーム（照合用）',
  `name_kanji_given_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '名（照合用）',

  `insurance_symbol_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '保険証記号（照合用）',
  `insurance_number_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '保険証番号（照合用）',

  `relationship_name_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '続柄名称（照合用、名称優先。名称なしの場合はコード→名称変換ルール適用時のみ生成）',

  -- identity / lookup cache
  `person_id_custom` varchar(190) DEFAULT NULL COMMENT 'カスタム個人ID',
  `identity_hash` char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin DEFAULT NULL COMMENT '同一人物識別ハッシュ',
  `matched_subscriber_id` bigint unsigned DEFAULT NULL COMMENT '既存加入者ID（突合結果）',

  -- company mapping enrichment cache
  `mapped_employer_code` int unsigned DEFAULT NULL COMMENT 'HIA向け事業所コード（会社部署マッピング後）',
  `mapped_department_code` int unsigned DEFAULT NULL COMMENT 'HIA向け部署コード（会社部署マッピング後）',
  `subscribers_employer_code` int unsigned DEFAULT NULL COMMENT '現行subscribers.employer_code（比較用キャッシュ）',
  `subscribers_department_code` int unsigned DEFAULT NULL COMMENT '現行subscribers.department_code（比較用キャッシュ）',

  -- diff judgment cache
  `diff_status` varchar(50) DEFAULT NULL COMMENT '差分判定結果（new / transfer / existing / unknown 等）',
  `diff_status_method` varchar(20) DEFAULT NULL COMMENT '判定手段（script / manual）',
  `diff_status_reason` varchar(255) DEFAULT NULL COMMENT '判定理由',

  -- timestamps
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'レコード作成日時',
  `loaded_at` datetime(3) DEFAULT NULL COMMENT '取込完了日時',

  PRIMARY KEY (`id`),

  KEY `idx_stgfund_import_run` (`import_run_id`),
  KEY `idx_stgfund_identity_hash` (`identity_hash`),
  KEY `idx_stgfund_matched_subscriber_id` (`matched_subscriber_id`),
  KEY `idx_stgfund_company_mapping` (`mapped_employer_code`, `mapped_department_code`),
  KEY `idx_stgfund_subscribers_company` (`subscribers_employer_code`, `subscribers_department_code`),
  KEY `idx_stgfund_symbol_number` (`insurance_symbol_match`, `insurance_number_match`),
  KEY `idx_stgfund_name_kana_match` (`name_kana_full_match`),
  KEY `idx_stgfund_relationship_name_match` (`relationship_name_match`),
  KEY `idx_stgfund_insurer` (`insurer_number_norm`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
