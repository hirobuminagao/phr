CREATE TABLE `dev_phr`.`staging_subscribers_fund` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,

  -- source / control
  `fund_id` bigint unsigned NOT NULL,
  `version` int NOT NULL,
  `import_run_id` bigint unsigned DEFAULT NULL,
  `src_file` varchar(190) DEFAULT NULL,
  `src_row_no` int DEFAULT NULL,
  `src_line_no` int DEFAULT NULL,

  -- raw values (原則は norm 格納。mapping上どうしても raw しか取れない項目のみ保持)

  -- normalized (norm)
  `name_kana_full_norm` varchar(190) DEFAULT NULL,
  `name_kanji_full_norm` varchar(190) DEFAULT NULL,
  `name_kanji_family_norm` varchar(190) DEFAULT NULL,
  `name_kanji_middle_norm` varchar(190) DEFAULT NULL,
  `name_kanji_given_norm` varchar(190) DEFAULT NULL,

  `gender_code_norm` tinyint unsigned DEFAULT NULL,
  `birth_norm` date DEFAULT NULL,

  `insurer_number_norm` char(8) DEFAULT NULL,
  `insurance_symbol_norm` varchar(190) DEFAULT NULL,
  `insurance_number_norm` varchar(190) DEFAULT NULL,
  `insurance_branchnumber_norm` varchar(190) DEFAULT NULL,
  `insurance_symbol_digits` int unsigned DEFAULT NULL,

  `relationship_code_norm` varchar(64) DEFAULT NULL,
  `relationship_name_norm` varchar(190) DEFAULT NULL,

  -- exceptional raw-retained field
  `connect_id_raw` varchar(190) DEFAULT NULL,

  `connect_id_norm` varchar(190) DEFAULT NULL,
  `received_company_code_norm` varchar(190) DEFAULT NULL,
  `received_company_name_norm` varchar(190) DEFAULT NULL,
  `department_code` varchar(190) DEFAULT NULL,
  `distribution_code` varchar(190) DEFAULT NULL,
  `employee_code` varchar(190) DEFAULT NULL,

  -- match values
  `name_kana_full_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `name_kanji_full_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `name_kanji_family_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `name_kanji_middle_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `name_kanji_given_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,

  `insurance_symbol_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `insurance_number_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,

  `relationship_name_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,

  -- identity / lookup cache
  `person_id_custom` varchar(190) DEFAULT NULL,
  `identity_hash` char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin DEFAULT NULL,
  `matched_subscriber_id` bigint unsigned DEFAULT NULL,

  -- timestamps
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `loaded_at` datetime(3) DEFAULT NULL,

  PRIMARY KEY (`id`),

  KEY `idx_stgfund_import_run` (`import_run_id`),
  KEY `idx_stgfund_identity_hash` (`identity_hash`),
  KEY `idx_stgfund_matched_subscriber_id` (`matched_subscriber_id`),
  KEY `idx_stgfund_symbol_number` (`insurance_symbol_match`, `insurance_number_match`),
  KEY `idx_stgfund_name_kana_match` (`name_kana_full_match`),
  KEY `idx_stgfund_relationship_name_match` (`relationship_name_match`),
  KEY `idx_stgfund_insurer` (`insurer_number_norm`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
