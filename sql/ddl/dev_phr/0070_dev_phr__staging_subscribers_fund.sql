CREATE TABLE `dev_phr`.`staging_subscribers_fund` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,

  -- source / control
  `fund_id` bigint unsigned NOT NULL,
  `template_ver` int NOT NULL,
  `import_run_id` bigint unsigned DEFAULT NULL,
  `src_file` varchar(190) DEFAULT NULL,
  `src_row_no` int DEFAULT NULL,
  `src_line_no` int DEFAULT NULL,

  -- raw values
  `name_kana_full_raw` varchar(190) DEFAULT NULL,
  `name_kanji_full_raw` varchar(190) DEFAULT NULL,
  `name_kanji_family_raw` varchar(190) DEFAULT NULL,
  `name_kanji_middle_raw` varchar(190) DEFAULT NULL,
  `name_kanji_given_raw` varchar(190) DEFAULT NULL,

  `gender_code_raw` varchar(190) DEFAULT NULL,
  `birth_raw` varchar(190) DEFAULT NULL,

  `insurer_number_raw` varchar(190) DEFAULT NULL,
  `insurance_symbol_raw` varchar(190) DEFAULT NULL,
  `insurance_number_raw` varchar(190) DEFAULT NULL,
  `insurance_branchnumber_raw` varchar(190) DEFAULT NULL,

  `relationship_code_raw` varchar(190) DEFAULT NULL,
  `relationship_name_raw` varchar(190) DEFAULT NULL,

  `connect_id_raw` varchar(190) DEFAULT NULL,

  -- normalized (norm)
  `name_kana_full_norm` varchar(190) DEFAULT NULL,
  `name_kanji_full_norm` varchar(190) DEFAULT NULL,
  `name_kanji_family_norm` varchar(190) DEFAULT NULL,
  `name_kanji_given_norm` varchar(190) DEFAULT NULL,

  `gender_code_norm` tinyint unsigned DEFAULT NULL,
  `birth_norm` date DEFAULT NULL,

  `insurer_number_norm` char(8) DEFAULT NULL,
  `insurance_symbol_norm` varchar(190) DEFAULT NULL,
  `insurance_number_norm` varchar(190) DEFAULT NULL,

  `relationship_norm` varchar(64) DEFAULT NULL,
  `connect_id_norm` varchar(190) DEFAULT NULL,

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
  `name_kanji_given_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,

  `insurance_symbol_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,
  `insurance_number_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL,

  `relationship_match` varchar(64) DEFAULT NULL,

  -- identity
  `person_id_custom` varchar(190) DEFAULT NULL,
  `identity_hash` char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin DEFAULT NULL,

  -- timestamps
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `loaded_at` datetime(3) DEFAULT NULL,

  PRIMARY KEY (`id`),

  KEY `idx_stgfund_import_run` (`import_run_id`),
  KEY `idx_stgfund_identity_hash` (`identity_hash`),
  KEY `idx_stgfund_symbol_number` (`insurance_symbol_match`, `insurance_number_match`),
  KEY `idx_stgfund_name_kana_match` (`name_kana_full_match`),
  KEY `idx_stgfund_insurer` (`insurer_number_norm`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
