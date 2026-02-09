CREATE TABLE `medi_zip_passwords` (
  `zip_password_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'PK',

  `scope_type` enum('FACILITY','ZIP_NAME','ZIP_SHA256')
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL
    COMMENT '適用範囲（FACILITY / ZIP_NAME / ZIP_SHA256）',

  `facility_code` varchar(64) DEFAULT NULL
    COMMENT '施設コード（scope=FACILITY用）',

  `facility_folder_name` varchar(255) DEFAULT NULL
    COMMENT '施設フォルダ名（scope=FACILITY用）',

  `zip_name` varchar(255) DEFAULT NULL
    COMMENT 'ZIP名（scope=ZIP_NAME用）',

  `zip_sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL
    COMMENT 'ZIP sha256（scope=ZIP_SHA256用）',

  `password_text` varchar(255) NOT NULL
    COMMENT 'パスワード（平文。運用上の取扱い注意）',

  `priority` int NOT NULL DEFAULT 100
    COMMENT '小さいほど優先（例: 10=sha, 20=name, 30=facility）',

  `is_active` tinyint(1) NOT NULL DEFAULT 1
    COMMENT '有効フラグ',

  `note` varchar(255) DEFAULT NULL
    COMMENT 'メモ（誰から/いつ等）',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`zip_password_id`),

  KEY `idx_zip_pw_scope`
    (`scope_type`, `is_active`, `priority`),

  KEY `idx_zip_pw_facility`
    (`facility_code`, `facility_folder_name`),

  KEY `idx_zip_pw_zip_name`
    (`zip_name`),

  KEY `idx_zip_pw_zip_sha`
    (`zip_sha256`)
)
ENGINE=InnoDB
AUTO_INCREMENT=10
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='【medi】ZIPパスワード管理（施設/ファイル名/sha単位。priorityで解決順制御）';
