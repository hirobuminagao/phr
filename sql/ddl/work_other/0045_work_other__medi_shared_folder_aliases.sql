CREATE TABLE `work_other`.`medi_shared_folder_aliases` (
  `alias_id` bigint NOT NULL AUTO_INCREMENT,

  `src_folder_raw` varchar(255) NOT NULL
    COMMENT '共有側の直下フォルダ名（生）',

  `dst_folder_norm` varchar(255) DEFAULT NULL
    COMMENT 'medi_input直下フォルダ名（正本）。NULL=未確定',

  `manual_judgement` enum('KENSHIN','NON_KENSHIN','UNREADABLE','SAMPLE')
    CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL
    COMMENT 'フォルダ単位の手動判定（任意）。ファイル側のmanual_judgementを上書きする用途にも使える',

  `note` varchar(1024) DEFAULT NULL COMMENT '根拠メモ',

  `is_active` tinyint(1) NOT NULL DEFAULT 1
    COMMENT '無効化フラグ（過去分の保持用）',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`alias_id`),

  UNIQUE KEY `uk_src_folder_raw_active`
    (`src_folder_raw`, `is_active`)
    COMMENT '有効レコードはsrc_folder_rawで一意'
)
ENGINE=InnoDB
AUTO_INCREMENT=189
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='共有フォルダ名(生)→input配置フォルダ名(正本)の対応表。手作業で確定するための台帳';
