CREATE TABLE `work_other`.`medi_shared_files` (
  `shared_file_id` bigint NOT NULL AUTO_INCREMENT COMMENT '共有ファイル台帳ID（採番）',

  `path_hash` char(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '共有パスのハッシュ（例: SHA1(path)）。TEXTのUNIQUE回避のための一意キー',
  `path` text NOT NULL COMMENT '共有フォルダ上のフルパス（UNC/ローカル/マップドドライブ等）。監査・追跡用',

  `src_folder_raw` varchar(255) DEFAULT NULL COMMENT '共有側の直下フォルダ名（生）。区切り文字ゆれ・誤記も含めてそのまま保持',
  `dst_folder_norm` varchar(255) DEFAULT NULL COMMENT 'medi_input側の配置フォルダ名（正規化後）。原則: <facility_code>_<facility_name>',
  `facility_hint` varchar(255) DEFAULT NULL COMMENT '施設ヒント（例: 親フォルダ名）。運用上の見やすさ用。厳密キーではない',

  `file_name` varchar(255) NOT NULL COMMENT 'ファイル名（拡張子込み）',
  `ext` varchar(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '拡張子（例: zip/pdf/xlsx）。検索・集計用',

  `file_size` bigint NOT NULL COMMENT 'ファイルサイズ（bytes）。差分検知・異常検知用',
  `mtime` datetime(6) DEFAULT NULL COMMENT '共有側の最終更新日時（取得できる場合のみ）。差し替え検知に利用',

  `sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL COMMENT 'ファイル内容SHA-256。健診候補ZIPなど必要なものだけ計算して保持（重複排除の最終キー）',

  `auto_judgement` enum('KENSHIN','NON_KENSHIN','UNREADABLE','UNKNOWN')
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'UNKNOWN'
    COMMENT '自動判定（スクリプト推定）。UNKNOWNあり。いつでも作り直せる補助情報',

  `manual_judgement` enum('KENSHIN','NON_KENSHIN','UNREADABLE','SAMPLE') DEFAULT NULL,

  `stage_status` enum('NEW','INPUT_COPIED','IMPORTED','SKIPPED')
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'NEW'
    COMMENT '処理状態: NEW=共有のみ / INPUT_COPIED=inputにコピー済 / IMPORTED=DB取込済 / SKIPPED=除外',

  `note` varchar(1024) DEFAULT NULL COMMENT '補足メモ（自動判定理由、読めない理由、手動判断根拠など）',

  `first_seen_at` datetime(6) NOT NULL COMMENT '初回観測日時（スクリプトが初めて見つけた時刻）',
  `last_seen_at` datetime(6) NOT NULL COMMENT '最終観測日時（スクリプトが最後に見つけた時刻）。共有から消えた/移動した検知のベース',

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '登録日時（DB記帳）',
  `updated_at` datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時（DB記帳）',

  `zip_has_xml` tinyint(1) DEFAULT NULL COMMENT 'ZIP内にxmlが1つでもあるか（1/0/NULL=未チェック）',
  `zip_xml_count` int DEFAULT NULL COMMENT 'ZIP内のxmlファイル数（未チェック/失敗はNULL）',
  `zip_xml_checked_at` datetime(6) DEFAULT NULL COMMENT 'ZIP内xml判定の実施時刻',

  PRIMARY KEY (`shared_file_id`),

  UNIQUE KEY `uk_path_hash` (`path_hash`) COMMENT '共有パス単位での一意制約（TEXTにUNIQUE貼らないための回避策）',

  KEY `idx_sha256` (`sha256`) COMMENT 'SHA-256検索用（重複排除/medi_zip_receiptsとの突合に使用）',
  KEY `idx_stage` (`stage_status`) COMMENT '進捗検索用（NEWだけ拾う等）',
  KEY `idx_judge` (`manual_judgement`, `auto_judgement`) COMMENT '判定検索用（manual優先の集計・抽出の補助）',
  KEY `idx_auto_judge_stage` (`auto_judgement`, `stage_status`)
)
ENGINE=InnoDB
AUTO_INCREMENT=3262
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='共有フォルダ上のファイル観測台帳（自動判定＋手動確定＋ステージ進捗。manual優先）';
