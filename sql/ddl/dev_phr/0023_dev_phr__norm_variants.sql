CREATE TABLE `dev_phr`.`norm_variants` (
  `variant_id` bigint NOT NULL AUTO_INCREMENT COMMENT '揺れ辞書ID',
  `result_code_oid` varchar(128) NOT NULL COMMENT '結果コードOID（CDの辞書キー）',
  `raw_token_norm` varchar(190) NOT NULL COMMENT '入力値を前処理した照合トークン（例: +, 1+, PLUS などを正規化した文字列）',
  `raw_value_utf8` varchar(190) NOT NULL DEFAULT '' COMMENT '入力値（揺れ受け止め用：照合キー本命）',
  `normalized_code` varchar(64) NOT NULL COMMENT '正規化後コード（XMLに出す code）',
  `code_system` varchar(190) DEFAULT NULL COMMENT 'codeSystem（必要なら）',
  `display_name` varchar(255) DEFAULT NULL COMMENT 'displayName（運用表示用）',
  `is_canonical` tinyint NOT NULL DEFAULT 0 COMMENT '1=正規値（このOIDの代表）',
  `priority` smallint NOT NULL DEFAULT 100 COMMENT '複数マッチ時の優先度（小さいほど優先）',
  `is_active` tinyint NOT NULL DEFAULT 1 COMMENT '1=有効/0=無効（廃止パターンを残す用）',
  `note` text COMMENT '備考（運用メモ）',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`variant_id`),
  UNIQUE KEY `uq_oid_rawvalue_utf8` (`result_code_oid`, `raw_value_utf8`),
  KEY `idx_oid_canonical` (`result_code_oid`, `is_canonical`, `priority`),
  KEY `idx_oid_normcode` (`result_code_oid`, `normalized_code`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診値 正規化辞書（揺れ対応）。CDの result_code_oid をキーに raw_token_norm -> normalized_code を引く。';
