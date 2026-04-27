
CREATE TABLE IF NOT EXISTS dev_phr.fund_company_mapping (
  fund_company_mapping_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '健保別会社マッピングID',

  insurer_number VARCHAR(8) NOT NULL COMMENT '保険者番号',

  match_style VARCHAR(50) NOT NULL COMMENT 'マッチ種別（employer / department など）',
  mapping_type VARCHAR(50) NOT NULL COMMENT 'マッピング方式（lookup_company_master / fixed）',

  -- staging/subscribers側照合キー生成ルール
  source_target_columns VARCHAR(255) NOT NULL COMMENT 'staging側対象カラム（カンマ区切り）',
  source_match_rule VARCHAR(100) NOT NULL COMMENT 'staging側加工ルール（例: left3 / concat_with_pipe / as_is）',
  source_match_key VARCHAR(255) DEFAULT NULL COMMENT 'staging側照合キー（fixed時の比較値、例: 100）',

  -- HIA company master側照合キー生成ルール（mapping_type=lookup_company_master の場合に使用）
  company_lookup_columns VARCHAR(255) DEFAULT NULL COMMENT 'HIA会社マスタ側対象カラム（カンマ区切り）',
  company_lookup_rule VARCHAR(100) DEFAULT NULL COMMENT 'HIA会社マスタ側加工ルール（例: left3_before_colon / concat_with_pipe）',

  -- 固定マッピング結果（mapping_type=fixed の場合に使用）
  fixed_employer_code INT UNSIGNED DEFAULT NULL COMMENT '固定HIA事業所コード',
  fixed_department_code INT UNSIGNED DEFAULT NULL COMMENT '固定HIA部署コード',

  -- 管理
  priority INT UNSIGNED NOT NULL DEFAULT 1 COMMENT '優先順位（小さいほど優先）',
  is_active TINYINT(1) NOT NULL DEFAULT 1 COMMENT '有効フラグ',
  notes TEXT DEFAULT NULL COMMENT '備考',

  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '作成日時',
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日時',

  PRIMARY KEY (fund_company_mapping_id),

  KEY idx_fund_company_mapping_insurer (
    insurer_number
  ),

  KEY idx_fund_company_mapping_rule (
    insurer_number,
    match_style,
    mapping_type,
    is_active,
    priority
  ),

  KEY idx_fund_company_mapping_fixed_target (
    fixed_employer_code,
    fixed_department_code
  )
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健保別会社・部署マッピング（HIAマスタlookup / fixed対応）';