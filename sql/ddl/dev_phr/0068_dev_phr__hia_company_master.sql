

CREATE TABLE IF NOT EXISTS dev_phr.hia_company_master (
  hia_company_master_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'HIA会社部署マスタID',

  insurer_number VARCHAR(8) NOT NULL COMMENT '保険者番号',
  insurance_symbol VARCHAR(20) NOT NULL COMMENT '被保険者証記号',

  employer_code INT UNSIGNED NOT NULL COMMENT 'HIA事業所（企業）コード',
  employer_name VARCHAR(255) NOT NULL COMMENT '企業名',
  employer_name_kana VARCHAR(255) DEFAULT NULL COMMENT '企業名（フリガナ）',

  postal_code VARCHAR(20) DEFAULT NULL COMMENT '郵便番号',
  address VARCHAR(500) DEFAULT NULL COMMENT '住所',
  phone VARCHAR(50) DEFAULT NULL COMMENT '電話番号',
  contact_email VARCHAR(255) DEFAULT NULL COMMENT '担当メールアドレス',

  department_code INT UNSIGNED DEFAULT NULL COMMENT 'HIA所属コード',
  department_name VARCHAR(255) DEFAULT NULL COMMENT '部署名',
  department_name_kana VARCHAR(255) DEFAULT NULL COMMENT '部署名（フリガナ）',

  source_file VARCHAR(255) DEFAULT NULL COMMENT '取込元ファイル名',
  loaded_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '取込日時',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '作成日時',
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新日時',

  PRIMARY KEY (hia_company_master_id),
  UNIQUE KEY uq_hia_company_master_code (
    insurer_number,
    insurance_symbol,
    employer_code,
    department_code
  ),
  KEY idx_hia_company_master_insurer_symbol (
    insurer_number,
    insurance_symbol
  ),
  KEY idx_hia_company_master_employer (
    insurer_number,
    employer_code
  )
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='HIA会社・部署マスタ';