

-- ============================================================
-- seed: fund_company_mapping for insurer_number = 06139463
-- ============================================================

-- 1. 部署ベース lookup（最優先）
INSERT INTO dev_phr.fund_company_mapping (
  insurer_number,
  match_style,
  mapping_type,
  source_target_columns,
  source_match_rule,
  source_match_conditions,
  company_lookup_columns,
  company_lookup_rule,
  priority,
  notes
) VALUES (
  '06139463',
  'department',
  'lookup_company_master',
  'received_company_code_norm',
  'left3',
  NULL,
  'department_name',
  'left3_before_colon',
  1,
  '部署コードprefixでHIA企業＋部署へ解決'
);

-- 2. 記号100かつ本人
INSERT INTO dev_phr.fund_company_mapping (
  insurer_number,
  match_style,
  mapping_type,
  source_target_columns,
  source_match_rule,
  source_match_conditions,
  fixed_employer_code,
  priority,
  notes
) VALUES (
  '06139463',
  'employer',
  'fixed',
  'insurance_symbol_norm',
  'as_is',
  '[{"column":"insurance_symbol_norm","operator":"eq","value":"100"},{"column":"relationship_name_match","operator":"eq","value":"本人"}]',
  103,
  5,
  '記号100かつ本人 → トランスコスモス株式会社'
);

-- 3. 記号100かつ本人以外（被扶養者）
INSERT INTO dev_phr.fund_company_mapping (
  insurer_number,
  match_style,
  mapping_type,
  source_target_columns,
  source_match_rule,
  source_match_conditions,
  fixed_employer_code,
  priority,
  notes
) VALUES (
  '06139463',
  'employer',
  'fixed',
  'insurance_symbol_norm',
  'as_is',
  '[{"column":"insurance_symbol_norm","operator":"eq","value":"100"},{"column":"relationship_name_match","operator":"neq","value":"本人"}]',
  115,
  6,
  '記号100かつ本人以外 → 被扶養者企業コード'
);

-- 4. 記号 → 企業コード fixed（通常）
INSERT INTO dev_phr.fund_company_mapping (
  insurer_number,
  match_style,
  mapping_type,
  source_target_columns,
  source_match_rule,
  source_match_key,
  fixed_employer_code,
  priority,
  notes
) VALUES
('06139463','employer','fixed','insurance_symbol_norm','as_is','117',104,10,'応用技術株式会社'),
('06139463','employer','fixed','insurance_symbol_norm','as_is','119',105,10,'トランスコスモス・アシスト'),
('06139463','employer','fixed','insurance_symbol_norm','as_is','122',106,10,'大宇宙ジャパン'),
('06139463','employer','fixed','insurance_symbol_norm','as_is','125',107,10,'トランスコスモスパートナーズ'),
('06139463','employer','fixed','insurance_symbol_norm','as_is','131',108,10,'TTピーエム'),
('06139463','employer','fixed','insurance_symbol_norm','as_is','132',109,10,'TTヒューマンアセット'),
('06139463','employer','fixed','insurance_symbol_norm','as_is','700',110,10,'デジタルテクノロジー'),
('06139463','employer','fixed','insurance_symbol_norm','as_is','200',111,10,'健保本体');

-- ============================================================
-- END
-- ============================================================