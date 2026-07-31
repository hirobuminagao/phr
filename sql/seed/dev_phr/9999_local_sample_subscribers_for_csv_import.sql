-- Local-only sample subscribers for CSV import verification.
-- Do not apply to shared / production environments.

INSERT INTO `m4_dev_phr`.`subscribers` (
  `insurer_number`,
  `insurance_symbol`,
  `insurance_number`,
  `insurance_branchnumber`,
  `birth`,
  `gender_code`,
  `name_kana_full`,
  `person_id_custom`,
  `hia_subscriber_id`,
  `name_kana_full_match`,
  `insurance_symbol_match`,
  `insurance_number_match`,
  `identity_hash`
) VALUES
  ('06139463', '100', '000001', NULL, '1970-11-17', 1, 'サンプル タロウ', 'ip3p3wzzr3ner5555555offgigkkhckixtjwaaaawaa', 'CSV_SAMPLE_001', 'サンプルタロウ', '100', '1', '90c942a384b263e9991b7cee07b4609a99024440b70d5f1a8a49963740c5b989'),
  ('06139463', '100', '000002', NULL, '2000-01-15', 2, 'サンプル ハナコ', 'ipywwwwz6rner5555555qffgigkkhckixtjwaaaawaa', 'CSV_SAMPLE_002', 'サンプルハナコ', '100', '2', 'c27332fcc041d5c7194f8faa88dbd8786f85bb14d81cc36aaa85fdd9bb296002'),
  ('06139463', '100', '000003', NULL, '2001-10-26', 2, 'サンプル ジロウ', 'ipywwzzwigner5555555sffgigkkhckixtjwaaaawaa', 'CSV_SAMPLE_003', 'サンプルジロウ', '100', '3', '6e10bb6e7654e36013aaec34e1f381962a77b40e6d80e7d90d6c236157526930'),
  ('06139463', '100', '000004', NULL, '1974-05-26', 2, 'サンプル サブロウ', 'ip3p3zwzg6ner5555555vffgigkkhckixtjwaaaawaa', 'CSV_SAMPLE_004', 'サンプルサブロウ', '100', '4', '7a6d1e21e11913c09bfc8c4fa44cbcde24414cc91ec3fb5a3d97659cb7f52cd5'),
  ('06139463', '100', '000005', NULL, '1984-09-24', 2, 'サンプル シロウ', 'ip3pz6y6riner555555n5ffgigkkhckixtjwaaaawaa', 'CSV_SAMPLE_005', 'サンプルシロウ', '100', '5', '985c3bd68e42562080bb86ec6ade52f73b977e050426c9a1d7c14a2fbad51a7c'),
  ('06139463', '100', '900001', NULL, '1988-12-16', 2, 'シケン ハナコ', 'ip3pm6zm6gner5nv5555offgigkkhckixtjwaaaawaa', 'CSV_MURAKAMI_001', 'シケンハナコ', '100', '900001', '12b80726cb500ef5dc3e2824a75f4342aabf852ec4ae9f6b611ae785aabe2a26'),
  ('06139463', '100', '900002', NULL, '1993-12-25', 2, 'シケン メグミ', 'ip3pipzmirner5nv5555qffgigkkhckixtjwaaaawaa', 'CSV_MURAKAMI_002', 'シケンメグミ', '100', '900002', '1d5121bcb4285bee8971d41df358dd797abe001a31f5cd7900a0992260161563')
ON DUPLICATE KEY UPDATE
  `hia_subscriber_id` = VALUES(`hia_subscriber_id`),
  `gender_code` = VALUES(`gender_code`),
  `identity_hash` = VALUES(`identity_hash`),
  `name_kana_full_match` = VALUES(`name_kana_full_match`),
  `insurance_symbol_match` = VALUES(`insurance_symbol_match`),
  `insurance_number_match` = VALUES(`insurance_number_match`),
  `updated_at` = CURRENT_TIMESTAMP(3);
