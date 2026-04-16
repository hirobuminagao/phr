

START TRANSACTION;

-- template_mappings.target_column を、新 staging_subscribers_fund DDL に合わせて更新する
-- ここでは target_column の rename のみを行い、rule / required は変更しない
-- 追加マッピング（match系・会社情報系など）が必要な場合は別 migration で扱う

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'insurance_symbol_norm'
 WHERE `target_column` = 'insurance_symbol';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'insurance_number_norm'
 WHERE `target_column` = 'insurance_number';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'birth_norm'
 WHERE `target_column` = 'birth';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'gender_code_norm'
 WHERE `target_column` = 'gender_code';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'relationship_code_norm'
 WHERE `target_column` = 'relationship_code';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'relationship_name_norm'
 WHERE `target_column` = 'relationship_name';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kanji_full_norm'
 WHERE `target_column` = 'name_kanji_full';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kanji_family_norm'
 WHERE `target_column` = 'name_kanji_family';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kanji_middle_norm'
 WHERE `target_column` = 'name_kanji_middle';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kanji_given_norm'
 WHERE `target_column` = 'name_kanji_given';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kana_full_norm'
 WHERE `target_column` = 'name_kana_full';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kana_family_norm'
 WHERE `target_column` = 'name_kana_family';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kana_middle_norm'
 WHERE `target_column` = 'name_kana_middle';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'name_kana_given_norm'
 WHERE `target_column` = 'name_kana_given';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'qualification_acquired_date_norm'
 WHERE `target_column` = 'qualification_acquired_date';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'qualification_lost_date_norm'
 WHERE `target_column` = 'qualification_lost_date';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'postal_code_norm'
 WHERE `target_column` = 'postal_code';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'address_line_norm'
 WHERE `target_column` = 'address_line';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'building_norm'
 WHERE `target_column` = 'building';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'phone_norm'
 WHERE `target_column` = 'phone';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'email_norm'
 WHERE `target_column` = 'email';

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'received_company_code_norm'
 WHERE `target_column` IN ('employer_code', 'received_company_code');

UPDATE `dev_phr`.`template_mappings`
   SET `target_column` = 'connect_id_norm'
 WHERE `target_column` = 'connect_id';

COMMIT;