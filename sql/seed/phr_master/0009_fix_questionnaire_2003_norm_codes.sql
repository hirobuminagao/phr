-- Fix questionnaire result-code OID 1.2.392.200119.6.2003 aliases.
--
-- Annex 1 defines this OID as:
-- - 1: はい
-- - 2: いいえ
--
-- Older norm_variants data included Y/N as normalized_code for this OID.
-- Those values are useful as raw aliases, but the normalized CD code must be
-- the Annex 1 code used in the XML value/@code.

UPDATE `phr_master`.`norm_variants`
SET
  `normalized_code` = '1',
  `code_system` = COALESCE(`code_system`, `result_code_oid`),
  `display_name` = 'はい',
  `note` = CASE
    WHEN `note` IS NULL OR `note` = '' THEN 'fix: 2003 yes alias normalized to Annex 1 code 1'
    WHEN `note` LIKE '%fix: 2003 yes alias normalized to Annex 1 code 1%' THEN `note`
    ELSE CONCAT(`note`, '; fix: 2003 yes alias normalized to Annex 1 code 1')
  END,
  `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `result_code_oid` = '1.2.392.200119.6.2003'
  AND BINARY `raw_value_utf8` IN (
    BINARY '1',
    BINARY 'はい',
    BINARY '有',
    BINARY 'Y',
    BINARY 'true',
    BINARY 'TRUE',
    BINARY 'Yes',
    BINARY 'YES'
  )
  AND `normalized_code` <> '1';

UPDATE `phr_master`.`norm_variants`
SET
  `normalized_code` = '2',
  `code_system` = COALESCE(`code_system`, `result_code_oid`),
  `display_name` = 'いいえ',
  `note` = CASE
    WHEN `note` IS NULL OR `note` = '' THEN 'fix: 2003 no alias normalized to Annex 1 code 2'
    WHEN `note` LIKE '%fix: 2003 no alias normalized to Annex 1 code 2%' THEN `note`
    ELSE CONCAT(`note`, '; fix: 2003 no alias normalized to Annex 1 code 2')
  END,
  `updated_at` = CURRENT_TIMESTAMP(6)
WHERE `result_code_oid` = '1.2.392.200119.6.2003'
  AND BINARY `raw_value_utf8` IN (
    BINARY '2',
    BINARY 'いいえ',
    BINARY '無',
    BINARY 'N',
    BINARY '0',
    BINARY 'false',
    BINARY 'FALSE',
    BINARY 'No',
    BINARY 'NO'
  )
  AND `normalized_code` <> '2';
