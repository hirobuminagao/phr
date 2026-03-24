

-- 20260324_006_work_other_add_person_id_custom_and_identity_hash_to_medi_xml_ledger.sql
--
-- Purpose:
-- - work_other.medi_xml_ledger に person_id_custom / identity_hash を追加する
-- - medi_xml_ledger は XML単位の台帳として維持し、人物軸を後付けで通す
-- - identity_hash は person_id_custom + name_kana_match + gender_code から生成する前提とする

START TRANSACTION;

ALTER TABLE work_other.medi_xml_ledger
  ADD COLUMN person_id_custom VARCHAR(64) DEFAULT NULL
    COMMENT 'raw値から custom_id_gen により生成した人物キー'
    AFTER name_kana_match,
  ADD COLUMN identity_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL
    COMMENT 'SHA256(person_id_custom|name_kana_match|gender_code)'
    AFTER person_id_custom;

ALTER TABLE work_other.medi_xml_ledger
  ADD KEY idx_medi_xml_ledger_person_id_custom (person_id_custom),
  ADD KEY idx_medi_xml_ledger_identity_hash (identity_hash);

COMMIT;