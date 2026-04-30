

-- =====================================================================
-- Migration: add name parts match columns
-- Target DB : dev_phr
-- Date      : 2026-04-30
-- Purpose   : major_candidate 判定（転籍・名字変更候補）で使用する
--             氏名partsの照合用カラムを追加する。
--
-- Notes:
-- - subscribers にはカナ/漢字 parts match を追加する。
-- - staging_subscribers_fund には不足しているカナ parts match を追加する。
-- - staging_subscribers_fund の漢字 parts match は既存のため追加しない。
-- - 既存データへの値埋めは別backfillで実施する。
-- =====================================================================

ALTER TABLE `dev_phr`.`subscribers`
  ADD COLUMN `name_kana_family_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '姓カナ（照合用）'
    AFTER `name_full_match`,
  ADD COLUMN `name_kana_middle_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT 'ミドルネームカナ（照合用）'
    AFTER `name_kana_family_match`,
  ADD COLUMN `name_kana_given_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '名カナ（照合用）'
    AFTER `name_kana_middle_match`,
  ADD COLUMN `name_kanji_family_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '姓漢字（照合用）'
    AFTER `name_kana_given_match`,
  ADD COLUMN `name_kanji_middle_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT 'ミドルネーム漢字（照合用）'
    AFTER `name_kanji_family_match`,
  ADD COLUMN `name_kanji_given_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '名漢字（照合用）'
    AFTER `name_kanji_middle_match`;

ALTER TABLE `dev_phr`.`staging_subscribers_fund`
  ADD COLUMN `name_kana_family_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '姓カナ（照合用）'
    AFTER `name_kana_full_match`,
  ADD COLUMN `name_kana_middle_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT 'ミドルネームカナ（照合用）'
    AFTER `name_kana_family_match`,
  ADD COLUMN `name_kana_given_match` varchar(190)
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL COMMENT '名カナ（照合用）'
    AFTER `name_kana_middle_match`;