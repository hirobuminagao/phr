

-- ============================================================
-- work_other.hia_import_zips に archive 後の物理パス追跡列を追加
-- ============================================================

ALTER TABLE `work_other`.`hia_import_zips`
  ADD COLUMN `archived_zip_path` VARCHAR(500) COLLATE utf8mb4_ja_0900_as_cs
    COMMENT 'archive後のZIP物理パス（相対または絶対）'
  AFTER `zip_sha256`,
  ADD COLUMN `archived_at` DATETIME
    COMMENT 'archive へ移動した日時'
  AFTER `archived_zip_path`;