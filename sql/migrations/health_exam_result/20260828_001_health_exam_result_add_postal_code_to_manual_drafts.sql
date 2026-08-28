SET @manual_draft_has_postal_code := (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'manual_exam_entry_drafts'
    AND column_name = 'postal_code'
);

SET @sql := IF(
  @manual_draft_has_postal_code = 0,
  'ALTER TABLE `manual_exam_entry_drafts` ADD COLUMN `postal_code` varchar(16) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL AFTER `insurance_branch_number`',
  'DO 0'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
