SET @manual_draft_has_address := (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'manual_exam_entry_drafts'
    AND column_name = 'address'
);

SET @sql := IF(
  @manual_draft_has_address = 0,
  'ALTER TABLE `manual_exam_entry_drafts` ADD COLUMN `address` varchar(255) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL AFTER `postal_code`',
  'DO 0'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
