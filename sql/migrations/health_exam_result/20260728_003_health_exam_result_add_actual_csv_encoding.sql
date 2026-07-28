ALTER TABLE `health_exam_result`.`file_receipts`
  ADD COLUMN `actual_character_encoding` varchar(32) DEFAULT NULL AFTER `actual_header_sha256`;
