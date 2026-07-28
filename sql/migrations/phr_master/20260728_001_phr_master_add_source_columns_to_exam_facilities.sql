ALTER TABLE `phr_master`.`exam_facilities`
  ADD COLUMN `data_source_name` varchar(255) DEFAULT NULL AFTER `management_entity`,
  ADD COLUMN `data_source_file_name` varchar(255) DEFAULT NULL AFTER `data_source_name`,
  ADD COLUMN `data_source_file_sha256` char(64) DEFAULT NULL AFTER `data_source_file_name`,
  ADD COLUMN `data_source_note` text AFTER `data_source_file_sha256`,
  ADD KEY `idx_exam_facilities_data_source` (`data_source_name`);

