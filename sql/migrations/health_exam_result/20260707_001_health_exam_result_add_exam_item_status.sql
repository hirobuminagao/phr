ALTER TABLE `health_exam_result`.`xml_ledger`
  ADD COLUMN `exam_item_status` varchar(32) DEFAULT NULL AFTER `subscriber_match_reason`,
  ADD COLUMN `exam_item_reason` text AFTER `exam_item_status`;
