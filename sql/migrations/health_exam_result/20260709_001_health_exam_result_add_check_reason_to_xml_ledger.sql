ALTER TABLE `health_exam_result`.`xml_ledger`
  ADD COLUMN `check_reason` text
    AFTER `check_status`;
