

ALTER TABLE `work_other`.`hia_dashboard_status`
  ADD COLUMN `insured_type` varchar(64) DEFAULT NULL
  AFTER `insurance_number`;
