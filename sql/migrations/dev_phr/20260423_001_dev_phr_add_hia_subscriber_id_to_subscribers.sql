-- 20260423_001_dev_phr_add_hia_subscriber_id_to_subscribers.sql
ALTER TABLE `dev_phr`.`subscribers`
  ADD COLUMN `hia_subscriber_id` varchar(190) DEFAULT NULL COMMENT 'HIA由来の加入者ID'
  AFTER `person_id_custom`;
