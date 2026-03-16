

ALTER TABLE `work_other`.`hia_dashboard_status`
  ADD COLUMN `subscriber_person_id_custom` varchar(64) DEFAULT NULL AFTER `name_match`,
  ADD COLUMN `subscriber_name_kana_full` varchar(190) DEFAULT NULL AFTER `subscriber_person_id_custom`,
  ADD COLUMN `subscriber_gender_code` tinyint unsigned DEFAULT NULL AFTER `subscriber_name_kana_full`,
  ADD COLUMN `subscriber_birth` date DEFAULT NULL AFTER `subscriber_gender_code`;

ALTER TABLE `work_other`.`hia_dashboard_status`
  ADD KEY `idx_hia_dashboard_subscriber_person_id_custom` (`subscriber_person_id_custom`);