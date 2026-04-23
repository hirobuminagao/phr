

-- 20260423_008_work_other_add_subscribers_link_ids_to_hia_dashboard_status.sql
ALTER TABLE `work_other`.`hia_dashboard_status`
  ADD COLUMN `subscribers_id` bigint unsigned DEFAULT NULL AFTER `subscriber_person_id_custom`,
  ADD COLUMN `hia_subscriber_id` varchar(190) DEFAULT NULL COMMENT 'HIA由来の加入者ID' AFTER `subscribers_id`,
  ADD KEY `idx_hia_dashboard_subscribers_id` (`subscribers_id`),
  ADD KEY `idx_hia_dashboard_hia_subscriber_id` (`hia_subscriber_id`);