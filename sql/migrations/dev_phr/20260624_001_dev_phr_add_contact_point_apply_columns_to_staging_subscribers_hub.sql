

-- dev_phr migration
-- Add contact_type-specific contact point compare/apply result columns to staging_subscribers_hub.

ALTER TABLE `staging_subscribers_hub`
  ADD COLUMN `phone_diff_status` varchar(50) DEFAULT NULL COMMENT 'phone contact point compare結果: noop/insert/switch_current/clear_current/review' AFTER `contact_point_diff_status`,
  ADD COLUMN `phone_target_contact_point_id` bigint unsigned DEFAULT NULL COMMENT 'phone contact point apply対象 subscriber_contact_points.contact_point_id' AFTER `phone_diff_status`,
  ADD COLUMN `email_diff_status` varchar(50) DEFAULT NULL COMMENT 'email contact point compare結果: noop/insert/switch_current/clear_current/review' AFTER `phone_target_contact_point_id`,
  ADD COLUMN `email_target_contact_point_id` bigint unsigned DEFAULT NULL COMMENT 'email contact point apply対象 subscriber_contact_points.contact_point_id' AFTER `email_diff_status`,
  ADD KEY `idx_stghub_phone_diff_status` (`phone_diff_status`),
  ADD KEY `idx_stghub_email_diff_status` (`email_diff_status`),
  ADD KEY `idx_stghub_phone_target_contact_point` (`phone_target_contact_point_id`),
  ADD KEY `idx_stghub_email_target_contact_point` (`email_target_contact_point_id`);