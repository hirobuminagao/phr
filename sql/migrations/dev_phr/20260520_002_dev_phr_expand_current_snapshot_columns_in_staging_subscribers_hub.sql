

ALTER TABLE `staging_subscribers_hub`
  ADD COLUMN `current_hia_subscriber_id` varchar(190)
    DEFAULT NULL
    COMMENT 'current snapshot subscribers.hia_subscriber_id。review時の外部ID確認用'
    AFTER `current_subscriber_id`,

  ADD COLUMN `current_compare_identity_norm_hash` char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    DEFAULT NULL
    COMMENT 'current snapshot identity登録値 compare hash'
    AFTER `current_identity_hash`,

  ADD COLUMN `current_compare_other_hash` char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    DEFAULT NULL
    COMMENT 'current snapshot subscriber属性 compare hash'
    AFTER `current_compare_identity_norm_hash`,

  ADD COLUMN `current_address_hash` char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    DEFAULT NULL
    COMMENT 'current snapshot subscriber_addresses.address_hash'
    AFTER `current_address_id`,

  ADD COLUMN `current_phone_contact_point_id` bigint unsigned
    DEFAULT NULL
    COMMENT 'current snapshot subscriber_contact_points phone current id'
    AFTER `current_address_hash`,

  ADD COLUMN `current_email_contact_point_id` bigint unsigned
    DEFAULT NULL
    COMMENT 'current snapshot subscriber_contact_points email current id'
    AFTER `current_phone_contact_point_id`;

ALTER TABLE `staging_subscribers_hub`
  CHANGE COLUMN `contact_diff_status` `contact_point_diff_status` varchar(50)
    DEFAULT NULL
    COMMENT 'contact point compare結果';

ALTER TABLE `staging_subscribers_hub`
  DROP COLUMN `current_contact_id`;

ALTER TABLE `staging_subscribers_hub`
  ADD KEY `idx_stghub_current_hia_subscriber_id` (`current_hia_subscriber_id`),
  ADD KEY `idx_stghub_current_compare_hashes` (
    `current_subscriber_id`,
    `current_compare_identity_norm_hash`,
    `current_compare_other_hash`
  ),
  ADD KEY `idx_stghub_current_address_hash` (
    `current_subscriber_id`,
    `current_address_hash`
  ),
  ADD KEY `idx_stghub_current_phone_contact_point` (`current_phone_contact_point_id`),
  ADD KEY `idx_stghub_current_email_contact_point` (`current_email_contact_point_id`),
  ADD KEY `idx_stghub_contact_point_diff_status` (`contact_point_diff_status`);