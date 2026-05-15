ALTER TABLE `dev_phr`.`staging_subscribers_fund`
ADD COLUMN `parts_apply_subscriber_id` bigint unsigned DEFAULT NULL
  COMMENT 'parts補完用加入者ID（適用直前の再確認結果）'
  AFTER `matched_subscriber_id`,

ADD COLUMN `parts_apply_status` varchar(50) DEFAULT NULL
  COMMENT 'parts補完用再確認ステータス（IDENTITY_MATCHED等）'
  AFTER `parts_apply_subscriber_id`,

ADD COLUMN `parts_apply_reason` varchar(255) DEFAULT NULL
  COMMENT 'parts補完用再確認理由・スキップ理由'
  AFTER `parts_apply_status`,

ADD COLUMN `parts_apply_checked_at` datetime(3) DEFAULT NULL
  COMMENT 'parts補完用再確認日時'
  AFTER `parts_apply_reason`;


ALTER TABLE `dev_phr`.`staging_subscribers_fund`
ADD KEY `idx_stgfund_parts_apply_subscriber_id`
  (`parts_apply_subscriber_id`),

ADD KEY `idx_stgfund_parts_apply_status`
  (`parts_apply_status`);
