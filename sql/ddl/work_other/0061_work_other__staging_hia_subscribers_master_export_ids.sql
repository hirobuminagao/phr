

CREATE TABLE `work_other`.`staging_hia_subscribers_master_export_ids` (
  `staging_hia_subscribers_master_export_ids_sid` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  -- HIA subscriber id (from Excel "id")
  `hia_subscriber_id` VARCHAR(190) DEFAULT NULL,

  -- raw columns from Excel
  `client_id` VARCHAR(64) DEFAULT NULL,
  `company_id` VARCHAR(64) DEFAULT NULL,
  `department_id` VARCHAR(64) DEFAULT NULL,
  `insurance_card_symbol` VARCHAR(32) DEFAULT NULL,
  `insurance_card_number` VARCHAR(32) DEFAULT NULL,
  `branch_number` VARCHAR(16) DEFAULT NULL,
  `insured_person_attribute_code` VARCHAR(16) DEFAULT NULL,
  `insured_classification` VARCHAR(64) DEFAULT NULL,
  `relationship_name` VARCHAR(64) DEFAULT NULL,
  `qualification_acquisition_date` DATE DEFAULT NULL,
  `qualification_loss_scheduled_date` DATE DEFAULT NULL,
  `qualification_loss_date` DATE DEFAULT NULL,
  `name` VARCHAR(190) DEFAULT NULL,
  `name_kana` VARCHAR(190) DEFAULT NULL,
  `date_of_birth` DATE DEFAULT NULL,
  `gender` VARCHAR(16) DEFAULT NULL,

  -- required for identity generation
  `insurer_number` CHAR(8) DEFAULT NULL,

  -- derived columns
  `identity_hash` VARCHAR(190) DEFAULT NULL,
  `subscribers_id` BIGINT UNSIGNED DEFAULT NULL,

  -- metadata
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  PRIMARY KEY (`staging_hia_subscribers_master_export_ids_sid`),
  KEY `idx_staging_identity_hash` (`identity_hash`),
  KEY `idx_staging_subscribers_id` (`subscribers_id`),
  KEY `idx_staging_hia_subscriber_id` (`hia_subscriber_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs;