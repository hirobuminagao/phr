CREATE TABLE `phr_master`.`exam_facilities` (
  `exam_facility_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exam_facility_code` varchar(64) DEFAULT NULL,
  `exam_facility_name` varchar(255) NOT NULL,
  `exam_facility_display_name` varchar(255) DEFAULT NULL,
  `exam_facility_type` varchar(64) DEFAULT NULL,
  `medical_institution_code` varchar(64) DEFAULT NULL,
  `reservation_system_medical_institution_code` varchar(64) DEFAULT NULL,
  `postal_code` varchar(16) DEFAULT NULL,
  `address` varchar(512) DEFAULT NULL,
  `phone_number` varchar(64) DEFAULT NULL,
  `website_url` varchar(512) DEFAULT NULL,
  `management_entity` varchar(255) DEFAULT NULL,
  `note` text,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_facility_id`),
  UNIQUE KEY `uq_exam_facilities_code` (`exam_facility_code`),
  KEY `idx_exam_facilities_medical_institution_code` (`medical_institution_code`),
  KEY `idx_exam_facilities_reservation_system_code` (`reservation_system_medical_institution_code`),
  KEY `idx_exam_facilities_type` (`exam_facility_type`),
  KEY `idx_exam_facilities_name` (`exam_facility_name`),
  KEY `idx_exam_facilities_active` (`is_active`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
