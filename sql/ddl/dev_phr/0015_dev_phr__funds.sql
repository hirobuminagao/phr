CREATE TABLE `dev_phr`.`funds` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `fund_code` varchar(64) DEFAULT NULL,
  `name_official` varchar(190) NOT NULL,
  `name_short` varchar(190) DEFAULT NULL,
  `name_kana` varchar(190) DEFAULT NULL,
  `name_display` varchar(190) DEFAULT NULL,
  `org_type` enum('健保','けんぽ','健康保険組合','共済組合') DEFAULT NULL,
  `health_exam_result_receive_method` varchar(64) DEFAULT NULL,
  `health_exam_result_receive_timing` varchar(64) DEFAULT NULL,
  `health_exam_result_file_format` varchar(64) DEFAULT NULL,
  `tokuho_xml_delivery_method` varchar(64) DEFAULT NULL,
  `tokuho_xml_delivery_service` varchar(64) DEFAULT NULL,
  `tokuho_xml_delivery_media` varchar(64) DEFAULT NULL,
  `tokuho_xml_delivery_policy` varchar(190) DEFAULT NULL,
  `tokuho_xml_initial_individual` tinyint(1) DEFAULT NULL,
  `tokuho_xml_delivery_schedule` enum('per_event','monthly','fiscal_close') DEFAULT NULL,
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `notes` text,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_funds_fund_code` (`fund_code`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
