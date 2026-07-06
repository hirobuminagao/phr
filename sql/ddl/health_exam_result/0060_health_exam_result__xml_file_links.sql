CREATE TABLE `health_exam_result`.`xml_file_links` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint NOT NULL,
  `file_receipt_id` bigint unsigned NOT NULL,
  `xml_ledger_id` bigint unsigned NOT NULL,
  `xml_inner_path` varchar(1024) DEFAULT NULL,
  `xml_inner_path_sha256` char(64) GENERATED ALWAYS AS (sha2(coalesce(`xml_inner_path`, _utf8mb4''), 256)) STORED,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_xml_file_links_file_xml_inner_path` (`file_receipt_id`, `xml_ledger_id`, `xml_inner_path_sha256`),
  KEY `idx_xml_file_links_event` (`event_id`),
  KEY `idx_xml_file_links_file_receipt` (`file_receipt_id`),
  KEY `idx_xml_file_links_xml_ledger` (`xml_ledger_id`),
  KEY `idx_xml_file_links_created` (`created_at`),

  CONSTRAINT `fk_health_exam_result_xml_file_links_file_receipt`
    FOREIGN KEY (`file_receipt_id`)
    REFERENCES `health_exam_result`.`file_receipts` (`id`)
    ON DELETE CASCADE,
  CONSTRAINT `fk_health_exam_result_xml_file_links_xml_ledger`
    FOREIGN KEY (`xml_ledger_id`)
    REFERENCES `health_exam_result`.`xml_ledger` (`id`)
    ON DELETE CASCADE
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
