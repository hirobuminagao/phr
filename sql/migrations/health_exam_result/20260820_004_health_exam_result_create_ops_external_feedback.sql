CREATE TABLE `health_exam_result`.`ops_external_feedback_reports` (
  `external_feedback_report_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint DEFAULT NULL,
  `feedback_source` varchar(32) NOT NULL COMMENT 'HIA_UPLOAD/FUND_DELIVERY/EMPLOYER_DELIVERY/MANUAL',
  `feedback_scope` varchar(32) NOT NULL DEFAULT 'CASE' COMMENT 'OUTPUT_LIST/ZIP/XML/CASE/OTHER',
  `report_status` varchar(32) NOT NULL DEFAULT 'OPEN' COMMENT 'OPEN/IN_PROGRESS/RESOLVED/CLOSED/CANCELLED',
  `received_at` datetime(3) DEFAULT NULL,
  `received_from` varchar(255) DEFAULT NULL,
  `channel` varchar(64) DEFAULT NULL COMMENT 'MAIL/PHONE/WEB/FILE/MANUAL等',
  `summary` text DEFAULT NULL,
  `source_file_name` varchar(255) DEFAULT NULL,
  `source_file_path` varchar(1024) DEFAULT NULL,
  `xml_export_list_id` bigint unsigned DEFAULT NULL,
  `xml_export_zip_id` bigint unsigned DEFAULT NULL,
  `fund_delivery_list_id` bigint unsigned DEFAULT NULL,
  `fund_delivery_run_id` bigint unsigned DEFAULT NULL,
  `created_by` varchar(190) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_by` varchar(190) DEFAULT NULL,
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`external_feedback_report_id`),
  KEY `idx_ops_external_feedback_reports_event` (`event_id`),
  KEY `idx_ops_external_feedback_reports_source` (`feedback_source`, `feedback_scope`),
  KEY `idx_ops_external_feedback_reports_status` (`report_status`),
  KEY `idx_ops_external_feedback_reports_received` (`received_at`),
  KEY `idx_ops_external_feedback_reports_xml_list` (`xml_export_list_id`),
  KEY `idx_ops_external_feedback_reports_xml_zip` (`xml_export_zip_id`),
  KEY `idx_ops_external_feedback_reports_fund_list` (`fund_delivery_list_id`),
  KEY `idx_ops_external_feedback_reports_fund_run` (`fund_delivery_run_id`),
  CONSTRAINT `fk_ops_external_feedback_reports_xml_list`
    FOREIGN KEY (`xml_export_list_id`) REFERENCES `health_exam_result`.`ops_xml_export_lists` (`xml_export_list_id`),
  CONSTRAINT `fk_ops_external_feedback_reports_xml_zip`
    FOREIGN KEY (`xml_export_zip_id`) REFERENCES `health_exam_result`.`xml_export_zips` (`xml_export_zip_id`),
  CONSTRAINT `fk_ops_external_feedback_reports_fund_list`
    FOREIGN KEY (`fund_delivery_list_id`) REFERENCES `health_exam_result`.`fund_delivery_lists` (`delivery_list_id`),
  CONSTRAINT `fk_ops_external_feedback_reports_fund_run`
    FOREIGN KEY (`fund_delivery_run_id`) REFERENCES `health_exam_result`.`fund_delivery_runs` (`delivery_run_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`ops_external_feedback_items` (
  `external_feedback_item_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `external_feedback_report_id` bigint unsigned NOT NULL,
  `event_id` bigint DEFAULT NULL,
  `exam_export_case_id` bigint unsigned DEFAULT NULL COMMENT '人単位の対応先。特定できないZIP全体エラーではNULL',
  `xml_export_list_case_id` bigint unsigned DEFAULT NULL,
  `xml_export_member_id` bigint unsigned DEFAULT NULL,
  `xml_export_zip_id` bigint unsigned DEFAULT NULL,
  `fund_delivery_list_member_id` bigint unsigned DEFAULT NULL,
  `fund_delivery_member_id` bigint unsigned DEFAULT NULL,
  `issue_level` varchar(16) NOT NULL DEFAULT 'ERROR' COMMENT 'ERROR/WARNING/INFO',
  `issue_category` varchar(64) NOT NULL DEFAULT 'OTHER' COMMENT 'SUBSCRIBER/BASIC_INFO/EXAM_ITEM/XML_SCHEMA/UPLOAD/DELIVERY/OTHER',
  `handling_status` varchar(32) NOT NULL DEFAULT 'OPEN' COMMENT 'OPEN/CONFIRMED/FIX_PLANNED/WAITING_RESUBMISSION/RESUBMITTED/RESOLVED/WONT_FIX/CANCELLED',
  `external_error_code` varchar(128) DEFAULT NULL,
  `external_message` text DEFAULT NULL,
  `namecode` varchar(64) DEFAULT NULL,
  `check_item_code` varchar(64) DEFAULT NULL COMMENT '44系/10系などの業務チェックID',
  `source_xml_file_name` varchar(255) DEFAULT NULL,
  `source_zip_file_name` varchar(255) DEFAULT NULL,
  `reported_value` text DEFAULT NULL,
  `resolution_note` text DEFAULT NULL,
  `assigned_to` varchar(190) DEFAULT NULL,
  `resolved_at` datetime(3) DEFAULT NULL,
  `resolved_by` varchar(190) DEFAULT NULL,
  `created_by` varchar(190) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_by` varchar(190) DEFAULT NULL,
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`external_feedback_item_id`),
  KEY `idx_ops_external_feedback_items_report` (`external_feedback_report_id`),
  KEY `idx_ops_external_feedback_items_event` (`event_id`),
  KEY `idx_ops_external_feedback_items_case` (`exam_export_case_id`),
  KEY `idx_ops_external_feedback_items_xml_list_case` (`xml_export_list_case_id`),
  KEY `idx_ops_external_feedback_items_xml_member` (`xml_export_member_id`),
  KEY `idx_ops_external_feedback_items_xml_zip` (`xml_export_zip_id`),
  KEY `idx_ops_external_feedback_items_fund_list_member` (`fund_delivery_list_member_id`),
  KEY `idx_ops_external_feedback_items_fund_member` (`fund_delivery_member_id`),
  KEY `idx_ops_external_feedback_items_status` (`handling_status`),
  KEY `idx_ops_external_feedback_items_category` (`issue_category`, `issue_level`),
  KEY `idx_ops_external_feedback_items_namecode` (`namecode`),
  KEY `idx_ops_external_feedback_items_check_item` (`check_item_code`),
  CONSTRAINT `fk_ops_external_feedback_items_report`
    FOREIGN KEY (`external_feedback_report_id`) REFERENCES `health_exam_result`.`ops_external_feedback_reports` (`external_feedback_report_id`),
  CONSTRAINT `fk_ops_external_feedback_items_case`
    FOREIGN KEY (`exam_export_case_id`) REFERENCES `health_exam_result`.`exam_export_cases` (`exam_export_case_id`),
  CONSTRAINT `fk_ops_external_feedback_items_xml_list_case`
    FOREIGN KEY (`xml_export_list_case_id`) REFERENCES `health_exam_result`.`ops_xml_export_list_cases` (`xml_export_list_case_id`),
  CONSTRAINT `fk_ops_external_feedback_items_xml_member`
    FOREIGN KEY (`xml_export_member_id`) REFERENCES `health_exam_result`.`xml_export_members` (`xml_export_member_id`),
  CONSTRAINT `fk_ops_external_feedback_items_xml_zip`
    FOREIGN KEY (`xml_export_zip_id`) REFERENCES `health_exam_result`.`xml_export_zips` (`xml_export_zip_id`),
  CONSTRAINT `fk_ops_external_feedback_items_fund_list_member`
    FOREIGN KEY (`fund_delivery_list_member_id`) REFERENCES `health_exam_result`.`fund_delivery_list_members` (`delivery_list_member_id`),
  CONSTRAINT `fk_ops_external_feedback_items_fund_member`
    FOREIGN KEY (`fund_delivery_member_id`) REFERENCES `health_exam_result`.`fund_delivery_members` (`delivery_member_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;

CREATE TABLE `health_exam_result`.`ops_external_feedback_item_audit_logs` (
  `external_feedback_item_audit_log_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `external_feedback_item_id` bigint unsigned NOT NULL,
  `action_type` varchar(64) NOT NULL COMMENT 'CREATE/STATUS_UPDATE/NOTE_UPDATE/LINK_UPDATE/RESOLVE等',
  `before_status` varchar(32) DEFAULT NULL,
  `after_status` varchar(32) DEFAULT NULL,
  `before_json` json DEFAULT NULL,
  `after_json` json DEFAULT NULL,
  `changed_by` varchar(190) DEFAULT NULL,
  `changed_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`external_feedback_item_audit_log_id`),
  KEY `idx_ops_external_feedback_item_audit_item` (`external_feedback_item_id`),
  KEY `idx_ops_external_feedback_item_audit_changed` (`changed_at`),
  CONSTRAINT `fk_ops_external_feedback_item_audit_item`
    FOREIGN KEY (`external_feedback_item_id`) REFERENCES `health_exam_result`.`ops_external_feedback_items` (`external_feedback_item_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
