CREATE DATABASE IF NOT EXISTS `phr_system_support`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_ja_0900_as_cs;

CREATE TABLE `phr_system_support`.`support_incidents` (
  `incident_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `incident_key` varchar(64) NOT NULL,
  `title` varchar(255) NOT NULL,
  `incident_type` varchar(32) NOT NULL COMMENT 'BUG/IMPLEMENTATION_GAP/SPEC_GAP/DATA_REPAIR等',
  `status` varchar(32) NOT NULL DEFAULT 'OPEN' COMMENT 'OPEN/SNAPSHOTTED/VERIFYING/COMPLETED/CANCELLED',
  `event_id` bigint DEFAULT NULL,
  `description` text,
  `resolution_note` text,
  `created_by` varchar(190) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `completed_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`incident_id`),
  UNIQUE KEY `uq_support_incidents_key` (`incident_key`),
  KEY `idx_support_incidents_status` (`status`),
  KEY `idx_support_incidents_event` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='通常処理外で対応するシステム不具合・仕様漏れ・データ補正の事象台帳。';

CREATE TABLE `phr_system_support`.`support_snapshot_targets` (
  `snapshot_target_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `incident_id` bigint unsigned NOT NULL,
  `capture_batch_id` char(36) NOT NULL,
  `snapshot_phase` varchar(16) NOT NULL COMMENT 'BEFORE/AFTER/VERIFY',
  `target_type` varchar(32) NOT NULL COMMENT 'CASE/LEDGER/FILE/SUBSCRIBER等',
  `target_schema` varchar(64) DEFAULT NULL,
  `target_table` varchar(64) DEFAULT NULL,
  `target_id` varchar(190) NOT NULL,
  `event_id` bigint DEFAULT NULL,
  `exam_export_case_id` bigint unsigned DEFAULT NULL,
  `source_exam_ledger_id` bigint unsigned DEFAULT NULL,
  `reprocess_required` tinyint(1) NOT NULL DEFAULT 0,
  `reexport_required` tinyint(1) NOT NULL DEFAULT 0,
  `action_status` varchar(32) NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING/PROCESSED/VERIFIED/COMPLETED/SKIPPED',
  `snapshot_data` json NOT NULL,
  `captured_by` varchar(190) DEFAULT NULL,
  `captured_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `processed_at` datetime(3) DEFAULT NULL,
  `verified_at` datetime(3) DEFAULT NULL,
  `completed_at` datetime(3) DEFAULT NULL,
  PRIMARY KEY (`snapshot_target_id`),
  UNIQUE KEY `uq_support_snapshot_batch_target` (`incident_id`, `capture_batch_id`, `target_type`, `target_id`),
  KEY `idx_support_snapshot_incident_phase` (`incident_id`, `snapshot_phase`),
  KEY `idx_support_snapshot_case` (`exam_export_case_id`),
  KEY `idx_support_snapshot_ledger` (`source_exam_ledger_id`),
  KEY `idx_support_snapshot_action` (`incident_id`, `action_status`, `reprocess_required`, `reexport_required`),
  CONSTRAINT `fk_support_snapshot_incident`
    FOREIGN KEY (`incident_id`) REFERENCES `phr_system_support`.`support_incidents` (`incident_id`)
    ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='事象ごとの修正前後対象と、その時点の業務データ状態を保持するスナップショット。';
