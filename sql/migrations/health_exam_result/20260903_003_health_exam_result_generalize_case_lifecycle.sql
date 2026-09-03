-- 001/002適用後に実行する。
-- case統合だけでなく、健診機関再解決による旧case差し替えも同じlifecycleで表現する。

ALTER TABLE `health_exam_result`.`exam_export_cases`
  DROP FOREIGN KEY `fk_exam_export_cases_merged_into`,
  CHANGE COLUMN `merged_into_case_id` `successor_case_id` bigint unsigned DEFAULT NULL
    COMMENT 'MERGED/SUPERSEDED時の移行先ACTIVE case ID',
  CHANGE COLUMN `merged_at` `lifecycle_closed_at` datetime(3) DEFAULT NULL
    COMMENT 'ACTIVE以外へ移行した日時',
  CHANGE COLUMN `merged_by_app_user_id` `lifecycle_closed_by_app_user_id` bigint unsigned DEFAULT NULL
    COMMENT 'lifecycle終了を実行したapp user ID。保守CLIではNULL',
  CHANGE COLUMN `merge_operation_reason` `lifecycle_close_reason` text DEFAULT NULL
    COMMENT '統合・差し替え等でACTIVEを終了した理由',
  RENAME INDEX `idx_exam_export_cases_merged_into` TO `idx_exam_export_cases_successor`,
  ADD CONSTRAINT `fk_exam_export_cases_successor`
    FOREIGN KEY (`successor_case_id`)
    REFERENCES `health_exam_result`.`exam_export_cases` (`exam_export_case_id`);

ALTER TABLE `health_exam_result`.`exam_export_cases`
  MODIFY COLUMN `case_lifecycle_status` varchar(16) NOT NULL DEFAULT 'ACTIVE'
    COMMENT 'case lifecycle: ACTIVE/MERGED/SUPERSEDED';
