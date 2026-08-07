ALTER TABLE `health_exam_result`.`exam_result_ledger_report`
  ADD COLUMN `hia_dashboard_status` varchar(64) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL
    COMMENT '報告作成時点のwork_other.hia_dashboard_status.status。hia_subscriber_idがある場合のみ参照'
    AFTER `qualification_lost_date`,
  ADD COLUMN `hia_dashboard_reservation_date` date DEFAULT NULL
    COMMENT '報告作成時点のwork_other.hia_dashboard_status.reservation_date'
    AFTER `hia_dashboard_status`,
  ADD COLUMN `hia_dashboard_exam_date` date DEFAULT NULL
    COMMENT '報告作成時点のwork_other.hia_dashboard_status.exam_date'
    AFTER `hia_dashboard_reservation_date`,
  ADD COLUMN `hia_dashboard_medical_institution` varchar(190) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL
    COMMENT '報告作成時点のwork_other.hia_dashboard_status.medical_institution'
    AFTER `hia_dashboard_exam_date`,
  ADD COLUMN `hia_dashboard_course_name` varchar(190) COLLATE utf8mb4_ja_0900_as_cs DEFAULT NULL
    COMMENT '報告作成時点のwork_other.hia_dashboard_status.course_name'
    AFTER `hia_dashboard_medical_institution`,
  ADD KEY `idx_exam_result_ledger_report_hia_dashboard_status` (`hia_dashboard_status`);
