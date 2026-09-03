-- 001適用後、既存の同一受診caseをmerge_exam_export_cases.pyで統合してから実行する。
-- ACTIVE重複が残っている場合、このALTERは1062で停止する。データの自動統合は行わない。

ALTER TABLE `health_exam_result`.`exam_export_cases`
  DROP INDEX `uq_exam_export_cases_natural`,
  ADD UNIQUE KEY `uq_exam_export_cases_active_natural` (
    `event_id`, `subscriber_id`, `exam_date`, `exam_facility_id`, `active_case_guard`
  );
