ALTER TABLE `health_exam_result`.`manual_exam_entry_draft_check_results`
  ADD COLUMN `specific_detail_json` json DEFAULT NULL
  AFTER `specific_reason_summary`;
