ALTER TABLE `health_exam_result`.`exam_case_check_review_items`
  ADD COLUMN `review_note` text DEFAULT NULL
    COMMENT '現在有効な人手判断理由。変更履歴はaudit_logsへ保存'
    AFTER `review_status`;

UPDATE `health_exam_result`.`exam_case_check_review_items` AS cri
INNER JOIN (
  SELECT audit.exam_case_check_review_item_id, audit.note
  FROM `health_exam_result`.`exam_case_check_review_item_audit_logs` AS audit
  INNER JOIN (
    SELECT exam_case_check_review_item_id, MAX(exam_case_check_review_item_audit_log_id) AS latest_id
    FROM `health_exam_result`.`exam_case_check_review_item_audit_logs`
    GROUP BY exam_case_check_review_item_id
  ) AS latest
    ON latest.latest_id = audit.exam_case_check_review_item_audit_log_id
) AS latest_audit
  ON latest_audit.exam_case_check_review_item_id = cri.exam_case_check_review_item_id
SET cri.review_note = NULLIF(latest_audit.note, '')
WHERE cri.review_status IN ('APPROVED_WITH_REASON', 'EXCLUDED')
  AND NULLIF(latest_audit.note, '') IS NOT NULL;
