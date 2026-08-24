UPDATE `health_exam_result`.`exam_item_values` AS eiv
INNER JOIN `health_exam_result`.`exam_ledgers` AS el
  ON el.`exam_ledger_id` = eiv.`ledger_id`
 AND el.`source_type` = eiv.`ledger_type`
   SET eiv.`ledger_type` = 'EXAM'
 WHERE el.`source_type` IN ('PAPER', 'MANUAL');
