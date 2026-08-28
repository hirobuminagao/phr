ALTER TABLE `phr_master`.`csv_exam_result_mapping_rules`
  ADD COLUMN `rule_origin_type` varchar(32) NOT NULL DEFAULT 'SEED'
    COMMENT 'ルール作成元。SEED=seed/migration等の裏投入、SCREEN=画面作成'
    AFTER `raw_unit`,
  ADD COLUMN `edit_capability` varchar(32) NOT NULL DEFAULT 'VIEW_ONLY'
    COMMENT '画面編集可否。VIEW_ONLY=表示のみ、BASIC_SIMPLE=単純/結合ルールを画面編集可、UNSUPPORTED=未実装'
    AFTER `rule_origin_type`;

UPDATE `phr_master`.`csv_exam_result_mapping_rules`
   SET `rule_origin_type` = COALESCE(NULLIF(`rule_origin_type`, ''), 'SEED'),
       `edit_capability` = COALESCE(NULLIF(`edit_capability`, ''), 'VIEW_ONLY');

UPDATE `phr_master`.`csv_exam_result_mapping_rules`
   SET `edit_capability` = 'VIEW_ONLY'
 WHERE `rule_origin_type` <> 'SCREEN';

CREATE INDEX `idx_csv_exam_result_mapping_rules_edit`
  ON `phr_master`.`csv_exam_result_mapping_rules` (`rule_origin_type`, `edit_capability`, `is_active`);
