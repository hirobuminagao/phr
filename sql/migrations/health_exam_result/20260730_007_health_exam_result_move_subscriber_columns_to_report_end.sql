-- Excel出力時に加入者付加列をrefreshed_atの直前へまとめる。
ALTER TABLE `health_exam_result`.`exam_result_ledger_report`
  MODIFY COLUMN `relationship_name` varchar(190) DEFAULT NULL
    COMMENT '報告作成時点のdev_phr.subscribers.relationship_name' AFTER `resume_approved_reason`,
  MODIFY COLUMN `qualification_lost_date` date DEFAULT NULL
    COMMENT '報告作成時点のdev_phr.subscribers.qualification_lost_date' AFTER `relationship_name`;
