CREATE TABLE `phr_master`.`exam_item_output_policies` (
  `exam_item_output_policy_id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `exam_facility_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT '0の場合は全施設共通',
  `namecode` char(17) NOT NULL COMMENT '出力制御対象の健診項目namecode',
  `output_policy` varchar(32) NOT NULL DEFAULT 'INCLUDE'
    COMMENT 'INCLUDE=XML出力対象, EXCLUDE=証跡のみでXML出力しない, REVIEW_REQUIRED=確認完了まで出力停止',
  `policy_reason` varchar(255) DEFAULT NULL COMMENT '出力/除外/確認待ちの理由',
  `confirmed_at` datetime(3) DEFAULT NULL COMMENT '医療機関確認または運用判断日時',
  `confirmed_by` varchar(190) DEFAULT NULL COMMENT '確認者',
  `confirmation_note` text DEFAULT NULL COMMENT '医療機関回答、判断根拠、参照JLAC等',
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`exam_item_output_policy_id`),
  UNIQUE KEY `uq_exam_item_output_policies_scope` (`exam_facility_id`, `namecode`, `is_active`),
  KEY `idx_exam_item_output_policies_namecode` (`namecode`, `is_active`),
  KEY `idx_exam_item_output_policies_facility` (`exam_facility_id`, `is_active`),
  KEY `idx_exam_item_output_policies_policy` (`output_policy`, `is_active`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目のXML出力可否ポリシー。独自項目・医療機関確認項目の出力/除外/確認待ちを制御する。';
