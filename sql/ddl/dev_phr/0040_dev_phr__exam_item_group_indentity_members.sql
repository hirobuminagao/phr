CREATE TABLE `dev_phr`.`exam_item_group_identity_members` (
  `group_code` varchar(64) NOT NULL COMMENT '健診項目グループコード',
  `identity_item_code` varchar(32) NOT NULL COMMENT '同一性項目コード',
  `required_flag` tinyint(1) NOT NULL DEFAULT 1 COMMENT '1=必須',
  `condition_expr` varchar(1024) DEFAULT NULL COMMENT '必須条件式（評価式）',
  `required_presence_namecodes` text COMMENT 'presence判定用のnamecode CSV（OR集合）',
  `presence_value_mode` varchar(32) DEFAULT NULL COMMENT 'presence判定モード（例: ANY_NONEMPTY）',
  `notes` varchar(1024) DEFAULT NULL COMMENT '備考',
  `sort_no` int DEFAULT NULL COMMENT '表示・評価順',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`group_code`, `identity_item_code`),
  KEY `idx_identity` (`identity_item_code`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目グループ: 同一性項目コードメンバー';
