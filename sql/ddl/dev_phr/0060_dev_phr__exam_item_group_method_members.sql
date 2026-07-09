CREATE TABLE `dev_phr`.`exam_item_group_method_members` (
  `group_code` varchar(64) NOT NULL COMMENT '健診項目グループコード',
  `xml_method_code` varchar(10) NOT NULL COMMENT 'XML検査種別コード（method）',
  `role` enum('PRESENCE_KEY','RESULT_VALUE','AUX') NOT NULL DEFAULT 'PRESENCE_KEY' COMMENT 'グループ内の役割',
  `priority` int NOT NULL DEFAULT 100 COMMENT '評価・表示優先度',
  `presence_value_mode` varchar(32) DEFAULT NULL COMMENT '存在判定方式',
  `required_flag` tinyint(1) DEFAULT NULL COMMENT '制度上の必須・任意判定',
  `rule_code` varchar(64) DEFAULT NULL COMMENT '共通Rule/Calculateライブラリ識別子',
  `rule_source_identity_codes` varchar(255) DEFAULT NULL COMMENT 'Ruleが参照するidentity_code一覧',
  `rule_source_method_codes` varchar(255) DEFAULT NULL COMMENT 'Ruleが参照するmethod_code一覧',
  `rule_source_namecodes` text NULL COMMENT 'Ruleが参照するnamecode一覧',
  `is_active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '1=有効, 0=無効',
  `notes` varchar(1024) DEFAULT NULL COMMENT '備考',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`group_code`, `xml_method_code`),
  KEY `idx_method_members_method` (`xml_method_code`),

  CONSTRAINT `chk_method_code_not_blank`
    CHECK ((`xml_method_code` <> _utf8mb4''))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目グループ所属（method基準）';
