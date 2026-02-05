CREATE TABLE `dev_phr`.`exam_item_group_method_members` (
  `group_code` varchar(64) NOT NULL COMMENT '健診項目グループコード',
  `xml_method_code` varchar(10) NOT NULL COMMENT 'XML検査種別コード（method）',
  `role` enum('PRESENCE_KEY','RESULT_VALUE','AUX') NOT NULL DEFAULT 'PRESENCE_KEY' COMMENT 'グループ内の役割',
  `priority` int NOT NULL DEFAULT 100 COMMENT '評価・表示優先度',
  `notes` varchar(1024) DEFAULT NULL COMMENT '備考',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`group_code`, `xml_method_code`),
  KEY `idx_method_members_method` (`xml_method_code`),

  CONSTRAINT `chk_method_code_not_blank`
    CHECK ((`xml_method_code` <> _utf8mb4''))
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目グループ所属（method基準）';
