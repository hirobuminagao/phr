CREATE TABLE `dev_phr`.`exam_item_group_members` (
  `group_code` varchar(64) NOT NULL COMMENT '健診項目グループコード',
  `namecode` char(17) NOT NULL COMMENT '健診項目namecode',
  `role` enum('PRESENCE_KEY','RESULT_VALUE','AUX') NOT NULL COMMENT 'グループ内の役割',
  `priority` int NOT NULL DEFAULT 100 COMMENT '評価・表示優先度',
  `notes` varchar(1024) DEFAULT NULL COMMENT '備考',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`group_code`, `namecode`),
  KEY `idx_members_namecode` (`namecode`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目グループ所属';
