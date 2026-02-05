CREATE TABLE `dev_phr`.`exam_item_groups` (
  `group_code` varchar(64) NOT NULL COMMENT '健診項目グループコード',
  `group_name` varchar(190) NOT NULL COMMENT '健診項目グループ名',
  `description` varchar(1024) DEFAULT NULL COMMENT '説明',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`group_code`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目グループ（法定/特定健診など）';
