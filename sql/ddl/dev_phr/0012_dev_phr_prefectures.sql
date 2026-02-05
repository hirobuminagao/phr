CREATE TABLE `dev_phr`.`prefectures` (
  `code` int NOT NULL COMMENT '都道府県コード',
  `jis_code` char(2) NOT NULL COMMENT 'JISコード',
  `name_ja` varchar(190) NOT NULL COMMENT '都道府県名（日本語）',

  PRIMARY KEY (`code`),
  UNIQUE KEY `uq_prefectures_jis_code` (`jis_code`),
  UNIQUE KEY `uq_prefectures_name_ja` (`name_ja`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs;
