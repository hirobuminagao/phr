CREATE TABLE `medi_lsio_missing_items` (
  `id` bigint NOT NULL AUTO_INCREMENT,

  `xml_sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `group_code` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `identity_item_code` varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,

  `required_flag` tinyint(1) NOT NULL,
  `missing_flag` tinyint(1) NOT NULL,

  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

  PRIMARY KEY (`id`),

  UNIQUE KEY `uq_xml_group_ident` (`xml_sha256`,`group_code`,`identity_item_code`),
  KEY `idx_xml` (`xml_sha256`),
  KEY `idx_group` (`group_code`),
  KEY `idx_ident` (`identity_item_code`)
) ENGINE=InnoDB
  AUTO_INCREMENT=3606408
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs
  COMMENT='【medi】労基（法定）健診の必要項目：XMLごとの欠損（縦持ち）';
