CREATE TABLE `medi_lsio_identity_presence` (
  `xml_sha256` char(64)
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `group_code` varchar(64)
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `identity_item_code` varchar(32)
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `present_flag` tinyint(1) NOT NULL COMMENT '1=present',
  PRIMARY KEY (`xml_sha256`,`group_code`,`identity_item_code`),
  KEY `idx_identity` (`group_code`,`identity_item_code`),
  KEY `idx_xml` (`xml_sha256`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs
  COMMENT='【medi】LSIO: identity presence（中間生成・再生成可）';
