CREATE TABLE `dev_phr`.`exam_item_master` (
  `namecode` char(17) NOT NULL COMMENT '健診項目namecode（付属2）',
  `item_name` varchar(190) NOT NULL COMMENT '項目名（日本語）',
  `xml_value_type` enum('PQ','CD','CO','ST') DEFAULT NULL COMMENT 'XML value type',
  `item_code_oid` varchar(190) DEFAULT NULL COMMENT 'MHLW item code OID',
  `result_code_oid` varchar(190) DEFAULT NULL COMMENT '結果のコード体系OID',
  `display_unit` varchar(190) DEFAULT NULL COMMENT '表示単位',
  `ucum_unit` varchar(190) DEFAULT NULL COMMENT 'UCUM単位',
  `method_name` varchar(190) DEFAULT NULL COMMENT '測定方法名',
  `category_name` varchar(190) DEFAULT NULL COMMENT 'カテゴリ（例：質問票・血液検査）',
  `data_type_label` varchar(190) DEFAULT NULL COMMENT 'データ型ラベル（例：コード・数字）',
  `xml_method_code` varchar(190) DEFAULT NULL COMMENT 'XMLのmethodCode',
  `xpath_template` varchar(1024) DEFAULT NULL COMMENT '値取得用XPathテンプレ',
  `value_method` varchar(190) DEFAULT NULL COMMENT '値取得方法（@code,text(),etc）',
  `nullflavor_allowed` tinyint(1) DEFAULT NULL COMMENT '0=不可 / 1=許可',
  `importance` varchar(190) DEFAULT NULL COMMENT '重要度ラベル',
  `importance_group` varchar(190) DEFAULT NULL COMMENT '重要度グルーピング',
  `notes` varchar(1024) DEFAULT NULL COMMENT '備考',
  `update_type` varchar(190) DEFAULT NULL COMMENT '更新種別（更新 / 変更なし 等）',
  `update_reason` varchar(1024) DEFAULT NULL COMMENT '更新理由（差分内容など）',
  `source_last_updated` date DEFAULT NULL COMMENT '厚労省元データの更新日',
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '登録日時',
  `updated_at` datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '更新日時',
  `kubun_no` int DEFAULT NULL COMMENT '付属2: 区分番号',
  `kubun_name` varchar(190) DEFAULT NULL COMMENT '付属2: 区分名称',
  `jun_no` int DEFAULT NULL COMMENT '付属2: 順番号',
  `identity_item_code` varchar(32) DEFAULT NULL COMMENT '付属2: 同一性項目コード',
  `identity_item_name` varchar(190) DEFAULT NULL COMMENT '付属2: 同一性項目名称',

  PRIMARY KEY (`namecode`),
  KEY `idx_exam_item_category` (`category_name`),
  KEY `idx_exam_item_xml_value_type` (`xml_value_type`),
  KEY `idx_exam_item_result_oid` (`result_code_oid`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_ja_0900_as_cs
COMMENT='健診項目マスタ（厚労省形式）';
