-- ============================================================
-- CURRENT: shg_result 定義（raw基準へ再設計）
--
-- 旧定義について:
-- person_id_custom 基準で設計されていた旧定義は
-- sql/ddl/work_other/_archive/0023_work_other_shg_result.sql
-- に退避済み。
--
-- 設計方針:
-- - 本テーブルは、健保由来の健診基準情報を保持し、
--   HIA から取得した特定保健指導結果XMLとの突合に使用する。
-- - raw を先に保持し、後段で person_id_custom / identity_hash を生成する。
-- - match 系列は本フェーズでは保持しない。
-- ============================================================
CREATE TABLE `shg_result` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'PK',

  `insurer_number_raw` varchar(8)
    NOT NULL COMMENT '保険者番号 raw',
  `insurance_symbol_raw` varchar(32)
    NOT NULL COMMENT '保険証記号 raw',
  `insurance_number_raw` varchar(32)
    NOT NULL COMMENT '保険証番号 raw',
  `name_kana_full_raw` varchar(128)
    NOT NULL COMMENT '氏名カナ raw（フル）',

  `birthdate` date
    NOT NULL COMMENT '生年月日（正規形）',
  `gender_code` varchar(1)
    NOT NULL COMMENT '性別コード（正規形）',

  `shg_year` int
    NOT NULL COMMENT '特定保健指導対象実施年度',
  `usage_ticket_number` varchar(64)
    DEFAULT NULL COMMENT '利用券番号',
  `expiration_date` date
    DEFAULT NULL COMMENT '利用券有効期限',
  `health_checkup_date` date
    DEFAULT NULL COMMENT '健診実施日',
  `exam_waist_cm` decimal(5,1)
    DEFAULT NULL COMMENT '健診時腹囲(cm)',
  `exam_weight_kg` decimal(5,1)
    DEFAULT NULL COMMENT '健診時体重(kg)',
  `received_date` date
    NOT NULL COMMENT 'データ受取日（YYYY-MM-DD）',

  `person_id_custom` varchar(190)
    CHARACTER SET ascii COLLATE ascii_bin
    DEFAULT NULL COMMENT 'rawから生成した person_id_custom',
  `identity_hash` varchar(64)
    CHARACTER SET ascii COLLATE ascii_bin
    DEFAULT NULL COMMENT 'rawから生成した identity_hash（固定長）',

  `created_at` datetime
    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '作成日時',
  `updated_at` datetime
    NOT NULL DEFAULT CURRENT_TIMESTAMP
    ON UPDATE CURRENT_TIMESTAMP COMMENT '更新日時',

  PRIMARY KEY (`id`),
  KEY `idx_shg_person_id_custom` (`person_id_custom`),
  KEY `idx_shg_identity_hash` (`identity_hash`),
  KEY `idx_shg_year` (`shg_year`),
  KEY `idx_shg_usage_ticket_number` (`usage_ticket_number`),
  KEY `idx_shg_received_date` (`received_date`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_ja_0900_as_cs
  COMMENT='【shg】特定保健指導結果XML突合用の健診基準テーブル（raw基準）';
