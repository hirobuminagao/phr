-- 20260323_005_work_other_hia_xml_events_latest_snapshot_model.sql
--
-- Purpose:
-- - hia_xml_events を ADR-0013 の月次最新スナップショットモデルへ寄せる
-- - xml_filename 依存の一意制約を廃止する
-- - facility_code の NULL を空文字へ正規化する
-- - is_deleted を追加する
-- - updated_at を追加する
-- - 一意キーを (person_year_id, zip_id, exam_date, facility_code) に変更する

START TRANSACTION;

-- ------------------------------------------------------------
-- 1) facility_code NULL → '' 正規化
-- ------------------------------------------------------------
UPDATE work_other.hia_xml_events
SET facility_code = ''
WHERE facility_code IS NULL;

-- ------------------------------------------------------------
-- 2) is_deleted 追加
-- ------------------------------------------------------------
ALTER TABLE work_other.hia_xml_events
  ADD COLUMN is_deleted TINYINT(1) NOT NULL DEFAULT 0
  COMMENT '0=active,1=deleted(latest zip snapshot missing)'
  AFTER xml_sha256;

-- ------------------------------------------------------------
-- 3) facility_code を NOT NULL + DEFAULT '' に変更
-- ------------------------------------------------------------
ALTER TABLE work_other.hia_xml_events
  MODIFY COLUMN facility_code VARCHAR(32)
  CHARACTER SET ascii
  COLLATE ascii_bin
  NOT NULL DEFAULT '';

-- ------------------------------------------------------------
-- 4) updated_at 追加
-- ------------------------------------------------------------
ALTER TABLE work_other.hia_xml_events
  ADD COLUMN updated_at DATETIME NOT NULL
  DEFAULT CURRENT_TIMESTAMP
  ON UPDATE CURRENT_TIMESTAMP
  AFTER created_at;

-- ------------------------------------------------------------
-- 5) 旧 unique key を削除
-- ------------------------------------------------------------
ALTER TABLE work_other.hia_xml_events
  DROP INDEX uq_hia_xml_unique;

-- ------------------------------------------------------------
-- 6) 新 unique key を追加
-- ------------------------------------------------------------
ALTER TABLE work_other.hia_xml_events
  ADD UNIQUE KEY uq_hia_xml_event_latest (
    person_year_id,
    zip_id,
    exam_date,
    facility_code
  );

-- ------------------------------------------------------------
-- 7) is_deleted 用 index 追加（再集計・絞り込み用）
-- ------------------------------------------------------------
ALTER TABLE work_other.hia_xml_events
  ADD KEY idx_hia_xml_events_person_year_deleted (
    person_year_id,
    is_deleted
  );

COMMIT;