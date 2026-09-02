-- Add the official Annex 2 author relationship to the execution schema.
ALTER TABLE m4_dev_phr.exam_item_master
  ADD COLUMN annex2_author_item_code char(17) DEFAULT NULL
    COMMENT '付属2: author要素で表現する診断者・記述者の項目コード'
    AFTER annex2_series_group_relation_code,
  ADD KEY idx_exam_item_author_item (annex2_author_item_code);

-- Apply sql/seed/m4_dev_phr/0001_m4_dev_phr__exam_item_master_annex2_authors.sql
-- after this migration.
