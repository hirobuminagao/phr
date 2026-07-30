-- Existing environments: add the official Annex 2 series-group metadata.
ALTER TABLE dev_phr.exam_item_master
  ADD COLUMN annex2_series_group_identifier char(17) DEFAULT NULL
    COMMENT '付属2: 一連検査グループ識別'
    AFTER cda_section_code_default,
  ADD COLUMN annex2_series_group_relation_code varchar(16) DEFAULT NULL
    COMMENT '付属2: 一連検査グループ関係コード（COMP / RSON）'
    AFTER annex2_series_group_identifier,
  ADD KEY idx_exam_item_series_group (annex2_series_group_identifier);

-- Apply sql/seed/dev_phr/0006_dev_phr__exam_item_master_annex2_series_groups.sql
-- after this migration. Keeping the seed separate makes GUI execution and
-- schema-name substitution for local M4 environments explicit.
