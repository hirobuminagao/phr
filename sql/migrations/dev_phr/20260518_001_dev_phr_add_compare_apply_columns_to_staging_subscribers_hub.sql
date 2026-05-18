ALTER TABLE staging_subscribers_hub
  ADD COLUMN identity_hash char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    DEFAULT NULL
    COMMENT 'compare / join 用 identity hash'
    AFTER person_id_custom,

  ADD COLUMN name_kana_full_match varchar(190)
    DEFAULT NULL
    COMMENT '氏名カナ全文match値'
    AFTER name_kana_full,

  ADD COLUMN name_kanji_full_match varchar(190)
    DEFAULT NULL
    COMMENT '氏名漢字全文match値'
    AFTER name_kanji_full,

  ADD COLUMN apply_subscriber_id bigint unsigned
    DEFAULT NULL
    COMMENT 'apply対象 subscribers.id'
    AFTER connect_id,

  ADD COLUMN apply_action varchar(50)
    DEFAULT NULL
    COMMENT 'insert/update/noop/review'
    AFTER apply_subscriber_id,

  ADD COLUMN apply_diff_columns json
    DEFAULT NULL
    COMMENT '差分列一覧'
    AFTER apply_action,

  ADD COLUMN identity_match_status varchar(50)
    DEFAULT NULL
    COMMENT 'identity compare結果'
    AFTER apply_diff_columns,

  ADD COLUMN address_diff_status varchar(50)
    DEFAULT NULL
    COMMENT 'address compare結果'
    AFTER identity_match_status,

  ADD COLUMN contact_diff_status varchar(50)
    DEFAULT NULL
    COMMENT 'contact compare結果'
    AFTER address_diff_status,

  ADD COLUMN apply_checked_at datetime(3)
    DEFAULT NULL
    COMMENT 'prepare/compare 実行時刻'
    AFTER contact_diff_status;

ALTER TABLE staging_subscribers_hub
  ADD KEY idx_stghub_identity_hash (identity_hash),
  ADD KEY idx_stghub_hia_subscriber_id (hia_subscriber_id),
  ADD KEY idx_stghub_apply_action (apply_action),
  ADD KEY idx_stghub_apply_subscriber_id (apply_subscriber_id);

ALTER TABLE staging_subscribers_hub
  DROP KEY idx_stghub_pending_apply,
  ADD KEY idx_stghub_pending_apply (
    processed_run_id,
    apply_action,
    insurer_number,
    id
  );
