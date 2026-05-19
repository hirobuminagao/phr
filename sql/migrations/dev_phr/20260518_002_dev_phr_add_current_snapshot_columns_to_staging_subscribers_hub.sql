

ALTER TABLE staging_subscribers_hub
  ADD COLUMN current_subscriber_id bigint unsigned
    DEFAULT NULL
    COMMENT 'current snapshot subscribers.id'
    AFTER identity_hash,

  ADD COLUMN current_identity_hash char(64)
    CHARACTER SET ascii
    COLLATE ascii_bin
    DEFAULT NULL
    COMMENT 'current snapshot identity_hash'
    AFTER current_subscriber_id,

  ADD COLUMN current_name_kana_full_match varchar(190)
    DEFAULT NULL
    COMMENT 'current snapshot 氏名カナ全文match値'
    AFTER current_identity_hash,

  ADD COLUMN current_address_id bigint unsigned
    DEFAULT NULL
    COMMENT 'current snapshot subscriber_addresses.id'
    AFTER current_name_kana_full_match,

  ADD COLUMN current_contact_id bigint unsigned
    DEFAULT NULL
    COMMENT 'current snapshot subscriber_contacts.id'
    AFTER current_address_id,

  ADD COLUMN current_lookup_status varchar(50)
    DEFAULT NULL
    COMMENT 'current snapshot lookup status'
    AFTER current_contact_id,

  ADD COLUMN current_lookup_checked_at datetime(3)
    DEFAULT NULL
    COMMENT 'current snapshot lookup checked at'
    AFTER current_lookup_status;

ALTER TABLE staging_subscribers_hub
  DROP KEY idx_stghub_apply_subscriber_id,
  ADD KEY idx_stghub_current_subscriber_id (current_subscriber_id),
  ADD KEY idx_stghub_current_lookup_status (current_lookup_status);

ALTER TABLE staging_subscribers_hub
  DROP COLUMN apply_subscriber_id;