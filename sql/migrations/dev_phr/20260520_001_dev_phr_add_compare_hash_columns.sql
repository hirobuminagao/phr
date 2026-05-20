

-- ============================================================
-- Migration:
--   add compare hash columns
--
-- Purpose:
--   compare hash 系カラムを追加する。
--
-- Tables:
--   - staging_subscribers_hub
--   - subscribers
--   - subscriber_addresses
-- ============================================================

ALTER TABLE staging_subscribers_hub
    MODIFY COLUMN identity_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT 'subscriber resolve / join 用 identity hash',

    ADD COLUMN compare_identity_norm_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT 'identity登録値差分検知用 compare hash'
        AFTER identity_hash,

    ADD COLUMN compare_other_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT 'identity以外のsubscriber属性差分検知用 compare hash'
        AFTER compare_identity_norm_hash,

    ADD COLUMN address_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT '住所値差分検知用 compare hash'
        AFTER building,

    ADD KEY idx_stghub_run_compare_hashes (
        import_run_id,
        compare_identity_norm_hash,
        compare_other_hash
    ),

    ADD KEY idx_stghub_run_address_hash (
        import_run_id,
        address_hash
    );


ALTER TABLE subscribers
    MODIFY COLUMN identity_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT 'subscriber resolve / join 用 identity hash（person_id_custom + name_kana_full_match + gender_code）',

    ADD COLUMN compare_identity_norm_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT 'identity登録値差分検知用 compare hash。apply時はstaging_subscribers_hubから反映。対象値更新時は再生成必須'
        AFTER identity_hash,

    ADD COLUMN compare_other_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT 'identity以外のsubscriber属性差分検知用 compare hash。apply時はstaging_subscribers_hubから反映。対象値更新時は再生成必須'
        AFTER compare_identity_norm_hash,

    ADD KEY idx_subscribers_compare_identity_norm_hash (
        compare_identity_norm_hash
    ),

    ADD KEY idx_subscribers_compare_other_hash (
        compare_other_hash
    );


ALTER TABLE subscriber_addresses
    ADD COLUMN address_hash
        char(64)
        CHARACTER SET ascii
        COLLATE ascii_bin
        DEFAULT NULL
        COMMENT '住所値差分検知用 compare hash。apply時はstaging_subscribers_hubから反映。対象値更新時は再生成必須'
        AFTER building,

    ADD KEY idx_addresses_address_hash (
        address_hash
    ),

    ADD KEY idx_addresses_subscriber_address_hash (
        subscriber_id,
        address_hash
    );