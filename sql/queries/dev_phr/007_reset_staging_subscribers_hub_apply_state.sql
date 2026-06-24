

-- ============================================================
-- Reset apply-related staging state for a specific import_run_id.
--
-- Usage:
--   SET @import_run_id = 188;
--   SOURCE 007_reset_staging_subscribers_hub_apply_state.sql;
-- ============================================================

SET @import_run_id = 188;

UPDATE staging_subscribers_hub
SET
    apply_action = NULL,
    apply_diff_columns = NULL,
    identity_match_status = NULL,

    address_diff_status = NULL,
    address_target_address_id = NULL,

    contact_point_diff_status = NULL,

    phone_diff_status = NULL,
    phone_target_contact_point_id = NULL,

    email_diff_status = NULL,
    email_target_contact_point_id = NULL,

    apply_checked_at = NULL,

    processed_run_id = NULL,
    processed_at = NULL,

    apply_error_code = NULL,
    apply_error_message = NULL,
    apply_error_at = NULL
WHERE import_run_id = @import_run_id;

SELECT
    import_run_id,
    COUNT(*) AS reset_rows
FROM staging_subscribers_hub
WHERE import_run_id = @import_run_id
GROUP BY import_run_id;