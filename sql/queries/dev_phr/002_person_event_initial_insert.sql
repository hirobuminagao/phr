-- ============================================================
-- person_event 初期投入SQL
--
-- 目的:
-- event のルールに基づき、対象加入者を person_event に登録する
--
-- 前提:
-- ・event_id = 1 を対象
-- ・subscribers を基に対象者抽出
-- ・資格判定は eligibility_reference_date を使用
-- ・除外テーブル subscribers_exclusions を考慮
--
-- 注意:
-- ・001_person_event_target_select.sql で事前確認すること
-- ・初期投入用（重複実行に注意）
-- ============================================================

INSERT INTO dev_phr.person_event (
    event_id,
    subscriber_id,
    person_id_custom,
    identity_hash,
    is_eligible,
    result_received_flag,
    delivery_target_flag,
    delivery_exported_flag,
    gap_flag,
    last_observed_at
)
SELECT
    e.event_id,
    s.id AS subscriber_id,
    s.person_id_custom,
    s.identity_hash,
    1 AS is_eligible,
    0 AS result_received_flag,
    0 AS delivery_target_flag,
    0 AS delivery_exported_flag,
    0 AS gap_flag,
    NOW()

FROM dev_phr.subscribers s
JOIN dev_phr.event e
  ON s.insurer_number COLLATE utf8mb4_unicode_ci = e.insurer_number

WHERE
    -- 対象イベント
    e.event_id = 1

    -- 資格判定（加入基準のみ）
    AND s.qualification_acquired_date <= e.eligibility_reference_date

    -- 資格喪失判定（基準日時点で資格が残っていること）
    AND (
        s.qualification_lost_date IS NULL
        OR s.qualification_lost_date >= e.eligibility_reference_date
    )

    -- 除外対象
    AND NOT EXISTS (
        SELECT 1
        FROM dev_phr.subscribers_exclusions ex
        WHERE ex.subscriber_id = s.id
    );