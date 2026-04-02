-- ============================================================
-- person_event 初期対象者抽出クエリ
--
-- 目的:
-- event のルールに基づき、対象となる加入者を抽出する
--
-- 前提:
-- ・event_id を指定して実行する
-- ・subscribers を基に判定
-- ・資格判定は eligibility_reference_date を使用
-- ・除外テーブル subscribers_exclusions を考慮
--
-- 注意:
-- ・これは SELECT（確認用）
-- ・問題なければ INSERT に転用する
-- ============================================================

SELECT
    e.event_id,
    s.id AS subscriber_id,
    s.person_id_custom,
    s.identity_hash,

    -- デバッグ確認用
    s.insurer_number,
    s.qualification_acquired_date,
    s.qualification_lost_date,
    e.eligibility_reference_date

FROM dev_phr.subscribers s
JOIN dev_phr.event e
  ON s.insurer_number COLLATE utf8mb4_unicode_ci = e.insurer_number

WHERE
    -- 対象イベント　ここを手動で変更
    e.event_id = 1

    -- 資格判定（基準日時点で有効）
    AND s.qualification_acquired_date <= e.eligibility_reference_date
    AND (
        s.qualification_lost_date IS NULL
        OR s.qualification_lost_date >= e.eligibility_reference_date
    )

    -- 除外対象
    AND NOT EXISTS (
        SELECT 1
        FROM dev_phr.subscribers_exclusions ex
        WHERE ex.subscriber_id = s.id
    )

ORDER BY s.id;
