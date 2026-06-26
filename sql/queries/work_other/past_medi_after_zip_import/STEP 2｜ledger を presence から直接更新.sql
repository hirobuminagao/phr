/* =========================================================
   LSIO_FULL_01_02.sql  (2026-01-28)
   目的:
     STEP1: medi_lsio_identity_presence を最新化（present=1のみ）
     STEP2: presence を集約して medi_xml_ledger の lsio_legal_* に反映
   方針:
     - missing_items（全組み合わせ縦持ち）は作らない（重いので）
     - missing_methods は別スクリプト/別SQLで“必要な人だけ”後段更新
   前提:
     - work_other.medi_xml_item_values に (xml_sha256, namecode, value...) が入っている
     - dev_phr.exam_item_group_identity_members が最新
     - dev_phr.exam_item_master が最新（identity→namecode対応）
   ========================================================= */

/* -------------------------
   0) 変数
   ------------------------- */
SET @group_code = CAST('LSIO_Legal_Item' AS CHAR CHARACTER SET ascii);

/* 必須identity数（定数として使う） */
SELECT @req_cnt := COUNT(*)
FROM dev_phr.exam_item_group_identity_members
WHERE group_code = @group_code
  AND required_flag = 1;

SELECT @req_cnt AS required_identity_count;


/* =========================================================
   STEP1: presence を再構築
   - 「存在したidentity」だけを入れる（present_flag=1）
   - 既存分は group_code 単位で削除してから入れ直す（クリーン再生成）
   ========================================================= */

/* 1-0) presence クリア（LSIO分だけ） */
DELETE FROM work_other.medi_lsio_identity_presence
WHERE group_code = @group_code;

/* 1-1) ルールあり identity（required_presence_namecodes を使う） */
INSERT INTO work_other.medi_lsio_identity_presence
  (xml_sha256, group_code, identity_item_code, present_flag)
SELECT
  v.xml_sha256,
  @group_code AS group_code,
  g.identity_item_code,
  1 AS present_flag
FROM dev_phr.exam_item_group_identity_members g
JOIN work_other.medi_xml_item_values v
  ON FIND_IN_SET(v.namecode, g.required_presence_namecodes) > 0
WHERE g.group_code = @group_code
  AND g.required_flag = 1
  AND g.presence_value_mode = 'ANY_NONEMPTY'
GROUP BY v.xml_sha256, g.identity_item_code;

/* 1-2) ルールなし identity（identity→namecode の存在で判定） */
INSERT INTO work_other.medi_lsio_identity_presence
  (xml_sha256, group_code, identity_item_code, present_flag)
SELECT
  v.xml_sha256,
  @group_code AS group_code,
  g.identity_item_code,
  1 AS present_flag
FROM dev_phr.exam_item_group_identity_members g
JOIN dev_phr.exam_item_master m
  ON m.identity_item_code = g.identity_item_code
JOIN work_other.medi_xml_item_values v
  ON v.namecode = m.namecode
WHERE g.group_code = @group_code
  AND g.required_flag = 1
  AND g.required_presence_namecodes IS NULL
GROUP BY v.xml_sha256, g.identity_item_code
ON DUPLICATE KEY UPDATE
  present_flag = VALUES(present_flag);

/* 1-3) 確認（件数・人件数・平均present数） */
SELECT
  COUNT(*) AS presence_rows,
  COUNT(DISTINCT xml_sha256) AS xml_cnt
FROM work_other.medi_lsio_identity_presence
WHERE group_code = @group_code;

SELECT
  AVG(cnt) AS avg_present_per_xml,
  MIN(cnt) AS min_present_per_xml,
  MAX(cnt) AS max_present_per_xml
FROM (
  SELECT xml_sha256, COUNT(*) AS cnt
  FROM work_other.medi_lsio_identity_presence
  WHERE group_code = @group_code
  GROUP BY xml_sha256
) t;


/* =========================================================
   STEP2: ledger へ反映（軽量版）
   - present_cnt を presence から集約して更新
   - missing_methods はここでは作らない（重いので別工程）
   ========================================================= */

/* 2-1) present_cnt 集約（確認用） */
SELECT
  COUNT(*) AS xml_cnt_in_presence,
  AVG(present_cnt) AS avg_present_cnt
FROM (
  SELECT
    xml_sha256,
    COUNT(DISTINCT identity_item_code) AS present_cnt
  FROM work_other.medi_lsio_identity_presence
  WHERE group_code = @group_code
  GROUP BY xml_sha256
) s;

/* 2-2) ledger 更新
   注意:
     ON で結んでいる l.zip_inner_path_sha256 は
     「xml_sha256が入っている」前提。
     （あなたの運用ではここにxml_sha256を入れている前提で進める）
*/
UPDATE work_other.medi_xml_ledger l
LEFT JOIN (
  SELECT
    p.xml_sha256,
    COUNT(DISTINCT p.identity_item_code) AS present_cnt
  FROM work_other.medi_lsio_identity_presence p
  WHERE p.group_code = @group_code
  GROUP BY p.xml_sha256
) s
  ON s.xml_sha256 = l.zip_inner_path_sha256
SET
  l.lsio_legal_required_count = @req_cnt,
  l.lsio_legal_present_count  = COALESCE(s.present_cnt, 0),
  l.lsio_legal_is_complete    = (@req_cnt = COALESCE(s.present_cnt, 0)),
  l.lsio_legal_judged_run_id  = NULL,
  l.lsio_legal_judged_at      = NOW(6);

/* 2-3) 更新結果サマリ */
SELECT
  COUNT(*) AS ledger_cnt,
  SUM(l.lsio_legal_is_complete = 1) AS complete_cnt,
  SUM(l.lsio_legal_is_complete = 0) AS incomplete_cnt,
  AVG(l.lsio_legal_present_count) AS avg_present_cnt
FROM work_other.medi_xml_ledger l;


/* =========================================================
   参考: 欠損が少ない XML を見る（上位100）
   - ここで「再提出候補」を見つける
   ========================================================= */
SELECT
  l.xml_ledger_id,
  l.zip_name,
  l.zip_sha256,
  l.zip_inner_path,
  l.xml_filename,
  l.facility_folder_name,
  l.facility_code,
  l.facility_name,
  l.name_kana_full,
  l.name_kana_match,
  l.birth_date,
  l.insurance_symbol,
  l.insurance_number,
  l.lsio_legal_required_count,
  l.lsio_legal_present_count,
  (l.lsio_legal_required_count - l.lsio_legal_present_count) AS missing_cnt,
  l.lsio_legal_judged_at
FROM work_other.medi_xml_ledger l
WHERE l.lsio_legal_required_count > 0
ORDER BY missing_cnt ASC, l.xml_ledger_id ASC
LIMIT 100;
