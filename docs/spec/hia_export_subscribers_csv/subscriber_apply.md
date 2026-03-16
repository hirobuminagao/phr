# Subscriber Apply Specification

HIA export 加入者 CSV を staging から **PHR subscriber マスターへ反映する処理仕様**。

対象スクリプト:

```
apply_subscribers_from_staging_hub.py
```

この処理は **staging → subscriber master** の反映エンジンであり、
加入者の名寄せ・更新・履歴管理を行う。

---

# 1. Apply Phase Overview

処理対象:

```
staging_subscribers_hub
WHERE processed_run_id IS NULL
```

フロー:

```
staging_subscribers_hub
      │
      │ identity match
      ▼
subscribers
      │
      ├ address history
      │
      ├ contact history
      │
      └ subscriber_audit
```

成功した staging 行には

```
processed_run_id
processed_at
```

を刻印する。

---

# 2. Subscriber Identity

同一 subscriber 判定キー:

```
(person_id_custom,
 name_kana_full_match,
 gender_code)
```

理由:

| column | role |
|------|------|
| person_id_custom | 保険証 + 生年月日ベースID |
| name_kana_full_match | 正規化・空白吸収後のカナ照合キー |
| gender_code | 同名異人対策 |

SQL:

```
SELECT *
FROM subscribers
WHERE person_id_custom = ?
AND name_kana_full_match = ?
AND gender_code IS ?
LIMIT 1
```

---

# 3. Subscriber Insert

既存 subscriber が存在しない場合:

```
INSERT subscribers
```

登録列:

```
insurer_number
insurance_symbol
insurance_symbol_digits
insurance_number
insurance_branchnumber
birth
gender_code

name_kana_full
name_kanji_full

name_kanji_family
name_kanji_middle
name_kanji_given

name_kana_family
name_kana_middle
name_kana_given

relationship_name

qualification_acquired_date
qualification_lost_date

employer_code
department_code
distribution_code
employee_code
connect_id

person_id_custom
```

メタ列:

```
created_at
last_change_run_id
```

---

# 4. Subscriber Update

既存 subscriber が存在する場合:

```
差分比較
```

対象カラム:

```
COMPARE_COLS
```

差分あり:

```
UPDATE subscribers
SET ...
updated_at = now()
last_change_run_id = run_id
```

差分なし:

```
noop
```

ただし住所・連絡先は別途差分判定を行う。

---

# 5. Address History

テーブル:

```
subscriber_addresses
```

ポリシー:

```
現用1件のみ
```

条件:

```
is_current = 1
```

フロー:

```
現在住所取得
↓
差分比較
↓
差分あり
   ↓
旧住所
  is_current = 0
  valid_to = now()

新住所
  INSERT
  is_current = 1
```

## Address Constraint Policy

`subscriber_addresses` は履歴・属性テーブルとしての柔軟性を優先する。

そのため `subscriber_contacts` と異なり、
`subscriber_addresses.subscriber_id` には外部キー制約を設定しない。

親子整合性は

```text
apply_subscribers_from_staging_hub.py
```

によって担保する。

この設計により、

- 住所履歴の補修
- 移行時の段階的データ投入
- 不完全データの一時保持

を柔軟に扱える。

---

# 6. Contact History

テーブル:

```
subscriber_contacts
```

ポリシー:

```
現用1件のみ
```

差分あり:

```
旧連絡先
  is_current = 0
  valid_to = now()

新連絡先
  INSERT
  is_current = 1
```

対象:

```
phone
email
```

---

# 7. Subscriber Audit

テーブル:

```
subscriber_audit
```

生成タイミング:

```
subscriber insert
subscriber update
```

実装方式:

```
Python apply script
```

理由:

- run_id を保持できる
- ETL context を保持できる
- DB trigger 依存を避ける

参照:

```
ADR-0010
subscriber-audit-implementation
```

---

# 8. Processed Mark

apply 成功後:

```
UPDATE staging_subscribers_hub
SET
  processed_run_id = run_id
  processed_at = now()
```

これにより staging は

```
未処理キュー
```

として機能する。

---

# 9. Run Management

実行は以下のテーブルで管理する:

```
etl_runs
etl_errors
```

保存情報:

```
run_id
phase
status
rows_inserted
rows_updated
errors
started_at
finished_at
```

行レベルエラー:

```
etl_errors
```

---

# Summary

処理全体:

```
staging_subscribers_hub
      │
      ▼
subscriber identity match
      │
      ├ insert
      │
      ├ update
      │
      └ noop
      │
      ▼
address history
      │
      ▼
contact history
      │
      ▼
subscriber audit
      │
      ▼
processed mark
```
