# Subscriber Apply Specification

## v1.1.0 改修ポイント

本仕様は v1.1.0 にて以下の問題を解消するために改修された。

### 背景

従来の処理では:

- HIA 由来データで `name_kana_given` に `name_kana_full` をそのまま格納していた
- その結果、`subscribers` の parts 列が「空欄ではない」と判定されてしまい
- 後続の健保CSV（fund）由来の正しい分割値が **上書きされない問題** が発生

さらに:

- `NULL` 判定のみで「未設定」を判定していたため
- 空文字（''）が実質的に「値あり」と扱われ、更新ロジックが阻害されていた

### 対応内容

v1.1.0 では以下を統一ルールとした:

- parts 列は **分割できた場合のみ格納する**
- 分割不能な場合は **NULL のまま保持する**
- 未設定判定は `NULL または空文字` とする
- identity 生成は parts に依存せず full ベースで行う

### 効果

- 健保CSV由来の高精度な split 情報が `subscribers` に正しく反映される
- HIA由来データとの整合性が崩れない
- 将来的なデータ補完・再処理が安全に行える

---

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

## identity_hash / person_id_custom 生成時の parts 依存禁止

HIA 由来 apply において、identity 系の生成・照合は `name_kana_family` / `name_kana_given` などの
parts 列を前提にしてはならない。

理由:

- HIA 由来データでは parts が未設定のケースがある
- parts が未設定でも `name_kana_full` は保持される
- parts に依存すると、同一人物判定が上流データの表現揺れに引っ張られる

したがって、HIA apply における identity 系生成・照合は、
**full ベースの値と既定の identity ルールに従って行う**。

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

## Name Parts Policy (HIA由来)

HIA 由来の加入者データについては、`name_kana_family` / `name_kana_middle` / `name_kana_given` を
**分割可能な場合のみ** `subscribers` に格納する。

分割不能な場合は以下とする。

```text
name_kana_full は格納する
name_kana_family / middle / given は格納しない（NULL のまま）
```

つまり、

- 「分割できないので `name_kana_given` に full を入れる」
- 「parts が不明なのに given だけ埋める」

という運用は行わない。

これは、後続でより正確な split 情報（例: 健保受領CSV由来）が得られたときに、
`subscribers` の parts 列を矛盾なく補完可能にするためである。

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

## 空欄判定ポリシー

本仕様における「値あり」「値なし」の判定は以下とする。

- 値あり: `NULL` ではない **かつ** 空文字ではない
- 値なし: `NULL` または空文字

したがって、parts 補完や差分判定においては、単に `IS NOT NULL` だけでは不十分であり、
**空文字も未設定として扱う**。

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

# 10. HIA由来 parts 列の補正方針

既存データで、HIA 由来の `name_kana_family` / `name_kana_middle` / `name_kana_given` に
不正な暫定値が入っている場合は、以下の補正を行う。

対象:

- 真の split 値ではなく、暫定的に full 相当値を parts 列へ入れてしまったデータ

方針:

- `name_kana_family` は `NULL` に補正する
- 必要に応じて `name_kana_middle` / `name_kana_given` も同様に見直す
- 補正は Navicat または SQL により実施可能とする

目的:

- HIA 由来 parts 列を「真に分割できた値のみ保持する」状態へ戻す
- 後続の fund / 健保受領CSV 由来の split 補完を正しく適用可能にする

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
