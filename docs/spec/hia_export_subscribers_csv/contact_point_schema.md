# contact_point_schema.md

## Purpose

このドキュメントは、Hub apply orchestration における
`subscriber_contact_points` の設計方針を定義する。

現行 `subscriber_contacts` は:

```text
phone + email 同居型
```

であり、以下を安全に扱いにくい。

```text
- phoneのみ変更
- emailのみ変更
- null時の current解除
- 複数連絡先保持
- 過去連絡先への戻り
```

そのため Hub apply では:

```text
subscriber_contact_points
```

を連絡先の正本構造として扱う。

---

## Scope

対象:

```text
Hub apply orchestration
```

対象処理:

```text
scripts/hia/apply_hia_subscriber_sync.py
scripts/hia/script_lib/hub_subscriber_prepare.py
scripts/hia/script_lib/hub_subscriber_compare.py
scripts/hia/script_lib/hub_subscriber_apply.py
scripts/hia/script_lib/hub_subscriber_audit.py
```

対象外:

```text
staging_subscribers_fund
fund側 diff / apply
```

fund側は Hub apply 完走後に見直す。

---

# 1. Table Overview

新テーブル:

```text
subscriber_contact_points
```

役割:

```text
1レコード = 1連絡先
```

として保持する。

現行 `subscriber_contacts` は:

```text
subscriber_id
phone
email
is_current
```

であり、1行に phone / email が混在する。

新構造では:

```text
subscriber_id
contact_type
contact_value
```

へ分離する。

---

# 2. Expected Columns

| column | type | meaning |
|---|---|---|
| `contact_point_id` | BIGINT / INTEGER | contact point ID |
| `subscriber_id` | BIGINT / INTEGER | 対象 subscribers.id |
| `contact_type` | TEXT / VARCHAR | `phone` / `email` |
| `contact_value` | TEXT / VARCHAR | 連絡先値 |
| `is_current` | INTEGER / BOOLEAN | current flag |
| `valid_from` | DATETIME / TEXT | current開始日時 |
| `valid_to` | DATETIME / TEXT | current終了日時 |
| `source` | TEXT / VARCHAR | データ由来 |
| `created_at` | DATETIME / TEXT | 作成日時 |
| `updated_at` | DATETIME / TEXT | 更新日時 |

---

# 3. contact_type

初期対応:

```text
phone
email
```

将来的には:

```text
mobile
home_phone
work_phone
sub_email
emergency
LINE
```

などの拡張余地を残す。

---

# 4. current Semantics

contact point は履歴型テーブルとして扱う。

基本方針:

```text
current row は複数可
history row は複数保持
```

例:

```text
phone current
email current
```

は同時存在可能。

ただし:

```text
同一 contact_type の current は原則1件
```

を期待する。

つまり:

```text
phone current は原則1件
email current は原則1件
```

運用とする。

---

# 5. Null Semantics

HIA CSV における:

```text
phone = null
email = null
```

は:

```text
何もしない
```

ではない。

意味:

```text
HIA正本上、現在値なし
```

と扱う。

## phone null

```text
subscriber_id に紐づく
contact_type='phone'
AND is_current=1
```

を全て current 解除する。

## email null

```text
subscriber_id に紐づく
contact_type='email'
AND is_current=1
```

を全て current 解除する。

---

# 6. Compare Policy

現時点では:

```text
contact compare hash
```

は導入しない。

compare では、current snapshot で staging に保持した contact point id を起点に current 値を取得する。

```text
current_phone_contact_point_id
current_email_contact_point_id
```

理由:

```text
- staging に current連絡先値や履歴情報を持ちすぎない
- phone / email は contact_type ごとの履歴型テーブルで管理する
- current値比較と history 検索を分離する
- 過去連絡先への switch_current 判定が必要
```

## phone compare

```text
1. current_phone_contact_point_id から current phone 値を取得する
2. staging.phone と current phone 値を比較する
3. 同じなら noop
4. staging.phone が NULL で current phone が存在する場合は clear_current
5. staging.phone が current phone と異なる場合のみ、同一 subscriber_id + contact_type='phone' + contact_value で history 行を検索する
6. history 行があれば switch_current
7. history 行がなければ insert
8. 判定不能なら review
```

## email compare

email も `current_email_contact_point_id` を起点に同様に判定する。

## Compare Result Columns

contact point は、集約ステータスと contact_type 別ステータスを分けて staging に保持する。

```text
contact_point_diff_status
  contact point 全体の集約ステータス

phone_diff_status
phone_target_contact_point_id
  phone の処理種別と対象 contact_point_id

email_diff_status
email_target_contact_point_id
  email の処理種別と対象 contact_point_id
```

`contact_point_diff_status` は `noop` / `phone_only` / `email_only` / `both` / `review` の集約判定に利用する。

| status | 意味 |
|---|---|
| `noop` | phone / email ともに変更なし |
| `phone_only` | phone のみ apply 対象あり |
| `email_only` | email のみ apply 対象あり |
| `both` | phone / email の両方に apply 対象あり |
| `review` | phone / email のいずれかが自動判定不能 |

`phone_diff_status` / `email_diff_status` は apply 実処理に利用する。

| status | target_contact_point_id | 意味 |
|---|---|---|
| `noop` | current contact_point_id または NULL | 更新なし |
| `insert` | NULL | 新規 contact point を current として追加 |
| `switch_current` | history contact_point_id | 既存 history row を current に戻す |
| `clear_current` | current contact_point_id | current row を history 化する |
| `review` | NULL | 自動更新しない |

---

# 7. Apply Policy

apply は `contact_point_diff_status` を集約フラグとして確認し、実処理は contact_type 別の status / target id に従う。

```text
contact_point_diff_status
  noop / phone_only / email_only / both / review

phone_diff_status
phone_target_contact_point_id

email_diff_status
email_target_contact_point_id
```

apply phase は contact point の current 判定を再実行しない。

## noop

```text
phone_diff_status = noop
email_diff_status = noop
```

該当 contact_type は何もしない。

## switch_current

```text
switch_current
```

```text
旧current
  is_current = 0

existing history row
  is_current = 1
```

## insert

```text
insert
```

```text
旧current
  is_current = 0

新row insert
  is_current = 1
```

## clear_current

```text
current row
  is_current = 0
```

HIA CSV 上で phone / email が NULL の場合、該当 contact_type の current を解除する。

---

# 8. Legacy Backfill

旧テーブル:

```text
subscriber_contacts
```

から backfill を行う。

## backfill flow

```text
subscriber_contacts
  ↓
phone が空でなければ
subscriber_contact_points(contact_type='phone')
  ↓
email が空でなければ
subscriber_contact_points(contact_type='email')
```

旧1行は最大2行へ分解される。

```text
旧:
subscriber_id + phone + email + is_current

新:
subscriber_id + contact_type='phone' + contact_value + is_current
subscriber_id + contact_type='email' + contact_value + is_current
```

`subscriber_contacts` は:

```text
legacy / backfill source / temporary reference
```

として扱う。

---

# 9. Migration Order

実装順:

```text
1. subscriber_contact_points DDL
2. migration適用
3. backfill script 作成
4. subscriber_contacts → contact_points backfill
5. Hub current projection 差し替え
6. Hub compare/apply を contact point 前提へ変更
7. Hub apply orchestration 完走
8. 最後に fund側見直し
```

理由:

```text
Hub apply を先に完走させるため。
fund側を同時に触ると、contact再設計とdiff設計が並行して中途半端になるため。
```

---

# 10. Audit / History Policy

contact point は履歴型テーブルとして保持する。

履歴管理は:

```text
is_current
valid_from
valid_to
```

により行う。

例:

```text
旧電話番号
  is_current = 0
  valid_to = 更新日時

新電話番号
  is_current = 1
  valid_from = 更新日時
```

contact point の変更は、履歴行だけでなく audit としても記録する。

現時点では専用の:

```text
subscriber_contact_point_audit
```

は作成しない。

contact point の変更履歴は:

```text
subscriber_audit
```

へ記帳する。

記帳対象:

```text
- phone current 変更
- email current 変更
- current解除
- history row からの switch_current
- 新規 contact point insert
```

記帳方針:

```text
contact_type 単位で old/current と new/current を記録する。

例:
phone: 旧current値 → 新current値
email: 旧current値 → 新current値
phone: 旧current値 → NULL
email: 旧current値 → NULL
```

したがって Hub apply では:

```text
contact point
  → 履歴あり
  → subscriber_audit あり
```

として扱う。

将来的に専用の:

```text
subscriber_contact_point_audit
```

を作成する場合は、本ドキュメントを更新する。

---

# Summary

Hub apply では:

```text
subscriber_contact_points
```

を連絡先の正本構造として扱う。

```text
1レコード = 1連絡先
```

とすることで:

```text
- phoneのみ変更
- emailのみ変更
- null current解除
- 複数連絡先保持
- 過去連絡先への戻り
- phone / email 別の apply status 管理
- current変更履歴のaudit記録
```

を安全に扱えるようにする。

現行 `subscriber_contacts` は:

```text
legacy / backfill source / temporary reference
```

として扱う。