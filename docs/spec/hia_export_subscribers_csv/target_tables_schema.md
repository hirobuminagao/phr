# HIA Export Subscribers CSV – Target Tables Schema

このドキュメントは、HIA export 加入者CSVの apply orchestration の prepare / compare / apply で参照・更新する列を定義する。

関連ADR:

- ADR-0021 HIA加入者 import / compare / apply フロー再設計

関連spec:

- `flow_overview.md`
- `import_phase.md`
- `staging_schema.md`
- `identity_policy.md`
- `compare_prepare_phase.md`
- `subscriber_apply.md`

---

# 1. Purpose

本specの目的は、以下を明確化することである。

- apply orchestration の prepare / compare が参照する本番列
- apply 本体が更新する本番列
- identity_hash と compare hash の役割分離
- compare_identity_norm_hash / compare_other_hash / address_hash の扱い
- address_hash + is_current による住所判定
- subscriber_contact_points による連絡先管理
- audit 対象列

HIA 側を最新正本として扱うため、差分が存在する場合は audit を必ず残したうえで本番テーブルへ反映する。

---

# 2. Target Tables

対象テーブル:

```text
subscribers
subscriber_addresses
subscriber_contact_points
subscriber_contacts (legacy / backfill source / temporary reference)
subscriber_audit
```

本ファイルでは、HIA加入者CSV apply 対象となる本番テーブルの参照・更新列を整理する。

---

# 3. subscribers

`subscribers` は加入者の現在状態を保持する業務参照用キャッシュである。

HIA export subscribers CSV を最新正本として扱い、
apply orchestration の prepare / compare で差分を確定したうえで、apply 本体が必要な列を更新する。

---

## 3.1 External ID

| column | 用途 |
|---|---|
| `hia_subscriber_id` | HIA加入者ID。HIA上の同一加入者を追跡する最優先外部ID |

方針:

- HIA subscriber ID は review 時の重要な外部IDとして扱う
- import時に `current_hia_subscriber_id` を staging に保持し、HIA側ID変更・別人候補・上流ID差し替え確認の足がかりにする
- 自動 apply は current_lookup_status / compare hash / detailed compare の結果に従う

---

## 3.2 Identity Columns

| column | 用途 |
|---|---|
| `person_id_custom` | 保険者番号・記号・番号・生年月日から生成する加入者識別キー |
| `identity_hash` | subscriber resolve / join 用 identity hash |
| `compare_identity_norm_hash` | identity登録値差分検知用 compare hash |
| `compare_other_hash` | identity以外の subscriber属性差分検知用 compare hash |
| `name_kana_full_match` | identity_hash 構成要素 |
| `gender_code` | identity_hash 構成要素 |

方針:

- `identity_hash` は subscriber resolve / join 用であり、登録値差分検知の中心にはしない
- 登録値差分検知には `compare_identity_norm_hash` / `compare_other_hash` を使用する
- 同じ identity_hash を持つ subscribers が複数存在する場合は review とし、自動 apply しない
- apply 時は staging 側の compare hash を subscribers 側へ反映する

compare_identity_norm_hash 対象値:

```text
insurance_symbol
insurance_number
name_kana_full
name_kanji_full
birth
gender_code
```

`insurance_branchnumber` は compare_identity_norm_hash 対象外とする。
枝番は健保・運用側が独自に採番する補助番号であり、本人/扶養/続柄/任意継続等の管理ルールが健保ごとに揺れるため、identity登録値差分の主軸として管理しない。

compare_other_hash 対象候補:

```text
insured_attribute_name
relationship_name
qualification_acquired_date
qualification_lost_date
employer_code
department_code
distribution_code
employee_code
connect_id
```

---

## 3.3 Name Columns

### Full Name

| column | 用途 |
|---|---|
| `name_kanji_full` | 氏名漢字全文 |
| `name_kana_full` | 氏名カナ全文 |
| `name_kanji_full_match` | 氏名漢字全文の照合用match値 |
| `name_kana_full_match` | 氏名カナ全文の照合用match値 |

### Name Parts

| column | 用途 |
|---|---|
| `name_kanji_family` | 漢字 姓 |
| `name_kanji_middle` | 漢字 middle |
| `name_kanji_given` | 漢字 名 |
| `name_kana_family` | カナ 姓 |
| `name_kana_middle` | カナ middle |
| `name_kana_given` | カナ 名 |
| `name_kanji_family_match` | 漢字 姓 match |
| `name_kanji_middle_match` | 漢字 middle match |
| `name_kanji_given_match` | 漢字 名 match |
| `name_kana_family_match` | カナ 姓 match |
| `name_kana_middle_match` | カナ middle match |
| `name_kana_given_match` | カナ 名 match |

---

## 3.4 Name Parts Clear Policy

HIA 由来データでは、name parts は分割できた場合のみ格納する。
分割不能な parts へ暫定値を流し込まない。

`name_kana_full` / `name_kanji_full` などの identity登録値が更新される場合は、必要に応じて compare / apply 側で parts の扱いを判定する。

基本方針:

```text
full は正本として保持
parts は分割済みの確定値のみ保持
split不可時は parts を暫定補完しない
```

既存 parts を clear するかどうかは、`compare_identity_norm_hash` の差分内容と詳細compare結果をもとに apply 側で判断する。

---

## 3.5 Insurance Columns

| column | 用途 |
|---|---|
| `insurer_number` | 保険者番号 |
| `insurance_symbol` | 保険証記号 |
| `insurance_symbol_export` | export用記号 |
| `insurance_symbol_digits` | 記号数字抽出値 |
| `insurance_symbol_match` | 記号match値 |
| `insurance_number` | 保険証番号 |
| `insurance_number_match` | 番号match値 |
| `insurance_branchnumber` | 枝番 |

方針:

- HIA側を正として差分があれば更新する
- 記号・番号は `compare_identity_norm_hash` の対象とする
- 枝番は compare_identity_norm_hash 対象外とし、必要に応じて通常属性として扱う

---

## 3.6 Qualification / Relationship Columns

| column | 用途 |
|---|---|
| `birth` | 生年月日 |
| `gender_code` | 性別コード |
| `insured_attribute_name` | 被保険者属性名 |
| `relationship_name` | 続柄名 |
| `qualification_acquired_date` | 資格取得日 |
| `qualification_lost_date` | 資格喪失日 |

方針:

- relationship / insured attribute / qualification は `compare_other_hash` の対象候補として扱う

---

## 3.7 Organization Columns

| column | 用途 |
|---|---|
| `employer_code` | 事業所コード |
| `department_code` | 所属コード |
| `distribution_code` | 配布先コード |
| `employee_code` | 社員コード |
| `connect_id` | 連携ID |

方針:

- HIA側を正として差分があれば更新する
- company mapping / organization mapping の結果が staging に反映済みであることを前提とする

---

## 3.8 Apply Management Columns

| column | 用途 |
|---|---|
| `last_change_run_id` | 最終変更run_id |
| `created_at` | 作成日時 |
| `updated_at` | 更新日時 |

apply phase で `insert` または `update` を実行した場合、`last_change_run_id` と `updated_at` を更新する。

---

# 4. subscriber_addresses

`subscriber_addresses` は加入者住所の履歴テーブルである。

current 行は `is_current = 1` により管理する。

compare / prepare phase では、`subscribers.id` に紐づく current address を取得し、
staging 側の住所情報と比較する。

---


## 4.1 Current Row Condition

current address の取得条件:

```sql
SELECT *
FROM subscriber_addresses
WHERE subscriber_id = :subscriber_id
  AND is_current = 1;
```

方針:

- `is_current = 1` は current active address row として扱う
- `is_current = 0` は historical address row として扱う
- current row は1件を期待し、history row は複数保持する
- current row が複数存在する場合は review 対象とする
- address compare では current row だけでなく、subscriber に紐づく住所履歴全体を対象に確認する

---

## 4.2 Compare Columns

DDL実態に合わせ、住所比較は以下の列で行う。

| column | 用途 |
|---|---|
| `postal_code` | 郵便番号 |
| `address_line` | 住所本文 |
| `building` | 建物名・部屋番号等 |
| `address_hash` | 住所値の存在確認・差分検知用 compare hash |

---

## 4.3 Address Diff Policy

住所 compare では、staging の `address_hash` を使い、既存 `subscriber_addresses.address_hash` と照合する。

判定:

| status | 条件 | 意味 |
|---|---|---|
| `noop` | 同一 address_hash の行が存在し、かつ `is_current = 1` | 現在住所と一致 |
| `switch_current` | 同一 address_hash の行が存在するが、`is_current = 0` | 既存住所へ current 切替候補 |
| `insert` | 同一 address_hash の行が存在しない | 新住所 insert 候補 |
| `review` | 同一 hash が複数 current 等、判定不能 | 自動apply不可 |

注意:

```text
address_hash 一致 = 同一住所値の存在
```

であり、current address 一致とは限らない。

subscriber_addresses は 1:n の履歴型テーブルであり、同一住所値が historical row として存在する可能性がある。

---

## 4.4 Apply Policy

address に差分がある場合、既存 current 行を直接上書きせず、履歴として扱う。

想定処理:

```text
same address_hash exists and is_current = 1
  -> noop

same address_hash exists and is_current = 0
  -> current切替
     既存 current 行を is_current = 0
     該当 existing address row を is_current = 1

same address_hash not exists
  -> insert
     既存 current 行を is_current = 0
     新 address row を insert
     新 address row に address_hash = staging.address_hash
     新 address row を is_current = 1
```

---

## 4.5 Audit / History Policy

address は履歴型テーブルとして保持する。

履歴管理は:

```text
is_current
valid_from
valid_to
```

により行う。

現時点では:

```text
subscriber_address_audit
```

のような専用 audit テーブルは持たない。

また住所変更は:

```text
subscriber_audit
```

への必須記帳対象ではない。

したがって現在の実装では:

```text
address
  → 履歴あり
  → auditなし
```

として扱う。

# 5. subscriber_contact_points

Hub apply では、現行 `subscriber_contacts` ではなく、新しい contact point 型テーブルを正本構造として扱う。

新テーブル:

```text
subscriber_contact_points
```

想定構造:

| column | 用途 |
|---|---|
| `contact_point_id` | contact point ID |
| `subscriber_id` | 対象 subscribers.id |
| `contact_type` | 連絡先種別。初期値は `phone` / `email` |
| `contact_value` | 連絡先値 |
| `is_current` | current flag |
| `valid_from` | 有効開始日時 |
| `valid_to` | 有効終了日時 |
| `source` | データ由来 |
| `created_at` | 作成日時 |
| `updated_at` | 更新日時 |

現行 `subscriber_contacts` は:

```text
phone + email 同居型
```

であり、phoneのみ変更 / emailのみ変更 / null時の current解除 / 複数連絡先管理を安全に扱いにくい。

そのため Hub apply では `subscriber_contact_points` を正として実装する。

---

## 5.1 Legacy backfill

既存 `subscriber_contacts` から `subscriber_contact_points` へ backfill する。

```text
subscriber_contacts
  ↓
phone が空でなければ subscriber_contact_points(contact_type='phone') へ insert
email が空でなければ subscriber_contact_points(contact_type='email') へ insert
```

旧1行は最大2行へ分解される。

```text
旧:
subscriber_id + phone + email + is_current

新:
subscriber_id + contact_type='phone' + contact_value + is_current
subscriber_id + contact_type='email' + contact_value + is_current
```

`subscriber_contacts` は legacy / backfill source / temporary reference として扱う。

---

## 5.2 Apply Policy

HIA CSV phoneあり:

```text
subscriber_id + contact_type='phone' + contact_value で既存確認
  exists:
    current切替
  not exists:
    insert + current化
```

HIA CSV phone null:

```text
subscriber_id に紐づく phone current を全て current から外す
```

HIA CSV emailあり:

```text
subscriber_id + contact_type='email' + contact_value で既存確認
  exists:
    current切替
  not exists:
    insert + current化
```

HIA CSV email null:

```text
subscriber_id に紐づく email current を全て current から外す
```

null は:

```text
何もしない
```

ではなく:

```text
HIA正本上、現在値なし
```

として扱う。

現時点では contact compare hash は導入しない。
contact は `contact_type + contact_value + is_current` を基準に compare / apply する。

---

## 5.3 Audit / History Policy

contact point は履歴型テーブルとして保持する。

履歴管理は:

```text
is_current
valid_from
valid_to
```

により行う。

現時点では:

```text
subscriber_contact_point_audit
```

のような専用 audit テーブルは持たない。

また contact point の変更は:

```text
subscriber_audit
```

への必須記帳対象ではない。

したがって現在の実装では:

```text
contact point
  → 履歴あり
  → auditなし
```

として扱う。

---

# 6. subscriber_audit

`subscriber_audit` は subscribers 系 apply の監査イベントを保持する audit テーブルである。

現在の Hub apply orchestration では、1 staging row (= 1 subscriber) の apply 単位で軽量 audit event を保存する。

HIA側を最新正本として扱うため、apply orchestration で本番テーブルを更新した場合は、audit を保存してから processed mark する。

---

## 6.1 Row Model

現在の実装では、audit は以下の粒度で保存する。

```text
1 staging row apply = 1 audit event
```

想定列:

| column | 用途 |
|---|---|
| `subscriber_id` | 対象 subscribers.id |
| `event_type` | audit event type |
| `event_source` | 変更元 |
| `event_payload` | apply_action / diff/status summary JSON |
| `run_id` | apply run_id |
| `created_at` | 作成日時 |

現在の payload には以下を含める。

```text
apply_run_id
staging_subscriber_hub_id
import_run_id
subscriber_id
apply_action
apply_diff_columns
identity_match_status
address_diff_status
contact_point_diff_status
hia_subscriber_id
current_subscriber_id
```

詳細な old_value / new_value の列単位 audit は、後続拡張として扱う。

---

## 6.2 Audit Required Events

以下は audit 対象とする。

```text
apply_action = insert
apply_action = update
```

`apply_action = noop` は実変更がないため audit 対象外とする。

`apply_action = review` は本番更新を行わないため audit 対象外とする。

update の内訳は `event_payload.apply_diff_columns` に保持する。

例:

```text
compare_identity_norm_hash
compare_other_hash
address
contact_point
```

---

## 6.3 Event Payload Policy

現在の audit は列単位ではなく、apply event payload として保存する。

payload の `apply_diff_columns` には、更新対象ブロックを保持する。

想定値:

```text
compare_identity_norm_hash
compare_other_hash
address
contact_point
```

意味:

| diff column | 意味 |
|---|---|
| `compare_identity_norm_hash` | subscriber identity fields 更新 |
| `compare_other_hash` | subscriber other fields 更新 |
| `address` | subscriber_addresses current change / insert |
| `contact_point` | subscriber_contact_points current change / insert / clear |

旧来想定していた以下のような列単位 field 名は、詳細audit拡張時に再検討する。

```text
address.address_line
contact_point.email
insurance_symbol
qualification_acquired_date
```

---

## 6.4 Source Policy

HIA export subscribers CSV 由来の apply では、原則として:

```text
event_source = hia_apply_orchestration
```

を使用する。

---

## 6.5 run_id Policy

`run_id` には apply orchestration の run_id を保持する。

方針:

- import run_id ではなく、実際に本番更新を行った apply run_id を入れる
- import_run_id は `event_payload` に保持する
- prepare / compare の判定結果は staging 側に保持する
- audit は apply 実行時に保存する

---

## 6.6 Address / Contact Audit

address / contact point の実更新は、それぞれ履歴テーブルにも残る。

```text
subscriber_addresses
subscriber_contact_points
```

`subscriber_audit` には、1 subscriber apply event の payload として address / contact point の変更ブロックを記録する。

例:

```json
{
  "apply_action": "update",
  "apply_diff_columns": "address,contact_point",
  "address_diff_status": "insert",
  "contact_point_diff_status": "changed"
}
```

詳細な old/new value audit は後続拡張とする。

---

## 6.7 Audit Timing

現在の apply orchestration では、1 staging row (= 1 subscriber) を以下の順で処理する。

```text
subscriber root apply
  ↓
address apply
  ↓
contact point apply
  ↓
audit
  ↓
processed mark
```

`apply_action = insert/update` の場合のみ audit を保存する。

`apply_action = noop` は本番更新がないため audit を保存しない。

理由:

- audit は変更履歴であり、変更されないものは記録しない
- noop の確認は staging / current snapshot / 実テーブル側の seen 情報で確認する
- audit と本番更新の不整合を避けるため、processed mark より前に audit を保存する

---

# Summary

`subscribers` は HIA export subscribers CSV に対する現在状態キャッシュであり、
HIA側を最新正本として同期する。

compare phase は以下を確認する。

```text
HIA subscriber ID
identity_hash
compare_identity_norm_hash
compare_other_hash
address_hash + is_current
contact point
qualification columns
organization columns
```

apply orchestration は prepare / compare が確定した `apply_action` と diff 情報に従い、1 subscriber row 単位で必要な処理を順番に実行する。

`identity_hash` は resolve / join 用であり、登録値差分検知の中心にはしない。
登録値差分検知には `compare_identity_norm_hash` / `compare_other_hash` を使用する。

住所は `address_hash` と `is_current` を組み合わせて noop / current切替 / insert を判定する。

Audit は `apply_action = insert/update` の場合のみ保存し、`noop` は変更履歴として記録しない。

連絡先は `subscriber_contact_points` を正本構造として扱い、現行 `subscriber_contacts` は legacy / backfill source / temporary reference として扱う。