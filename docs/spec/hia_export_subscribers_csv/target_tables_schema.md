# HIA Export Subscribers CSV – Target Tables Schema

このドキュメントは、HIA export 加入者CSVの apply 対象となる本番テーブル群のうち、
compare / prepare / apply phase で参照・更新する列を定義する。

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

- prepare / compare phase が参照する本番列
- apply phase が更新する本番列
- identity_hash 変更時の扱い
- name parts clear 対象列
- address / contact compare 対象列
- audit 対象列

HIA 側を最新正本として扱うため、差分が存在する場合は audit を必ず残したうえで本番テーブルへ反映する。

---

# 2. Target Tables

対象テーブル:

```text
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```

本ファイルでは、HIA加入者CSV apply 対象となる本番テーブルの参照・更新列を整理する。

---

# 3. subscribers

`subscribers` は加入者の現在状態を保持する業務参照用キャッシュである。

HIA export subscribers CSV を最新正本として扱い、
prepare / compare phase で差分を確定したうえで、apply phase が必要な列を更新する。

---

## 3.1 External ID

| column | 用途 |
|---|---|
| `hia_subscriber_id` | HIA加入者ID。HIA上の同一加入者を追跡する最優先外部ID |

方針:

- HIA subscriber ID が一致する場合、原則として同一 HIA subscriber の情報更新とみなす
- identity_hash が変わっていても、HIA subscriber ID 一致を優先する
- identity_hash 変更内容は compare phase で確認する

---

## 3.2 Identity Columns

| column | 用途 |
|---|---|
| `person_id_custom` | 保険者番号・記号・番号・生年月日から生成する加入者識別キー |
| `identity_hash` | compare / join 用 identity hash |
| `name_kana_full_match` | identity_hash 構成要素 |
| `gender_code` | identity_hash 構成要素 |

identity_hash 構成:

```text
person_id_custom
name_kana_full_match
gender_code
```

方針:

- compare phase では `identity_hash` を中心に本番 subscriber と比較する
- HIA subscriber ID が一致する場合、identity_hash changed でも HIA 最新状態へ同期する
- 同じ identity_hash を持つ subscribers が複数存在する場合は review とし、自動 apply しない

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

`name_kana_full_match` が変更された場合、既存の name parts は clear 対象とする。

理由:

```text
旧 parts が新 full name と不整合になる可能性が高いため
```

clear は `NULL` へ戻すことを基本とする。

clear 対象:

```text
name_kanji_family
name_kanji_middle
name_kanji_given
name_kana_family
name_kana_middle
name_kana_given
name_kanji_family_match
name_kanji_middle_match
name_kanji_given_match
name_kana_family_match
name_kana_middle_match
name_kana_given_match
```

記号・番号のみ変更の場合は、name parts は維持する。

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
- 記号・番号変更により identity_hash が変わる場合でも、HIA subscriber ID が一致するなら更新候補とする
- 記号・番号のみ変更の場合、name parts は維持する

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

- HIA側を正として差分があれば更新する
- identity_hash 構成要素に関わる差分は compare phase で identity change として記録する

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
  AND is_current = 1
LIMIT 1;
```

方針:

- current 行が存在しない場合は address insert 候補とする
- current 行が存在し、住所差分がない場合は noop とする
- current 行が存在し、住所差分がある場合は履歴更新対象とする

---

## 4.2 Compare Columns

DDL実態に合わせ、住所比較は以下の列で行う。

| column | 用途 |
|---|---|
| `postal_code` | 郵便番号 |
| `address_line` | 住所本文 |
| `building` | 建物名・部屋番号等 |
| `prefecture` | 都道府県 |
| `city` | 市区町村 |
| `prefecture_code` | 都道府県コード |

注意:

旧spec上で `address1` / `address2` / `address3` と表現している箇所がある場合、
本DDLでは `address_line` / `building` / `prefecture` / `city` 等に対応させて読み替える。

---

## 4.3 Address Diff Policy

prepare / compare phase で current address と staging address を比較する。

差分なし:

```text
address_diff_status = noop
```

差分あり:

```text
address_diff_status = changed
```

current 行なし:

```text
address_diff_status = insert
```

---

## 4.4 Apply Policy

address に差分がある場合、既存 current 行を直接上書きせず、履歴として扱う。

想定処理:

```text
1. 既存 current 行を close
2. 新しい address 行を insert
3. 新しい行を is_current = 1 とする
```

差分なしの場合は address apply を行わない。

---

## 4.5 Audit / History Policy

住所変更は audit / history 対象とする。

保持すべき情報:

- 変更前住所
- 変更後住所
- apply_run_id
- source staging row
- changed_at

HIA側を最新正本として扱うため、住所差分がある場合は、audit を残したうえで HIA 側値へ追従する。

---


# 5. subscriber_contacts

`subscriber_contacts` は加入者連絡先の履歴テーブルである。

current 行は `is_current = 1` により管理する。

compare / prepare phase では、`subscribers.id` に紐づく current contact を取得し、
staging 側の連絡先情報と比較する。

---

## 5.1 Current Row Condition

current contact の取得条件:

```sql
SELECT *
FROM subscriber_contacts
WHERE subscriber_id = :subscriber_id
  AND is_current = 1
LIMIT 1;
```

方針:

- current 行が存在しない場合は contact insert 候補とする
- current 行が存在し、連絡先差分がない場合は noop とする
- current 行が存在し、連絡先差分がある場合は履歴更新対象とする

---

## 5.2 Compare Columns

DDL実態に合わせ、連絡先比較は以下の列で行う。

| column | 用途 |
|---|---|
| `phone` | 電話番号 |
| `email` | メールアドレス |

注意:

旧spec上で `mobile` を compare 対象としている箇所がある場合、
現DDLでは `subscriber_contacts` に `mobile` 列は存在しない。

---

## 5.3 Contact Diff Policy

prepare / compare phase で current contact と staging contact を比較する。

差分なし:

```text
contact_diff_status = noop
```

差分あり:

```text
contact_diff_status = changed
```

current 行なし:

```text
contact_diff_status = insert
```

---

## 5.4 Apply Policy

contact に差分がある場合、既存 current 行を直接上書きせず、履歴として扱う。

想定処理:

```text
1. 既存 current 行を close
2. 新しい contact 行を insert
3. 新しい行を is_current = 1 とする
```

差分なしの場合は contact apply を行わない。

---

## 5.5 Source Policy

`subscriber_contacts.source` は、連絡先データの由来を保持する。

想定値例:

```text
hia_apply
manual
migration
```

HIA export subscribers CSV 由来の apply では、source に HIA apply 系値を設定する。

---

## 5.6 Audit / History Policy

連絡先変更は audit / history 対象とする。

保持すべき情報:

- 変更前連絡先
- 変更後連絡先
- apply_run_id
- source staging row
- changed_at

HIA側を最新正本として扱うため、連絡先差分がある場合は、audit を残したうえで HIA 側値へ追従する。

---

# 6. subscriber_audit

`subscriber_audit` は subscribers 系更新の差分を列単位で保持する audit テーブルである。

HIA側を最新正本として扱うため、apply phase で本番テーブルを更新する場合は、変更前後差分を必ず保存する。

---

## 6.1 Row Model

DDL実態に合わせ、audit は以下の粒度で保存する。

```text
1 changed field = 1 audit row
```

主な列:

| column | 用途 |
|---|---|
| `subscriber_id` | 対象 subscribers.id |
| `field` | 変更フィールド名 |
| `old_value` | 変更前値 |
| `new_value` | 変更後値 |
| `changed_at` | 変更日時 |
| `source` | 変更元 |
| `note` | 補足理由 |
| `change_run_id` | apply run_id |

---

## 6.2 Audit Required Events

以下は audit 必須対象とする。

```text
subscriber insert
subscriber update
identity_hash change
name parts clear
address change
contact change
qualification change
organization change
```

---

## 6.3 Field Naming Policy

`field` には、変更対象が分かる名前を保持する。

subscribers 本体:

```text
identity_hash
name_kana_full_match
insurance_symbol
insurance_number
qualification_acquired_date
employer_code
...
```

address 変更:

```text
address.postal_code
address.address_line
address.building
address.prefecture
address.city
address.prefecture_code
```

contact 変更:

```text
contact.phone
contact.email
```

name parts clear:

```text
name_kana_family
name_kana_given
name_kanji_family
name_kanji_given
...
```

---

## 6.4 Source Policy

`source` は変更元を表す。

想定値例:

```text
hia_apply
manual
migration
backfill
```

HIA export subscribers CSV 由来の apply では、原則として:

```text
source = hia_apply
```

を使用する。

---

## 6.5 change_run_id Policy

`change_run_id` には apply phase の run_id を保持する。

方針:

- import run_id ではなく、実際に本番更新を行った apply run_id を入れる
- compare / prepare phase の判定結果は staging 側に保持する
- 永続auditは apply 実行時に保存する

---

## 6.6 Address / Contact Audit

`subscriber_audit.subscriber_id` は必須のため、address / contact の変更も subscriber 単位の audit として保存する。

address / contact の変更は、履歴テーブル自体にも残るが、subscriber apply の監査性を高めるため、`subscriber_audit` にも変更概要を保存する。

例:

```text
field = address.address_line
old_value = 旧住所
new_value = 新住所
source = hia_apply
change_run_id = <apply_run_id>
```

```text
field = contact.email
old_value = old@example.com
new_value = new@example.com
source = hia_apply
change_run_id = <apply_run_id>
```

---

## 6.7 Name Parts Clear Audit

`name_kana_full_match` が変更され、既存 name parts を clear する場合も audit 対象とする。

例:

```text
field = name_kana_family
old_value = 旧姓カナparts
new_value = NULL
source = hia_apply
note = name_kana_full_match changed; clear name parts
```

---

## 6.8 Audit Timing

compare / prepare phase では audit 用差分情報を生成する。

実際の `subscriber_audit` 永続保存は apply phase で行う。

理由:

- compare phase は判定フェーズであり、本番更新を確定しない
- apply phase が本番更新と audit 保存を同一transactionで扱う
- audit と本番更新の不整合を避ける

---

# Summary

`subscribers` は HIA export subscribers CSV に対する現在状態キャッシュであり、
HIA側を最新正本として同期する。

compare phase は以下を確認する。

```text
HIA subscriber ID
identity_hash
name_kana_full_match
insurance columns
qualification columns
organization columns
address
contact
```

apply phase は compare phase が確定した `apply_action` と diff 情報に従い、
必要な列を更新する。

`name_kana_full_match` が変更された場合は、既存 name parts を `NULL` へ clear し、
後続 normalize / split により再生成可能な状態へ戻す。