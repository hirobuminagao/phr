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

```text
# 旧実装
apply_subscribers_from_staging_hub.py

# ADR-0021 以降の新構成
scripts/hia/apply_hia_subscriber_sync.py
scripts/hia/script_lib/hub_subscriber_prepare.py
scripts/hia/script_lib/hub_subscriber_compare.py
scripts/hia/script_lib/hub_subscriber_apply.py
scripts/hia/script_lib/hub_subscriber_audit.py
```

旧実装では apply phase 内で compare / update / audit を同時に実施していた。

ADR-0021 以降は:

```text
import orchestration
  ↓
apply orchestration
```

へ責務分離する。

Apply orchestration は:

```text
prepare / compare
↓
apply
↓
audit
```

をまとめて実行する親処理として扱う。

このうち `hub_subscriber_apply.py` は、prepare / compare により確定した `apply_action` を実行する反映エンジンとして扱う。

---

# 1. Apply Orchestration Overview

処理対象:

```
staging_subscribers_hub
WHERE import_run_id = :import_run_id
  AND processed_run_id IS NULL
```

フロー:

staging_subscribers_hub
      │
      │ prepare / compare
      ▼
apply_action / diff status
      │
      │ apply
      ▼
subscribers
      │
      ├ address apply
      │
      ├ contact point apply
      │
      └ subscriber_audit
```

apply orchestration は compare 判定を含むが、`hub_subscriber_apply.py` 自身は compare 判定を行わない。

```text
apply_action = insert
  → insert

apply_action = update
  → update

apply_action = noop
  → skip

apply_action = review
  → skip
```

---

# 2. Subscriber Identity Relationship

旧実装では apply phase 内で subscriber identity compare を実施していた。

旧 compare 条件:

```text
(person_id_custom,
 name_kana_full_match,
 gender_code)
```

ADR-0021 以降は import orchestration 側で current snapshot を staging に反映し、apply orchestration 側で compare hash による候補絞り込みと詳細compareを行う。

```text
HIA subscriber ID
↓
identity_hash
↓
current_subscriber_id
↓
compare hash
↓
compare status
```

を生成する。

Apply orchestration は compare phase により生成された:

```text
current_subscriber_id
apply_action
compare_identity_norm_hash
compare_other_hash
address_hash
compare status
```

を利用して処理を実行する。

identity compare の詳細は:

```text
identity_policy.md
compare_prepare_phase.md
```

を参照する。

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

条件:

```text
apply_action = insert
```

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
identity_hash
compare_identity_norm_hash
compare_other_hash
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

条件:

```text
apply_action = update
```

compare / diff 判定は prepare / compare phase 側で実施済みとする。

Apply phase は compare phase により生成された:

```text
apply_diff_columns
identity_match_status
compare_identity_norm_hash
compare_other_hash
address_diff_status
contact_point_diff_status
```

を参照して UPDATE を実行する。

実行:

```text
UPDATE subscribers
SET ...
updated_at = now()
last_change_run_id = run_id
```

`identity_hash` は subscriber resolve / join 用であり、登録値差分判定の中心にはしない。

subscriber 本体の差分反映は以下を利用する。

```text
compare_identity_norm_hash
compare_other_hash
```

apply 時は staging 側の compare hash を `subscribers` へ反映する。

```text
subscribers.compare_identity_norm_hash = staging.compare_identity_norm_hash
subscribers.compare_other_hash = staging.compare_other_hash
```

`name_kana_full` 等の identity登録値が更新される場合は、必要に応じて parts の扱いを compare / apply 側で判定する。
ただし HIA 由来データでは、分割不能な parts へ暫定値を流し込まない。

---

# 5. Address History

テーブル:

```
subscriber_addresses
```

ポリシー:

```
current row は1件
history row は複数保持
```

compare / diff 判定は apply orchestration の prepare / compare により実施済みとする。

条件:

```
is_current = 1
```

フロー:

```
staging.address_hash
  ↓
subscriber_addresses.address_hash を subscriber_id 単位で検索
  ↓
same address_hash exists?
  yes:
    is_current = 1?
      yes -> noop
      no  -> current切替
  no:
    新住所 insert + current化
```

apply 時は staging 側の `address_hash` を `subscriber_addresses.address_hash` へ反映する。

```text
subscriber_addresses.address_hash = staging.address_hash
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

# 6. Contact Point History

Hub apply では、現行 `subscriber_contacts` ではなく、新しい contact point 型テーブルを正本構造として扱う。

新テーブル:

```text
subscriber_contact_points
```

想定構造:

```text
contact_point_id
subscriber_id
contact_type
contact_value
is_current
valid_from
valid_to
source
created_at
updated_at
```

`contact_type` の初期値:

```text
phone
email
```

現行 `subscriber_contacts` は:

```text
phone + email 同居型
```

であり、phoneのみ変更 / emailのみ変更 / null時の current解除 / 複数連絡先管理を安全に扱いにくい。

そのため Hub apply では `subscriber_contact_points` を正として実装する。

## Legacy backfill

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

## Apply policy

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

# 7. Subscriber Audit

テーブル:

```
subscriber_audit
```

生成タイミング:

```text
subscriber insert
subscriber update
compare_identity_norm_hash change
compare_other_hash change
address current switch / insert
contact point current change
qualification change
```

実装方式:

```
Python apply script
```

理由:

- run_id を保持できる
- ETL context を保持できる
- DB trigger 依存を避ける

HIA 側を最新正本として同期するため、
変更前後差分は必ず audit として永続保存する。

参照:

```
ADR-0010
subscriber-audit-implementation
```

---

# 8. Processed Mark

apply_action 実行成功後:

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

staging_subscribers_hub
      │
      │ prepare / compare
      ▼
apply_action / diff status
      │
      │ apply
      ▼
subscriber apply
      │
      ├ insert
      │
      ├ update
      │
      ├ noop
      │
      └ review(skip)
      │
      ▼
address apply
      │
      ▼
contact point apply
      │
      ▼
subscriber audit
      │
      ▼
processed mark
