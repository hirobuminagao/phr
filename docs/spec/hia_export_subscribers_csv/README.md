# HIA Export Subscribers CSV

## Purpose
このディレクトリは **HIA 側から提供される加入者CSV（export）を PHR に取り込む処理仕様**をまとめたものです。

対象範囲は以下の処理フローです。

```
HIA export subscribers CSV
        ↓
import orchestration
  - CSV import
  - compare hash generation
  - current snapshot update to staging
        ↓
staging_subscribers_hub
        ↓
apply orchestration
  - prepare
  - compare
  - apply orchestration
    - subscriber root apply
    - address apply
    - contact point apply
    - audit
    - processed mark
        ↓
subscribers
subscriber_addresses
subscriber_contact_points (Hub apply target)
subscriber_contacts (legacy source for backfill / temporary reference)
subscriber_audit
```

本spec群は旧版では `import → apply` の2段階を中心に記述していたが、
ADR-0021 以降は、実行単位を `import orchestration` と `apply orchestration` に分離する。

現在の責務分割:

```text
import orchestration
  = CSV import
  + compare hash generation
  + current snapshot を staging に反映

apply orchestration
  = prepare
  + compare
  + apply orchestration
    - subscriber root apply
    - address apply
    - contact point apply
    - audit
    - processed mark
```

旧実装・旧specの内容は、移行前の事実として残しつつ、
本ディレクトリ内で新フローへ順次更新する。

## Reading Map

この spec 群は、以下の順に読むと処理全体が追いやすい。

```
HIA Export CSV
   ↓
flow_overview.md
   ↓
import_phase.md
   ↓
staging_schema.md
   ↓
identity_policy.md
   ↓
compare_prepare_phase.md
   ↓
subscriber_apply.md
apply_orchestration.md
   ↓
target_tables_schema.md
subscriber_contact_points
subscriber_contacts (legacy)
subscriber_audit
contact_point_schema.md
```

読み始めは `flow_overview.md`、詳細確認は各個別 spec を参照する。

---

## Scope

対象システム: **PHR**

対象データ:  
HIA 側から提供される加入者 CSV（export）

処理フェーズ:

1. Import orchestration
   - CSV Import
   - Staging 保存
   - Current snapshot update
2. Apply orchestration
   - Prepare
   - Compare
   - Subscriber Root Apply
   - Address Apply
   - Contact Point Apply
   - Subscriber Audit
   - Processed Mark

---

## Related ADR

この仕様は以下の ADR に基づきます。

- `ADR-0008` subscribers identity matching
- `ADR-0009` DB connection policy
- `ADR-0010` subscriber audit implementation
- `ADR-0021` HIA加入者 import / compare / apply フロー再設計

---

## Directory Structure

```
hia_export_subscribers_csv/

README.md
    この仕様の入口ドキュメント

flow_overview.md
    全体処理フロー

import_phase.md
    CSV → staging_subscribers_hub の取込仕様

staging_schema.md
    staging_subscribers_hub テーブル構造と役割

identity_policy.md
    subscriber 同一人物判定ポリシー

compare_prepare_phase.md
    staging と subscribers / address / contact point の比較・apply_action 作成仕様

subscriber_apply.md
    staging → subscribers 反映仕様
apply_orchestration.md
    apply orchestration と apply_action_* 分割構成

target_tables_schema.md
    subscribers / addresses / contact point target schema

contact_point_schema.md
    subscriber_contact_points 設計
```

---

## Design Principles

### 1. DBロジックは最小化
履歴管理や audit 生成は **DB trigger ではなく Python apply script で制御する**

理由:
- デバッグ容易性
- run context を扱いやすい
- ETL責務をスクリプト側へ集約

---

### 2. 履歴テーブルは audit 対象外

以下は subscriber_audit の直接対象ではなく、history/current 管理を主目的とする。

---

### 3. Subscriber Identity

subscriber の同一人物 resolve / join には `identity_hash` を使用する。

```
person_id_custom
name_kana_full_match
gender_code
```

注意:

```text
identity_hash は resolve / join 用であり、登録値差分検知用 hash ではない。
```

登録値差分検知には以下の compare hash を使用する。

```text
compare_identity_norm_hash
compare_other_hash
address_hash
```

compare hash は `scripts/lib/hash/compare_hash.py` の `build_compare_hash()` で生成する。

基本方針:

```text
- compare hash は norm 値を渡す
- match 値は compare hash 化しない
```

identity_hash は resolve / join 用、compare hash は detailed compare 候補絞り込み用として扱う。

---

### 4. Import / Prepare / Apply の分離

旧実装では apply スクリプト内で以下を同時に行っていた。

- 既存 subscribers 照合
- 差分比較
- insert / update / noop 判定
- subscribers 更新
- address / contact point 同期
- audit 保存

ADR-0021 以降は、staging を compare workspace として扱い、比較結果を staging 側へ保持する。
apply は判定済み action を orchestration として順番実行する。

staging_subscribers_hub は:

```text
- import values
- current snapshot values
- compare status
- apply_action
```

を同一行へ保持する compare workspace として扱う。

```text
import orchestration
  = CSV → staging
  + current snapshot update

apply orchestration
  = prepare
  + compare
  + apply orchestration
    - subscriber root apply
    - address apply
    - contact point apply
    - audit
    - processed mark
```

---

### 5. HIA を最新正本として扱う

HIA export subscribers CSV を最新正本として扱う。

`subscribers` は業務参照用の現在状態キャッシュであり、
差分がある場合は audit を必ず保存したうえで HIA 側値へ追従する。

---

### 6. audit は必ず保存する

HIA側値を正として反映するため、更新前後の差分は必ず audit に残す。

compare_identity_norm_hash / compare_other_hash による登録値変更、住所 current 切替・追加、contact point current 変更も audit / 履歴管理対象とする。

---

## Implementation Entry Points

# 旧実装
scripts/work_folder/scripts/import_subscribers_to_staging_hub.py
scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py

# 新構成（ADR-0021）
scripts/hia/import_subscribers_to_staging_hub.py
scripts/hia/apply_hia_subscriber_sync.py
scripts/hia/script_lib/

---

## Current Implementation Status

### Import orchestration

現在実装済み:

```text
scripts/hia/import_subscribers_to_staging_hub.py
  ↓
scripts/hia/script_lib/hub_subscriber_import.py
  ↓
scripts/hia/script_lib/hub_subscriber_current_snapshot.py
```

責務:

```text
CSV import
compare hash generation
staging_subscribers_hub INSERT
current snapshot update to staging
```

現在の Hub contact 方針:

```text
subscriber_contact_points を Hub apply の正本構造として導入予定
subscriber_contacts は backfill元 / temporary reference として扱う
```

注意:

```text
compare hash generation は import orchestration 側へ統合予定。
```

今後追加する値:

```text
staging_subscribers_hub.compare_identity_norm_hash
staging_subscribers_hub.compare_other_hash
staging_subscribers_hub.address_hash
```

---

### Compare hash / backfill order

compare hash 導入後の実装順:

```text
1. import orchestration 側で staging_subscribers_hub の compare hash を生成
2. current snapshot compare workspace を staging へ反映
3. Hub側だけ subscriber_contact_points 前提へ移行
4. apply orchestration 側で compare hash を利用する prepare / compare を実装
5. apply orchestration を実装
   - subscriber root apply
   - address apply
   - contact point apply
   - audit
   - processed mark
6. 実装が固まった後に subscribers / subscriber_addresses の backfill を実行
7. 最後に fund側 diff / projection を見直す
```

backfill 対象:

```text
subscribers.compare_identity_norm_hash
subscribers.compare_other_hash
subscriber_addresses.address_hash
subscriber_contacts → subscriber_contact_points
```

backfill は、生成ロジックが確定してから実行する。

contact point 移行は、Hub apply orchestration を先に完成させてから fund 側へ展開する。

---

## Version

PHR v1.1.0

### Next Refactor Direction (ADR-0021)

今後の実装では、旧 `import → apply` 構成を以下へ再整理する。

```text
import orchestration
  ↓
prepare
  ↓
compare
  ↓
apply orchestration
```

主な変更方針:

- `scripts/work_folder/scripts/` から `scripts/hia/` へ移設
- orchestration と処理関数を分離
- apply orchestration は 1 subscriber row 単位で順番実行する
- apply_action_* モジュールへ責務分割する
- current snapshot / compare status / apply_action を staging 側へ保持
- HIA を最新正本として扱う
- identity_hash は resolve / join 用として扱う
- compare_identity_norm_hash / compare_other_hash / address_hash を差分候補絞り込みに使う
- identity_hash は resolve / join 用として扱う
- compare hash は detailed compare 候補絞り込み用として扱う
- Hub側は subscriber_contact_points を正本構造として先行導入する
- subscriber_contacts は legacy / backfill source として一時保持する
- fund側 contact diff は後工程で見直す

### Current Apply Orchestration Structure

現在の apply orchestration は、1 staging row (= 1 subscriber) を単位として順番処理する。

```text
staging row
  ↓
apply_action dispatch
  ↓
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

現在の責務分割:

```text
hub_subscriber_apply.py
  = orchestration / dispatch

apply_action_subscriber.py
  = subscribers root apply

apply_action_subscriber_address.py
  = subscriber_addresses apply

apply_action_subscriber_contact_point.py
  = subscriber_contact_points apply

apply_action_subscriber_audit.py
  = subscriber_audit apply

apply_action_staging_mark.py
  = processed/error mark
```

apply orchestration は「1 subscriber を最後まで処理してから次へ進む」構造とする。

理由:

```text
- subscribers.id を child apply に引き継ぐ必要がある
- history/current 管理を transaction 単位で閉じやすい
- audit と実更新の整合を取りやすい
- エラー時の追跡が容易
```

### v1.1.0 Changes (Subscriber Apply / Identity Handling)

背景:
- HIA 由来データで `name_kana_given` に `name_kana_full` をそのまま格納していた
- その結果、parts 列が「空欄ではない」と判定され、fund 由来の正しい分割値が上書きされない問題が発生
- `NULL` 判定のみだったため、空文字（''）が未設定として扱われなかった

対応:
- parts 列は「分割できた場合のみ格納」、それ以外は NULL を保持
- 未設定判定を「NULL または空文字」に統一
- identity 生成は parts に依存せず `name_kana_full_match` ベースで実施

効果:
- fund 側の高精度な name split が正しく反映される
- HIA / fund のデータ整合性が安定
- 再処理・バックフィル時の安全性向上