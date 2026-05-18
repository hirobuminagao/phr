# HIA Export Subscribers CSV

## Purpose
このディレクトリは **HIA 側から提供される加入者CSV（export）を PHR に取り込む処理仕様**をまとめたものです。

対象範囲は以下の処理フローです。

```
HIA export subscribers CSV
        ↓
import phase
        ↓
staging_subscribers_hub
        ↓
prepare / compare phase
        ↓
apply phase
        ↓
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```

本spec群は旧版では `import → apply` の2段階を中心に記述していたが、
ADR-0021 以降は `import → prepare / compare → apply` の3段階へ再整理する。

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
   ↓
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```

読み始めは `flow_overview.md`、詳細確認は各個別 spec を参照する。

---

## Scope

対象システム: **PHR**

対象データ:  
HIA 側から提供される加入者 CSV（export）

処理フェーズ:

1. CSV Import
2. Staging 保存
3. Prepare / Compare
4. Subscriber Apply
5. Address / Contact Apply
6. Subscriber Audit 生成

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
    staging と subscribers / address / contact の比較・apply_action 作成仕様

subscriber_apply.md
    staging → subscribers 反映仕様
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

以下は **履歴管理テーブルのため audit しない**

```
subscriber_addresses
subscriber_contacts
```

変更履歴は

```
valid_from
valid_to
is_current
```

で管理する。

---

### 3. Subscriber Identity

subscriber の同一人物判定は以下の組み合わせを使用する。

```
person_id_custom
name_kana_full
gender_code
```

詳細は `ADR-0008` を参照。

---

### 4. Import / Prepare / Apply の分離

旧実装では apply スクリプト内で以下を同時に行っていた。

- 既存 subscribers 照合
- 差分比較
- insert / update / noop 判定
- subscribers 更新
- address / contact 同期
- audit 保存

ADR-0021 以降は、比較結果を staging 側へ保持し、apply は判定済み action を実行するだけにする。

```text
import
  = CSV → staging

prepare / compare
  = staging と本番テーブルを比較し、apply_action / diff を作成

apply
  = apply_action に従って insert / update / noop を実行
```

---

### 5. HIA を最新正本として扱う

HIA export subscribers CSV を最新正本として扱う。

`subscribers` は業務参照用の現在状態キャッシュであり、
差分がある場合は audit を必ず保存したうえで HIA 側値へ追従する。

---

### 6. audit は必ず保存する

HIA側値を正として反映するため、更新前後の差分は必ず audit に残す。

identity_hash 変更、住所変更、連絡先変更も audit / 履歴管理対象とする。

---

## Implementation Entry Points

# 旧実装
scripts/work_folder/scripts/import_subscribers_to_staging_hub.py
scripts/work_folder/scripts/apply_subscribers_from_staging_hub.py

# 新構成予定（ADR-0021）
scripts/hia/import_subscribers_to_staging_hub.py
scripts/hia/prepare_subscriber_apply_actions.py
scripts/hia/apply_subscribers_from_staging_hub.py
scripts/hia/script_lib/

---

## Version

PHR v1.1.0

### Next Refactor Direction (ADR-0021)

今後の実装では、旧 `import → apply` 構成を以下へ再整理する。

```text
import
  ↓
prepare / compare
  ↓
apply
```

主な変更方針:

- `scripts/work_folder/scripts/` から `scripts/hia/` へ移設
- orchestration と処理関数を分離
- 比較結果を staging 側へ保持
- HIA を最新正本として扱う
- identity_hash 変更も audit 保存のうえ subscribers へ反映
- name_kana_match 変更時は既存 name parts をクリアする

### v1.1.0 Changes (Subscriber Apply / Identity Handling)

背景:
- HIA 由来データで `name_kana_given` に `name_kana_full` をそのまま格納していた
- その結果、parts 列が「空欄ではない」と判定され、fund 由来の正しい分割値が上書きされない問題が発生
- `NULL` 判定のみだったため、空文字（''）が未設定として扱われなかった

対応:
- parts 列は「分割できた場合のみ格納」、それ以外は NULL を保持
- 未設定判定を「NULL または空文字」に統一
- identity 生成は parts に依存せず `name_kana_full` ベースで実施

効果:
- fund 側の高精度な name split が正しく反映される
- HIA / fund のデータ整合性が安定
- 再処理・バックフィル時の安全性向上