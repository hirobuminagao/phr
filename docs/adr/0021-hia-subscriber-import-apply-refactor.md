

# ADR-0021: HIA加入者 import / compare / apply フロー再設計

## Status

Draft

---

## Context

現行の HIA 加入者取り込み処理は、`scripts/work_folder/scripts/` 配下を起点に段階的に拡張されてきた。

現状は以下の責務が密結合している。

- CSV import
- 正規化（norm / match / hash）
- staging insert
- subscribers 既存照合
- insert / update / noop 判定
- subscribers 更新
- address / contact 同期
- audit 保存
- processed 管理

特に `apply_subscribers_from_staging_hub.py` は、
1行ごとに複数 SELECT / UPDATE / INSERT を実行する構造となっており、
処理時間・責務分離・保守性の観点で課題が大きくなっている。

また、現在の構造では「比較」と「適用」が同時に行われるため、
以下の問題が発生しやすい。

- noop 行でも address / contact を毎回確認する
- identity_hash 変更時の扱いが不明瞭
- apply 判断根拠が staging 側へ保持されない
- HIA 最新情報への追従ルールが曖昧
- audit はあるが、比較結果の状態保持が不足している

---

## Decision

HIA 加入者同期処理を、以下の phase に分離する。

```text
import
  ↓
current snapshot update
  ↓
prepare / compare
  ↓
apply
  ↓
audit
```

また、既存の `scripts/work_folder/scripts/` 起点の構造を整理し、
`scripts/hia/` 配下へ移設・再編成する。

---

## New Structure

```text
scripts/hia/
├── import_subscribers_to_staging_hub.py
├── apply_hia_subscriber_sync.py
└── script_lib/
    ├── hub_subscriber_import.py
    ├── hub_subscriber_current_snapshot.py
    ├── hub_subscriber_prepare.py
    ├── hub_subscriber_apply.py
    ├── hub_subscriber_compare.py
    ├── hub_subscriber_audit.py
    └── ...
```

方針:

- orchestration は entrypoint に限定する
- 実処理は script_lib へ寄せる
- 共通処理は既存 `scripts/lib/` を利用する
- DB接続・identity normalize・audit 共通化を継続する

運用方針:

- `import_subscribers_to_staging_hub.py`
  - CSV → staging import
  - current subscriber state snapshot 更新
  - 本番 subscriber 更新は行わない

- `apply_hia_subscriber_sync.py`
  - prepare / compare 実行
  - apply_action 決定
  - 本番 subscriber 更新
  - audit 保存

---

## Import Phase

責務:

- CSV 読み込み
- raw / norm / match / hash 生成
- staging insert
- current subscriber state snapshot 更新
- ETL run / error 管理

非責務:

- subscribers 更新
- address 更新
- contact 更新
- insert / update / noop 判定

Import phase は:

```text
CSV → staging canonicalization
+ current state snapshot 更新
```

までを責務とする。

本番 subscriber 更新は行わない。

### Current Snapshot Policy

import phase では、本番 subscriber 系 current 状態を staging 側へ snapshot として保持する。

目的:

- import 完了時点で「本番に既存 subscriber が存在するか」を人間が確認可能にする
- apply 前レビューを容易にする
- compare phase の入力を固定化する

snapshot 対象例:

```text
current_subscriber_id
current_identity_hash
current_name_kana_full_match
current_address_id
current_contact_id
current_lookup_status
current_lookup_checked_at
```

import phase は current 状態を参照するが、本番 subscriber を更新しない。

---

## Prepare / Compare Phase

責務:

- staging snapshot と current state の比較
- HIA subscriber ID 比較
- identity_hash 比較
- address 比較
- contact 比較
- diff columns 作成
- apply_action 決定

Prepare / compare phase は、import phase が staging 側へ保持した current snapshot を利用して比較を行う。

比較結果は staging 側へ保持する。

想定カラム例:

```text
matched_subscriber_id
apply_subscriber_id
apply_action
apply_diff_columns
identity_match_status
address_diff_status
contact_diff_status
apply_checked_at
```

apply_action 例:

```text
insert
update
noop
identity_changed
review
```

---

## Apply Phase

Apply phase は「判定済み action を実行するだけ」とする。

Apply phase 自身は compare 判定を行わない。

Apply phase は:

```text
prepare / compare 済み staging row
```

のみを入力とする。

例:

```text
apply_action = insert
  → insert

apply_action = update
  → update

apply_action = noop
  → skip
```

---

## HIA Source of Truth Policy

HIA 側情報を最新正本として扱う。

```text
HIA CSV
  = 最新正本

subscribers
  = 業務参照用キャッシュ
```

そのため、compare phase において差分が存在する場合、
原則として HIA 側値を subscribers へ反映する。

---

## Audit Policy

audit は必ず保存する。

以下は必須保持対象とする。

- import 実行
- compare 判定
- apply 実行
- subscribers 更新前後差分
- address 更新前後差分
- contact 更新前後差分
- identity_hash 変更

---

## identity_hash Change Policy

identity_hash が変更された場合でも、
HIA 側を正として subscribers.identity_hash を更新する。

ただし、parts 系は差分内容に応じて扱いを変更する。

### 記号・番号変更のみ

```text
insurance_symbol
insurance_number
```

のみ変更の場合:

```text
parts は維持する
```

### 氏名カナ match 変更

```text
name_kana_match changed
```

の場合:

```text
既存 parts をクリアする
```

理由:

旧 parts が新 full name と不整合になる可能性が高いため。

parts クリア後は、後続 normalize / split により再生成する。

---

## Consequences

期待される効果:

- apply 処理時間削減
- compare と apply の責務分離
- noop 行の不要処理削減
- HIA 最新状態との同期ルール明確化
- identity_hash 変更時の扱い明文化
- staging を比較結果保持レイヤとして利用可能
- audit / tracing 強化
- scripts/hia への責務整理
- import 完了時点で current subscriber 状態を可視化可能
- apply 前レビュー容易化
- compare 入力の固定化
- orchestration と実処理の責務分離

一方で、以下の追加実装が必要となる。

- prepare / compare phase 新規実装
- staging compare 系カラム追加
- apply_action 管理
- address/contact compare 設計見直し
- orchestration 再構築