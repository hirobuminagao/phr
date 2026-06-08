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
subscriber audit
  ↓
processed mark

apply failure
  ↓
etl_errors
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
    ├── apply_action_subscriber.py
    ├── apply_action_subscriber_address.py
    ├── apply_action_subscriber_contact_point.py
    ├── apply_action_staging_mark.py
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
  - subscribers / address / contact point 反映
  - subscriber root 変更の subscribers_audit 保存
  - apply成功時の processed mark
  - apply失敗時の etl_errors 記帳

### Current Implementation Notes

2026-06 時点では、`apply_hia_subscriber_sync.py` は VSCode RUN ボタンで実行できるように、以下の YAML config を読む。

```text
scripts/hia/config/apply_staging_to_subscribers.yml
```

基本設定:

```yaml
import_run_id: auto
dry_run: true
limit: 0
skip_prepare: false
skip_compare: false
```

`import_run_id: auto` の場合、`staging_subscribers_hub` 内の未処理行から最新の `import_run_id` を取得する。

```sql
SELECT import_run_id
FROM staging_subscribers_hub
WHERE processed_run_id IS NULL
  AND import_run_id IS NOT NULL
GROUP BY import_run_id
ORDER BY import_run_id DESC
LIMIT 1;
```

`etl_runs` 全体の最新 run_id は参照しない。

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
current_hia_subscriber_id
current_identity_hash
current_compare_identity_norm_hash
current_compare_other_hash
current_name_kana_full_match
current_address_id
current_address_hash
current_phone_contact_point_id
current_email_contact_point_id
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
current_subscriber_id
apply_action
apply_diff_columns
identity_match_status
address_diff_status
contact_point_diff_status
apply_checked_at
```

apply_action 例:

```text
insert
update
noop
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

apply_action = review
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

## HIA Duplicate ID / 99999 Policy

HIA 側で長音符ゆれ等により同一人物が複数 HIA加入者ID として存在する場合がある。
HIA に物理削除がない、または削除運用できないケースでは、片方の記号を `99999` などに変更して別人化退避する。

この `99999` は削除フラグではない。
HIA からDLされる限り、データとしては存在する加入者レコードとして扱う。

したがって apply では:

```text
正しいHIA ID側
  → 既存 subscribers.hia_subscriber_id に紐づける

99999退避側
  → identity_hash / person_id_custom が変わる
  → not_found
  → insert対象
```

HIA 側で物理削除または論理削除が行われた場合に、`subscribers` 側で削除フラグ・inactive・retired 等を立てる処理は別フローとする。

---

## Audit Policy

audit は必ず保存する。

以下は必須保持対象とする。

- import 実行
- compare 判定
- apply 実行
- subscribers 更新前後差分
- identity_hash 変更

address / contact point は履歴型テーブルで current / history を保持する。
現時点では address / contact point 専用 audit テーブルは持たず、`subscribers_audit` への必須記帳対象にもしていない。

apply失敗は staging に `apply_error_*` を保持せず、`etl_errors` へ記帳する。

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

なお、HIA由来のカナ parts は原則として信用しない。
`name_kana_full` と `name_kana_family` 等が同じ値しか入っていない場合は未分割扱いとして parts は `NULL` にする。
また、parts 用 match列は HIA apply では使用しない。

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
- VSCode RUN ボタン前提の YAML config 運用が可能
- `import_run_id: auto` により、未処理 staging の最新 import_run_id を自動選択可能
- apply失敗を `etl_errors` で追跡可能
- address / contact point は履歴型テーブルとして管理し、subscriber audit と責務分離可能
- HIA重複IDの別人化退避（例: 記号99999）を削除扱いせず insert 対象として扱える

一方で、以下の追加実装が必要となる。

- prepare / compare phase 新規実装
- staging compare 系カラム追加
- apply_action 管理
- address/contact compare 設計見直し
- orchestration 再構築