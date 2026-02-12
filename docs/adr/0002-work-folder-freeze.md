

# ADR 0002: work_folder v1.0 Freeze（意味固定）

## ステータス
Accepted（2026-02-12）

---

## 背景（Context）

ADR 0001 にて `kenshin_list_pydir` を中心とした Baseline Freeze を定義したが、
その後、`scripts/work_folder/` 系（hub/fund 取込・apply・共通lib・mat）についても
実質的に基盤として使用していることが明確になった。

しかし、以下の点が暗黙知のままであった：

- work_folder が前提としている DB スキーマ座標
- `mat/`（person_id_custom 生成仕様）の配置と変更ポリシー
- SQLite 系（fund_enrollee_loader）との関係整理

そのため、v1.0-freeze タグ付与時点での「現状の意味」を明文化し、
リファクタとは別に“契約として固定”する必要があった。

---

## 決定（Decision）

### 1. v1.0-freeze タグで work_folder を意味固定対象とする

対象ディレクトリ：

- `scripts/work_folder/`
  - import_subscribers_to_staging_hub.py
  - import_subscribers_to_staging_fund.py
  - apply_subscribers_from_staging_hub.py
  - lib/*（db / etl / normalize / errors / custom_id_gen など）
  - mat/*（custom_id_config.json / custom_id_mapping.json / README.md）

目的：
- ロジック凍結ではなく「現状の意味・前提・契約の明文化」
- docstring / README による仕様の可視化

---

### 2. DB座標の固定（v1.0前提）

v1.0 現状において、work_folder が参照する主要テーブルは
**すべて `dev_phr` スキーマに存在する前提**とする。

対象例：

- staging_subscribers_hub
- staging_subscribers_fund
- subscribers
- templates
- template_mappings
- funds
- fund_insurer_numbers
- etl_runs / etl_errors

将来的にスキーマ分離を行う場合は v2 扱いとする。

---

### 3. mat（person_id_custom 生成仕様）の扱い

配置：
- `scripts/work_folder/mat/`

必須ファイル：
- custom_id_config.json
- custom_id_mapping.json（mapping_file）

方針：
- mat は「仕様そのもの」であり、変更＝ID仕様変更
- v1.0 中は変更禁止
- 変更する場合は `mat/v2/` 等でバージョン分離し、`--mat` 引数で切替える

---

### 4. legacy（SQLite）系の整理

`scripts/fund_enrollee_loader/` は：

- SQLite（hub_stg/hub_prod）前提
- v1.0 正規運用対象外
- legacy として明示

新規拡張は work_folder 系で行う。

---

## 影響（Consequences）

### 良い点

- work_folder の前提が明文化され、属人性が低減
- ID仕様（mat）の変更リスクを明示化
- SQLite / MySQL の系統が整理された

### 制約

- dev_phr 前提が強くなる
- mat の変更には明示的なバージョン戦略が必要

---

## 今後（Next）

- fund templates / template_mappings の seed を固定化
- v2 設計時に DB スキーマ分離を再検討
- ID仕様のバージョニング戦略を設計

---

## 関連

- ADR 0001: baseline-freeze-v1.0
- Git Tag: v1.0-freeze
