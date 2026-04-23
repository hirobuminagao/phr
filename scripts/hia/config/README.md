# HIA 設定ファイル一覧（scripts/hia/config）

このディレクトリは、HIA系スクリプトの実行設定を定義するYAMLファイルを格納する。
各設定ファイルは「どのスクリプトに対する設定か」を明示し、運用時の再現性と可読性を確保する。

---

## 1. snapshot_hia_dashboard_year_end_status.yml

### 対象スクリプト
`scripts/hia/snapshot_hia_dashboard_year_end_status.py`

### 目的
HIAダッシュボードの年度末状態をスナップショットテーブルへ固定するための設定

---

### 設定項目

#### fiscal_year
- 型: int
- 必須: ✔
- 内容: 固定する年度（例: 2025）

---

#### target_mode
- 型: string
- 必須: ✔
- 値:
  - `all`      : 全保険者を対象
  - `selected` : 指定保険者のみ対象

---

#### insurer_numbers
- 型: list[string]
- 必須: target_mode=selected の場合 ✔
- 内容:
  - 対象とする保険者番号の一覧
  - 文字列で指定（先頭ゼロ保持）

例:
```
insurer_numbers:
  - "12345678"
  - "87654321"
```

---

#### on_conflict
- 型: string
- 必須: ✔
- 値:
  - `error`     : 既存データがある場合は処理停止（推奨）
  - `overwrite` : 既存データを削除して再作成

---

#### notes
- 型: string
- 必須: 任意
- 内容:
  - 実行理由・備考など自由記述

---

## 設計方針

- 設定は「実行条件の記録」として扱う
- CLI引数ではなくYAMLで管理し、再現性を担保する
- スクリプトはこの設定を唯一の入力とする
- スナップショットは「事実の固定」であり、加工・補正は行わない

---

## 命名規則

- ファイル名: `処理内容 + 対象テーブル`
- 例:
  - snapshot_hia_dashboard_year_end_status.yml

---

## 注意事項

- YAMLはインデント（スペース2つ）を厳守
- 文字コードはUTF-8
- コメントは `#` で自由に記述可能

---

## 2. from_dev_team_to_subscribers_hia_ids.yml

### 対象スクリプト
`scripts/hia/backfill_scripts/from_dev_team_to_subscribers_hia_ids.py`

### 目的
開発部提供のExcelデータを用いて `subscribers.hia_subscriber_id` を補完するための設定

---

### 設定項目

#### run_resolve_identity_and_subscribers_id
- 型: boolean
- 必須: ✔
- 内容:
  - `true`  : stagingテーブル上で identity_hash の生成と subscribers_id の解決を実行
  - `false` : 実行しない

---

#### run_apply_hia_subscriber_id_to_subscribers
- 型: boolean
- 必須: ✔
- 内容:
  - `true`  : subscribers テーブルへ hia_subscriber_id を反映
  - `false` : 実行しない

---

#### db_schema_staging
- 型: string
- 必須: ✔
- 内容:
  - staging テーブルが存在するスキーマ（通常: `work_other`）

---

#### db_schema_subscribers
- 型: string
- 必須: ✔
- 内容:
  - subscribers テーブルが存在するスキーマ（通常: `dev_phr`）

---

#### table_staging
- 型: string
- 必須: ✔
- 内容:
  - staging テーブル名
  - 例: `staging_hia_subscribers_master_export_ids`

---

#### table_subscribers
- 型: string
- 必須: ✔
- 内容:
  - subscribers テーブル名（通常: `subscribers`）

---

#### insurer_number
- 型: string
- 必須: ✔
- 内容:
  - identity_hash 生成に使用する保険者番号（8桁文字列）

---

#### update_policy
- 型: string
- 必須: ✔
- 値:
  - `fill_only` : subscribers.hia_subscriber_id が空の場合のみ更新（推奨）
  - `overwrite` : 既存値があっても上書きする

---

#### notes
- 型: string
- 必須: 任意
- 内容:
  - 実行理由・備考など自由記述

---

## 設計方針（開発部データ取込）

- 開発部提供データは直接 `subscribers` に投入せず、必ず staging テーブルを経由する
- identity_hash を生成し、`subscribers` と突合した上で更新を行う
- 更新は安全性を考慮し、原則 `fill_only` とする
- 処理は resolve（突合）と apply（反映）を分離し、設定で制御する

---

## 注意事項（開発部Excel）

- Excelには NULL を表す文字列として「« NULL »」が含まれる場合がある
- 投入前に必ず以下の前処理を行うこと
  - 置換前: `« NULL »`
  - 置換後: 空文字（""）
- この処理を行わない場合、identity生成および突合が不正確になる可能性がある
