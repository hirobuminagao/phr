


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
