

# csv ライブラリ

## 概要

本ディレクトリは、CSVファイルの読み込みに関する共通ライブラリを配置する。

現在は以下のモジュールを提供する。

- `csv_loader.py`

本ライブラリは、CSV読込処理の共通化と、スクリプト間での実装ブレ防止を目的とする。

---

## 配置

```text
scripts/lib/csv/
  ├── csv_loader.py
  └── README.md
```

---

## csv_loader の責務

`csv_loader` は以下の責務のみを持つ。

- CSVファイルのオープン
- エンコーディング判定（UTF-8 / UTF-8 BOM / CP932）
- BOM除去
- delimiter処理
- ヘッダー読込（複数行対応）
- ヘッダー → index マッピング生成
- 行イテレータ提供
- 行数カウント

### 非責務

以下は本ライブラリの責務外とする。

- mapping適用
- norm / match 生成
- businessロジック
- DB処理

---

## 使い方

### 基本

```python
from scripts.lib.csv.csv_loader import load_csv

loader = load_csv(
    path="input/06139463/sample.csv",
    header_count=1,
)
```

---

### ヘッダー取得

```python
headers = loader.get_headers()
header_map = loader.get_header_dict()

# 例: "氏名（カナ）" のindex
idx = header_map["氏名（カナ）"]
```

---

### 行処理（list）

```python
for row in loader.iter_rows():
    print(row)
```

---

### 行処理（dict）

```python
for row in loader.iter_dict_rows():
    print(row["氏名（カナ）"])
```

---

### 行数取得

```python
row_count = loader.count_rows()
```

---

## 設計方針

- CSVの「読み込み」はすべて本ライブラリを経由する
- 個別スクリプトでの独自CSV処理は禁止
- 仕様変更（header拡張など）は本ライブラリ側で吸収する
- 呼び出し側の書き方は将来にわたって固定化する

---

## 今後の拡張候補

- delimiter自動判定
- header validation
- ログ出力
- エラー分類（フォーマット不正 / encoding不正 など）
