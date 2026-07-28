# csv_loader

## 目的

本specは、CSVファイルを共通的に読み込む `csv_loader` ライブラリの責務と入出力を定義する。

本ライブラリの目的は以下とする。

- CSVファイルを安全にオープンする
- 文字コード / BOM / delimiter / header を共通的に扱う
- 後続スクリプトが同じ戻り値規格でCSVを扱えるようにする

---

## 本specの位置づけ

`csv_loader` は、個別業務スクリプトの前段に置く共通ライブラリである。

- 個別スクリプトは `csv_loader` を使って CSV を開く
- `csv_loader` は CSV を「標準化された読み込み結果」に変換する
- mapping適用、rule実行、business判定は `csv_loader` の責務に含めない

---

## 配置先

`csv_loader` の実装配置先は以下とする。

```text
scripts/lib/csv/csv_loader.py
```

### 配置方針

- `csv_loader` は個別業務処理ではなく、CSV読込の共通部品として扱う
- そのため、個別スクリプト配下ではなく `scripts/lib/csv/` 配下へ配置する
- 後続の import / export / 検証系スクリプトは、本ライブラリを共通利用する前提とする

---

## 責務

`csv_loader` の責務は以下とする。

- ファイル存在確認
- CSVオープン
- 文字コード判定
- BOM除去
- delimiter 決定
- ヘッダー読込
- ヘッダー辞書化
- 行数カウント（オプション）
- データ行イテレータ生成

---

## 非責務

以下は `csv_loader` の責務外とする。

- `fund_id` 解決
- template / mapping 解決
- rule 実行
- 値正規化
- identity生成
- subscribers照合
- 業務ロジック判定

---

## 現行API

```python
load_csv(
    path: str,
    header_count: int = 1,
    delimiter: str = ",",
    encoding: str | None = None,
    quote_char: str = '"',
) -> CSVLoader

load_csv_result(
    path: str,
    header_count: int = 1,
    delimiter: str = ",",
    encoding: str | None = None,
    quote_char: str = '"',
    active_header_row_no: int | None = None,
    data_start_row_no: int | None = None,
) -> CsvLoadResult
```

### 引数

- `path`
  - 対象CSVファイルの絶対パスまたは相対パス

- `header_count`
  - ヘッダー行数
  - 無指定時は `1`
  - `2` を指定した場合は、2行ヘッダー前提で処理する
  - 将来的には `3` 以上にも拡張可能とする

- `delimiter`
  - 無指定時は `,`
  - 現行実装では自動判定しない

- `encoding`
  - 明示指定時はその文字コードを使う
  - 未指定時は UTF-8 BOM / UTF-8 / CP932 の簡易判定を使う

- `quote_char`
  - `csv.reader` へ渡す引用符文字
  - 引用符が存在しないCSVも同じ設定で読み込める

- `active_header_row_no`
  - `load_csv_result()` で列名として扱うヘッダー行番号

- `data_start_row_no`
  - `load_csv_result()` が返すCSV上のデータ開始行番号

---

## 戻り値規格

既存 `load_csv()` は互換維持のため `CSVLoader` を返す。構造化結果が必要な処理は `load_csv_result()` を使う。

### CsvHeaderSet

- `header_rows`
- `active_header_row_no`
- `normalized_columns`
  - `column_no`, `context`, `header_name`, `occurrence` を列順込みで保持する
- `header_sha256`

### CsvLoadResult

- `path`
- `encoding`
- `delimiter`
- `quote_char`
- `header_set` (`CsvHeaderSet`)
- `rows`
- `data_start_row_no`

---

## ヘッダー処理方針

### 基本方針

- ヘッダー処理は `csv_loader` 側で共通化する
- 呼び出し側は `header_count` / `active_header_row_no` を渡す
- ヘッダー処理ロジックの追加・変更はライブラリ側だけで吸収する

### 1行ヘッダー

- `header_count=1`
- `header_rows` に1行目を保持する
- `normalized_columns` は1行目を列名として構築する

### 2行ヘッダー

- `header_count=2`
- `active_header_row_no` の行を列名として使う
- 他のヘッダー行は `context` の生成材料として保持する

---

## BOM処理

- UTF-8 BOM は `csv_loader` 側で除去する
- 呼び出し側で BOM を意識させない

---

## delimiter処理

### 基本方針

- `delimiter` が明示指定されている場合は、その値を使用する
- 現行実装では無指定時に `,` を使用する
- delimiter自動判定やfallbackは現行実装に含めない

---

## 行数カウント

`CSVLoader.count_rows()` で読込済み行数を取得する。`load_csv_result()` は初期実装ではデータ行を `rows` に保持する。

---

## ヘッダー辞書化方針

### 基本方針

- `CSVLoader.get_header_dict()` は最終ヘッダー行を基準に辞書化する
- 同名ヘッダーを一意に扱う処理では、`CsvHeaderSet.normalized_columns` の `context` / `occurrence` / `column_no` を使う

### やらないこと

`csv_loader` では以下を行わない。

- ヘッダー名の推測変換
- fuzzy match
- 業務用の別名吸収

これらは template / mapping 側で吸収する。

---

## 最低限許容する前処理

ヘッダーおよびセルの読込前後で、以下は許容する。

- BOM除去
- 前後trim
- 空行検出

一方で、ヘッダー名そのものの業務解釈は行わない。

---

## 利用イメージ

```python
result = load_csv_result(
    path=csv_path,
    header_count=2,
    active_header_row_no=2,
    encoding="CP932",
    quote_char='"',
)

headers = result.header_set.normalized_columns
encoding = result.encoding
rows = result.rows
```

---

## 設計意図

- CSV読み込みの入口を統一する
- 各importスクリプトで毎回ヘッダー処理を書かない
- 既存 `load_csv()` / `CSVLoader` の戻り値と主要メソッドを維持する
- 構造化されたヘッダー情報は `load_csv_result()` に集約する
- 共通化できるI/O処理は共通ライブラリへ寄せる

---

## 本specで次に詰めること

- delimiter 自動判定の実装方針
- 文字コードfallbackは利用側のformat照合で制御し、共通loader単体の自動判定とどう統合するか
- 空行・不正列数行の扱い
- 例外クラス設計
