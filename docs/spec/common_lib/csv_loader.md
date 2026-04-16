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

## 想定API

```python
load_csv(
    path: str,
    *,
    header_count: int = 1,
    disp_mode: int = 0,
    delimiter: str | None = None,
    count_rows: bool = False,
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

- `disp_mode`
  - 表示用ヘッダー生成モード
  - 無指定時は `0`
  - mode追加はライブラリ側で吸収し、呼び出し側の書き方は固定する

- `delimiter`
  - 明示指定時はその delimiter を使う
  - 無指定時は自動判定する

- `count_rows`
  - `True` の場合、行数をカウントする
  - `False` の場合、行数カウントは省略可能とする

---

## 戻り値規格

戻り値は、少なくとも以下の情報を保持する規格化オブジェクトとする。

### CsvHeaderSet

- `key_headers`
  - 実処理で使うヘッダー名一覧

- `disp_headers`
  - 表示・ログ用ヘッダー名一覧

- `header_index_map`
  - `key_headers` を基準にした `header -> index` 辞書

- `header_count`
  - 実際に処理したヘッダー行数

### CsvLoadResult

- `path`
- `file_name`
- `encoding`
- `delimiter`
- `bom_removed`
- `headers` (`CsvHeaderSet`)
- `total_line_count`
- `data_row_count`
- `rows`

---

## ヘッダー処理方針

### 基本方針

- ヘッダー処理は `csv_loader` 側で共通化する
- 呼び出し側は `header_count` / `disp_mode` を渡すだけでよい
- ヘッダー処理ロジックの追加・変更はライブラリ側だけで吸収する

### 1行ヘッダー

- `header_count=1`
- `key_headers` = 1行目
- `disp_headers` = 1行目

### 2行ヘッダー

- `header_count=2`
- `key_headers` は最下段ヘッダーを基準とする
- `disp_headers` は `disp_mode` に応じて生成する

### disp_mode

初期仕様では以下を持つ。

- `0`
  - `disp_headers = key_headers`

- `1`
  - 上段 + 下段を結合した表示ヘッダーを生成する
  - 例: `基本情報 / 氏名（カナ）`

将来 `disp_mode` の種類が増えても、呼び出し側のAPIは変えない。

---

## BOM処理

- UTF-8 BOM は `csv_loader` 側で除去する
- BOM除去有無は `bom_removed` に保持する
- 呼び出し側で BOM を意識させない

---

## delimiter処理

### 基本方針

- `delimiter` が明示指定されている場合は、その値を使用する
- 無指定の場合はライブラリ側で自動判定する

### 初期対応候補

- `,`
- `\t`
- `;`

---

## 行数カウント

### 基本方針

- `count_rows=True` の場合のみ行数を数える
- 返却値は以下を基本とする

- `total_line_count`
  - ヘッダー含む総行数

- `data_row_count`
  - ヘッダー除外後のデータ行数

---

## ヘッダー辞書化方針

### 基本方針

- `header_index_map` は `key_headers` を基準に作る
- 後続の mapping / rule は `key_headers` を使って値取得する

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
result = load_csv(
    path=csv_path,
    header_count=2,
    disp_mode=1,
    count_rows=True,
)

headers = result.headers.key_headers
header_map = result.headers.header_index_map
encoding = result.encoding
rows = result.rows
```

---

## 設計意図

- CSV読み込みの入口を統一する
- 各importスクリプトで毎回ヘッダー処理を書かない
- 後から `disp_mode` や `header_count` の対応を増やしても、呼び出し側を変えない
- 共通化できるI/O処理は共通ライブラリへ寄せる

---

## 本specで次に詰めること

- `CsvLoadResult` / `CsvHeaderSet` の最終データ構造
- delimiter 自動判定の実装方針
- 文字コード判定ロジック
- 空行・不正列数行の扱い
- 例外クラス設計