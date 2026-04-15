# XPath記載ポリシー（phase4 共通）

## ■ 目的

本ポリシーは、厚生労働省 第4期XML仕様におけるXPathの扱い方を定義する。

XPathは単なる補助情報ではなく、以下に直接関与する。

- XML生成処理（export）
- XML解析処理（import / validation）
- チェック・補正ロジック（check / fix）

そのため、phase4における辞書化・spec整理では、XPathを一次情報の一部として扱う。

---

## ■ 基本方針

### 1. XPathは省略しない

- XPathは実装参照上重要なため、省略しない
- 「構造がわかるから不要」という扱いはしない

### 2. XPathは一次情報ベースで記載する

- 厚労省PDF（表・サンプル）に基づいて記載する
- 推測・補完によるXPath生成は禁止する

### 3. XPathは仕様情報として扱う

- 実装都合の派生情報ではない
- spec（辞書）の一部として保持する

---

## ■ 記載対象

XPathは以下の単位で整理する。

### セクション単位

- ClinicalDocument 配下の section
- 例：90010 指導共通情報セクション

### エントリ単位

- section / entry / observation

### 属性単位

- code/@code
- code/@codeSystem
- value/@xsi:type
- value/@code
- value/@displayName

### その他重要ノード

- recordTarget
- participant
- documentationOf

---

## ■ 記載レベル

XPathの記載レベルは、以下のいずれかとする。

### レベル1：セクションパス

例：

- `/ClinicalDocument/component/structuredBody/component/section`

### レベル2：entryパス

例：

- `/ClinicalDocument/.../section/entry/observation`

### レベル3：項目レベル

例：

- `/ClinicalDocument/.../observation/code/@code`
- `/ClinicalDocument/.../observation/value`

※ 原則として、**仕様として必要な粒度（項目単位）まで記載する**

---

## ■ ファイル分割方針

XPathは記述量が大きくなりやすいため、以下の方針で管理する。

### パターンA：本体specに含める

- XPath量が少ない場合
- セクション単位で完結する場合

### パターンB：補助ファイルへ分離

- XPathが長大になる場合
- observation単位で多数存在する場合

例：

- `90010_section_spec.md`（概要・構造）
- `90010_xpath_reference.md`（詳細XPath）

---

## ■ 参照関係の明示

補助ファイルへ分離した場合は、必ず以下を記載する。

- 本体spec側
  - XPath詳細の記載先ファイル名
  - 一次情報の参照元（PDF名・表番号）

- 補助ファイル側
  - 対象sectionコード（例：90010）
  - 対象表（例：表19）

---

## ■ 一次情報の明示ルール

XPathは必ず一次情報と紐付けて記載する。

例：

- 出典：特定保健指導情報ファイル仕様書（5-1A.pdf）
- 表：表19
- ページ：該当ページ番号

---

## ■ 禁止事項

以下は禁止とする。

- PDF未確認のXPathを推測で書くこと
- 実装コードから逆算してspecを作ること
- XPathを省略し、文章説明のみで済ませること

---

## ■ このポリシーの適用範囲

本ポリシーは以下に適用する。

- phase4/common
- phase4/shg
- phase4/health_examination

---

## ■ 補足

XPathは「後で使うための情報」ではなく、
**辞書化の時点で確定しておくべき仕様情報**である。

この前提により、

- 実装時の迷いを排除する
- check / fix ロジックの基準を統一する
- RAG（AnythingLLM）での参照精度を向上させる

ことを目的とする。
