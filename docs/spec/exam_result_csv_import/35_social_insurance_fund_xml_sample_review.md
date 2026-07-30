# 支払基金 特定健診XMLサンプル確認

## 対象

- 保存先: `downloads/kensin_kihon_tokutei.xml`
- SHA-256: `0b78e3401e96b631f8d450d765ed4fcb159a8a7d2a28fce8fb15cf9a10ccbe8d`
- 形式: 厚生労働省V08 特定健診情報XML
- 報告区分: `10`
- 健診プログラム: `010`
- 健診実施日: `2024-04-15`

リポジトリ内の `hc08_V08.xsd` で適合を確認した。

## 含まれる種類

サンプルは、CSVからXMLを作る際に必要な代表構造を一つの個人XMLで確認できる。

- 基本的な健診・問診結果: section `01010`
- 医師判断で行う詳細健診: section `01010` 内の一連検査グループ
- 保険者等が任意に追加する項目: section `01990`

値型は `PQ`、`CD`、`CO`、`ST` を含む。さらに `nullFlavor`、`negationInd`、`methodCode`、`interpretationCode`、`referenceRange` を含む。

## 一連検査グループ

詳細健診等は、対象namecodeをsection直下へ平坦に並べず、`<code nullFlavor="NA"/>` を持つ親observationの下へ束ねる。

| グループ | 識別値 | 主なCOMP | 主なRSON |
| --- | --- | --- | --- |
| 貧血検査 | `2A020161001930149` | ヘマトクリット、血色素量、赤血球数 | 実施理由 |
| 心電図 | `9A110161000000049` | 所見有無、所見 | 対象者、実施理由 |
| 眼底検査 | `9E100161000000049` | 分類結果 | 対象者、実施理由 |
| 血清クレアチニン | `3C015161002399949` | クレアチニン、eGFR | 対象者、実施理由 |

付属2 `001082795.xlsx` の `一連検査グループ識別` と `一連検査グループ関係コード` を確認すると、53 namecodeに `COMP` または `RSON` が定義されている。
CSVフォーマットや健診機関別ruleから推測せず、この付属2定義を `exam_item_master` へ保持してXMLを組み立てる。

## 判定と基準範囲

- `interpretationCode` は、原本CSVに判定列があり、mappingで明示的に取り込まれた場合だけ出力する。
- 数値から `N` / `H` / `L` を自動判定しない。
- 健診機関固有のABC判定や総合判定を、`interpretationCode` として推測出力しない。
- 原本の基準下限・上限は、値がある場合だけ `referenceRange/observationRange/value` の `low` / `high` へ出力する。
- 基準範囲の単位は検査値と共通とし、初期版では単位変換しない。

## 実装への反映

- `exam_item_master` に付属2の一連検査グループ2列を追加する。
- 既存DBは `20260730_003_dev_phr_add_annex2_series_group_to_exam_item_master.sql` の後、`0006_dev_phr__exam_item_master_annex2_series_groups.sql` を適用する。
- XML builderは同じグループ識別値の項目を一つの親observationへまとめ、各子を `COMP` / `RSON` で出力する。
- `exam_item_values.interpretation_*` と `source_reference_lower` / `source_reference_upper` は明示値がある場合だけ出力する。
- 値なし・`negationInd=true` の項目は、支払基金サンプルと同じくvalue要素を作らない。
