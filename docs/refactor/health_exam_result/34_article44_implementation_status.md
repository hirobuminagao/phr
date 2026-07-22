# Article44実装後の現状整理

## 目的

本資料は、`33_check_framework_design.md` 以降に実装で確定した内容と、既存設計資料との差分を整理する。

本資料は課題管理表ではなく、`05_design_history.md` の後続として、Article44判定基盤の現在地を記録するための実装後整理資料である。

過去資料に残る旧72項目方式や特定健診集約方針は履歴として残しつつ、本資料では現時点の実装上の正を明確にする。

---

## 2026-07-22 現在の正

### Article44法定判定

現行の法定健診判定は、旧72項目横持ち方式ではなく、労働安全衛生規則第44条の法令項目詳細No 23項目を判定単位とする。

判定結果は `Article44Result` として生成し、`exam_check_results` には以下の横持ち列へ保存する。

```text
a44_<法令項目詳細No>_status
a44_<法令項目詳細No>_reason
```

現行DDLでは23項目分、合計46列を保持する。

### 実装ルート

`03_check_exam_results.py` は、以下のArticle44ルートへ接続済みである。

```text
fetch_article44_required_namecodes()
    ↓
load_article44_value_map()
    ↓
check_article44()
    ↓
Article44Result
    ↓
exam_check_results a44 46列保存
    ↓
legal_check_result / legal_reason_summary
    ↓
xml_ledger.check_status / check_reason
```

`required_namecodes` はrun単位で1回取得し、`ValueMap` は `xml_ledger` 単位で生成する。

### 特定健診

特定健診判定は現行Article44実装では未接続とする。

`specific_check_result` / `specific_reason_summary` は、現行Article44実行ルートではNULL保存とする。

特定健診を旧72項目処理から推測して接続しない。特定健診は別フェーズで再設計する。

### legal reason summary

`legal_reason_summary` および `xml_ledger.check_reason` は、検索性と人間の確認しやすさを両立するため、同じ文字列を二重保持する。

形式は以下とする。

```text
<法令項目詳細No>:<日本語項目名>:<reason>
```

例:

```text
4403003001:腹囲:MISSING | 4411001001:心電図:DUPLICATE_NAMECODE:count=2
```

全23項目がOK相当の場合、`legal_reason_summary` と `xml_ledger.check_reason` はNULLとする。

---

## 実装済み内容

### 第1層 required namecodes

`article44_required_namecodes.py` は、`dev_phr.exam_item_group_members` からArticle44判定に必要なnamecode定義を取得する。

現行seedは以下を正とする。

```text
v2_2026_ARTICLE44_CHECK_ITEMS
```

現行seed上のArticle44 required namecodesは73件である。

これは判定項目数23件とは別であり、複数namecodeを組み合わせて1つの法令項目詳細Noを判定するためである。

### 第2層 ValueMap

`article44_value_loader.py` は、`required_namecodes` に含まれる全namecodeを `ValueMap` のキーとして返す。

値がDBに存在しない場合もキーを省略せず、期待値型に応じた `PQValue` / `CDValue` / `STValue` を `value_state=NOT_FOUND` として返す。

`ValueMap` の型は以下とする。

```python
ValueMap = dict[str, PQValue | CDValue | STValue]
```

`CO` は現バージョンでは `CDValue` として扱う。

### section_code優先

`exam_item_values` には親CDA section情報を保存する。

```text
section_code
section_code_system
section_name
```

Article44のValueMap生成では、同一namecodeが複数sectionに存在する場合、`section_code = '01030'` を優先する。

現行方針は以下である。

```text
01030が1件
    → 01030を採用

01030が複数件
    → DUPLICATE_NAMECODE

01030が無く、他sectionが1件
    → 互換性のため採用

01030が無く、他sectionが複数件
    → DUPLICATE_NAMECODE
```

この方針により、特定健診セクションやがん検診セクションに同じnamecodeが存在しても、労働安全衛生法健診セクションの値を優先できる。

### interpretationCode保存

`exam_item_values` には、observation直下の `interpretationCode` を保存する。

```text
interpretation_code
interpretation_code_system
interpretation_name
```

これは医療機関側の高値・低値・正常などの解釈コードを受け止めるための情報である。

現時点ではArticle44の必須項目判定には使用しない。

将来、異常値一覧、受診勧奨、PHR表示、医療機関側判定との比較などで利用する可能性がある。

### 第3層 checker

`article44_checker.py` は、法令項目詳細No 23項目のcheckerを持つ。

`ARTICLE44_CHECKERS` のキーは、`exam_check_results` の `a44_*` 列と一致する。

腹囲・血糖などの複数成立経路は、制度上許容された成立経路として扱う。

単にfallback的に見える経路であっても、制度上同格または許容された入力であれば `ALTERNATIVE` にはしない。

`ALTERNATIVE` は、checkerが明示的に代替扱いする経路に限定する。

### 結果保存

`03_check_exam_results.py` は、`Article44Result` を `a44_<detail_no>_status` / `a44_<detail_no>_reason` へ展開して保存する。

`legal_check_result` は以下で集約する。

```text
全23項目が OK / CALCULATED / ALTERNATIVE
    → OK

1件でも MISSING / INVALID
    → NG
```

Article44項目statusとして `WARNING` / `NG` は使用しない。

---

## 既存資料との差分

|資料|旧記述・未反映内容|現行実装|扱い|
|---|---|---|---|
|`03_decisions.md`|旧72項目横持ち方式を前提とする記述が残る|Article44 23項目方式へ置換済み|履歴として残し、後続決定で置換済みと扱う|
|`03_decisions.md`|法定健診・特定健診を72項目から独立集約する記述が残る|現行はArticle44法定のみ接続、特定健診は未接続|特定健診は別フェーズ|
|`33_check_framework_design.md`|section優先取得が未同期|`section_code='01030'` 優先を実装済み|33へ同期候補|
|`33_check_framework_design.md`|interpretationCode保存が未同期|importで保存済み、判定未使用|33へ同期候補|
|`33_check_framework_design.md`|Article44Resultから横持ちカラムへの変換が保留事項に残る|`03_check_exam_results.py` で実装済み|33へ同期候補|
|会話・一部テスト表現|Article44 required namecodesを72件として扱う表現が残る|現行seedでは73件|説明・テスト名の修正候補|

---

## 実DB確認で必要な作業

section情報とinterpretationCodeはimport時に保存するため、既存import済みデータには反映されない。

実データで確認する場合は、section関連migrationとinterpretationCode関連migrationを適用した上で、対象import結果を削除し、再importする必要がある。

check結果だけを削除して `03_check_exam_results.py` を再実行しても、既存 `exam_item_values` に `section_code` が入っていないため、section優先ロジックの確認にはならない。

必要な流れは以下である。

```text
1. health_exam_result migration適用
2. 対象import_run_idのimport/check結果を削除
3. 02_import_xml.py 再実行
4. 03_check_exam_results.py 再実行
5. NG理由再集計
6. DUPLICATE_NAMECODE減少確認
```

---

## 現時点で意図的に行わないこと

- 特定健診判定の接続。
- 旧72項目法定ルートの復活。
- `interpretationCode` をArticle44必須項目判定へ利用すること。
- `section_code='01030'` 以外を一律に不正扱いすること。
- 法令項目詳細Noマスターを新規DBテーブル化すること。
- 03の旧決定事項を大規模に削除・改稿すること。

---

## 次アクション

### DOC-001 33への同期

`33_check_framework_design.md` へ以下を同期する。

- `section_code='01030'` 優先取得。
- `interpretationCode` import保存。
- Article44 required namecodesが現行seedで73件であること。
- Article44Resultからa44 46列への保存展開が実装済みであること。

### DOC-002 03への最小追記

`03_decisions.md` は過去決定の履歴として残す。

必要な場合のみ、旧72項目方式は後続のArticle44 23項目方式により置換済みである旨を短く追記する。

### OPS-001 実DB再import

section情報とinterpretationCodeを含む状態で、対象データを再importする。

再check後、`DUPLICATE_NAMECODE` とNG理由を再集計する。

### SPC-001 特定健診

特定健診はArticle44とは別フェーズで扱う。

旧72項目処理をそのまま復活させず、特定健診側の目的・保存先・総合判定への影響を再整理してから接続する。

