# SHG XML ライブラリ

## 目的

本ディレクトリは、特定保健指導結果XMLから値を抽出するためのライブラリを配置する。

`check_shg_result_xml.py` から XML構造依存の処理を切り離し、XMLからの値取得と、その値の利用ロジックを分離することを目的とする。

## 役割

この層の責務は以下に限定する。

- XMLから対象項目の値を取得する
- section / observation / value の構造をたどる
- report_code などの基本情報を取得する
- XML構造変更時の影響をこの層で吸収する

## 責務外

この層では以下を行わない。

- identity生成
- person単位集約
- CSV出力
- DB参照
- 判定ロジックの集約

これらは `scripts/shg/check_shg_result_xml.py` 側の orchestration 層で扱う。

## 想定構成

```text
scripts/lib/shg/xml/
  ├ common.py
  ├ basic.py
  ├ role.py
  ├ section_90030_initial.py
  ├ section_90060_final.py
  ├ section_90070_support_summary.py
  └ README.md
```

## 各ファイルの役割

### common.py
- XML探索の共通処理
- namespace（NS）の定義
- section / observation / value の探索関数
- 値取得の共通ヘルパ（code / value / displayName / 数値変換など）

※ 各sectionファイルは common.py を利用し、XML構造の探索ロジックを重複定義しない

### basic.py
- XMLの基本情報抽出
- report_code
- insurer / symbol / number
- name / gender / birth
- ticket_no / ticket_exp

### role.py
- report_code から `initial` / `final` を判定するロジック

### section_90030_initial.py
- 90030 初回面談情報セクションの抽出
- 目標
- 初回面談に関する基本情報

### section_90060_final.py
- 90060 最終評価セクションの抽出
- 達成状況
- アウトカムポイント
- 最終腹囲 / 最終体重などの結果値

### section_90070_support_summary.py
- 90070 支援実施内容セクションの抽出
- 支援手段ごとの回数・時間の集計値
- 90040（支援明細）の集約結果

## 利用箇所

- `scripts/shg/check_shg_result_xml.py`

## 設計方針

- `xml.etree.ElementTree` を前提とする
- 旧スクリプトの `lxml + xpath` 依存をそのまま持ち込まない
- 可能な限り「値取得」に責務を限定する
- 600行以上の単一スクリプトを避けるため、小さい関数へ分割する
- SHG XML 特有の処理として閉じ、安易に共通lib化しない
- namespace（NS）およびXML探索ロジックは common.py に集約し、各sectionでは再定義しない

## 補足

- 本ディレクトリは ADR-0018 に基づく
- 詳細仕様は `docs/spec/shg_xml/README.md` を参照する