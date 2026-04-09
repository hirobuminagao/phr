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
  ├ section_90010_guidance_info.py
  ├ section_90030_initial.py
  ├ section_90040_support_detail.py
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

- 保健指導実施年月日（final_date）の取得
  - report_code = 22 の場合に有効
  - `documentationOf / serviceEvent / effectiveTime` から取得

### role.py
- report_code から `initial` / `final` を判定するロジック

### section_90010_guidance_info.py
- 90010 保健指導情報セクションの抽出
- 保健指導区分（積極的支援 / 動機付け支援 など）
- level_code / level_text の元データ取得

### section_90030_initial.py
- 90030 初回面談情報セクションの抽出
- 目標
- 初回面談に関する基本情報
- 初回面接実施日（initial_date）の取得
  - 90030 内の `entry / act / effectiveTime` から取得
  - `act/codeSystem = 1.2.392.200119.6.24010` を対象とする

### section_90040_support_detail.py
- 90040 支援明細セクションの抽出
- 支援イベント単位の実施情報
- mode_code / date / minutes / points の取得
- `_total_points` / `_total_minutes` の集計

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

- XML構造は完全ではない前提で扱う
  - セクションが存在しない場合がある
  - observation / value が欠損する場合がある
  - 欠損は例外ではなく正常系とする
  - 取得できない場合は None を返却する

- OIDは仕様上の意味定義として扱う
  - XML抽出時は code 値を基準に判定する
  - codeSystem(OID)は仕様の文脈理解に使用し、分岐条件には原則使わない

- section単位で責務を分割する
  - 90010 / 90030 / 90060 / 90070 など、OIDベースでファイルを分離する
  - 各sectionは「そのセクションの値取得のみ」を責務とする

- orchestration層との責務分離
  - 本ライブラリは「値を取るだけ」
  - 値の優先順位（initial / final）や再計算ロジックは持たない
  - CSV項目とのマッピングや business rule は上位層で実装する

- 日付項目の扱い
  - initial_date は 90030 セクションから取得する
  - final_date は basic 情報（report_code=22）から取得する
    - `documentationOf / serviceEvent / effectiveTime` を使用する
  - sectionごとに責務を分け、日付も例外扱いしない

## 補足

- 本ディレクトリは ADR-0018 に基づく
- 詳細仕様は `docs/spec/shg_xml/README.md` を参照する