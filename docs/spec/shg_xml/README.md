# SHG XML 処理仕様（shg_xml）

## 目的

本ディレクトリは、SHG（特定保健指導）結果XMLの解析・抽出・チェック処理に関する仕様を管理する。

本仕様は、以下の責務を対象とする。

- XMLからの値抽出（basic / role / CDAセクション別抽出）
- XML構造の理解と項目マッピング
- スクリプト処理（check_shg_result_xml.py）の設計方針

## スコープ

本specは「XML処理レイヤー」を対象とし、以下は対象外とする。

- DB設計 → `docs/spec/shg_result/`
- identity生成 → `docs/spec/identity_canonicalization/`

## ディレクトリ構成方針

## 入出力ディレクトリ仕様（固定）

本処理は VSCode Run 前提とし、入出力パスは固定とする。

```text
data/hia_export_shg/
├── input/
│   └── <root_folder_name>/
│       ├── ix08_V08.xml
│       ├── su08_V08.xml
│       ├── DATA/*.xml
│       ├── CLAIMS/
│       └── XSD/
└── output/
    └── <yyyymmdd_hhmmss>/
        ├── export_shg_report.csv
        └── export_outcome_report.csv
```

### 入力仕様

- `input/<root_folder_name>/` 配下は厚生労働省の「送付用ファイルアーカイブ仕様」に準拠する
- ZIP解凍後のルートフォルダ構造をそのまま配置する
- 本スクリプトは `DATA/*.xml` のみを解析対象とする
- `ix08_V08.xml`、`su08_V08.xml`、`CLAIMS/`、`XSD/` は保持するが、fase1.0では解析対象外とする

### 出力仕様

- 実行ごとに timestamp フォルダを生成
- 同一入力でも複数回実行した結果を保持する

```text
scripts/lib/shg/xml/
  ├ basic.py
  ├ role.py
  ├ section_90030_initial.py
  ├ section_90040_support_detail.py
  ├ section_90060_final.py
  ├ section_90070_support_summary.py
  └ README.md
```

### 各ファイルの責務

#### basic.py
- XMLの基本情報抽出
- report_code
- final_date
- insurer / symbol / number
- name / gender / birth
- ticket_no / ticket_exp
- final_date は report_code = 22 の場合のみ取得する
- 取得元は `documentationOf / serviceEvent / effectiveTime`

#### role.py
- report_code から initial / final 判定

#### section_90030_initial.py
- 90030 初回面談情報セクションの抽出
- 初回面談情報セクション内の目標関連項目
- 初回面談に関する基本情報
- initial_date の取得
- 取得元は 90030 内の `entry / act / effectiveTime`
- 対象 act は `codeSystem = 1.2.392.200119.6.24010`

#### section_90060_final.py
- 90060 最終評価セクションの抽出
- 達成状況
- アウトカムポイント
- 最終腹囲 / 最終体重などの結果値


#### section_90070_support_summary.py
- 90070 支援実施内容集計セクションの抽出
- 支援手段ごとの回数・時間の集計値
- 90040（支援明細）の集約結果

#### section_90010_guidance.py（未定義 → 追加）

- 90010 保健指導情報セクションの抽出
- 保健指導区分（積極的支援 / 動機付け支援 等）
- OID:
  - 項目OID: 1.2.392.200119.6.1006
  - 結果OID: 1.2.392.200119.6.1112
- 注意:
  - XML上は code 値（例: 1020000001）を参照し、OIDは仕様上の意味定義として扱う

## 実行スクリプトとの責務分離

### XML層（lib/shg/xml）
- XMLから値を「取得するだけ」
- 業務ロジックは持たない（最小限に留める）

### orchestration層（check_shg_result_xml.py）
- XML列挙
- DB取得
- identity生成
- person単位集約
- CSV出力

## キー設計

| 種別 | 用途 |
|------|------|
| identity_hash | 内部束ねキー（主キー） |
| person_key | CSV確認用 |
| person_id_custom | 既存連携用 |

## フェーズ設計

### fase1.0
- 旧スクリプトからの横移行
- 展開済みXMLを入力とする
- CSV構造を維持

### fase1.1
- ZIP直読みに変更
- DATA/*.xml を対象

### fase2
- チェックロジック整理
- 比較・判定列の強化

## 設計方針

- XML構造に沿って責務を分割する
- 値ベースではなく、CDAセクションの意味に沿ってファイルを分割する
- 「値取得」と「値の利用」を分離する
- 600行以上の単一スクリプトを避ける
- 将来的な仕様変更に耐える構造とする

## 現状ギャップ（2026-04時点）

以下は、実装・検証の過程で判明した「spec未反映事項」であり、現行specは未完全である。

### 1. 利用券情報（basic）

- functionCode = 2（利用券）を優先取得する必要がある
- 利用券番号 / 有効期限は必須取得対象
- 当該ブロックが存在しないケースがある（正常系）

### 2. outcome の構造不足

- 計画時目標が存在しない場合がある
- 達成/未達のみでは評価不能となるケースあり
- 判定ロジックは未確定（TODO）

### 3. XML前提ルールの不足

現行specでは以下が明文化されていない：

- セクション欠損は正常系
- 要素欠損（valueなし等）も正常系
- None返却を基本とする（例外禁止）

### 4. specと実装の乖離

- 90010セクションがspec未定義だった（対応済）
- basicでの券種優先ロジックが未定義だった（対応済）
- initial_date / final_date の取得責務が未定義だった（対応済）
- outcomeの評価前提未定義

→ 現状、specは「完全な一次情報ではなく、途中状態」である


## CSV出力仕様（check_shg_result_xml）

本スクリプトは以下のCSVを出力する：

- export_shg_report.csv
- export_outcome_report.csv

### 計画・結果カラム命名ルール

旧スクリプトとの対応：

| 旧カラム | 新カラム |
|---------|---------|
| goal_腹囲体重 | 計_腹囲体重 |
| goal_食 | 計_食 |
| goal_運動 | 計_運動 |
| goal_喫煙 | 計_喫煙 |
| goal_休養 | 計_休養 |
| goal_その他 | 計_その他 |
| achieve_腹囲体重 | 結_腹囲体重 |
| achieve_食 | 結_食 |
| achieve_運動 | 結_運動 |
| achieve_喫煙 | 結_喫煙 |
| achieve_休養 | 結_休養 |
| achieve_その他 | 結_その他 |

※ ロジックは旧スクリプトと同一  
※ 列名のみ新仕様として短縮・日本語化

### initial_date / final_date の扱い

- `initial_date` は 90030（初回面談情報）から取得する
- `final_date` は basic 情報から取得する
  - `report_code = 22` の場合のみ有効
  - `documentationOf / serviceEvent / effectiveTime` を使用する

補足：
- lib/shg/xml は「どこから取るか」のみを責務とする
- initial / final の優先順位判定や代表値の選択は orchestration 層で行う

### level_code / level_text の扱い

`level_code` / `level_text` は、90010（保健指導情報）の保健指導区分を出力する。

値は以下を使用する：

- `level_code` : 保健指導区分コード
- `level_text` : 正式名称（例：積極的支援 / 動機づけ支援 / 動機づけ支援相当）

出力時の優先順は以下とする：

1. final XML に値があれば final 側を採用
2. final に無く initial XML に値があれば initial 側を採用
3. どちらにも無ければ空とする

補足：
- 旧スクリプトでは final の場合のみ level を出力していた
- 新スクリプトではチェック用途を考慮し、initial のみ存在する場合も出力対象とする

### process_source 判定ルール

process_source は、プロセス情報（支援回数・時間・ポイント）の取得元を示す。

判定ルールは以下とする：

| 条件 | process_source |
|------|----------------|
| 90040（支援明細イベント）が存在する | 90040 |
| 90040が存在せず、90070（支援集計）から値が取得できる | 90070_evn |
| 上記いずれからも取得できない | none |

補足：
- 90070_evn は「90040の代替ソースとして90070を使用した」ことを示す
- 動機づけ支援など、90040が存在しないケースで使用される
- 判定は XMLの存在ではなく、「実際に値（回数・時間・ポイント）が取得できたか」で判断する

### grand_total_points 計算ルール

grand_total_points は XMLの集計済み値をそのまま使用せず、チェック用CSVとして再計算値を出力する。

計算式は以下とする：

- grand_total_points = outcome_total_points + process_total_points

補足：
- outcome_total_points は 90060（最終評価）由来
- process_total_points は process_source に応じて 90040 または 90070_evn 由来
- XML内の集計済み合計値との差異確認を目的とする（検算用）

### proc_電子メール等_分 の扱い

`proc_電子メール等_分` は CSV列としては保持するが、値は常に `0` とする。

理由：
- 厚生労働省定義上、電子メール等には実施時間の定義がない
- したがって、電話・個別支援・グループ支援のような「分」集計は行わない
- 旧仕様との列互換のため、列は保持する