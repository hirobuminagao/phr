# SHG XML 処理仕様（shg_xml）

## 目的

本ディレクトリは、SHG（特定保健指導）結果XMLの解析・抽出・チェック処理に関する仕様を管理する。

本仕様は、以下の責務を対象とする。

- XML / ZIP入力からの対象XML収集
- XMLからの値抽出（basic / role / CDAセクション別抽出）
- XML構造の理解と項目マッピング
- スクリプト処理（check_shg_result_xml.py）の現行仕様
- 利用券fix / outcome判定 / CSV出力の責務境界

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
│   ├── <root_folder_name>/
│   │   ├── ix08_V08.xml
│   │   ├── su08_V08.xml
│   │   ├── DATA/*.xml
│   │   ├── CLAIMS/
│   │   └── XSD/
│   └── *.zip
└── output/
    └── <yyyymmdd_hhmmss>/
        ├── _work_zip_extract/
        ├── export_shg_report.csv
        └── export_outcome_report.csv
```

### 入力仕様

- `input/` 配下には、ZIPファイルまたはZIP解凍後のルートフォルダ構造を配置できる
- ZIPファイルは `output/<yyyymmdd_hhmmss>/_work_zip_extract/` 配下へ展開してから対象XMLを収集する
- 本スクリプトは `DATA/*.xml` 相当の特定保健指導結果XMLのみを解析対象とする
- `ix08_V08.xml`、`su08_V08.xml`、`CLAIMS/`、`XSD/` は保持・展開される場合があるが、解析対象外とする

### 出力仕様

- 実行ごとに timestamp フォルダを生成
- 同一入力でも複数回実行した結果を保持する
- ZIP由来XMLの作業フォルダは `_work_zip_extract/` 配下で管理し、fix有無に応じて保持/削除する

```text
scripts/lib/shg/xml/
  ├ common.py
  ├ outcome_checks.py
  ├ basic.py
  ├ role.py
  ├ section_90010_guidance_info.py
  ├ section_90030_initial.py
  ├ section_90040_support_detail.py
  ├ section_90060_final.py
  ├ section_90070_support_summary.py
  └ README.md
```

### 各ファイルの責務

#### common.py
- XML探索の共通ヘルパ
- namespace（NS）定義
- section / observation / value の共通取得補助

#### outcome_checks.py
- outcome 系の加工・判定ロジック
- XML値の抽出は行わず、抽出済み値の加工・評価のみを担当する
- 代表例
  - plan_goal_map / outcome_map を受け取り、一般カテゴリの矛盾判定結果を返す
  - 腹囲体重について、計画値 / 報告判定結果 / 実測差分を突き合わせるチェックを行う
  - initial_date / final_date から継続日数判定を行う
- section_* には置かない cross-section 判定をここで扱う

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

#### section_90010_guidance_info.py
- 90010 保健指導情報セクションの抽出
- 保健指導区分（積極的支援 / 動機付け支援 等）
- OID:
  - 項目OID: 1.2.392.200119.6.1006
  - 結果OID: 1.2.392.200119.6.1112
- 注意:
  - XML上は code 値（例: 1020000001）を参照し、OIDは仕様上の意味定義として扱う

#### section_90030_initial.py
- 90030 初回面談情報セクションの抽出
- 初回面談情報セクション内の目標関連項目
- 初回面談に関する基本情報
- initial_date の取得
- 取得元は 90030 内の `entry / act / effectiveTime`
- 対象 act は `codeSystem = 1.2.392.200119.6.24010`
- 初回面談方式の取得
  - `extract_initial_interview_mode(root) -> dict[str, str]`
  - 対象 observation code は `1022000012`
  - `code` と `display` を返す
- 目標値の raw level 取得
  - `extract_initial_goals(root) -> dict[str, bool]`
  - `extract_initial_goal_levels(root) -> dict[str, Optional[int]]`
  - 腹囲体重は `0 / 1 / 2`、他カテゴリは `0 / 1` を想定する

#### section_90040_support_detail.py
- 90040 支援明細セクションの抽出
- 支援イベント単位の実施情報
- mode_code / date / minutes / points の取得
- `_total_points` / `_total_minutes` の集計

#### section_90060_final.py
- 90060 最終評価セクションの抽出
- 達成状況
- 達成状況の raw level 取得
  - `extract_final_outcomes(root) -> tuple[dict[str, bool], int, str]`
  - `extract_final_outcome_levels(root) -> dict[str, Optional[int]]`
- アウトカムポイント
- 最終腹囲 / 最終体重などの結果値
  - `extract_final_measurements(root) -> tuple[Optional[float], Optional[float]]`

#### section_90070_support_summary.py
- 90070 支援実施内容集計セクションの抽出
- 支援手段ごとの回数・時間の集計値
- 90040（支援明細）の集約結果

## 実行スクリプトとの責務分離

### XML層（lib/shg/xml）
- section_* は XMLから値を「取得するだけ」
- basic / role / section_* は XML構造に責務を寄せる
- outcome_checks.py は抽出済み値を受けた加工・判定のみを担当する
- person単位集約やCSV列マッピングは持たない

### orchestration層（check_shg_result_xml.py）
- XML / ZIP入力収集
- XML読込
- DB取得
- identity生成
- 利用券fix判定
- 利用券XML更新
- person単位集約
- outcome判定
- CSV出力

### 現行 orchestration フロー（2026-06確認時点）

現行 `check_shg_result_xml.py` の処理フローは、以下の順序を基本とする。

```text
DB読込
→ XML / ZIP収集
→ XML単位抽出
→ 利用券fix判定 / 必要時XML更新
→ people集約
→ outcome判定
→ CSV出力
```

実際の処理粒度としては、概ね以下の流れで構成される。

```text
1. DBから shg_result 読込
2. XML / ZIP入力収集
   - ZIPは作業フォルダへ展開
   - 展開済みXMLも直接収集
3. XML単位ループ
   - basic抽出
   - role判定
   - section抽出
   - DB行取得
   - identity_bundle生成
   - 利用券fix判定
   - 必要時、利用券XML更新
   - peopleへ格納
   - XML単位CSV行生成

4. people単位ループ
   - initial/final整理
   - level決定
   - 継続判定
   - outcome判定
   - CSV行生成

5. CSV出力
```

補足:

- 上記は推察ではなく、2026-06時点の現行 `check_shg_result_xml.py` 実装確認ベースである
- 今後の改修では、この幹フローを維持しながら必要箇所のみを外出しする
- fix処理追加時も、現行フローを破壊しないことを優先する

## キー設計

| 種別 | 用途 |
|------|------|
| identity_hash | ZIP内の initial / final XML の pair 判定、および内部束ねキー |
| person_key | CSV確認用 |
| person_id_custom | 既存連携用 |

## XML role / pairing 方針

### report_code の扱い

現フェーズでは、以下の report_code を処理対象とする。

| report_code | 役割 | 現フェーズでの扱い |
|-------------|------|--------------------|
| 21 | initial XML / 初回報告 | チェック対象 |
| 22 | final XML / 最終報告 | チェック対象 |
| 23 | 中間・その他報告系 | 現時点ではチェック対象外 |

`report_code = 23` は、現時点では `21 / 22` と同列のチェック対象には含めない。

後続フェーズで、作成補助・不足確認などの支援用途として扱う可能性はあるが、現フェーズでは pair 対象にも fix 対象にも含めない。

### ZIP内の pair 判定

現フェーズでは、ZIP内に存在する XML 同士を `identity_hash` ベースで pair とする。

- 同一 `identity_hash` の `report_code = 21` を initial XML とする
- 同一 `identity_hash` の `report_code = 22` を final XML とする
- initial / final の両方が存在する場合は pair として扱う
- initial のみ、または final のみの場合も単独XMLとして処理対象にする
- `report_code = 23` は pair 対象に含めない

### 将来方針

現在は ZIP内の XML を対象に pair 判定を行う。

ただし将来的には、XMLから抽出した値をDBに格納し、DB上のイベント・対象者単位で initial / final を対応させる方針とする。

- 現フェーズ: ZIP内 `identity_hash` ベースの pair
- 将来フェーズ: DB格納値を用いた pair / event 管理

## 直近の追加改修テーマ

現フェーズの主目的は、単なるリファクタリングではなく、以下の追加改修を安全に実装することである。


### 1. 利用券情報のDB値によるXML修正

XML内の利用券整理番号・利用券有効期限をDB値と比較し、差異がある場合はDB側を正としてXMLを修正する。

修正対象は利用券整理番号・利用券有効期限に限定する。

詳細なfix運用、利用券/受診券の区別、ZIP展開フォルダの保持/削除ルールは以下を参照する。

- `docs/spec/shg_xml/fix_workflow.md`


### 2. finalのみ動機づけ支援のoutcome矛盾判定除外

`report_code = 22` の final XML のみが存在し、かつ保健指導区分が動機づけ支援の場合、腹囲体重以外の outcome については計画情報不足により矛盾を確定できない。

そのため、このケースでは腹囲体重以外の outcome 矛盾として扱わない。

詳細な判定方針は以下を参照する。

- `docs/spec/shg_xml/fix_workflow.md`


### 3. final動機づけ支援のアウトカム合計ポイント0ブロック削除fix

基幹システム取込時のエラー回避のため、以下の条件をすべて満たす場合に限り、XML内のアウトカム合計ポイント0の既存ブロックを削除するfixを検討する。

条件:

- `report_code = 22`（最終評価XML）
- 保健指導区分が動機づけ支援
- `90060` section 内に `code = 1042001060` を持つ `entryRelationship[typeCode="COMP"]` が存在する
- アウトカム合計ポイント値が `0`

背景:

- 動機づけ支援では、アウトカム合計ポイントが0となること自体は業務上自然である
- ただし、納品後に基幹システムへ取り込む際、アウトカム合計ポイント0のブロックが存在するとエラーになるケースがある
- 以前は許容されていたが、基幹側の取込チェックが厳格化された可能性がある

方針:

- 利用券fixとは別系統のXML補正として扱う
- XML単位ループ内に別処理として追加する
- DB値比較ではなく、XML内の report_code / 保健指導区分 / アウトカム合計ポイント値を条件にする
- 削除処理そのものは `scripts/lib/xml/delete.py` の共通XML削除ヘルパへ外出しする
- SHG固有の条件判定と削除対象ブロック特定は、SHG側の専用処理に置く
- 削除対象は `90060` section 内の `code = 1042001060` を持つ `entryRelationship[typeCode="COMP"]` に固定する
- `observation` 単体削除ではなく、対応する `entryRelationship` を丸ごと削除する
- section 全体削除や entry 全体削除は行わない

実装前に確認すること:

1. `section_90060_final.py` でアウトカム合計ポイント（`1042001060`）をどのように抽出しているか確認する
2. `90060` section 内で、`1042001060` を持つ `entryRelationship[typeCode="COMP"]` とその parent element を特定できるか確認する
3. `scripts/lib/xml/delete.py` の共通責務を確定する
4. SHG側の専用fix処理をどこに置くか決める

注意:

- これは既存属性値更新ではなく、既存XMLブロック削除である
- XMLブロック削除は強いfixのため、条件を限定して扱う
- 推定で削除してはならない
- 削除対象特定時は、実際の XML Element と parent Element を保持して削除する
- 新規XMLブロック作成は行わない
- 削除後のXML保存は、条件に合致して実際に削除が発生した場合のみ行う

## バージョン位置づけ

### 現行 v1
- 旧スクリプトの単純移植フェーズは終了
- `check_shg_result_xml.py` を現行 SHG XMLチェック本体として扱う
- XML / ZIP入力に対応する
- `identity_hash` を主束ねキーとして initial / final を集約する
- 利用券fix、outcome判定、腹囲体重チェック、継続判定、processポイント集計、CSV出力を行う

### 後続改修
- チェックロジックの整理
- 比較・判定列の強化
- CSV列の追加・表示整理
- DB/event管理への発展

## 設計方針

- XML構造に沿って責務を分割する
- 値ベースではなく、CDAセクションの意味に沿ってファイルを分割する
- 「値取得」と「値の利用」を分離する
- 600行以上の単一スクリプトを避ける
- 将来的な仕様変更に耐える構造とする
- 判定ロジックは section_* に寄せすぎない
- XML抽出と無関係な加工・判定は outcome_checks.py のような補助ライブラリへ分離する
- 腹囲体重のチェックは他カテゴリと分けて扱う
  - 他カテゴリは「計画あり/なし」と「達成/未達成」の整合を主に見る
  - 腹囲体重は「計画値」「報告判定結果」「実測差分」の三点で見る
  - 優先順位は「実測差分」→「計画値 fallback」とする
- 現フェーズでは改修途中のため `xml/` 配下は平置きとし、ファイル数増加時に責務別の階層化を検討する
- 継続判定は days のみで扱う
  - `継続日数 = final_date - initial_date`
  - 判定基準は固定で `93日以上`
  - 動機づけ支援も含めて同じ基準で判定する
  - `calendar` 判定は採用しない
- XML内の意味判定は、必ず `code` / `codeSystem` / `functionCode` などの明示識別子で行う
- fix運用・禁止事項・ZIP保持ルールの詳細は `docs/spec/shg_xml/fix_workflow.md` を参照する


## 現状ギャップ / 現在位置（2026-06時点）

本specは、2026-06 時点の SHG XMLチェック改修状況を反映している。

以前存在していた「利用券fix詳細」「禁止事項」「ZIP保持ルール」は、`fix_workflow.md` へ分離済みである。

ただし、実装・spec ともにまだ進行途中であり、以下は今後の整理・改修対象とする。

### 現時点で概ね固定できたもの

- `report_code = 21 / 22` を対象とする role / pairing 方針
- ZIP内 `identity_hash` ベースの pair 判定
- `report_code = 23` を現フェーズ対象外とする方針
- 利用券整理番号・利用券有効期限のみを fix 対象とする方針
- 利用券と受診券を厳密に別物として扱う方針
- 出現順によるXML意味推定を禁止する方針
- finalのみ動機づけ支援時の outcome 判定除外方針
- 腹囲体重チェックの「実測差分優先」方針
- orchestration層 / XML抽出層 / outcome判定層 の責務分離方針
- XML / ZIP入力対応済みとする方針
- `xml_io.py` によるXML収集・ZIP展開・XML読込の責務分離
- `ticket_fix.py` / `xml_ticket_writer.py` / `outcome_policy.py` への orchestration 補助処理外出し方針

### 現時点で未完成・今後調整予定のもの

- 初回面談方式の取得精度確認
- 動機づけ支援の `計_*` 系目標取得漏れ対応
- フォルダ名 / ZIP名 のCSV出力
- `結_腹囲体重` の表示整理（`目標なし` / `-` など）
- outcome矛盾判定の最終ルール整理

### 現在の開発方針

現在は「全部を綺麗に作り直す」のではなく、現行 `check_shg_result_xml.py` の動作フローを維持しながら、必要な改修のみ安全に追加する方針を採用する。

そのため、先に現行フローを写し取り、責務境界を確認したうえで、必要箇所だけを段階的に外出しする。

### リファクタリング方針

現行 `check_shg_result_xml.py` の幹フローは維持する。

本フェーズでは全面的な再設計は行わず、追加改修により肥大化した責務を段階的に外出ししている。

外出し対象は以下に限定する。

- XML I/O
- 利用券fix判定
- 利用券XML書き換え
- outcome矛盾判定ポリシー

外出し先は、現フェーズでは `scripts/shg/script_lib/` 配下にフラットに配置する。

理由:

- 現時点では `check_shg_result_xml.py` 専用の処理であり、共通ライブラリ化するには早い
- `scripts/lib/shg/xml/` はXML抽出層として維持し、fixやCSV用の運用処理を混ぜない
- `script_lib` 配下では、フォルダを細かく分けず、ファイル名でXML処理か判定処理かを区別する
- 将来的に他スクリプトでも再利用する段階になったら、`scripts/lib/shg/` 配下への昇格を検討する


想定する外出し単位は以下とする。

```text
scripts/shg/script_lib/
  ├ xml_io.py              # XML収集・ZIP展開・XML読込
  ├ ticket_fix.py          # 利用券差異判定・fix候補作成
  ├ xml_ticket_writer.py   # 利用券ノードのXML書き換え
  └ outcome_policy.py      # outcome矛盾判定の除外ポリシー
```

### 現時点で追加外出しを検討している責務


補足:

- `script_lib` は orchestration 補助層であり、DB接続・identity生成・normalize処理を再実装しない
- DB接続は既存 `scripts/lib/db/` 系の共通libを使用する
- `identity_hash` / `person_id_custom` / normalize 系処理は既存共通libを使用する
- `script_lib` 側で独自のidentity生成やDB接続実装を増やさない

#### xml_io 系

責務:

- ZIP展開
- XML列挙
- XML収集
- XML読込

想定ファイル:

```text
scripts/shg/script_lib/xml_io.py
```

補足:

- orchestration の前半に集中していた「ファイルシステム責務」を分離済み
- ZIP入力と展開済みXML入力の両方に対応する
- XML抽出ロジックそのものは `scripts/lib/shg/xml/` 側へ残す

#### shg_result_loader 系

責務:

- `shg_result` のDB読込
- XMLとの照合用データ取得
- 利用券比較用データ取得

想定ファイル:

```text
scripts/shg/script_lib/shg_result_loader.py
```

補足:

- orchestration 本体から DBアクセス責務を分離する
- repository / data access に近い責務として扱う

#### ticket_fix 系

責務:

- XML利用券値とDB利用券値の比較
- fix必要判定
- fix候補情報生成
- ticket_fix_status 判定

想定ファイル:

```text
scripts/shg/script_lib/ticket_fix.py
```

補足:

- 現フェーズでは「利用券整理番号」「利用券有効期限」のみを対象とする
- 汎用XML比較エンジン化は行わない
- 利用券と受診券を混同しないことを最優先とする

#### xml_ticket_writer 系

責務:

- 利用券ノードのXML書き換え
- 利用券整理番号更新
- 利用券有効期限更新

想定ファイル:

```text
scripts/shg/script_lib/xml_ticket_writer.py
```

補足:

- `functionCode/@code = "2"` で識別できた利用券のみを書き換える
- 出現順による推定は禁止する

#### outcome_policy 系

責務:

- finalのみ動機づけ支援時の除外判定
- outcome矛盾判定ポリシー
- 特例ルール適用

想定ファイル:

```text
scripts/shg/script_lib/outcome_policy.py
```

補足:

- outcome計算そのものではなく「判定ルール」を責務とする
- XML抽出処理は持たない

#### 現時点では外出ししない責務

以下は orchestration の幹フローと密結合しているため、現フェーズでは `check_shg_result_xml.py` 側へ残す。

- people集約
- initial / final bucket管理
- CSV出力dict生成
- export rows append
- 最終CSV出力

理由:

- 現在も仕様変化が多い
- 列構造変更頻度が高い
- 無理に分離すると、逆に責務境界が不安定になる
- まずは fix追加を安全に実装することを優先する

以下は現フェーズでは orchestration 側に残す。

- DB読込
- XML単位ループ
- people集約
- CSV出力

目的は、現行動作を壊さずに利用券fixとfinalのみ動機づけ支援の判定除外を追加することである。

現時点では以下の幹フローを維持対象とする。

```text
DB読込
→ XML / ZIP収集
→ XML単位抽出
→ 利用券fix判定 / 必要時XML更新
→ people集約
→ outcome判定
→ CSV出力
```

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

### 初回面談方式の扱い

初回面談方式は 90030 から取得する。

- 取得元: `section_90030_initial.extract_initial_interview_mode`
- 対象 code: `1022000012`
- codeSystem: `1.2.392.200119.6.24010`

CSV では以下の 4 列に出力する。

- `初回面談方式_初回XML_コード`
- `初回面談方式_初回XML_内容`
- `初回面談方式_最終XML_コード`
- `初回面談方式_最終XML_内容`

補足：
- 初回XMLが存在する場合は、初回XML側の 90030 から取得する
- 最終XMLが存在する場合は、最終XML側の 90030 から取得する
- 抽出責務は lib/shg/xml 側、CSV列への流し込みは orchestration 層の責務とする


### 継続判定の扱い

継続判定は、`initial_date` と `final_date` を用いて日数差で判定する。

- `継続日数` = `final_date - initial_date` の日数差
- `継続判定モード` = `days`
- `継続しきい値` = `93日以上`
- `継続期間_XML判定`
  - `継続日数 >= 93` の場合 `OK`
  - `継続日数 < 93` の場合 `NG`
  - 日付不足などで計算不能な場合は空

補足：
- 動機づけ支援 / 積極的支援 / 動機づけ支援相当を区別せず、同一ルールで判定する
- 旧スクリプトにあった `calendar` 判定は新仕様では採用しない

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

### 腹囲体重チェックの方針

腹囲体重は、他カテゴリの単純な bool 判定とは分けて扱う。

チェック優先順位は以下とする：

1. 実測差分で判定可能なら、報告判定結果と実測差分の結果が一致するかを見る
2. 1 が計算不能な場合のみ、報告判定結果と計画値が一致するかを見る

判定に使用する値は以下とする：
- 計画値: `section_90030_initial.extract_initial_goal_levels`
- 報告判定結果: `section_90060_final.extract_final_outcome_levels`
- 実測値: `section_90060_final.extract_final_measurements`
  - 健診時腹囲 / 健診時体重は DB (`shg_result`) 側の値を使用する
  - 最終腹囲 / 最終体重は XML 90060 側の実測値を使用する

補足：
- 本CSVは「正しく記帳されているか」のチェックを目的とする
- そのため XML内の報告判定結果をそのまま信用せず、実測値から再確認できる場合はそちらを優先する
- 計画なし → 報告判定結果 1cm/1kg → 実測差分 2cm/2kg のような場合は、XML修正対象として扱う

### proc_電子メール等_分 の扱い

`proc_電子メール等_分` は CSV列としては保持するが、値は常に `0` とする。

理由：
- 厚生労働省定義上、電子メール等には実施時間の定義がない
- したがって、電話・個別支援・グループ支援のような「分」集計は行わない
- 旧仕様との列互換のため、列は保持する
## 実装レビュー / 再開アンカー（check_shg_result_xml）

### 目的

本節は、`scripts/shg/check_shg_result_xml.py` および関連ライブラリの現時点の実装到達点と、今後の修正ポイントを固定するためのメモである。

目的は以下の2点。

- 会話や作業が分岐しても、再開時に「次に何を直すか」をすぐ把握できるようにする
- 現在のスクリプトで「最低限動いていること」と「未完成のこと」を切り分ける

### 現時点で最低限動いていること

- `check_shg_result_xml.py` は、最低限のチェックCSVを生成できる状態まで到達している
- 入力は展開済みXMLに加え、ZIP配置からのCSV生成にも対応済み
- `basic.py` では、特定保健指導XMLの利用券情報を `participant[functionCode/@code="2"]` から取得するよう修正済み
- `section_90030_initial.py` では、目標系 raw level と初回面談方式の取得処理を追加済み
- `section_90060_final.py` では、腹囲体重の報告値を `value/@code` ベースで取得するよう修正済み
- outcome CSV では、腹囲体重に対して以下の列を最低限出力できる
  - `計_腹囲体重`
  - `結_腹囲体重`
  - `腹囲体重_計画値`
  - `腹囲体重_報告値`
  - `腹囲体重_実測判定`
  - `conflict_腹囲体重_XML判定`

### 現時点の表示 / 判定仕様（暫定確定）

- `計_腹囲体重`
  - `計画なし`
  - `1cm・1kg`
  - `2cm・2kg`
- `結_腹囲体重`
  - `未達成`
  - `1cm/1kg`
  - `2cm/2kg`
- `腹囲体重_実測判定`
  - `2` = 腹囲も体重も 2以上改善
  - `1` = 腹囲も体重も 1以上改善（ただし 2 には届かない）
  - `0` = 上記以外
  - 空 = 計算不能
- 目標値の優先順
  - final XML がある場合は final 側の 90030 を優先
  - final が無い場合のみ initial 側の 90030 を使う

### 納品優先のため一旦スルーした未完成点

以下は現時点で未完成、または挙動に不安があるため、後続修正対象とする。

- 初回面談方式が正しく取得できていない
- 動機づけ支援で `計_*` 系の目標が取り切れていないケースがある
- `結_腹囲体重` は将来的に `目標なし` や `-` を明示する必要がある
- 元のフォルダ名 / ZIP名をCSVに出していない
- 一部の矛盾判定はまだ暫定実装であり、完全な最終仕様ではない

### 今後の修正優先順位

1. 初回面談方式の取得位置と条件を実XMLで再確認して修正する
2. 動機づけ支援で取れていない目標値の取得ロジックを修正する
3. フォルダ名 / ZIP名をCSVに出力する
4. `結_腹囲体重` の表示ルール（`目標なし` / `-` を含む）を整理する
5. outcome矛盾判定の最終ルールを整理する
6. 必要に応じて、DB/event管理へ発展させる

### fixロジック検討時の前提

- XML自動修正対象は、利用券整理番号と利用券有効期限のみとする
- 利用券整理番号と利用券有効期限は、DB側を正としてXML修正対象にできる
- 利用券と受診券は必ず別物として扱う
- 受診券情報を利用券として転記してはならない
- 出現順による利用券推定は禁止する
- その他の判定結果はチェック結果のみ出力し、人手確認・手動修正の対象とする
- 詳細なfix運用は `docs/spec/shg_xml/fix_workflow.md` を参照する

### 再開時の合言葉

- 「SHGチェックは現行 v1 本体として運用。XML / ZIP入力、identity_hash pairing、利用券番号・期限のDB値fix、finalのみ動機づけ支援のoutcome矛盾除外は実装済み。次は初回面談方式、動機づけ支援の目標取得、フォルダ名/ZIP名CSV出力、腹囲体重表示、outcome最終ルールを順に整理する」