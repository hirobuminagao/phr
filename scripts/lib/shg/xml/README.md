# SHG XML ライブラリ

## 目的

本ディレクトリは、特定保健指導結果XMLから値を抽出するためのライブラリを配置する。

`check_shg_result_xml.py` から XML構造依存の処理を切り離し、XMLからの値取得と、その値の利用ロジックを分離することを目的とする。

## 役割

この層の責務は以下に限定する。

- XMLから対象項目の値を取得する
- 必要に応じて、値だけでなくXML上の値位置（element / attribute）も取得する
- 指定された値位置に対して既存XML値を更新するための共通ヘルパを提供する
- section / observation / value の構造をたどる
- report_code などの基本情報を取得する
- XML構造変更時の影響をこの層で吸収する

## 責務外

この層では以下を行わない。

- identity生成
- person単位集約
- CSV出力
- DB参照
- XML抽出・XML更新と無関係な orchestration 固有ロジックの集約
- XML更新可否や修正対象の業務判断

これらは `scripts/shg/check_shg_result_xml.py` 側の orchestration 層で扱う。

## 想定構成

```text
scripts/lib/shg/xml/
  ├ common.py
  ├ update.py
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

## 各ファイルの役割

### common.py
- XML探索の共通処理
- namespace（NS）の定義
- section / observation / value の探索関数
- 値取得の共通ヘルパ（code / value / displayName / 数値変換など）

※ 各sectionファイルは common.py を利用し、XML構造の探索ロジックを重複定義しない

### update.py
- XML値更新の共通処理
- 更新対象パス・ファイル名・更新項目・更新場所・更新値を受け取り、既存XMLの該当属性値へ反映する
- 更新結果は dict で返す
  - `ok`
  - `status`
  - `item_name`
  - `old_value`
  - `new_value`
  - `message`
- 更新場所は、値取得層が返した location（element / attribute）を前提とする
- 新規XMLブロック作成は行わない
- 利用券か受診券か、どの項目を修正対象にするか、といった業務判断は持たない

### outcome_checks.py
- outcome 系の加工・判定ロジック
- XML値の抽出は行わず、抽出済み値の加工・評価だけを担当する
- 代表例
  - plan_goal_map / outcome_map を受け取り、矛盾判定結果を返す
  - 腹囲体重のような「計画値 / 報告判定結果 / 実測差分」を突き合わせるチェックを行う
  - initial_date / final_date から継続日数判定を行う
  - section_* には置かない cross-section 判定をここで扱う

### basic.py
- XMLの基本情報抽出
- report_code
- insurer / symbol / number
- name / gender / birth
- ticket_no / ticket_exp

- 利用券情報の値位置取得
  - 利用券整理番号: 利用券 participant 配下の `id/@extension`
  - 利用券有効期限: 利用券 participant 配下の `time/high/@value` または同等の有効期限位置
  - 利用券 participant は `functionCode/@code = "2"` で識別する
  - 出現順による利用券 / 受診券の推定は禁止する

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
- 目標（bool）
- 目標の raw level（0 / 1 / 2 など）
- 初回面談に関する基本情報
- 初回面接実施日（initial_date）の取得
  - 90030 内の `entry / act / effectiveTime` から取得
  - `act/codeSystem = 1.2.392.200119.6.24010` を対象とする
- 初回面談方式の取得
  - 対象 observation code は `1022000012`
  - codeSystem は `1.2.392.200119.6.24010`
  - code / display を返す

- 提供関数
  - extract_initial_interview_mode(root) -> dict[str, str]
  - extract_initial_goals(root) -> dict[str, bool]
  - extract_initial_goal_levels(root) -> dict[str, Optional[int]]

### section_90040_support_detail.py
- 90040 支援明細セクションの抽出
- 支援イベント単位の実施情報
- mode_code / date / minutes / points の取得
- `_total_points` / `_total_minutes` の集計

### section_90060_final.py
- 90060 最終評価セクションの抽出
- 達成状況（bool）
- 達成状況の raw level（0 / 1 / 2 / 9 など）
- アウトカムポイント
- 最終腹囲 / 最終体重などの結果値

- 提供関数
  - extract_final_outcomes(root) -> tuple[dict[str, bool], int, str]
  - extract_final_outcome_levels(root) -> dict[str, Optional[int]]
  - extract_final_measurements(root) -> tuple[Optional[float], Optional[float]]

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

- 値取得・値位置取得・値更新の扱い
  - 既存の値取得関数は壊さず、必要な場合のみ location 付き取得を追加する
  - location は「どの element のどの attribute が値か」を示す
  - XML値更新は、値位置と更新値を受け取る共通処理として提供する
  - XML更新対象にするかどうかの業務判断は orchestration / script_lib 側で行う
  - 書き換え側が同じ探索条件を再実装しないよう、値位置の一次情報はこの層で提供できるようにする

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

- 判定ロジックの置き場所
  - section_* は XMLからの値抽出に集中する
  - 単一sectionで完結しない加工・判定は `outcome_checks.py` で扱う
  - 例：goal / achieve の矛盾判定、腹囲体重の整合チェック
  - ただし person単位集約や CSV列への流し込みは orchestration 層の責務とする

- 日付項目の扱い
  - initial_date は 90030 セクションから取得する
  - final_date は basic 情報（report_code=22）から取得する
    - `documentationOf / serviceEvent / effectiveTime` を使用する
  - sectionごとに責務を分け、日付も例外扱いしない

- 腹囲体重チェックの扱い
  - 腹囲体重は他カテゴリの単純な bool 判定と分けて扱う
  - 優先順位は「実測差分」→「計画値 fallback」とする
  - 報告判定結果と実測差分が計算可能なら、その一致可否を優先して判定する
  - 実測差分が計算不能な場合のみ、報告判定結果と計画値の一致可否を見る
  - 判定に使用する値は以下とする
    - 計画値: section_90030_initial.extract_initial_goal_levels
    - 報告判定結果: section_90060_final.extract_final_outcome_levels
    - 実測値: section_90060_final.extract_final_measurements

- 継続判定の扱い
  - 継続日数 = final_date - initial_date
  - 判定基準は固定で 93日以上
  - 動機づけ支援 / 積極的支援 / 動機づけ支援相当を区別せず同一ルールで判定する
  - calendar（月数）ベースの判定は採用しない
  - ライブラリ側では日数計算のみを提供する

## 値位置取得・値更新の方針

本ライブラリは原則として「XMLから値を取得する」層であり、業務判断を持たない。

ただし、利用券整理番号・利用券有効期限のように、後続処理でXML修正対象となる項目については、値だけでなくXML上の値位置も返せるようにする。

### 方針

- 値取得と値位置取得は同じ探索条件を使う
- 値位置は element と attribute 名の組み合わせで表現する
- XML値の更新自体は `scripts/lib/shg/xml/update.py` の共通ヘルパで行う
- XML更新可否や修正対象の判断は `scripts/shg/script_lib/` または orchestration 側で行う
- `scripts/shg/script_lib/` 側で利用券探索条件を再実装しない

### 利用券情報

利用券情報は、受診券と形式が似ているため、必ず `functionCode/@code = "2"` で利用券 participant を識別する。

利用券の値位置は、現フェーズでは以下を想定する。

| 項目 | 値位置 |
|------|--------|
| 利用券整理番号 | `id/@extension` |
| 利用券有効期限 | `time/high/@value` または同等の有効期限位置 |

注意:

- participant の出現順による推定は禁止する
- 受診券情報を利用券として扱ってはならない
- 値位置取得は探索条件の共有が目的であり、値位置取得関数自体はXML更新を行わない

## 補足

- 本ディレクトリは ADR-0018 に基づく
- 詳細仕様は `docs/spec/shg_xml/README.md` を参照する
- 継続判定はライブラリ側では「日数計算のみ」を提供する
  - 判定結果（OK/NG）への変換やCSV列へのマッピングは orchestration 層で行う
  - 実装関数は `outcome_checks.compute_duration_days` を想定する
- 現フェーズでは改修途中のため `xml/` 配下は平置きとし、ファイル数増加時に責務別の階層化を検討する