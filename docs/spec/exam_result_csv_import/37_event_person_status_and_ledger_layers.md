# Event Person Status and Exam Ledger Layers

## Status

Decision note updated as of 2026-08-05.

CSV/XML取込からXML出力まで一通り動いたため、次の実装は「複数の結果を1つの論理健診結果にまとめる」ことと、「eventに対する人の状況を管理する」ことを分けて設計する。

第一段階として `health_exam_result.exam_ledgers` を追加し、XML/CSV importの通常保存先を統合ledgerへ寄せた。
既存 `xml_ledger` / `csv_row_ledger` から統合ledgerを作る同期スクリプトは、初回移行、復旧、再構築用に残す。
また、運用確認用の `exam_result_ledger_report` は `exam_ledgers` 起点で作成する方針へ更新した。

人単位状態は新しい横持ちテーブルを増やさず、既存 `dev_phr.person_event` を親にする。
可変なチェック項目、出力状態、要対応理由は `dev_phr.person_event_status_items` に縦持ちする。

本設計で扱う `person_event` は、汎用イベント管理ではなく、健診イベントに対する人単位の進捗・確認状態に限定する。
予約、結果受領、健診結果check、HIA状態、健保・事業所納品確認を、eventごと・人ごとに見られることを目的とする。

## Layer Model

```text
dev_phr.event
  |
  +-- dev_phr.person_event
        eventに対して加入者が今どこにいるかを表す人単位の親
        |
        +-- dev_phr.person_event_status_items
              人単位の可変状態項目
              受領、check、出力、HIA、補正待ちなどを縦持ち

health_exam_result.exam_ledgers
  XML/CSV/紙入力から取り込んだsource 1件
  XMLならXML内の1人分、CSVならCSV 1行
  source単位の加入者突合、normalize、法定checkの単位
  |
  +-- health_exam_result.exam_item_values
        source ledger に紐づくnamecode単位の検査結果値

health_exam_result.exam_export_cases
  人単位の1回分XML出力候補
  source ledgerを複数束ね、出力可否、結合状態、出力証跡を持つ
  |
  +-- health_exam_result.exam_export_case_sources
        構成元exam_ledgers
  |
  +-- health_exam_result.exam_export_case_values
        XML出力用の採用済み整値
```

既存の `xml_ledger` / `csv_row_ledger` は直ちに廃止しない。
これらは原本取込の台帳、source証跡、移行元として残し、今後の業務処理・画面・出力制御は `exam_ledgers`、`exam_export_cases`、`person_event` 系へ寄せる。

未突合のledgerはまだ「人」として確定していないため、`person_event` は作らない。
未突合、加入者確認待ち、施設確認待ちのsource状態は `exam_ledgers` で管理し、加入者確定後に `person_event` へ反映する。

## Health Exam Person Check Scope

今回の人単位状態管理は、以下の健診業務フローを横断して確認するためのものである。

```text
予約サイト
  予約申込日
  受診日

健診機関からの結果受領
  結果ファイル受領
  健診機関視点では送信済み

健診結果チェック
  加入者突合
  人の基本情報チェック
  法定健診項目チェック
  HIAアップロード用XML出力

HIA状態チェック
  HIAステータス
  資格状況
  情報更新履歴

健保・事業所納品チェック
  HIAからのダウンロードXML有無
  健保への納品
  事業所への納品
```

このため、`person_event` / `person_event_status_items` は「健診イベントにおける人の現在状態」を見るためのレイヤーとする。
予約、結果、HIA、納品の詳細テーブルをすべて統合するのではなく、それぞれのsourceから必要な状態を集約して表示・判断できるようにする。

### Current Event Example

現在の `event_id = 2` は、以下のイベント枠として扱う。

```text
保険者: トランス・コスモス健康保険組合
年度: 2026年度
イベント: 定期健診
```

同一人物が年度内に複数回受診すること、また今後特殊健診系が追加されることを想定する。
そのため、人単位の親は `event_id + subscriber_id` だが、健診結果そのものは `exam_ledgers` で複数件持てるようにする。

例:

```text
person_event
  event_id=2, subscriber_id=1001

exam_ledgers
  2026-05-20 定期健診
  2026-09-10 特殊健診
  2026-11-15 再検査または追加受領
```

`person_event` はその人のイベント全体の状態を示し、個別の受診・source結果は `exam_ledgers`、結合・XML出力候補は `exam_export_cases` 側で扱う。
年度内複数受診を潰して1件にしない。
画面では `person_event` から、その人に紐づく複数の `exam_ledgers` と `exam_export_cases` を展開して確認できる形を目指す。

### Person Event Population Source

`person_event` の母集団は、結果ファイルを受領した人ではなく、eventに紐づく保険者の加入者全員とする。

初期作成手順は以下とする。

```text
dev_phr.event
  event_idからinsurer_numberを取得
        |
        v
dev_phr.subscribers
  insurer_number一致の加入者を全件抽出
  資格喪失者も除外しない
        |
        v
dev_phr.person_event
  event_id + subscriber_id で人単位の箱を作成
        |
        v
dev_phr.person_event_status_items
  資格状態、結果受領、check、HIA状態、納品状態をitemとして埋める
```

資格喪失日は対象者リストから除外するための条件に使わない。
健診eventの状況確認では、資格喪失者も含めて「このevent上でどう扱うべきか」を確認する必要があるためである。
資格喪失情報は `person_event_status_items` の資格状態item、または `person_event.gap_flag` / `gap_reason` の判断材料として扱う。

現在の `sync_person_event_status_items.py` は `exam_ledgers` に存在し、かつ加入者突合済みの人だけを同期対象にしている。
これは結果受領後の集計には使えるが、未受領者を含むevent全体の母集団作成としては不足している。
そのため、次段階では以下の2ステップに分ける。

1. `event_id` から `subscribers` を抽出し、`person_event` の母集団を作成・更新する。
2. `exam_ledgers`、HIAダッシュボード、予約、納品などのsourceから `person_event_status_items` を更新する。

`subscribers` はHIA取込や健保データ反映により、加入者追加、氏名・記号番号・資格喪失日・identity系情報の更新が発生する。
そのため母集団作成は初回insertではなく、再実行可能なupsert処理とする。
`subscribers` に追加された加入者は `person_event` へ追加し、既存加入者の `person_id_custom` / `identity_hash` / 資格喪失状態は最新の `subscribers` から再反映する。

結果状態同期は、母集団系itemを消さない。
`EVENT_POPULATION_STATUS`、`QUALIFICATION_STATUS`、`QUALIFICATION_LOST_DATE` は母集団同期の責務とし、健診結果同期は結果受領・check・XML出力に関するitemだけを更新する。

### Out of Scope

以下は今回の人単位状態管理へ直接混ぜない。

- 汎用問い合わせタスク
- 請求金額の確定
- 予約サイトそのものの業務DB置き換え
- HIA台帳取込そのものの正本化
- 事業所納品ファイルの詳細生成

ただし、各領域の状態を `person_event_status_items` のitemとして参照・集約する余地は残す。

## Value Layer Concept

検査値は、原本台帳としての値と、納品・出力用の清書値を分けて扱う。
同じ `namecode` の値が二重に見える場合があるが、責務が違うため同一データの無意味な複製ではない。

### Source Value Layer

source value layer は「受け取ったファイルをどう読んだか」を保存する層である。

対象:

- XML由来のsource ledger
- CSV由来のsource ledger
- 紙/Excel等から登録したsource ledger

保持する情報:

- raw値
- normalize後の値
- code値、PQ値、ST値など型別の値
- 単位
- normalize status / reason
- validation status / reason
- file receipt、元ファイル、元行、元XMLへの参照
- 取込run、処理run、エラー理由

source value layer は、監査、再処理、原因調査、健診機関への確認、辞書追加後の再取込のために使う。
原本由来の情報は不変寄りに扱い、出力都合で直接上書きしない。

### Adopted Value Layer

adopted value layer は「XML出力や業務画面で使う採用済みの値」を保存する層である。

保持する情報は出力・集計に必要な最小限とする。
ただしXML出力時に毎回source値を全探索するとDB接続・JOIN負荷が高くなり、再出力時の状態も揺れやすいため、清書済みの採用値はDBへ保持する。

保持する情報:

- `exam_export_case_id`
- `namecode`
- `occurrence_no`
- 採用後の型別値
- 単位
- 採用元 `source_exam_item_value_id`
- 採用状態
- 採用理由
- 採用run
- 更新日時

adopted value layer はraw値やnormalize過程を主責務にしない。
必要な場合は採用元source値を参照して確認する。

### Initial Implementation Direction

初期実装では、source値と清書値を別テーブルとして扱う。

```text
source値:
  exam_item_values
  ledger_type = EXAM
  ledger_id = source exam_ledgers.exam_ledger_id

清書値:
  exam_export_case_values
  exam_export_case_id = exam_export_cases.exam_export_case_id
  source_exam_item_value_id でsource値へ戻る
```

`exam_item_values` は取込sourceのraw/normalize/validation証跡に集中させる。
`exam_export_case_values` はrawを持たず、XML出力に必要な最小限の採用済み値と採用元参照を持つ。
これにより、XML/CSV/紙入力のsource証跡を壊さず、人単位のXML出力用に清書値を再生成できる。

### Source Precedence Exceptions

複数sourceを1つの清書ledgerへ結合する場合、同じ `namecode + occurrence_no` の値は原則XML優位で採用する。
XMLは標準様式として受領した原本であり、項目・型・コード体系がCSVより厳密であるためである。

一方で、XMLが常に業務上もっとも有用な値とは限らない。
例として `9N511 医師の診断(判定)` に、`メタボリックシンドローム判定にて非該当です。` のような `9N501` の口語説明だけが入る施設がある。
この場合、CSV側の指導コメントに医師の診断・再検査指示として有用な文章が入っていれば、CSV値を清書値として採用した方が納品上の意味が通る。

この制御は全項目に優位フラグを持たせず、`health_exam_result.exam_item_value_precedence_rules` による例外ルールで扱う。
取り込み済みsource値はXML/CSVとも証跡として保持し、結合済み `ledger_type = EXAM` の清書値を再生成する時だけ例外を適用する。

初期actionは以下とする。

| action | 意味 |
| --- | --- |
| `XML_FIRST` | 条件一致時もXMLを採用する |
| `CSV_FIRST` | CSV値があればCSVを採用する |
| `CSV_IF_XML_MATCHES_PATTERN` | XML値が指定パターンに一致し、CSV値が条件を満たす場合にCSVを採用する |
| `JOIN_XML_CSV` | XML値とCSV値を重複除去して結合する |
| `MANUAL_REVIEW` | 自動採用はXML優位のまま、人の確認対象として残す |

## Three-Layer Summary

今後の中心は以下の3層とする。

```text
1. source ledger / source item values
   原本、raw、normalize、validation、処理結果、証跡

2. exam_ledgers / adopted item values
   論理健診結果、結合済み結果、XML出力用の採用値

3. person_event / person_event_status_items
   人×イベントの進捗、出力状態、HIA状態、要対応状態
```

ファイル側を処理・証跡の層、人側を業務・納品の層として分ける。
これにより、複数ファイル結合、基本情報補正、再出力、HIAアップロード状態管理を、原本値の上書きなしで扱える。

## HIA Dashboard Status Layers

HIAダッシュボードCSV由来の状態は、健診結果ledgerそのものとは別の観測情報として扱う。
ただし、健診イベントに対する人の進捗確認では重要な入力になるため、次の3層に分けて接続する。

```text
work_other.hia_dashboard_status
  HIAダッシュボードCSVの最新観測状態
  CSV取込のたびに現在値として更新される

work_other.hia_dashboard_year_end_status
  年度末または年度最終状態の固定スナップショット
  2025年度最終状態はこのテーブルへ退避済み

dev_phr.person_event / person_event_status_items
  健診eventに対する人単位の確認状態
  HIA最新状態や年度スナップを必要なitemへ集約して表示・判断する
```

`hia_dashboard_status` は最新観測であり、年度が変わると状態が初期化または上書きされる可能性がある。
そのため、過年度eventの状態判定に `hia_dashboard_status` の最新値を直接使わない。
過年度の最終状態を参照する場合は `hia_dashboard_year_end_status` を使う。

一方、進行中年度のeventでは `hia_dashboard_status` を現在状態の入力として利用できる。
この場合も、HIA取込テーブルを人チェックの正本にはせず、`person_event_status_items` へ必要な状態を同期する。

HIAダッシュボードCSV取込側は既存実装があるが、新フォーマットでは先頭にHIA加入者IDが追加されている。
今後の改修では、HIA加入者IDがある場合はそれを優先して加入者照合し、旧来の漢字氏名照合は補助またはfallbackとして扱う。
ただし、HIAダッシュボードCSVの取込・照合方式変更は、健診結果CSV取込そのものとは別責務とする。

## Responsibilities

### `dev_phr.person_event`

`person_event` は「このeventで、この加入者が今どこにいるか」を表す人単位の親である。

想定する責務:

- event対象者としての現在状態を持つ。
- 受領件数、最終受領日時、納品対象、納品済み、HIA状態など既存の横持ち概要を保持する。
- 画面の一覧、進捗管理、担当者の確認作業、HIAアップロード後のステータス更新の主キーになる。
- 加入者が確定したものだけを対象にする。

`person_event` には、増減しやすいチェック項目を横持ちで増やさない。
細かい状態は `person_event_status_items` に逃がす。

### `dev_phr.person_event_status_items`

`person_event_status_items` は `person_event_id + item_code` の縦持ち状態項目である。

初期 item_code:

- `PERSON_STATUS`: 人単位の代表状態。`XML_EXPORTED`, `XML_EXPORTABLE`, `CHECK_NG`, `CHECK_PENDING`, `RESULT_RECEIVED` など。
- `EVENT_POPULATION_STATUS`: event母集団上の状態。`IN_EVENT_INSURER_POPULATION`, `NOT_IN_SUBSCRIBER_MASTER` など。
- `QUALIFICATION_LOST_DATE`: 加入者台帳上の資格喪失日。
- `QUALIFICATION_STATUS`: event上で確認する資格状態。資格喪失者も母集団から除外せず、このitemで状態を表す。
- `RESERVATION_APPLIED_AT`: 予約申込日。
- `EXAM_VISITED_AT`: 受診日または受診日時。
- `RESULT_FILE_RECEIVED_COUNT`: 結果ファイル受領件数。
- `RESULT_RECEIVED_COUNT`: 受領済み健診結果数。
- `MATCHED_LEDGER_COUNT`: 加入者突合済みledger数。
- `CHECK_OK_LEDGER_COUNT`: 法定OKのledger数。
- `CHECK_NG_LEDGER_COUNT`: 法定NGのledger数。
- `CHECK_PENDING_LEDGER_COUNT`: 法定check未実行または保留のledger数。
- `EXPORTABLE_LEDGER_COUNT`: XML出力候補ledger数。
- `EXPORTED_LEDGER_COUNT`: XML出力済みledger数。
- `HIA_STATUS`: HIA上の現在状態。
- `HIA_QUALIFICATION_STATUS`: HIAまたは加入者台帳上の資格状態。
- `HIA_DOWNLOADED_XML_COUNT`: HIAからダウンロードできたXML件数。
- `INSURER_DELIVERY_STATUS`: 健保納品状態。
- `EMPLOYER_DELIVERY_STATUS`: 事業所納品状態。
- `LATEST_EXAM_LEDGER_ID`: 最新または代表の統合ledger参照。
- `LATEST_EXAM_DATE`: 最新健診日。
- `LATEST_FACILITY_CODE`: 最新健診機関コード。
- `LATEST_FACILITY_NAME`: 最新健診機関名。
- `REQUIRES_BASIC_INFO_CORRECTION`: 基本情報補正待ち。
- `REQUIRES_MANUAL_EXPORT_APPROVAL`: 手動出力許可を含む。

今後、住所補完、HIAアップロード状態、再提出対象、結合候補、手動確認項目が増えても、テーブル定義ではなく item_code の追加で表現する。

### `health_exam_result.exam_ledgers`

`exam_ledgers` は「受け取ったXML/CSV/紙入力を1人分として読んだsource結果」を表す。

想定する責務:

- XML由来、CSV由来、紙入力由来のsource結果を表す。
- `event_id`, `subscriber_id`, `exam_date`, `exam_facility_id`, `insurer_number`, `health_exam_report_category`, `program_code` を持つ。
- 加入者突合結果、source単位の法定チェック結果、基本情報、住所補完状態を持つ。
- source file、source row、`file_receipts` を辿れるようにする。
- 複数sourceの結合結果やXML出力候補判定は `exam_export_cases` 側へ分ける。
- `exam_item_values` の親として、sourceごとの検査値集合を定義する。
- XML出力時に採用する清書済み検査値集合は `exam_export_case_values` 側で定義する。

このレイヤーは「event全体での人の作業状態」を主責務にしない。
同じ人に複数の健診日、複数の健診機関、複数のプログラムがある場合、`exam_ledgers` は複数件になりうる。

## Export Control Placement

出力制御は2つのレイヤーに分けて扱う。

### 出力候補判定

「この1回分健診をXMLにできるか」は `exam_export_cases` の責務とする。

判定材料:

- 報告区分とプログラムコードがある。
- 加入者突合が `MATCHED`。
- 法定チェックが `OK`、またはMISSINGのみで手動出力許可済み。
- 基本情報のXML出力値が揃っている。
- 採用済み整値に出力不可の `INVALID` が残っていない。

出力候補には以下の2種類を区別して扱う。

1. 法定チェックOK
   - source単体、または結合後の清書値で法定項目が満たされている状態。
   - `check_status = OK` を基本とする。
2. 条件付き出力OK
   - 妊娠中、医師判断、施設回答等により、法定項目が実施不能・対象外・未提出であることを業務確認済みの状態。
   - 架空の検査値は作らない。
   - `check_status = NG` と `check_reason` は残したまま、`manual_export_approved = 1` と承認理由、承認者、承認日時で出力を許可する。
   - 対象は `check_reason` が `MISSING` のみの場合に限定する。
   - `INVALID`、`PARSE_ERROR`、加入者不一致、基本情報不足、健診機関不一致、XML生成/XSD検証エラーは条件付き出力OKで通過させない。

条件付き出力OKの入力欄は、初期実装では既存 `manual_export_*` を使う。
画面化時には少なくとも以下を入力できるようにする。

| 項目 | 内容 |
|---|---|
| `manual_export_approved` | 出力許可フラグ |
| `manual_export_reason` | 業務理由。例: `妊娠中のため胸部X線未実施。健診機関確認済み。` |
| `manual_export_approved_by` | 承認者または入力者 |
| `manual_export_approved_at` | 承認日時 |
| 確認先 | 健診機関、担当者、メール、電話など。初期は `manual_export_reason` に含める |
| 確認対象項目 | MISSINGの分類。初期は `check_reason` から読めるため別カラム化しない |

後続で履歴性が必要になった場合は、`manual_export_approval_history` 等の専用履歴テーブルを追加する。
初期はXML出力履歴 `xml_export_members` に出力時点の `manual_export_*` snapshotを保存することで、出力事実との紐付けを担保する。

### 出力後の業務状態

「このeventでこの人は出力済みか、HIAへアップロード済みか、再提出対象か」は `person_event` / `person_event_status_items` の責務とする。

管理したい状態:

- XML未出力
- XML出力済み
- HIAアップロード待ち
- HIAアップロード済み
- HIAエラー
- 修正待ち
- 再出力対象
- 施設確認待ち
- 対象外

この分離により、同じ `exam_ledger` を再出力しても履歴を失わず、人単位の現在状態だけを更新できる。

## Multi-Source Merge Position

複数結果を1つにする処理は `exam_export_cases` と `exam_export_case_values` を作る、または更新する処理として扱う。
元source ledgerを直接上書きせず、`exam_export_case_sources` で構成元を保持し、`exam_export_case_values` へ清書値を生成する。

候補キー:

- `event_id`
- `insurer_number`
- `subscriber_id`
- `exam_date`
- canonicalな健診機関コード

結合方針:

- XMLとCSVが同じ論理健診結果に属する場合、正常なXML値を優先する。
- CSVにしかない項目は不足補完として採用できる。
- XMLとCSVで同じnamecodeの値が異なる場合はXMLを採用し、CSV差異を警告・証跡として残す。
- CSV同士、またはXML同士で同じnamecodeの値が異なる場合は自動採用せず停止する。
- 健診日、加入者、健診機関を解決できないsourceは自動結合しない。

結合後の法定チェックは、source個別のcheckではなく、結合後の `exam_ledgers` + `exam_item_values` に対して実行する。
これにより、単独CSVではMISSINGでも、別sourceで補完されれば法定OKにできる。

### 初期補完診断対象

XML側で法定項目が不足し、CSVで不足補完される可能性が高い初期対象は以下の7分類とする。

これは補完診断で重点的に確認する分類であり、CSV取込対象を制限するものではない。
CSVは健診機関ごとの通常マッピングを作り、取込可能な検査値・基本情報を全てsource値として取り込む。
補完処理は、取り込まれたsource値の中から、不足している分類に必要な値を採用する。

| 同一性項目コード | 分類 | 補完診断で見る内容 |
|---|---|---|
| `4403004001` | 視力 | 左右視力など、法定チェックが要求する視力値 |
| `4403005001` | 聴力 | 所見有無または聴力検査値。CSV施設により表現差あり |
| `4404001001` | 胸部X線 | 所見有無CD、所見本文ST |
| `4411001001` | 心電図 | 所見有無CD、所見本文ST |
| `4401001001` | 既往歴 | 所見有無CD、既往歴本文ST |
| `4402001001` | 自覚症状 | 所見有無CD、自覚症状本文ST |
| `4402001002` | 他覚症状 | 所見有無CD、他覚症状本文ST |

初期診断スクリプトはDBを更新せず、以下を見える化する。

- XML source ledgerでMISSINGになっている分類。
- 同一 `event_id + subscriber_id + exam_date + exam_facility_id + insurer_number` のCSV source ledger候補。
- CSV source ledgerに補完可能なnamecode/値があるか。
- XMLとCSVで同じ分類の値が衝突していないか。
- 結合すれば法定チェックOKになりそうか。
- 条件付き出力OKの対象にすべきMISSINGだけが残るか。

診断結果の状態案:

| 状態 | 意味 |
|---|---|
| `COMPLETABLE` | CSV補完により不足解消できる見込み |
| `PARTIAL` | 一部は補完できるがMISSINGが残る |
| `NO_CSV` | 同一人物・同一健診日のCSV候補がない |
| `CONFLICT` | 自動採用できない値差異がある |
| `MANUAL_APPROVAL_CANDIDATE` | MISSINGのみが残り、妊娠中等の条件付き出力OK候補 |
| `NOT_TARGET` | 初期7分類以外の不足または対象外 |

## Implementation Steps

推奨順:

1. `exam_ledgers` のDDLと、旧個別ledgerからの復旧用同期を作る。
   - 追加済み。
   - 通常importはXML/CSVとも `exam_ledgers` へ直接登録する。
2. source単位の法定チェック入口を `03_00_check_imported_exam_ledgers.py` として分離する。
   - `exam_ledgers` を対象にする。
   - 追加済み。
3. `exam_export_cases` / `exam_export_case_sources` / `exam_export_case_values` のDDLを作る。
   - 追加済み。
4. `03_01_build_exam_export_cases.py` でsource ledgerを人単位のcaseへ束ねる。
   - 追加済み。
5. `03_02_build_exam_export_case_values.py` で採用済み整値を作る。
   - 追加済み。
6. `03_04_check_exam_export_cases.py` でcase単位の法定チェックを行う。
   - 追加済み。
7. `exam_export_cases.export_readiness_status` / `export_readiness_reason` を更新する。
   - 追加済み。
8. `person_event_status_items` のDDLを作る。
   - 追加済み。
9. `exam_ledgers` / `exam_export_cases` から `person_event` / `person_event_status_items` へ同期する。
10. `exam_result_ledger_report` は `exam_ledgers` 起点のまま維持する。
11. `04_export_hia_xml.py` を `exam_export_cases` / `exam_export_case_values` 起点へ寄せる。
6. 複数source候補を検出し、`MULTIPLE_SOURCE_ROWS_UNRESOLVED` を出せるようにする。
7. 明示選択されたsourceを結合し、結合後 `exam_item_values` を作る。
8. 結合後checkを実行し、`person_event_status_items` へ現在状態を反映する。
9. `04_export_hia_xml.py` を `exam_ledgers` 起点へ寄せる。
10. 出力後に `person_event_status_items` のHIAアップロード待ち状態を更新する。
11. 画面から出力条件選択、基本情報補正、HIAアップロード状態更新を行えるようにする。

## Deferred Decisions

- `PERSON_STATUS` の正式な状態定数セット。
- HIAアップロード状態を人単位だけで持つか、ZIP単位・member単位の状態も別途持つか。
- 基本情報補正履歴を統合ledger IDだけで持つか、source ledgerへの逆参照も保持するか。
- 未突合ledgerを画面で人候補として見せるための専用viewを作るか。
