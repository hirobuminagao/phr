# Event Person Status and Exam Ledger Layers

## Status

Decision note as of 2026-08-03.

CSV/XML取込からXML出力まで一通り動いたため、次の実装は「複数の結果を1つの論理健診結果にまとめる」ことと、「eventに対する人の状況を管理する」ことを分けて設計する。

第一段階として `health_exam_result.exam_ledgers` / `exam_ledger_sources` を追加し、既存 `xml_ledger` / `csv_row_ledger` から統合ledgerを作る同期スクリプトを追加した。
また、運用確認用の `exam_result_ledger_report` は `exam_ledgers` 起点で作成する方針へ更新した。

人単位状態は新しい横持ちテーブルを増やさず、既存 `dev_phr.person_event` を親にする。
可変なチェック項目、出力状態、要対応理由は `dev_phr.person_event_status_items` に縦持ちする。

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
  1人1健診結果または結合後の論理健診結果
  check、補正、XML出力候補判定の単位
  |
  +-- health_exam_result.exam_item_values
        source ledger または combined ledger に紐づくnamecode単位の検査結果値
```

既存の `xml_ledger` / `csv_row_ledger` は直ちに廃止しない。
これらは原本取込の台帳、source証跡、移行元として残し、今後の業務処理・画面・出力制御は `exam_ledgers` と `person_event` 系へ寄せる。

未突合のledgerはまだ「人」として確定していないため、`person_event` は作らない。
未突合、加入者確認待ち、施設確認待ちのsource状態は `exam_ledgers` で管理し、加入者確定後に `person_event` へ反映する。

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

- `person_event_id` または結合済み `exam_ledger_id`
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

初期実装では、既存 `exam_item_values` を使い分ける案を基本とする。

```text
source値:
  ledger_type = XML / CSV
  ledger_id = source ledger id

清書値:
  ledger_type = EXAM
  ledger_id = combined or adopted exam_ledger_id
```

将来、清書値の責務が大きくなった場合は `adopted_exam_item_values` 等の専用テーブルへ分離する。
ただし初期段階では、テーブルを増やすこと自体よりも、source値と清書値の責務分離、採用元参照、再生成可能性を優先する。

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
- `RESULT_RECEIVED_COUNT`: 受領済み健診結果数。
- `MATCHED_LEDGER_COUNT`: 加入者突合済みledger数。
- `CHECK_OK_LEDGER_COUNT`: 法定OKのledger数。
- `CHECK_NG_LEDGER_COUNT`: 法定NGのledger数。
- `CHECK_PENDING_LEDGER_COUNT`: 法定check未実行または保留のledger数。
- `EXPORTABLE_LEDGER_COUNT`: XML出力候補ledger数。
- `EXPORTED_LEDGER_COUNT`: XML出力済みledger数。
- `LATEST_EXAM_LEDGER_ID`: 最新または代表の統合ledger参照。
- `LATEST_EXAM_DATE`: 最新健診日。
- `LATEST_FACILITY_CODE`: 最新健診機関コード。
- `LATEST_FACILITY_NAME`: 最新健診機関名。
- `REQUIRES_BASIC_INFO_CORRECTION`: 基本情報補正待ち。
- `REQUIRES_MANUAL_EXPORT_APPROVAL`: 手動出力許可を含む。

今後、住所補完、HIAアップロード状態、再提出対象、結合候補、手動確認項目が増えても、テーブル定義ではなく item_code の追加で表現する。

### `health_exam_result.exam_ledgers`

`exam_ledgers` は「XMLに出せる可能性がある1つの健診結果」を表す。

想定する責務:

- XML由来、CSV由来、結合由来を問わず、論理健診結果を表す。
- `event_id`, `subscriber_id`, `exam_date`, `exam_facility_id`, `insurer_number`, `health_exam_report_category`, `program_code` を持つ。
- 加入者突合結果、法定チェック結果、基本情報補正後の現在値、XML出力候補判定を持つ。
- 複数sourceから結合した場合は、構成元 `xml_ledger` / `csv_row_ledger` / `file_receipts` を辿れるようにする。
- `exam_item_values` の親として、XML出力時に採用する検査値集合を定義する。
- source ledgerから清書済みの採用値を作る場合、その採用値の親になる。

このレイヤーは「event全体での人の作業状態」を主責務にしない。
同じ人に複数の健診日、複数の健診機関、複数のプログラムがある場合、`exam_ledgers` は複数件になりうる。

## Export Control Placement

出力制御は2つのレイヤーに分けて扱う。

### 出力候補判定

「この健診結果はXMLにできるか」は `exam_ledgers` の責務とする。

判定材料:

- 報告区分とプログラムコードがある。
- 加入者突合が `MATCHED`。
- 法定チェックが `OK`、またはMISSINGのみで手動出力許可済み。
- 基本情報のXML出力値が揃っている。
- 検査値に出力不可の `INVALID` が残っていない。

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

複数結果を1つにする処理は `exam_ledgers` を作る、または更新する処理として扱う。
元source ledgerを直接上書きせず、結合後の `COMBINED` または採用済み `exam_ledgers` を作成し、そこへ清書値を生成する。

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

## Implementation Steps

推奨順:

1. `exam_ledgers` / `exam_ledger_sources` のDDLと同期を作る。
   - 追加済み。
2. `03_check_exam_results.py` を統合ledger単位でも実行できるようにする。
   - `--ledger-type EXAM` で `exam_ledgers` を対象にする。
   - 追加済み。
3. `person_event_status_items` のDDLを作る。
4. `exam_ledgers` から `person_event` / `person_event_status_items` へ同期する。
5. `exam_result_ledger_report` は `exam_ledgers` 起点のまま維持する。
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
