# Event Person Status and Exam Ledger Layers

## Status

Draft decision note as of 2026-08-02.

CSV/XML取込からXML出力まで一通り動いたため、次の実装は「複数の結果を1つの論理健診結果にまとめる」ことと、「eventに対する人の状況を管理する」ことを分けて設計する。

2026-08-02時点で、第一段階として `exam_ledgers` / `exam_ledger_sources` のDDLと、既存 `xml_ledger` / `csv_row_ledger` から統合ledgerを作る同期スクリプトを追加した。
また、運用確認用の `exam_result_ledger_report` は `exam_ledgers` 起点で作成する方針へ更新した。

## Layer Model

```text
dev_phr.event
  |
  +-- health_exam_result.event_person_statuses
        eventに対する人の現在状態
        出力制御、HIAアップロード状態、要対応理由の管理単位
        |
        +-- health_exam_result.exam_ledgers
              1人1健診結果または結合後の論理健診結果
              check、補正、XML出力候補判定の単位
              |
              +-- health_exam_result.exam_item_values
                    namecode単位の検査結果値
```

既存の `xml_ledger` / `csv_row_ledger` は直ちに廃止しない。
これらは原本取込の台帳、source証跡、移行元として残し、今後の業務処理・画面・出力制御は上記2レイヤーへ寄せる。

## Responsibilities

### `event_person_statuses`

`event_person_statuses` は「このeventで、この人が今どこにいるか」を表す人単位の業務状態である。

想定する責務:

- event対象者としての現在状態を持つ。
- 受領済み、未受領、突合済み、補正待ち、施設確認待ち、法定OK、XML出力可能、XML出力済み、HIAアップロード済みなどの人単位状態を表す。
- 画面の一覧、進捗管理、担当者の確認作業、HIAアップロード後のステータス更新の主キーになる。
- 最新または代表の `exam_ledger_id` を参照できる。
- 出力済みか、再出力対象か、HIAアップロード済みかはこのレイヤーで扱う。

このレイヤーは検査値そのものを持たない。
検査値の正当性や結合結果は `exam_ledgers` と `exam_item_values` を参照して判断する。

### `exam_ledgers`

`exam_ledgers` は「XMLに出せる可能性がある1つの健診結果」を表す。

想定する責務:

- XML由来、CSV由来、結合由来を問わず、論理健診結果を表す。
- `event_id`, `subscriber_id`, `exam_date`, `exam_facility_id`, `insurer_number`, `health_exam_report_category`, `program_code` を持つ。
- 加入者突合結果、法定チェック結果、基本情報補正後の現在値、XML出力候補判定を持つ。
- 複数sourceから結合した場合は、構成元 `xml_ledger` / `csv_row_ledger` / `file_receipts` を辿れるようにする。
- `exam_item_values` の親として、XML出力時に採用する検査値集合を定義する。

このレイヤーは「event全体での人の作業状態」を主責務にしない。
同じ人に複数の健診日、複数の健診機関、複数のプログラムがある場合、`exam_ledgers` は複数件になりうる。

### `xml_export_zips` / `xml_export_members`

出力履歴テーブルは「いつ、どのZIPへ、どの個人XMLを出したか」という事実を保存する。

想定する責務:

- 正常に作成したZIPと収録した個人XMLの履歴を追記する。
- 再出力時も過去履歴を更新せず、新しい履歴として追加する。
- 出力履歴は現在状態ではなく、event_person_statusesが現在状態を持つための参照元になる。

HIAアップロード済み、アップロードエラー、再提出対象などの現在状態は、出力履歴そのものではなく `event_person_statuses` 側で扱う。

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

「このeventでこの人は出力済みか、HIAへアップロード済みか、再提出対象か」は `event_person_statuses` の責務とする。

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

## Initial Table Candidates

### `event_person_statuses`

初期候補カラム:

- `event_person_status_id`
- `event_id`
- `subscriber_id`
- `identity_hash`
- `insurer_number`
- `person_status`
- `person_status_reason`
- `latest_exam_ledger_id`
- `has_received_result`
- `has_matched_subscriber`
- `has_check_ok_ledger`
- `has_exportable_ledger`
- `xml_export_status`
- `hia_upload_status`
- `requires_basic_info_correction`
- `requires_facility_confirmation`
- `requires_manual_export_approval`
- `last_xml_export_member_id`
- `last_xml_export_zip_id`
- `created_at`
- `updated_at`

状態名は実装時に定数化する。
初期DDLでは過剰に細かい状態列を増やしすぎず、一覧・絞り込み・手動更新に必要な最小から開始する。

### `exam_ledgers`

初期候補カラム:

- `exam_ledger_id`
- `event_id`
- `source_type`
- `source_xml_ledger_id`
- `source_csv_row_ledger_id`
- `file_receipt_id`
- `subscriber_id`
- `identity_hash`
- `subscriber_match_status`
- `subscriber_match_reason`
- `exam_facility_id`
- `facility_code`
- `facility_name`
- `insurer_number`
- `exam_date`
- `health_exam_report_category`
- `program_code`
- `check_status`
- `check_reason`
- `manual_export_approved`
- `manual_export_reason`
- `manual_export_approved_by`
- `manual_export_approved_at`
- `xml_export_status`
- `correction_status`
- `merge_status`
- `merge_reason`
- `created_at`
- `updated_at`

結合sourceが複数になる場合は、別テーブル `exam_ledger_sources` を追加し、`exam_ledgers` 本体には代表sourceだけを持つか、sourceなしの `COMBINED` として扱う。

## Implementation Steps

推奨順:

1. `event_person_statuses` / `exam_ledgers` / `exam_ledger_sources` のDDL案を作る。
   - `exam_ledgers` / `exam_ledger_sources` は追加済み。
   - `event_person_statuses` は次段階で作る。
2. 既存 `xml_ledger` / `csv_row_ledger` から `exam_ledgers` を同期するスクリプトを作る。
   - `scripts/from_medical/dev_tools/sync_exam_ledgers.py` を追加済み。
3. `event_person_statuses` を `event + subscriber` 単位で作成・更新するスクリプトを作る。
4. `03_check_exam_results.py` を統合ledger単位でも実行できるようにする。
   - `--ledger-type EXAM` で `exam_ledgers` を対象にする。
   - 既存の `--ledger-type XML` / `CSV` 実行時も、対応する `exam_ledgers.check_status` / `check_reason` へ結果を反映する。
5. 複数source候補を検出し、`MULTIPLE_SOURCE_ROWS_UNRESOLVED` を出せるようにする。
6. 明示選択されたsourceを結合し、結合後 `exam_item_values` を作る。
7. 結合後checkを実行し、`event_person_statuses` へ現在状態を反映する。
8. `04_export_hia_xml.py` を `exam_ledgers` 起点へ寄せる。
9. 出力後に `event_person_statuses` のHIAアップロード待ち状態を更新する。
10. 画面から出力条件選択、基本情報補正、HIAアップロード状態更新を行えるようにする。

## Deferred Decisions

- `event_person_statuses` の正式テーブル名。
- 人単位状態の最小定数セット。
- `exam_ledgers` のsource複数保持を本体JSONにするか、`exam_ledger_sources` 別テーブルにするか。
- XML/CSV個別ledgerから統合ledgerへ移行するか、再scan/再importで作り直すか。
- HIAアップロード状態を人単位で持つか、ZIP単位・member単位の状態も別途持つか。
- 基本情報補正履歴を統合ledger IDだけで持つか、source ledgerへの逆参照も保持するか。
