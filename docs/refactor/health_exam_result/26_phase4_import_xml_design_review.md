# 26 Phase4 import_xml Design Review

## 1. 目的

Phase4 `02_import_xml.py` 実装前に、`03_decisions.md` を正本として、責務範囲、共通lib利用方針、入力/出力、ZIP/XML処理、identity基本情報抽出、`xml_ledger` / `xml_file_links` / `exam_item_values` 登録、`file_receipts.status` 更新、ETL記帳、未決事項を整理する。

本資料は実装前レビュー資料であり、スクリプト、DDL、migration、seed SQL は作成・変更しない。未決事項は勝手に決定せず、Phase4実装前に必要な判断材料として整理する。

## 2. 参照資料

- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/11_v2_script_design_notes.md`
- `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`
- `docs/refactor/health_exam_result/19_implementation_ready_summary.md`
- `docs/refactor/health_exam_result/20_implementation_plan.md`
- `docs/refactor/health_exam_result/25_phase3_scan_files_design_review.md`
- `scripts/from_medical/01_scan_files.py`
- `scripts/lib/etl/`
- `scripts/lib/db/`
- `scripts/lib/identity/README.md`
- `scripts/lib/identity/generator.py`
- `scripts/lib/identity/field/`
- `scripts/lib/identity/builder/`
- `docs/refactor/health_exam_result/01_codex_findings.md`
- `docs/refactor/health_exam_result/07_current_script_specs_codex.md`
- `sql/ddl/health_exam_result/`

## 3. Phase4の責務範囲

Phase4 `02_import_xml.py` は、Phase3で `file_receipts.status = DISCOVERED` として登録された ZIP / XML を入力に、XML内容をDBへ取り込む処理を担当する。

Phase4で実装する範囲:

- `file_receipts.status = DISCOVERED` かつ `file_type = ZIP / XML` の対象取得。
- 対象 `file_receipt` 単位のトランザクション管理。
- ZIPファイルから対象XMLを列挙する。
- 単体XMLをXMLとして読み込む。
- XML bytes の `xml_sha256` を計算する。
- `xml_sha256` による `xml_ledger` 一意判定。
- 新規XMLの基本情報抽出。
- `scripts/lib/identity.generator` を使った `person_id_custom` / `identity_hash` 生成。
- subscriber lookup 共通libを使った加入者照合。
- `xml_ledger` 登録または既存 `xml_ledger` 参照。
- `xml_file_links` 登録。
- XML内に実際に存在した健診項目値の `exam_item_values` 登録。
- `file_receipts.processable_count`、`content_checked_at`、`processed_at`、`status` の更新。
- `scripts/lib/etl` 共通APIによるRun開始・終了・エラー記録。

Phase4で完結するもの:

- 物理受領ファイルとXML内容の対応付け。
- XML内容台帳の作成。
- XML内健診値の縦持ち登録。
- XML単位の取込状態 `xml_status` / `xml_reason` の初期反映。
- `file_receipt` 単位の処理状態確定。

Phase7以降へ渡すもの:

- `xml_ledger.check_status` の制度チェック結果生成。
- `xml_ledger.xml_export_status` の最終的な出力可否判定。
- `exam_check_results` の72項目チェック結果。
- HIAアップロード用XMLの生成。
- 人＋イベント単位の最終運用状態管理。

Phase4では制度チェックの合否判定、HIA出力、CSV直取込、`file_type = OTHER` 対応は実装しない。

## 4. 入力・出力

### 入力

- `scripts/from_medical/config/import_xml.yml`。
- CLI引数は指定時のみconfig値の一時的な上書き用途とする。
- `event_id`。
- `health_exam_result.file_receipts`
  - `status = DISCOVERED`
  - `file_type IN ('ZIP', 'XML')`
  - `event_id` 一致
- CLIから `etl_run_id` を指定した場合のみ、対象を当該Runの `file_receipts` へ限定する。
- `file_receipts.source_path` の実ファイル。

### 出力

- `health_exam_result.xml_ledger`
- `health_exam_result.xml_file_links`
- `health_exam_result.exam_item_values`
- `health_exam_result.file_receipts` の状態・件数・時刻更新。
- XML取込Runの `health_exam_result.etl_runs`
- 運用上対応が必要なエラーの `health_exam_result.etl_errors`

### config / CLI指定

Phase4設定ファイルは `scripts/from_medical/config/import_xml.yml` とする。configを正本とし、CLI引数は指定時のみ一時的な上書き用途とする。通常実行は `event_id + file_receipts.status = DISCOVERED` を入力条件とし、CLIから `etl_run_id` を指定した場合のみ対象Runへ限定する。

## 5. 共通lib利用方針

### 必ず利用する共通lib

- `scripts/lib/etl`
  - `start_run`
  - `finish_run`
  - `log_error`
  - `RunMetrics`
- `scripts/lib/db`
  - DB接続、dict cursor、schema名管理。
- `scripts/lib/identity`
  - `generator.py` をPhase4のidentity生成入口とする。
  - Phase4から `builder/` を直接呼ばない。
- `scripts/lib/db/lookup/subscriber_identity.py`
  - `identity_hash` / `person_id_custom` / `hia_subscriber_id` などによるsubscriber照合候補。

### XML読込・検査項目抽出

既存 `scripts/lib/xml/` はXML操作の薄い共通処理であり、XMLファイル読込や健診項目抽出の共通実装はまだ持たない。`scripts/lib/shg/xml/` はSHG固有のXML抽出層であり、健診結果XMLへ直接流用しない。

Phase4では、健診結果XML固有の基本情報抽出・項目抽出処理は `scripts/from_medical/script_lib/` 配下の業務固有補助モジュールとして開始するのが推奨である。複数ドメインで再利用できるXML処理が明確になった場合のみ、後続で `scripts/lib/xml/` へ共通化する。

## 6. ZIP処理方針

ZIPはPhase4で初めて中身を確認する。

現時点の推奨:

- ZIPファイルは処理直前に `work` へコピーし、標準ライブラリ `zipfile` で展開または読み取りを行う。
- 小さなZIPであっても、実装の見通しと障害調査性を優先し、初期実装は一時展開方式を推奨する。
- 処理完了後は `work` を削除する。
- `--keep-work` 指定時のみ調査用に保持する。
- ZIP内XMLは、ZIP内相対パスを `xml_file_links.xml_inner_path` に保持する。

ZIP内対象XML:

- `h*.xml` のみ対象。
- `ix08*.xml` / `su08*.xml` は除外。
- schema関連 / XSD関連XMLは除外。
- 対象外XMLは原則 `etl_errors` に記録せず、Runサマリーまたは `file_receipts.summary_message` 相当の補足で十分かを検討する。

`processable_count`:

- Phase4でZIP内の対象XML件数を数え、`file_receipts.processable_count` に更新するのが推奨。
- 対象XMLが0件の場合は、`file_receipts.status = ERROR`、`xml_ledger` / `xml_file_links` / `exam_item_values` は作らず、`etl_errors` に運用対応が必要なエラーとして記録するのが推奨。
- 除外XMLは `processable_count` に含めない。

## 7. 単体XML処理方針

Phase3で登録済みの単体XMLは、すでに `h*.xml` のみである前提とする。

現時点の推奨:

- Phase4でも防御的に同じ除外条件を再確認する。
- `h*.xml` 以外の単体XMLが見つかった場合は、Phase3以前の登録不整合として当該 `file_receipt` を `ERROR` にするか、スキップするかをPhase4前に決める。
- 単体XMLの `processable_count` は通常1。
- 単体XMLの `xml_file_links.xml_inner_path` は `NULL`。

ZIPと単体XMLで、XML bytes 以降の処理は同じパイプラインに寄せる。

## 8. identity基本情報抽出方針

Phase4では、XMLから抽出したraw基本情報をもとに `scripts/lib/identity.generator` を必ず利用して `person_id_custom` / `identity_hash` を生成する。

禁止すること:

- XML raw値を直接 `builder/` に渡さない。
- Phase4から `builder.person_id_custom` / `builder.identity_hash` を直接呼ばない。
- XML parser内で独自正規化しない。
- Phase4固有ロジックで `person_id_custom` / `identity_hash` を組み立てない。

責務分担:

- XML抽出層: XMLからraw値を取得する。業務項目の場所を知るが、identity正規化はしない。
- `identity.generator`: raw値を受け、fieldを呼び、builderへ渡すオーケストレーションを行う。
- `identity.field`: 型差異、表記揺れ、欠損、照合用値、正規化値を扱う。
- `identity.builder`: canonical input から完成値を生成する。不足時の補完・推測はしない。

データフロー:

```text
XML raw
  ↓
identity.generator
  ↓
field
  ↓
builder
  ↓
person_id_custom / identity_hash
  ↓
xml_ledger
```

`generator` 戻り値の扱い:

- `ok = true`
  - `person_id_custom` と `identity_hash` を `xml_ledger` に記録する。
  - `field_results` から `name_kana_match`、`insurance_symbol_match`、`insurance_number_match` など、DDLにあるmatch系項目へ記録できる値を採用する。
- `ok = false`
  - `reason` を `xml_ledger.xml_reason` または `subscriber_match_reason` に記録する候補とする。
  - `xml_status` を `ERROR` にするか、XML取込自体は登録して `subscriber_match_status` で失敗を表現するかはPhase4前の人間判断が必要。
  - 運用上対応が必要なidentity生成失敗は `etl_errors` に `field = IDENTITY`、`error_code = IDENTITY_GENERATION_FAILED` 相当で記録するのが推奨。

加入者照合:

- `identity_hash` が生成できた場合は、`scripts/lib/db/lookup/subscriber_identity.py` の利用を第一候補とする。
- 1件一致なら `subscriber_id` / `hia_subscriber_id` / `subscriber_match_status` を `xml_ledger` へ記録する。
- 0件・複数件の場合の `xml_status` / `subscriber_match_status` / `xml_reason` はPhase4前に決める必要がある。

## 9. xml_ledger登録方針

`xml_ledger` はXML内容の一意台帳であり、`xml_sha256` を一意性判定の基準とする。

新規XML:

- XML bytes から `xml_sha256` を計算する。
- `xml_sha256` 未登録の場合のみ `xml_ledger` を作成する。
- XML基本情報を抽出し、`document_id`、`insurer_number`、`facility_code`、`facility_name`、`exam_date`、raw系・match系項目を記録する。
- identity生成とsubscriber照合を行い、結果を `person_id_custom`、`identity_hash`、`subscriber_id`、`hia_subscriber_id`、`subscriber_match_*` へ記録する。
- 取込成功時の `xml_status` は `IMPORTED` が推奨。
- `check_status` と `xml_export_status` はDDL初期値 `PENDING` のままとし、Phase7以降へ渡す。

既存XML再受領:

- `xml_ledger` は重複作成しない。
- `xml_file_links` のみ追加し、受領事実を記録する。
- 既存XMLは同一人物ではなく `xml_sha256` 一致で判定する。
- 同一 `xml_sha256` のXMLを再受領した場合、`xml_ledger` は新規作成せず、`xml_file_links` のみ追加する。
- 既存XMLに対して `exam_item_values` は重複登録しない。

XML parse不能・基本情報抽出不能:

- XML内容台帳を作るかどうかが未決である。
- parse不能では `xml_sha256` は計算可能だが、XMLとしての基本情報が取れないため、`xml_ledger` を `ERROR` で作るか、`file_receipts.ERROR` と `etl_errors` のみに留めるかをPhase4前に決める必要がある。

## 10. xml_file_links登録方針

`xml_file_links` は物理ファイルとXML内容の対応台帳である。

登録タイミング:

- XML bytes の `xml_sha256` を計算し、対応する `xml_ledger_id` が確定した後に登録する。
- 新規XMLでも既存XMLでも登録する。
- 同一 `file_receipt_id` / `xml_ledger_id` / `xml_inner_path` は重複登録しない。

登録値:

- `event_id`
- `file_receipt_id`
- `xml_ledger_id`
- `xml_inner_path`

ZIP内複数XML:

- 対象XMLごとに `xml_file_links` を1行作成する。
- `xml_inner_path` はZIP内相対パス。
- 同一ZIP内に同一内容XMLが複数ある場合も、`xml_inner_path` が異なれば別受領事実としてリンクできる。

単体XML:

- `xml_inner_path = NULL`。

## 11. exam_item_values登録方針

Phase4では、XML内に実際に存在した健診項目値のみ `exam_item_values` に登録する。

登録範囲:

- `ledger_type = XML`
- `ledger_id = xml_ledger.id`
- `event_id`
- `subscriber_id`
- `hia_subscriber_id`
- `namecode`
- `occurrence_no`
- raw値、raw unit、nullFlavor、code系情報
- `identity_item_code`
- `jun_no`
- `extracted_run_id`
- `extracted_at`

正規化:

- `normalized_value` / `normalized_unit` はPhase4登録処理内で生成する方針が03で決定済み。
- ただし、健診項目値の正規化ルールと `validation_status` の正式値は未決である。
- 初期実装では、raw値を保持し、可能な範囲で安全な型・単位正規化のみ行う案が現実的である。

登録しないもの:

- 制度チェック上の不足項目。
- XML内に存在しない健診値。
- Phase7の判定結果。

既存XML再受領時:

- `xml_ledger` が既存の場合、`exam_item_values` は重複登録しない。
- 新しい `xml_file_links` のみ追加する。

## 12. file_receipts.status更新方針

Phase4では `file_receipts.status` を `DISCOVERED / IMPORTING / IMPORTED / WARNING / ERROR` を前提に整理する。

状態遷移:

```text
DISCOVERED
  ↓
IMPORTING
  ↓
IMPORTED / WARNING / ERROR
```

推奨:

- 対象 `file_receipt` の処理開始時に `IMPORTING` へ更新する。
- 対象XMLがすべて正常に処理できた場合は `IMPORTED`。
- ZIP内の対象XMLが一部のみ正常処理できた場合は `WARNING`。
- ZIP内対象XMLが0件の場合は `ERROR`。
- ファイル読込不能、ZIP展開不能、XML parse不能、DB登録失敗など、当該 `file_receipt` として後続へ渡せない場合は `ERROR`。

一部成功時:

- ZIP内複数XMLのうち一部成功・一部失敗した場合は `file_receipts.status = WARNING` とする。
- 正常XMLのみ `xml_ledger` を登録する。
- 失敗XMLは `xml_ledger` を作成せず、`etl_errors` に記録する。
- `file_receipts` はファイル全体の総合状態、`xml_ledger` は正常XML、`etl_errors` は失敗詳細を管理する。

件数・時刻:

- `processable_count` は対象XML件数。
- `content_checked_at` はZIP/XML内容確認完了時。
- `processed_at` は `file_receipt` 処理完了時。

## 13. ETL記帳方針

Phase4では `scripts/lib/etl` 共通APIを必ず利用する。独自の `etl_runs` / `etl_errors` SQLは禁止する。

Run:

- `phase = IMPORT_XML` が推奨。
- `source = FROM_MEDICAL` が推奨。
- `input_base` は `event_id=<id>` や対象Run条件を表現する。
- `input_file` はRun全体では `NULL`、ファイル単位のエラーでは `etl_errors.src_file` に記録する。
- `status` は共通ETL仕様の `running / success / partial / failed`。
- `notes` は人間が読みやすい短いサマリーとする。

Metrics:

- `files`: 処理対象 `file_receipts` 件数。
- `rows_seen`: 検出した対象XML件数。
- `rows_inserted`: 新規 `xml_ledger` または `exam_item_values` のどちらを基準にするかは要判断。
- `rows_updated`: `file_receipts` 更新や既存XMLリンク追加を含めるかは要判断。
- `rows_skipped`: 既存XML再受領、除外XML、対象外ファイル。
- `errors`: `etl_errors` 登録件数。

Errors:

- `log_error` を利用する。
- Phase4の `etl_errors` は `field`、`error_code`、`message` を基本構成として記録する。
- `field` は `ZIP_READ`、`XML_PARSE`、`XML_BASIC_INFO`、`IDENTITY`、`SUBSCRIBER_LOOKUP`、`ITEM_EXTRACT`、`DB_WRITE` などの大分類候補。
- `error_code` は実装で必要最小限から開始する。
- `src_file` は `file_receipts.source_path` またはZIP内XMLを含む識別子。
- ZIP内XMLの場合、`field_value` に `xml_inner_path` を入れる案がある。

## 14. 未決事項一覧

| 論点 | 現時点の推奨 | 理由 | 人間判断 |
| --- | --- | --- | --- |
| Phase4の入力条件 | 通常実行は `event_id + file_receipts.status = DISCOVERED`、CLI `etl_run_id` 指定時のみ対象Runへ限定する。 | 通常運用ではイベント単位の未処理ファイル処理が扱いやすく、障害解析時のみRun限定が有効なため。 | 決定済み。 |
| Phase4 configファイル名 | `scripts/from_medical/config/import_xml.yml` とする。 | Phase3のconfig正本方針と揃えるため。 | 決定済み。詳細設定項目は未決。 |
| ZIP読取方式 | 初期実装は一時展開方式。 | 障害調査と実装の見通しが良い。 | ストリーム読みにする必要があるか。 |
| ZIP内対象XML | `h*.xml` のみ対象、`ix08/su08/schema/xsd` は除外。 | Phase3単体XMLと同じ除外基準に揃える。 | 除外XML件数をどこに記録するか。 |
| `processable_count` | 除外後の対象XML件数。 | 後続処理可能なXML数として扱える。 | 0件時のメッセージ・error_code。 |
| 単体XMLの再バリデーション | Phase4でも同じ除外条件を防御的に確認する。 | DB汚染や手動投入に備える。 | 不整合時にERRORかスキップか。 |
| parse不能XMLの `xml_ledger` | `file_receipts.ERROR` + `etl_errors` に留める案が安全。 | 基本情報が取れないXMLを台帳化すると後続対象の扱いが曖昧になる。 | `xml_sha256` だけでERROR台帳を作るか。 |
| identity生成失敗時 | `xml_ledger` は作成し、`xml_status = ERROR` または `subscriber_match_status` で表現する候補。 | XML内容自体は受領済みで、運用確認対象にできる。 | `xml_status` をERRORにするか、subscriber照合状態に閉じるか。 |
| 既存XML再受領 | `xml_sha256` 一致で判定し、同一 `xml_sha256` 再受領時は `xml_file_links` のみ追加する。 | `xml_ledger` はXML内容の一意台帳であり、同一内容XMLを重複登録しないため。 | 決定済み。`SKIPPED` の詳細表現は未決。 |
| `xml_file_links` 重複 | UNIQUE衝突は重複リンクとしてスキップ。 | 再実行時の冪等性を確保する。 | 重複リンクをRunスキップ件数に含めるか。 |
| `exam_item_values.validation_status` | 初期は `NULL` または最小値のみ。 | 正式値が未決のため。 | Phase4前に正式値を決めるか、NULL許容で進めるか。 |
| 一部成功ZIPの `file_receipts.status` | `WARNING` とする。正常XMLのみ `xml_ledger` 登録、失敗XMLは `etl_errors` 記録。 | ファイル全体の総合状態、正常XML、失敗詳細の責務を分離できるため。 | 決定済み。 |
| ETL metricsの基準 | `files = file_receipts`、`rows_seen = 対象XML` を推奨。 | Runサマリーでファイル数とXML数を分けられる。 | inserted/updated/skippedの粒度。 |
| Phase4 `etl_errors` 基本構成 | `field` / `error_code` / `message` を基本構成とする。 | 共通ETL構造に合わせ、Phase4固有の独自構造を避けるため。 | 基本方針は決定済み。正式 `error_code` 一覧は未決。 |
| `--keep-work` | デバッグ用に用意する候補。 | ZIP展開・XML解析失敗の調査に有用。 | オプション名と保持先命名。 |

### Phase4前に決めるもの

- ZIP内対象XML0件時の扱い。
- parse不能XMLを `xml_ledger` に作るか。
- identity生成失敗時の `xml_status` / `subscriber_match_status` / `etl_errors` 反映方針。
- `import_xml.yml` の詳細設定項目。
- `file_receipts.status` の正式コード一覧。
- `etl_errors.error_code` の正式コード一覧。

### 実装中でよいもの

- ZIP展開ディレクトリ名。
- `--keep-work` の詳細。
- ETL `notes` の具体フォーマット。
- XML抽出補助モジュールの分割粒度。
- `exam_item_values` の `occurrence_no` 採番ロジックの細部。

### Phase7以降でよいもの

- `check_status` の算出。
- `xml_export_status` の最終出力可否。
- `exam_check_results` 登録。
- `validation_status` の詳細判定拡張。
- CSV直取込。
- 人＋イベント単位の状態管理台帳。

## 15. Phase4実装GO判定

Phase4の責務範囲と共通lib利用方針は概ね整理できている。ただし、以下は実装前に人間判断が必要である。

- parse不能XMLを `xml_ledger` に作るか。
- identity生成失敗時に `xml_status = ERROR` とするか、subscriber照合状態に閉じるか。
- ZIP内対象XML0件時の扱い。
- `import_xml.yml` の詳細設定項目。
- `etl_errors.error_code` の正式コード一覧。

Phase4の入力条件、configファイル名、一部成功ZIP、既存XML再受領、Phase4 `etl_errors` 基本構成は決定済みである。残る未決をPhase4実装前に解消できればGOと判断できる。未決のまま実装する場合は、実装内で仮決定せず、エラー側へ倒すか、対象外として明示的にスキップする必要がある。
