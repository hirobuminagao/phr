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
  - `generator.py` をPhase4のidentity生成の唯一の入口・正本とする。
  - Phase4から `builder/` / `field/` を直接呼ばない。
  - `02_import_xml.py` 内にidentity生成ロジックを持たせない。
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

- Phase4でZIP内の対象XML件数を数え、`file_receipts.processable_count` に更新する。
- Phase3ではZIP内件数を算出せず、`processable_count` は設定しない。
- 対象XML件数は、Phase4のZIP内XML除外ルール適用後の取込対象XML件数とする。
- 対象XMLが0件の場合は、`file_receipts.status = ERROR`、`xml_ledger` / `xml_file_links` / `exam_item_values` は作らず、`etl_errors` に `field = ZIP`、`error_code = ZIP_NO_TARGET_XML` を基本として記録する。
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

Phase4では、XMLから抽出したraw基本情報をもとに `scripts.lib.identity.generator.generate_identity_bundle(**kwargs)` を必ず利用して `person_id_custom` / `identity_hash` を生成する。

Phase4ではXML raw基本情報からdictを作成し、`generate_identity_bundle(**raw)` に渡す。Phase4で渡すidentity入力キーは以下とする。

- `birthdate`
- `insurer_number_raw`
- `insurance_symbol_raw`
- `insurance_number_raw`
- `name_kana_full_raw`
- `gender_code`

禁止すること:

- XML raw値を直接 `builder/` に渡さない。
- Phase4から `field/` を直接呼ばない。
- Phase4から `builder.person_id_custom` / `builder.identity_hash` を直接呼ばない。
- XML parser内で独自正規化しない。
- Phase4固有ロジックで `person_id_custom` / `identity_hash` を組み立てない。

責務分担:

- XML抽出層: XMLからraw値を取得する。業務項目の場所を知るが、identity正規化はしない。
- `identity.generator`: raw値を受け、fieldを呼び、builderへ渡すオーケストレーションを行う。
- `identity.field`: 型差異、表記揺れ、欠損、照合用値、正規化値を扱う。
- `identity.builder`: canonical input から完成値を生成する。不足時の補完・推測はしない。

`scripts/lib/identity/generator.py` を `identity_hash` / `person_id_custom` 生成のSingle Source of Truthとする。identity生成仕様を変更する場合は共通identity lib側を修正し、利用側スクリプトには生成ロジックを持たせない。

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

Phase4が `generate_identity_bundle()` の戻り値として利用するのは以下のみとする。

- `ok`
- `reason`
- `person_id_custom`
- `identity_hash`
- `field_results`

Phase4は上記以外の内部構造へ依存しない。

- `ok = true`
  - `person_id_custom` と `identity_hash` を `xml_ledger` に記録する。
  - `field_results` から `name_kana_match`、`insurance_symbol_match`、`insurance_number_match` など、DDLにあるmatch系項目へ記録できる値を採用する。
- `ok = false`
  - XMLとして正常に読み込み可能な場合は `xml_ledger` を作成する。
  - `generator.reason` を代表理由として扱う。
  - 失敗fieldの詳細は `field_results` を参照する。
  - `reason` を `xml_ledger.xml_reason` または `subscriber_match_reason` に記録する候補とする。
  - `identity_hash` / `person_id_custom` / `subscriber_id` / `hia_subscriber_id` は未設定とする。
  - identity生成失敗の詳細は `etl_errors` に `field = IDENTITY` として記録する。
  - `etl_errors.message` には失敗fieldと理由を人が読める形式で一覧化して記録する。
  - `error_code` は機械判定用、`message` は人間確認用、`field_results` は詳細ソースとして役割を分ける。
  - `error_code` は下記のコード体系を基本とする。
  - `message` は下記のフォーマットを基本とし、複数fieldが失敗した場合はカンマ区切りで列挙する。

`etl_errors.message` の基本形式:

```text
identity generation failed: <field>=NG(<reason>), <field>=NG(<reason>)
```

例:

```text
identity generation failed: birthdate=NG(EMPTY), insurance_number=NG(EMPTY)
```

`etl_errors.error_code` は以下を基本とする。

- `IDENTITY_BIRTHDATE_INVALID`
- `IDENTITY_INSURER_NUMBER_INVALID`
- `IDENTITY_INSURANCE_SYMBOL_INVALID`
- `IDENTITY_INSURANCE_NUMBER_INVALID`
- `IDENTITY_NAME_KANA_FULL_INVALID`
- `IDENTITY_HASH_BUILD_FAILED`

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

- parse不能XMLでもXMLファイル自体のSHA256から `xml_sha256` を算出し、最小情報で `xml_ledger` を作成する。
- parse不能XMLの `xml_status` は `PARSE_ERROR` とする。
- parse不能XMLでは `identity_hash`、`person_id_custom`、`subscriber_id`、`hia_subscriber_id` は設定しない。
- parse不能XMLでは `exam_item_values` を登録しない。
- parse不能XMLの詳細は `etl_errors` に `field = XML`、`error_code = XML_PARSE_FAILED` を基本として記録する。
- 同一 `xml_sha256` のparse不能XMLを再受領した場合、`xml_ledger` は新規作成せず `xml_file_links` のみ追加する。
- 以前の「parse不能XMLは `xml_ledger` を作成しない」方針は更新する。parse不能でもファイルバイト列から `xml_sha256` を算出でき、同一壊れXMLの再受領・重複判定・監査管理が可能なため、XML解析可否とXML台帳管理を分離する。

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

`exam_item_values` はPhase4（`02_import_xml.py`）で、`xml_ledger` 作成後に登録する。XML解析が成功している場合は、identity生成に失敗しても検査値はXML内容として保持できるため、`exam_item_values` を登録する。

状態管理:

- XML状態は `xml_status` で管理する。
- 加入者照合状態は `subscriber_match_status` で管理する。
- 検査値抽出・バリデーション状態は `exam_item_status` で管理する。
- `xml_status` に加入者照合結果や検査値バリデーション結果を混在させない。
- 加入者照合NG時に `xml_status` は変更しない。
- `subscriber_match_status` と `exam_item_status` はそれぞれ独立した状態として扱う。
- `xml_ledger` 側には検査値全体の総合状態として `exam_item_status` を保持する。
- `xml_ledger.exam_item_status` / `exam_item_reason` はDDLへ追加済みであり、既存DB向けMigrationも作成済みである。
- `exam_item_reason` は必要に応じて検査値総合状態の理由・サマリーを保持する。
- health_exam_result のMigrationファイル名は `YYYYMMDD_NNN_health_exam_result_<description>.sql` とし、連番はその日の `sql/migrations/health_exam_result/` 配下で採番する。
- `description` は英小文字 + snake_case とし、例は `20260707_001_health_exam_result_add_exam_item_status.sql` とする。
- DDLのみ更新してMigrationを後回しにしない。

Phase4で使用する正式コード:

- `file_receipts.status`: `DISCOVERED` / `IMPORTING` / `IMPORTED` / `WARNING` / `ERROR`
- `xml_status`: `READY` / `PARSE_ERROR`
- `subscriber_match_status`: `MATCHED` / `NOT_FOUND` / `IDENTITY_ERROR` / `NOT_EXECUTED`
- `exam_item_status`: `OK` / `WARNING` / `ERROR` / `NOT_EXECUTED`

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
- `exam_item_values` 登録時の値検証は `item_master` を参照して実施する。
- 型、単位、必須可否、namecode存在有無などの判定は `item_master` の定義を基準とする。
- 検査値読込・値としての妥当性エラーは制度チェックとは分離する。
- 法定健診・特定健診などの制度チェックは後続Phaseの `exam_check_results` 側で扱う。
- XML内に項目entryとして存在したものは、値や型に問題があっても可能な限り `exam_item_values` に行を作る。
- 項目単位の結果は `exam_item_values.normalize_status` / `normalize_reason` および `validation_status` / `validation_reason` に保持する。
- `normalize_status` はraw値から `normalized_value` / `normalized_unit` を作成できたかを表す。
- `validation_status` は `exam_item_master` 定義に照らして値として妥当かを表す。
- 正常な場合は `normalize_status = OK`、`validation_status = OK` を基本とする。
- 数値変換できない場合は `normalize_status = ERROR`、`validation_status = INVALID` を基本とする。
- namecodeがmasterにない場合は `normalize_status = SKIPPED`、`validation_status = INVALID`、`validation_reason = UNKNOWN_NAMECODE` を基本とする。
- 型不一致は `normalize_status = ERROR`、`validation_status = INVALID`、`validation_reason = INVALID_VALUE_TYPE` を基本とする。
- null不可なのにnullの場合は `validation_status = INVALID`、`validation_reason = NULL_NOT_ALLOWED` を基本とする。
- 単位不一致は `normalize_status = WARNING`、`validation_status = WARNING`、`validation_reason = UNIT_MISMATCH` を基本とする。

`item_master` lookup:

- `item_master` 参照処理は共通Lookupライブラリへ集約する。
- 呼び出し側スクリプトで個別SQLを直接実装しない。
- Phase4も後続フェーズも同一Lookupライブラリを利用する。
- 共通Lookupライブラリは単品取得・複数取得の両APIを提供する。
- 単品取得は、単一 `namecode` を指定し、該当項目情報をdictまたはNoneで返す。
- 複数取得は、複数 `namecode` を指定し、`namecode -> item情報` のdictで返す。
- 複数取得はPhase4のXML内namecode一括処理や後続制度チェックで利用する。
- 単品取得は調査・個別処理・後続スクリプトで利用する。

登録しないもの:

- 制度チェック上の不足項目。
- XML内に存在しない健診値。
- Phase7の判定結果。

既存XML再受領時:

- `xml_ledger` が既存の場合、`exam_item_values` は重複登録しない。
- 新しい `xml_file_links` のみ追加する。

一部検査値の取得失敗時:

- 取得可能な検査値は `exam_item_values` に登録する。
- 不足・異常がある項目は `etl_errors` に記録する。
- 一部検査値の取得失敗だけでXML全体の処理を停止しない。

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
- ZIP内全件parse不能など、対象XMLはあるが全件失敗の場合は `ERROR`。
- 単体XML parse不能の場合は、`xml_ledger` を `PARSE_ERROR` で作成し、`file_receipts.status = ERROR` とする。
- ファイル読込不能、ZIP展開不能、DB登録失敗など、当該 `file_receipt` として後続へ渡せない場合は `ERROR`。

一部成功時:

- ZIP内複数XMLのうち一部成功・一部失敗した場合は `file_receipts.status = WARNING` とする。
- 正常XMLとparse不能XMLを `xml_ledger` に登録する。
- parse不能XMLは `xml_ledger` を `PARSE_ERROR` で作成し、`etl_errors` に記録する。
- XMLファイルとして扱えない失敗は `xml_ledger` を作成せず、`etl_errors` に記録する。
- `file_receipts` はファイル全体の総合状態、`xml_ledger` はXML内容の一意台帳、`etl_errors` は失敗詳細を管理する。

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
- `rows_seen`: 対象XML件数。
- `rows_inserted`: 新規 `xml_ledger` 件数。
- `rows_updated`: `xml_file_links` 登録件数 + `file_receipts` 更新件数。
- `rows_skipped`: 既存 `xml_sha256` 再受領・対象外XML件数。
- `errors`: `etl_errors` 登録件数。
- `exam_item_values` 件数は `rows_inserted` に含めず、必要に応じて `etl_runs.notes` のサマリーへ記録する。

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
| `processable_count` | Phase4でZIP展開後に、除外後の対象XML件数を設定する。ZIP内対象XML0件は `file_receipts.status = ERROR` とし、`etl_errors` に `field = ZIP`、`error_code = ZIP_NO_TARGET_XML` を基本として記録する。 | ZIP内XML件数は展開後でなければ確定できず、実際の処理対象件数と一致した値を保持するため。 | 決定済み。0件時の詳細messageフォーマットは未決。 |
| 単体XMLの再バリデーション | Phase4でも同じ除外条件を防御的に確認する。 | DB汚染や手動投入に備える。 | 不整合時にERRORかスキップか。 |
| parse不能XMLの `xml_ledger` | parse不能XMLでもXMLファイルSHA256から `xml_sha256` を算出し、最小情報で `xml_ledger` を作成する。`xml_status = PARSE_ERROR`、identity系項目は未設定、`exam_item_values` は登録しない。詳細は `etl_errors` に `field = XML`、`error_code = XML_PARSE_FAILED` を基本として記録する。 | XML解析可否とXML台帳管理を分離し、同一壊れXMLの再受領・重複判定・監査管理を可能にするため。 | 決定済み。`xml_reason` の正式メッセージは未決。 |
| identity generator API | `generate_identity_bundle(**raw)` を利用する。入力キーは `birthdate`、`insurer_number_raw`、`insurance_symbol_raw`、`insurance_number_raw`、`name_kana_full_raw`、`gender_code`。戻り値は `ok`、`reason`、`person_id_custom`、`identity_hash`、`field_results` のみ利用する。 | identity生成の入口を単一化し、利用側に生成ロジックと内部構造依存を持たせないため。 | 決定済み。戻り値仕様全体の詳細記載粒度は未決。 |
| identity生成失敗時 | XMLとして正常に読み込み可能なら `xml_ledger` は作成し、詳細は `etl_errors` に `field = IDENTITY` として記録する。`generator.reason` を代表理由、`field_results` を詳細ソース、`message` を人間確認用とする。 | XML内容自体は受領済みで、運用確認対象にできるため。 | 決定済み。 |
| 状態管理の責務分離 | XML状態は `xml_status`、加入者照合状態は `subscriber_match_status`、検査値抽出・バリデーション状態は `exam_item_status` で管理する。`xml_status` に加入者照合結果や検査値バリデーション結果を混在させず、加入者照合NG時も `xml_status` は変更しない。 | XML解析、加入者照合、検査値バリデーションは責務が異なるため。 | 決定済み。 |
| Phase4 status正式コード | `file_receipts.status = DISCOVERED / IMPORTING / IMPORTED / WARNING / ERROR`、`xml_status = READY / PARSE_ERROR`、`subscriber_match_status = MATCHED / NOT_FOUND / IDENTITY_ERROR / NOT_EXECUTED`、`exam_item_status = OK / WARNING / ERROR / NOT_EXECUTED` とする。 | Phase4内のXML状態、加入者照合状態、検査値状態を混在させず実装するため。 | 決定済み。 |
| `xml_ledger.exam_item_status` DDL | `xml_ledger.exam_item_status` / `exam_item_reason` をDDLへ追加し、既存DB向けMigration `20260707_001_health_exam_result_add_exam_item_status.sql` を作成済み。 | Phase4で検査値抽出・妥当性の総合状態を `xml_ledger` に保持するため。 | 決定済み。 |
| 既存XML再受領 | `xml_sha256` 一致で判定し、同一 `xml_sha256` 再受領時は `xml_file_links` のみ追加する。 | `xml_ledger` はXML内容の一意台帳であり、同一内容XMLを重複登録しないため。 | 決定済み。`SKIPPED` の詳細表現は未決。 |
| `xml_file_links` 重複 | UNIQUE衝突は重複リンクとしてスキップ。 | 再実行時の冪等性を確保する。 | 重複リンクをRunスキップ件数に含めるか。 |
| `exam_item_values` 登録 | Phase4で `xml_ledger` 作成後に登録する。XML解析が成功していればidentity生成失敗時も登録し、同一 `xml_sha256` 再受領時は再登録しない。一部検査値取得失敗時は取得可能な値を登録し、不足・異常は `etl_errors` に記録して継続する。 | 検査値は加入者照合結果とは独立したXML内容であり、XML解析成功時に保持できるため。 | 決定済み。検査値関連の `etl_errors.field` 名称と `error_code` 一覧は未決。 |
| `item_master` lookup | `item_master` 参照処理は共通Lookupライブラリへ集約し、呼び出し側スクリプトで個別SQLを直接実装しない。単品取得と複数取得の両APIを提供する。 | Phase4と後続制度チェックで同じ項目定義参照を再利用し、SQL実装を一元管理するため。 | 決定済み。キャッシュ方式、返却dictの正式キー、取得対象カラム一覧は未決。 |
| `normalize_status` / `validation_status` | `normalize_status` はraw値から `normalized_value` / `normalized_unit` を作成できたか、`validation_status` は `exam_item_master` 定義に照らした妥当性を表す。数値変換不可は `ERROR / INVALID`、namecode未登録は `SKIPPED / INVALID`、単位不一致は `WARNING / WARNING`、正常は `OK / OK` とする。 | 正規化可否とマスタ定義上の妥当性を分離し、制度チェックとは別に扱うため。 | 決定済み。詳細reason一覧は未決。 |
| 一部成功ZIPの `file_receipts.status` | `WARNING` とする。正常XMLとparse不能XMLは `xml_ledger` に登録し、失敗詳細は `etl_errors` に記録する。XMLファイルとして扱えない失敗は `xml_ledger` を作成しない。 | ファイル全体の総合状態、XML内容台帳、失敗詳細の責務を分離できるため。 | 決定済み。 |
| ETL metricsの基準 | `files = file_receipts`、`rows_seen = 対象XML`、`rows_inserted = 新規xml_ledger`、`rows_updated = xml_file_links登録件数 + file_receipts更新件数`、`rows_skipped = 既存xml_sha256再受領・対象外XML件数`、`errors = etl_errors登録件数` とする。`exam_item_values` 件数は `rows_inserted` に含めず、必要に応じて `etl_runs.notes` へ記録する。 | Runサマリーでファイル数、XML数、台帳作成、リンク・状態更新、スキップ、エラーを分けて把握するため。 | 決定済み。`notes` の具体フォーマットは未決。 |
| Phase4 `etl_errors` 基本構成 | `field` / `error_code` / `message` を基本構成とする。identity生成失敗時の `error_code` は `IDENTITY_BIRTHDATE_INVALID`、`IDENTITY_INSURER_NUMBER_INVALID`、`IDENTITY_INSURANCE_SYMBOL_INVALID`、`IDENTITY_INSURANCE_NUMBER_INVALID`、`IDENTITY_NAME_KANA_FULL_INVALID`、`IDENTITY_HASH_BUILD_FAILED` を基本とする。 | 共通ETL構造に合わせ、Phase4固有の独自構造を避けるため。 | 決定済み。 |
| `--keep-work` | デバッグ用に用意する候補。 | ZIP展開・XML解析失敗の調査に有用。 | オプション名と保持先命名。 |

### Phase4前に決めるもの

- `xml_ledger.exam_item_reason` に保持する理由・サマリーの具体内容。
- `import_xml.yml` の詳細設定項目。
- `etl_errors.error_code` の正式コード一覧。
- parse不能XMLで `xml_reason` に保持する正式メッセージ。
- ZIP内対象XML0件時の詳細messageフォーマット。
- `etl_errors.field` に使用する検査値関連の正式名称。
- 検査値取得エラーの `error_code` 一覧。
- 検査値バリデーションをどこまでPhase4で実施するか。
- `dev_phr.norm_rules` / `dev_phr.norm_variants` をPhase4検査値正規化・バリデーションに利用するか。
- Lookupライブラリのキャッシュ方式。
- Lookupライブラリが返却するdictの正式キー構成。
- `item_master` の取得対象カラム一覧。

### 実装中でよいもの

- ZIP展開ディレクトリ名。
- `--keep-work` の詳細。
- ETL `notes` の具体フォーマット。
- `etl_runs.notes` に記録する検査値サマリーの具体フォーマット。
- XML抽出補助モジュールの分割粒度。
- `exam_item_values` の `occurrence_no` 採番ロジックの細部。

### Phase7以降でよいもの

- `check_status` の算出。
- `xml_export_status` の最終出力可否。
- `exam_check_results` 登録。
- CSV直取込。
- 人＋イベント単位の状態管理台帳。

## 15. Phase4実装GO判定

Phase4の責務範囲と共通lib利用方針、status正式コード、ETL metrics基準は整理できている。ただし、以下は実装前に対応または人間判断が必要である。

- `xml_ledger.exam_item_reason` に保持する理由・サマリーの具体内容。
- `import_xml.yml` の詳細設定項目。
- `etl_errors.error_code` の正式コード一覧。
- parse不能XMLで `xml_reason` に保持する正式メッセージ。
- ZIP内対象XML0件時の詳細messageフォーマット。
- `etl_errors.field` に使用する検査値関連の正式名称。
- 検査値取得エラーの `error_code` 一覧。
- 検査値バリデーションをどこまでPhase4で実施するか。
- `dev_phr.norm_rules` / `dev_phr.norm_variants` をPhase4検査値正規化・バリデーションに利用するか。
- Lookupライブラリが返却するdictの正式キー構成。
- `item_master` の取得対象カラム一覧。

Phase4の入力条件、configファイル名、一部成功ZIP、既存XML再受領、Phase4 `etl_errors` 基本構成、identity生成失敗時の記録仕様、parse不能XMLの台帳化、`processable_count` 更新、状態管理の責務分離、status正式コード、ETL metrics基準、`xml_ledger.exam_item_status` / `exam_item_reason` のDDL・Migration対応、`item_master` Lookup方針、`exam_item_values` 登録・正規化・バリデーション方針は決定済みである。残る未決をPhase4実装前に解消できればGOと判断できる。未決のまま実装する場合は、実装内で仮決定せず、エラー側へ倒すか、対象外として明示的にスキップする必要がある。
