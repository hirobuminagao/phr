# CSV Exam Result Import Implementation Status and XML Handoff

## Status

Current as of 2026-08-06.

この文書は、CSV健診結果取込について、採用済み決定事項と現行実装の差分を同期し、次工程のCSVからXML作成へ引き継ぐための現在正である。

優先順位は以下とする。

1. 採用済みの仕様判断: `03_decisions.md`
2. 実装到達点、未実装範囲、次工程への引継ぎ: 本書
3. 実際のカラム・処理挙動: 適用対象のDDL、migration、seed、コード
4. 協議経緯: `05_design_history.md`、各draft/review文書

## Current Pipeline

```text
01_scan_files.py
  -> file_receipts
  -> exam_facility lookup
  -> CSV format matching

01_01_match_csv_format.py
  -> mapping登録後のformat再照合

02_02_exam_result_csv_import.py
  -> exam_ledgers
  -> subscriber identity matching
  -> exam_item_values + normalize

03_00_check_imported_exam_ledgers.py
  -> exam_check_results
  -> exam_ledgers.check_status / check_reason

03_01_build_exam_export_cases.py
  -> exam_export_cases
  -> exam_export_case_sources

03_02_build_exam_export_case_values.py
  -> exam_export_case_values

03_04_check_exam_export_cases.py
  -> exam_check_results
  -> exam_export_cases.check_status / check_reason
  -> exam_export_cases.export_readiness_status / export_readiness_reason

03_05_create_xml_export_list.py
  -> xml_export_lists
  -> xml_export_list_cases
  -> 画面実装前の正式CLI入口として、出力可能caseをREADYリスト化

04_export_hia_xml.py
  -> V08個人XML + ix08_V08.xml
  -> XSD検証
  -> 公式命名ZIP + 出力履歴
```

実行履歴の根は既存どおり `etl_runs` とし、CSV専用のrun親テーブルは追加していない。

`04_export_hia_xml.py` は `exam_export_cases` / `exam_export_case_values` 起点へ切り替え済みである。
出力対象は `export_readiness_status` が `EXPORT_READY` または `APPROVED_WITH_REASON` のcaseとし、出力後は `exam_export_cases` の出力ファイル証跡カラムと `xml_export_zips` / `xml_export_members` の履歴を更新する。
画面実装前は `03_05_create_xml_export_list.py` で出力リストを作成し、`04_export_hia_xml.py --xml-export-list-id ...` で出力する。
`xml_export_members` は `ledger_type = CASE`, `ledger_id = exam_export_cases.exam_export_case_id` で出力対象を記録する。

## Implemented Scope

### Master and Receipt

- `phr_master` DBと以下のテーブルをDDL化済み。
  - `exam_facilities`
  - `medical_folder_aliases`
  - `exam_item_concept_groups`
  - `exam_item_concept_group_members`
  - `norm_variants`
  - `csv_format_versions`
  - `csv_exam_result_mapping_rules`
  - `csv_exam_result_mapping_conditions`
- 支払基金CSV由来の健診機関と既存aliasをseed化済み。
- 公開CSV由来であることを `exam_facilities.data_source_*` に保持する。
- `01_scan_files.py` はフォルダaliasから健診機関を解決し、CSVについてformat照合を行う。
- format未登録とヘッダー不一致を区別し、`WAITING_CONFIRM` と理由を残す。
- `01_01_match_csv_format.py` で、初回scan後に追加したmappingを再照合できる。
- CSV import通常対象は `READY` と、確認Go済みの `WAITING_CONFIRM` に限定する。
- 同一event・同一相対パスへ別shaのファイルを再scanした場合、旧 `DISCOVERED` / `READY` / `WAITING_CONFIRM` は `SUPERSEDED` とし、現物と一致しない未処理receiptを後続Run対象から外す。取込済みreceiptは履歴として変更しない。
- `DISCOVERED` はCSV import対象外である。
- source fileがscan時の `file_sha256` と異なる場合は取込を行わない。

### Format and Mapping

以下のformatをseedへ登録済みである。

| facility | mapping version | header |
| --- | --- | --- |
| ヒロオカクリニック | `HIROOKA_2026_05_PATTERN_A_V1` | 1行 |
| ヘルスケアクリニック厚木 | `ATSUGI_2026_05_PATTERN_A_V1` | 1行 |
| 渋谷ウェストヒルズクリニック | `SHIBUYA_WESTHILLS_2026_05_PATTERN_A_V1` | 1行 |
| ハートクロス健診プラザ赤坂駅前 | `HEARTCROSS_2026_05_PATTERN_B_V1` | 2行 |
| 医療法人 禄寿会 小禄病院 | `OROKU_2026_05_JOINED_PATTERN_C_V1` | 結合済みCSV |
| 医療法人社団平世会村上医院 | `MURAKAMI_IIN_2026_05_PAPER_CSV_V1` | 紙入力由来CSV |
| 医療法人社団明日佳 札幌きたはち健診センター | `SAPPORO_KITAHACHI_2026_05_PATTERN_A_V1` | 1行 |

小禄病院CSVの `医療機関コード` は施設内コードのためmapping対象外とし、健診機関コード・名称は `file_receipts` のマスタ由来スナップショットを採用する。

- ヘッダー名は完全一致で扱い、表記ゆれを推測しない。
- UTF-8 BOM、UTF-8、CP932はformat設定に従ってfallback読込できる。
- 採用文字コードは `file_receipts.actual_character_encoding` に残す。
- delimiterはformat登録値を使い、quoteの有無だけでは不一致にしない。
- `LEDGER_FIELD` と `EXAM_ITEM_VALUE` を同じrule/condition形式で抽出する。
- header名、context、occurrence、column numberのlocator構造を保持できる。
- fixed value、複数列の文字列結合、除外値、条件groupのAND/ORを扱える。

### Unified Source Ledger and Subscriber Matching

- `exam_ledgers` は、XML/CSV/紙入力を問わず、取込結果1件を表す統合source ledgerである。
- CSVは1行1件、XMLはXML内の1人分1件、紙入力は入力1人分1件として登録する。
- 原CSV行は `exam_ledgers.raw_row_json` と `row_sha256` に保存する。
- 完全空行はskipし、それ以外は行台帳を作る。
- 同一sourceの再取込は `exam_ledgers` をUPDATEする。
- そのsourceに紐づく `exam_item_values` はdelete+insertで再作成する。
- 加入者突合は `generate_identity_bundle()` と `resolve_subscriber_identity()` を使い、XML側と同じidentity系共通libへ寄せている。
- CSV/XMLに保険者番号がない場合は、`file_receipts`、eventの順で補完する。
- `health_exam_report_category` と `program_code` は、正しい厚生労働省コードの明示値があればその値を保存する。
- mapping対象がない、または値がNULLの場合は、eventの年齢判定規則により40～74歳を `10/010`、それ以外を `40/990` として不足値を補完する。
- 施設側コースコード、コース名、検査構成から報告区分・プログラムコードを推測しない。

### XML Import and Program Codes

- `02_import_xml.py` は元XMLの `ClinicalDocument/code` と `documentationOf/serviceEvent/code` を抽出して、`exam_ledgers` へそのまま保存する。
- XML由来コードは年齢から再判定しない。
- XML由来の `exam_item_values` も `ledger_type = 'EXAM'`, `ledger_id = exam_ledgers.exam_ledger_id` で登録する。
- `exam_facility_id` は `file_receipts` から引き継ぐ。XML本文に施設コード・名称がない場合は `file_receipts` のscan時スナップショットを表示値として使う。
- 受診者住所は `recordTarget/patientRole/addr` のみから抽出し、医療機関住所を受診者住所として使わない。
- `--include-imported` を指定すると、取込済みfile receipt、`WARNING` receipt、既存XML ledgerが `READY/PENDING` のものも再読込し、既存ledgerのNULLカラムや追加カラムをbackfillする。
- `exam_result_ledger_report` にもXML由来2カラムを追加し、報告用snapshotへ引き継ぐ。

### Exam Item Values and Normalize

- `exam_item_values` は `ledger_type = 'EXAM'`、`ledger_id = exam_ledgers.exam_ledger_id` で登録する。
- raw値を証跡として残し、型に応じて `normalized_value` または `code_value` へ正規化する。
- `normalize_status` / `normalize_reason` に、raw完全一致、正規化後一致、数値比較記号除去、辞書不足、型不正などを残す。
- CD/COは `phr_master.norm_variants` を参照する。
- PQ/INT/REALは単位変換を行わず、単位不一致をエラーにする。
- `0.1未満` などXMLのPQで比較演算子を直接表せない値は、rawを残して数値部へ正規化する。
- 未実施、キャンセル、測定不能などはrawを残し、`SKIPPED` / `WARNING` と理由を残す。
- CSVサンプルで確認したコード値の表記は追加seedで整備済みである。
- 辞書にない値は自動推測せず `NORMALIZE_VARIANT_NOT_FOUND` とする。
- m4 fixtureと実行環境エラーから、血清アミラーゼ、BUN、尿pH、尿定性、便潜血、腫瘍マーカー、BNP/NT-proBNP、骨密度、胃がんリスク検査、CA125/CA19-9 CLIA法variantなど、検査結果として受け止める任意項目を `exam_item_master` へ正式追加済みである。
- ヘリコバクターピロリ抗体、ペプシノゲン、ABCD分類、CA125、CA19-9は検査結果として受け止めるため、出力ポリシーseedでは止めない。
- `Z...` / `ZG...` 系の施設独自コード、指導区分、施設由来総合判定、標準項目との同一視に確認が必要な項目は、`exam_item_master` へ追加せず、取込エラーまたは確認待ちとして残す。
- 現時点で残るnormalize/masterエラーは、主に値なし、施設確認待ち、`Z...` / `ZG...` 系、標準コードと表示内容の不一致、raw値ゆれであり、構造不足ではなく個別判断の領域に寄っている。

### Check Results

- source単位の法定チェックは `03_00_check_imported_exam_ledgers.py` で実行する。
- `exam_ledgers` と `exam_item_values` を参照し、`exam_check_results` を再作成する。
- 数値項目は `normalized_value`、コード項目は `code_value` を優先して法定チェックする。
- 結果は `exam_ledgers.check_status` / `check_reason` へ戻す。
- 結合出力用case単位の法定チェックは `03_04_check_exam_export_cases.py` で実行し、結果は `exam_export_cases.check_status` / `check_reason` へ戻す。
- case作成、case値作成、case単位checkの後に `exam_export_cases.export_readiness_status` / `export_readiness_reason` を更新する。
- 施設別のABC判定や総合判定は、法定項目チェックと混同せず取込対象外とする。

### Export Case Readiness

`exam_export_cases` は、人単位の1回分XML出力候補である。
同一人物、同一健診日、同一健診機関、同一保険者のXML/CSV sourceは同じcaseへ束ねる。

- `source_mode` は `XML_ONLY` / `CSV_ONLY` / `XML_CSV` / `MULTI_SOURCE` 等を表す。
- `exam_export_case_sources` は構成元 `exam_ledgers` を保持する。
- `exam_export_case_values` はXML出力用の採用済み整値を保持する。raw証跡は採用元 `exam_item_values` へ戻って確認する。
- 人が見る総合状態は `export_readiness_status` / `export_readiness_reason` を見る。
- `EXPORT_READY` は出力可能、`APPROVED_WITH_REASON` は理由あり手動許可済み、`BLOCKED` は加入者・結合・case・法定check等で停止、`WAITING_VALUES` は採用値作成待ち、`WAITING_CHECK` はcase check待ち、`EXPORTED` はXML出力済み、`EXPORT_ERROR` はXML生成・検証等の出力失敗を表す。
- 出力後は `output_zip_path`, `output_zip_file_name`, `output_xml_file_name`, `xml_exported_at`, `xml_export_etl_run_id` に証跡を保持する。

## Current Transaction and Re-run Behavior

- `etl_runs` はCSV import開始時に先行commitする。
- CSV本体は1ファイル単位のtransactionで処理し、ファイル処理後にcommitする。
- 行ごとの台帳・結果値作成は順番に行うが、現実装は1行ごとのcommitではない。
- `--include-imported` を指定しない通常Runは `IMPORTED` を再取込しない。
- `--include-imported` を指定した場合は、既存行台帳をUPDATEし、結果値をdelete+insertする。
- item normalizeエラーがあってもファイルは `IMPORTED` となり、行・項目のERRORと理由を台帳へ残す。

## Modeled but Not Fully Executed

以下はDDL・lookup・抽出構造にカラムまたは型があるが、汎用実行ロジックが未完成である。
現行5 formatの採用ruleはこれらに依存していないため、現在の取込結果を壊す差分ではない。

| item | current state | required follow-up |
| --- | --- | --- |
| `EXCLUSIVE_ONE` | `selection_mode` / `selection_group_code` は読込可能だが排他件数を検証しない | 同一groupで0件・1件・複数件の扱いを実行時に検証する |
| `MULTI_ENTRY` | 独立した複数ruleとしては登録できる | group意味とentry重複検証を追加する |
| `LOWER_LIMIT` / `UPPER_LIMIT` | extractorはrole別に値を返せる | importerから `source_reference_lower` / `source_reference_upper` へ保存する |
| `JUDGEMENT` / `METHOD` / `QUALIFIER` | extractorはrole別に値を返せる | 保存先とXML出力要否を決めて実装する |
| header mismatch確認Go | 承認済み `WAITING_CONFIRM` は選択対象になる | 登録header不一致のまま必要列を再検証して続行する処理は未実装 |
| `SKIP_CHECKED_OK` | policyカラムと `row_sha256` は存在する | 既存check OK行を探してskipする処理は未実装 |
| `header_snapshot_json` | カラムは存在する | seedに完全snapshotを保存していない |
| concept groups | テーブルは存在する | `ANNEX2_IDENTITY` 197件と入力支援bundleのseedは未作成 |
| 非測定値語YAML | YAML管理を採用済み | 現状は `value_normalizer.py` のPython定数 |
| norm辞書一括lookup | 単品・一括APIは存在する | CSV importerは単品APIを使用中 |
| 出力画面 | モックで出力リスト、case詳細、HIAアップロード作業の方向性を確認済み | 次工程でFastAPI/ローカル画面の要件モックを再整理する |

上表は採用済み仕様を廃止するものではない。
現行サンプルの処理に必要になった時点、またはFastAPIテンプレート登録・出力画面へ進む時点で実装する。

## Evidence Placement

- ファイル単位の読込・format・SHA異常は `etl_errors` に残す。
- 行の元内容は `exam_ledgers.raw_row_json` に残す。
- mapping必須列不足と項目normalizeエラーは `exam_ledgers.row_reason` / `exam_item_reason` に集約する。
- 項目ごとのraw、normalize、validation結果は `exam_item_values` に残す。
- 現実装では項目normalizeエラーを1件ずつ `etl_errors` に重複記録しない。

この配置により原因調査は可能である。`etl_errors` に行・項目エラーも複製するかは、運用で横断検索が必要になった時点で追加判断する。

## Facility Confirmation Items

以下はシステムのマッピング構造不足ではなく、施設CSVの意味または不足情報の確認事項である。

- ハートクロス
  - 元CSVに健診日と性別がなく、サンプル末尾へ取込検証用の暫定列を追加している。
  - 他覚症状のnamecodeに所見本文相当の値が入る1件は、施設の項目定義確認待ちである。
- ヘルスケアクリニック厚木
  - 尿糖 `（４＋）` は標準結果コードで表現できないため、辞書追加せずエラーのままとして施設確認する。
- 小禄病院
  - 既往歴、自覚症状、他覚症状の元列は施設確認待ちである。
  - 現サンプルは2ファイルを事前結合した入力であり、ファイル結合ユーティリティは後続バージョンとする。
- 報告区分
  - 元CSVに正しい明示値がない場合は、eventの年齢判定規則で `10/010` または `40/990` を補完する。

## CSV to XML Handoff

2026-07-30以降の詳細設計は `34_csv_to_hia_xml_export_design_draft.md` を参照する。

### Available Inputs

CSVからXMLを作成するための主要データは、すでに以下へ揃っている。

- `exam_ledgers`
  - event、加入者ID、保険者番号、健診機関、健診日、氏名等の原文、identity、program/report category、住所、check/export状態
- `exam_export_cases`
  - 人単位の1回分XML出力候補、構成元source、結合状態、出力可否summary、出力後ファイル証跡
- `exam_export_case_values`
  - XML出力に採用する正規化済み検査値
- `exam_item_values`
  - namecode、section、occurrence、raw値、型、単位、正規化値、コード値、normalize/validation結果
- `exam_check_results`
  - 法定項目チェック結果
- `file_receipts` / `etl_runs`
  - 受領ファイルと処理証跡

### Confirmed Export Handoff

CSV取込の再設計は不要である。XML出力について以下を確定した。

- 出力対象は、報告区分とプログラムコードが設定済み、加入者突合が `MATCHED` で、法定項目チェックが `OK` またはMISSINGのみを理由として手動出力許可された行とする。
- 報告区分とプログラムコードは、CSV mappingによって正しい値が登録されている場合はその値を使用する。
- mapping値がない場合は、CSV取込時にevent年齢規則から補完された値を使用する。event年齢規則または生年月日を解決できずNULLが残る場合だけ出力不可とする。
- 検査値は `VALID` のみ出力し、`WARNING/SKIPPED` と `INVALID` のentryは初期版では省略する。
- 妊娠中等によるMISSINGの手動出力許可後もcheck結果はNGのまま保持し、架空値を作らない。許可理由、承認者、承認日時は行台帳と出力履歴へ残す。
- 基本情報norm失敗、手動許可条件を満たさない法定項目NG、個人XML生成失敗、XSD検証失敗は出力失敗とする。
- 健診機関情報は `phr_master.exam_facilities` を正とし、ledgerの健診機関コードと一致しなければ該当ZIPを停止する。
- 同日分割送信回数は既存ZIPからの自動採番を既定とし、`0`から`9`の明示指定も許可する。初回の自動採番結果は `0` とする。
- 個人XMLファイル名21桁目の種別は、特定健診情報を表す `1` 固定とする。
- ZIP対象者のうち1人でもXML生成またはXSD検証に失敗した場合は、そのZIP全体を出力しない。別のZIP単位は処理を継続する。
- 正常出力したZIPと個人XMLは、`etl_runs` に紐づく専用履歴テーブルへ追記する。再出力時も過去履歴を変更せず、新しい履歴を追加する。
- 履歴は出力事実の保存に限定し、個人単位の業務状態や修正版の正本判定、後続データへの反映は今回行わない。
- 人向け不足情報CSVの追加は後続で決め、初期実装を止めない。確認後の手動Goを表す `manual_export_approved` / `manual_export_reason` とは別概念である。

初期実装では、既出力者を含めるかを対象抽出条件で選択できるようにし、ZIP・個人XMLの履歴構造を実装済みである。詳細は `34_csv_to_hia_xml_export_design_draft.md` を参照する。

### Implemented Export Components

- 旧 `scripts/kenshin_list_pydir/scripts/medi_export_xml.py` は変更せず、XML構造と既存出力仕様の参照元として残す。
- `scripts/from_medical/04_export_hia_xml.py` から共通XML builderを呼び、健診機関・保険者単位のZIPを作成する。
- `scripts/lib/examination/mhlw_v08_xml.py` で個人CDA、IX08、公式命名、XSD bundleコピーと検証を行う。
- `xml_export_zips` / `xml_export_members` に正常出力履歴を追記し、失敗は `etl_errors` に残す。
- `20260730_009_health_exam_result_create_xml_export_history.sql` が出力履歴と手動承認証跡のDB変更である。
- DBのraw証跡は保持し、XMLには採用した正規化値だけを使う。
- 付属2の一連検査グループ53 namecodeを `exam_item_master` へ保持し、詳細健診等を `COMP` / `RSON` で親observation配下へ出力する。
- `interpretationCode` と `referenceRange` は原本CSVから明示的に取り込まれた場合だけ出力し、自動判定・単位変換は行わない。
- 施設別の推測をexporterへ埋め込まず、確定したmappingまたは出力設定で明示する。

### M4 End-to-End Result

- ヒロオカfixture 7人のうち、ローカルsubscriber seedと一致する5人が `MATCHED / check OK`、基本情報不足の2人が停止となった。
- 5人を1 ZIPへ出力し、個人XML5件、`ix08_V08.xml`、V08 XSD bundleを公式フォルダ構成で格納した。
- 5個人XMLはすべてXSD適合し、付属2一連検査グループも実データ上で生成された。
- `xml_export_zips` 1件、`xml_export_members` 5件、当時のCSV行台帳の出力済み状態5件を確認した。
- 基本情報不足の2人は出力対象外のまま `PENDING` を維持した。

## Readiness Conclusion

現行登録済みformatについて、CSV受領から `exam_item_values` 登録、normalize、加入者突合、法定チェック、結合出力用case作成までの基盤は実装済みである。
高度な汎用mapping機能には未実装が残るが、現在のCSV/XMLサンプルをXML出力へ進めることを妨げない。

CSV/XML由来の統合ledgerと採用済み整値からXMLを組み立て、M4および実行環境で個人XML、XSD検証、ZIP、DB履歴、出力リストまで一連確認済みである。
次工程は、残normalizeエラーの個別判断を継続しつつ、出力リスト作成、case詳細確認、HIAアップロード記帳を扱う画面要件モックへ進むことである。
