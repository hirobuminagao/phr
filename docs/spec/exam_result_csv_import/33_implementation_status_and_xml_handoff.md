# CSV Exam Result Import Implementation Status and XML Handoff

## Status

Current as of 2026-07-29.

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
  -> csv_row_ledger
  -> subscriber identity matching
  -> exam_item_values + normalize

03_check_exam_results.py
  -> exam_check_results
  -> csv_row_ledger.check_status / check_reason

次工程
  -> CSV台帳・結果値からXML作成
```

実行履歴の根は既存どおり `etl_runs` とし、CSV専用のrun親テーブルは追加していない。

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
- `DISCOVERED` はCSV import対象外である。
- source fileがscan時の `file_sha256` と異なる場合は取込を行わない。

### Format and Mapping

以下の5 formatを同一seedへ登録済みである。

| facility | mapping version | header |
| --- | --- | --- |
| ヒロオカクリニック | `HIROOKA_2026_05_PATTERN_A_V1` | 1行 |
| ヘルスケアクリニック厚木 | `ATSUGI_2026_05_PATTERN_A_V1` | 1行 |
| 渋谷ウェストヒルズクリニック | `SHIBUYA_WESTHILLS_2026_05_PATTERN_A_V1` | 1行 |
| ハートクロス健診プラザ赤坂駅前 | `HEARTCROSS_2026_05_PATTERN_B_V1` | 2行 |
| 医療法人 禄寿会 小禄病院 | `OROKU_2026_05_JOINED_PATTERN_C_V1` | 結合済みCSV |

小禄病院CSVの `医療機関コード` は施設内コードのためmapping対象外とし、健診機関コード・名称は `file_receipts` のマスタ由来スナップショットを採用する。

- ヘッダー名は完全一致で扱い、表記ゆれを推測しない。
- UTF-8 BOM、UTF-8、CP932はformat設定に従ってfallback読込できる。
- 採用文字コードは `file_receipts.actual_character_encoding` に残す。
- delimiterはformat登録値を使い、quoteの有無だけでは不一致にしない。
- `LEDGER_FIELD` と `EXAM_ITEM_VALUE` を同じrule/condition形式で抽出する。
- header名、context、occurrence、column numberのlocator構造を保持できる。
- fixed value、複数列の文字列結合、除外値、条件groupのAND/ORを扱える。

### Row Ledger and Subscriber Matching

- `csv_row_ledger` は、XML側台帳に準じた加入者突合、検査値、check、export状態を持つ。
- 原CSV行は `raw_row_json` と `row_sha256` に保存する。
- 完全空行はskipし、それ以外は行台帳を作る。
- 同一 `file_receipt_id + src_row_no` の再取込は行台帳をUPDATEする。
- その行の `exam_item_values` はdelete+insertで再作成する。
- 加入者突合は `generate_identity_bundle()` と `resolve_subscriber_identity()` を使い、XML側と同じidentity系共通libへ寄せている。
- CSVに保険者番号がない場合は、`file_receipts`、eventの順で補完する。
- `health_exam_report_category` は明示mapping値だけを保存し、コース名や検査構成から推測しない。
- `program_code` は施設側の元値として保持し、報告区分へ自動変換しない。

### Exam Item Values and Normalize

- `exam_item_values` は `ledger_type = 'CSV'`、`ledger_id = csv_row_ledger_id` で登録する。
- raw値を証跡として残し、型に応じて `normalized_value` または `code_value` へ正規化する。
- `normalize_status` / `normalize_reason` に、raw完全一致、正規化後一致、数値比較記号除去、辞書不足、型不正などを残す。
- CD/COは `phr_master.norm_variants` を参照する。
- PQ/INT/REALは単位変換を行わず、単位不一致をエラーにする。
- `0.1未満` などXMLのPQで比較演算子を直接表せない値は、rawを残して数値部へ正規化する。
- 未実施、キャンセル、測定不能などはrawを残し、`SKIPPED` / `WARNING` と理由を残す。
- CSVサンプルで確認したコード値の表記は追加seedで整備済みである。
- 辞書にない値は自動推測せず `NORMALIZE_VARIANT_NOT_FOUND` とする。

### Check Results

- `03_check_exam_results.py` はXMLとCSVを `ledger_type` で区別して対象にできる。
- CSVでは `csv_row_ledger` と `exam_item_values` を参照し、`exam_check_results` を再作成する。
- 数値項目は `normalized_value`、コード項目は `code_value` を優先して法定チェックする。
- 結果は `csv_row_ledger.check_status` / `check_reason` へ戻す。
- 施設別のABC判定や総合判定は、法定項目チェックと混同せず取込対象外とする。

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

上表は採用済み仕様を廃止するものではない。
現行サンプルの処理に必要になった時点、またはFastAPIテンプレート登録へ進む時点で実装する。

## Evidence Placement

- ファイル単位の読込・format・SHA異常は `etl_errors` に残す。
- 行の元内容は `csv_row_ledger.raw_row_json` に残す。
- mapping必須列不足と項目normalizeエラーは `csv_row_ledger.row_reason` / `exam_item_reason` に集約する。
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
  - 元CSVまたは確定済み変換元がないformatはNULLのままとする。

## CSV to XML Handoff

2026-07-30以降の詳細設計は `34_csv_to_hia_xml_export_design_draft.md` を参照する。

### Available Inputs

CSVからXMLを作成するための主要データは、すでに以下へ揃っている。

- `csv_row_ledger`
  - event、加入者ID、保険者番号、健診機関、健診日、氏名等の原文、identity、program/report category、住所、check/export状態
- `exam_item_values`
  - namecode、section、occurrence、raw値、型、単位、正規化値、コード値、normalize/validation結果
- `exam_check_results`
  - 法定項目チェック結果
- `file_receipts` / `etl_runs`
  - 受領ファイルと処理証跡

### Confirmed Export Handoff

CSV取込の再設計は不要である。XML出力について以下を確定した。

- 出力対象は、報告区分とプログラムコードが設定済み、加入者突合が `MATCHED`、法定項目チェックが `OK` の行とする。
- 報告区分とプログラムコードは、CSV mappingによって正しい値が登録されている場合はその値を使用する。
- 現時点では対象となるCSV項目を持つ健診機関がないため、予約データまたは健診機関への確認結果を基に人が登録する。システムではコース名称や施設内コードから推測しない。
- 検査値は `VALID` のみ出力し、`WARNING/SKIPPED` と `INVALID` のentryは初期版では省略する。
- 基本情報norm失敗、法定項目NG、個人XML生成失敗、XSD検証失敗は出力失敗とする。
- 健診機関情報は `phr_master.exam_facilities` を正とし、ledgerの健診機関コードと一致しなければ該当ZIPを停止する。
- 同日分割送信回数は既存ZIPからの自動採番を既定とし、`0`から`9`の明示指定も許可する。初回の自動採番結果は `0` とする。
- 個人XMLファイル名21桁目の種別は、特定健診情報を表す `1` 固定とする。
- ZIP対象者のうち1人でもXML生成またはXSD検証に失敗した場合は、そのZIP全体を出力しない。別のZIP単位は処理を継続する。
- 人向け不足情報CSVの追加は後続で決め、初期実装を止めない。確認後の手動Goを表す `manual_export_approved` / `manual_export_reason` とは別概念である。

初期実装前に残る出力管理上の詳細は、再出力の許可方法と、出力済みRun・ZIP・個人XMLの証跡カラムである。詳細は `34_csv_to_hia_xml_export_design_draft.md` のDeferred Decisionsを参照する。

### Implementation Direction

- 旧 `scripts/kenshin_list_pydir/scripts/medi_export_xml.py` はXML構造と既存出力仕様の参照元にする。
- CSV台帳へ直接合わせるため、新しいfrom-medical処理から共通XML builderを呼ぶ構成を第一候補とする。
- DBのraw証跡は保持し、XMLには採用した正規化値だけを使う。
- 施設別の推測をexporterへ埋め込まず、確定したmappingまたは出力設定で明示する。

## Readiness Conclusion

現行5 formatについて、CSV受領から `exam_item_values` 登録、normalize、加入者突合、法定チェックまでの基盤は実装済みである。
高度な汎用mapping機能には未実装が残るが、現在の5サンプルをCSVからXMLへ進めることを妨げない。

次工程はCSV解析やマッピングの作り直しではなく、残る出力管理の詳細を確定し、CSV台帳・結果値からXMLを組み立てる実装である。
