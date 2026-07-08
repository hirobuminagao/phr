## 3. 実装フェーズ

各 Phase は以下の観点で整理する。

- 目的
- 新規作成ファイル
- 更新ファイル
- 参照資料
- 完了条件
- Codex 指示単位

DDLは設計変更に合わせて更新し、新規環境は最新DDLから構築する。既存DBが対象となるDDL変更を行う場合は、既存環境が追従できるようMigrationを同時に作成し、DDLのみ更新してMigrationを後回しにしない。

health_exam_result のMigrationファイル名は `YYYYMMDD_NNN_health_exam_result_<description>.sql` とする。連番はその日の `sql/migrations/health_exam_result/` 配下で採番し、`description` は英小文字 + snake_case とする。例: `20260707_001_health_exam_result_add_exam_item_status.sql`。

### Phase1 Core DDL

#### 依存Phase
- なし

#### 目的
`health_exam_result` 新規DBの初期コアテーブルを作成する。

#### 新規作成ファイル
- `sql/ddl/health_exam_result/0010_health_exam_result__etl_runs.sql`
- `sql/ddl/health_exam_result/0020_health_exam_result__etl_errors.sql`
- `sql/ddl/health_exam_result/0030_health_exam_result__medical_folder_aliases.sql`
- `sql/ddl/health_exam_result/0040_health_exam_result__file_receipts.sql`
- `sql/ddl/health_exam_result/0050_health_exam_result__xml_ledger.sql`
- `sql/ddl/health_exam_result/0060_health_exam_result__xml_file_links.sql`
- `sql/ddl/health_exam_result/0070_health_exam_result__exam_item_values.sql`

#### 更新ファイル
- なし

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`
- `docs/refactor/health_exam_result/19_implementation_ready_summary.md`

#### 完了条件
- core DDL 7ファイルが作成されている。
- `exam_check_results` はこのPhaseでは作成しない。
- DDLファイル名が `NNNN_health_exam_result__<table_name>.sql` 形式になっている。
- `git diff --check` が通る。

#### Codex 指示単位
Phase1 Core DDLのみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

### Phase2 medical_folder_aliases 初期データ / event migration

#### 依存Phase
- Phase1 Core DDL

#### 目的
`01_scan_files.py` が医療機関フォルダを探索できるよう、フォルダ別名マスタの初期データと `event.result_root_path` の準備を行う。

#### 新規作成ファイル
- 未定

#### 更新ファイル
- 未定

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md`
- `docs/refactor/health_exam_result/19_implementation_ready_summary.md`

#### 完了条件
- `medical_folder_aliases` 初期データ投入SQLが作成されている。
- `03_medical_folder_aliases_initial_data_v2_0_0.md` の内容が初期データへ反映されている。
- `event_id = 2` の医療機関フォルダ188件が初期データ候補として扱われている。
- `dev_phr.event.result_root_path` の既存DDL / migration要否が確認されている。

#### Codex 指示単位
Phase2 medical_folder_aliases 初期データ / event migrationのみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

### Phase3 01_scan_files.py

#### 依存Phase
- Phase1 Core DDL
- Phase2 medical_folder_aliases 初期データ / event migration

#### 目的
医療機関フォルダ配下の投入ファイルを検出し、初期実装では ZIP / XML の未登録ファイルのみ `file_receipts.status = DISCOVERED` で登録する。

Phase3はファイル検出と `file_receipts` 登録に責務を限定し、ZIP展開・XML読込・健診値抽出は行わない。CSVは初期実装では登録せず、将来対応時にスキャン対象へ追加する。

#### 新規作成ファイル
- `scripts/from_medical/01_scan_files.py`

#### 更新ファイル
- `scripts/from_medical/config/scan_files.yml`
- `scripts/from_medical/script_lib/` 配下の補助モジュール（必要に応じて）

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/11_v2_script_design_notes.md`
- `docs/spec/health_examinations/03_medical_folder_aliases_initial_data_v2_0_0.md`

#### 完了条件
- `event.result_root_path` と `medical_folder_aliases` を参照して対象フォルダを探索できる。
- 重複ファイルを `file_receipts` に新規登録しない。
- 新規検出ファイルは `file_receipts.status = DISCOVERED` で登録される。
- Phase3の実行設定は `scripts/from_medical/config/scan_files.yml` を正本とし、CLI引数は指定時のみ一時的な上書き用途とする。
- 登録対象は初期実装では ZIP と健診結果本体XMLのファイル名規定に合う単体XMLとし、CSV、隠しファイル、一時ファイル、対象外拡張子は `file_receipts` に登録しない。
- 単体XMLは `h*.xml` のみ登録対象とし、`ix08*.xml` / `su08*.xml` / schema関連 / XSD関連のXMLは登録しない。
- 対象外XMLは `etl_errors` に記録しない。
- ZIPはPhase3では中身を確認せず、ZIPファイル自体を登録する。
- `file_sha256` はPhase3スキャン時に計算される。
- `processable_count` はPhase3では設定せず `NULL` とする。
- `file_role = FROM_MEDICAL`、`storage_folder_type = MEDICAL_RESULT_ROOT` で登録される。
- 登録対象の `file_type` は初期実装では `ZIP / XML` とする。
- `file_type = OTHER` は初期実装では登録対象としない。
- `relative_path` は `event.result_root_path` からの相対パスとする。
- 重複判定は `event_id` / `relative_path` / `file_sha256` を基準とする。
- 未知フォルダ、`is_active = 0` alias、`manual_judgement = 1` alias はスキップし、運用上対応が必要な事象として `etl_errors` に記録される。
- 対象外ファイル（CSV、隠しファイル、一時ファイル等）は原則スキップし、`etl_errors` にも記録しない。
- 対象 `event_id` の `result_root_path` 未設定時はPhase3実行時 `ERROR` とする。
- ETL記帳は `scripts/lib/etl` の共通APIを利用し、`phase = SCAN_FILES`、`source = FROM_MEDICAL` とする。
- `etl_runs.status` は共通ETL仕様の `running / success / partial / failed` を利用する。
- Phase3固有のエラー分類は共通ETL構造の `field` / `error_code` に記録する。
- scan結果サマリーは標準出力に表示し、可能な範囲で `etl_runs.notes` に人間が読みやすい短いテキストとして記録される。

#### Codex 指示単位
Phase3 01_scan_files.pyのみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

### Phase4 02_import_xml.py

#### 依存Phase
- Phase1 Core DDL
- Phase2 medical_folder_aliases 初期データ / event migration
- Phase3 01_scan_files.py

#### 目的
`file_receipts` に登録されたファイルからXMLを取り込み、`xml_ledger` / `xml_file_links` / `exam_item_values` を作成する。

#### 新規作成ファイル
- `scripts/from_medical/02_import_xml.py`

#### 更新ファイル
- `scripts/from_medical/script_lib/` 配下のXML取込補助モジュール（必要に応じて）

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/11_v2_script_design_notes.md`
- `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`

#### 完了条件
- `file_receipts.status` を `IMPORTING / IMPORTED / WARNING / ERROR` へ更新できる。
- Phase4で使用する正式コードは、`file_receipts.status = DISCOVERED / IMPORTING / IMPORTED / WARNING / ERROR`、`xml_status = READY / PARSE_ERROR`、`subscriber_match_status = MATCHED / NOT_FOUND / IDENTITY_ERROR / NOT_EXECUTED` とする。`exam_item_status = OK / WARNING / ERROR / NOT_EXECUTED` は後続正規化Phaseで使用する。
- Phase4では `scripts/from_medical/config/import_xml.yml` を読み込んで処理する。configを正本とし、CLI引数は指定時のみ上書き用途とする。ZIPパスワード管理テーブル参照先として `work_db` を設定に含める。
- `xml_status` はXMLそのものの状態のみを表し、加入者照合NG時に変更しない。
- `xml_ledger.exam_item_status` を追加し、必要に応じて `xml_ledger.exam_item_reason` も追加するDDL更新と既存DB向けMigrationが作成されている。
- health_exam_result のMigrationファイル名は `YYYYMMDD_NNN_health_exam_result_<description>.sql` とし、例は `20260707_001_health_exam_result_add_exam_item_status.sql` とする。
- ZIP展開後に取込対象XML件数を数え、`file_receipts.processable_count` を更新できる。
- ZIP内対象XMLが0件の場合は `file_receipts.status = ERROR` とし、`etl_errors` に `field = ZIP`、`error_code = ZIP_NO_TARGET_XML` を基本として記録できる。
- パスワード付きZIPはPhase4で対応する。パスワードはスクリプトへハードコードせず既存のZIPパスワード管理情報から取得し、lookup優先順位は `ZIP_SHA256`、`ZIP_NAME`、`FACILITY` とする。
- パスワード未検出時は `ZIP_PASSWORD_NOT_FOUND`、不一致・復号失敗時は `ZIP_DECRYPT_FAILED` として記録し、XML解析へ進まない。パスワード平文はログ・`etl_errors.message` に出力しない。
- XML内容の一意性は `xml_ledger.xml_sha256` で判定される。
- parse不能XMLでもXMLファイル自体のSHA256から `xml_sha256` を算出し、最小情報で `xml_ledger` を作成できる。
- parse不能XMLの `xml_status` は `PARSE_ERROR` とし、`etl_errors` に `field = XML`、`error_code = XML_PARSE_FAILED` を基本として記録できる。
- Phase4の `etl_errors.field` は `CONFIG` / `FILE` / `ZIP` / `XML` / `IDENTITY` / `SUBSCRIBER` / `DB` を基本とする。
- Phase4の主な `etl_errors.error_code` は `CONFIG_INVALID`、`FILE_NOT_FOUND`、`FILE_READ_FAILED`、`ZIP_OPEN_FAILED`、`ZIP_NO_TARGET_XML`、`ZIP_PASSWORD_NOT_FOUND`、`ZIP_DECRYPT_FAILED`、`XML_READ_FAILED`、`XML_PARSE_FAILED`、`XML_RAW_EXTRACT_FAILED`、`XML_UNSUPPORTED_NAMECODE`、`IDENTITY_GENERATION_FAILED`、`SUBSCRIBER_NOT_FOUND`、`SUBSCRIBER_LOOKUP_FAILED`、`DB_XML_LEDGER_SAVE_FAILED`、`DB_XML_FILE_LINK_SAVE_FAILED`、`DB_EXAM_ITEM_VALUES_SAVE_FAILED`、`DB_FILE_RECEIPT_STATUS_UPDATE_FAILED` を基本とする。
- 検査値raw抽出エラーは `field = XML`、`error_code = XML_RAW_EXTRACT_FAILED` に寄せる。unsupported namecode はraw抽出失敗とは分け、`field = XML`、`error_code = XML_UNSUPPORTED_NAMECODE` として記録する。normalize / validation 専用エラーコードはPhase4では作成しない。
- `etl_errors.message` は人間確認用とし、parse不能XMLは `xml parse failed: path=<path>, inner_path=<inner_path>, reason=<parser_error>`、ZIP内対象XML0件は `zip has no target xml: path=<path>, pattern=h*.xml, excludes=ix08,su08,schema,xsd`、raw抽出失敗は `xml raw extract failed: path=<path>, inner_path=<inner_path>, field=<field>, reason=<reason>` を基本形式とする。複数fieldの場合は `fields=<field1>,<field2>` とする。
- parse不能XMLでは `identity_hash` / `person_id_custom` / `subscriber_id` / `hia_subscriber_id` は設定せず、`exam_item_values` も登録しない。
- 物理ファイルとXML内容の対応は `xml_file_links` に記録される。
- XML状態は `xml_status`、加入者照合状態は `subscriber_match_status`、検査値抽出・バリデーション状態は `exam_item_status` で分離して管理される。ただしPhase4では `exam_item_status` を更新しない。
- `xml_status` に加入者照合結果や検査値バリデーション結果を混在させない。
- `identity_hash` / `person_id_custom` 生成は `scripts.lib.identity.generator.generate_identity_bundle(**raw)` を唯一の入口とし、`02_import_xml.py` 内で独自生成しない。
- identity入力キーは `birthdate`、`insurer_number_raw`、`insurance_symbol_raw`、`insurance_number_raw`、`name_kana_full_raw`、`gender_code` とする。
- Phase4が `generate_identity_bundle()` の戻り値として利用するのは、`ok`、`reason`、`person_id_custom`、`identity_hash`、`field_results` のみとする。
- XML parserはraw値抽出のみを担当し、identity用の独自正規化を実装しない。
- 健診値は `exam_item_values` に縦持ちで登録される。
- `exam_item_values` はXMLから健診値候補として取得できた事実を保持するテーブルとし、正しいデータだけ保存する方針は採用しない。
- `exam_item_values` は `xml_ledger` 作成後に登録される。
- XML解析が成功した場合は、identity生成に失敗しても `exam_item_values` が登録される。
- 同一 `xml_sha256` の再受領時は `exam_item_values` を再登録しない。
- 一部検査値の取得に失敗した場合は、取得可能な検査値を登録し、不足・異常は `etl_errors` に記録して処理が継続される。
- Phase4では `exam_item_values` にraw値、raw unit、nullFlavor、code系情報などを登録する。
- Phase4では `exam_item_values.normalized_value` / `normalized_unit` を生成せず、`normalize_status` / `validation_status` も更新しない。
- Phase4では検査値の正規化・バリデーション、`exam_item_status` 更新、`file_receipts` への検査値サマリー集約を実施しない。
- Phase4では厚生労働省HL7仕様に完全準拠したextractorを最初から作り込まず、旧medi系実装で実績のある基本情報取得方法とentry探索思想を優先する。
- 健診項目取得はentry / observation 配下を広めに探索し、XMLを安全に台帳化して取得できたraw値を失わず保持することを優先する。
- `entryRelationship` 配下の `observation` は通常の健診項目候補として取り込み、17文字namecodeを持つ `observation` はPQ/ST/CD/COなど型に関わらず保持する。
- wrapper `observation` は登録せず、`raw_value` は対象 `observation` の direct child `value` または `text` のみから取得する。
- `displayName` は `raw_value` として扱わず、`code_display` として保持する。
- XML内に項目entryとして存在したものは、可能な限り `exam_item_values` にraw値の行が作成される。
- namecodeが判定できない unsupported namecode は `namecode = NULL` としてraw値・raw unit・nullFlavor・code系情報を保持し、あわせて `etl_errors` に `XML_UNSUPPORTED_NAMECODE` を記録する。
- `namecode = NULL` は「検査値候補として届いたが、検査項目コードとして未対応・未判定」を表す。
- 厚生労働省HL7仕様に沿った Section / Organizer / Entry 単位の構造解析はPhase5以降のリファクタリング対象とする。
- 後続の正規化Phaseでは `exam_item_values` のraw値を入力とし、`exam_item_master`、必要に応じて `norm_variants`、`normalize_exam_item_value()` を用いて正規化・バリデーションを実施する。
- `norm_variants` はCD/CO系検査値の表記ゆれ辞書として限定利用し、`result_code_oid + raw_value_utf8` の完全一致を基本とする。
- `norm_rules`、`raw_token_norm`、`normalize_plus_tokens`、`exam_value_normalizer.py` 相当の高度な前処理は後続Phaseでも初期採用せず、将来拡張とする。
- ETL metricsは、`files = 処理対象file_receipts件数`、`rows_seen = 対象XML件数`、`rows_inserted = 新規xml_ledger件数`、`rows_updated = xml_file_links登録件数 + file_receipts更新件数`、`rows_skipped = 既存xml_sha256再受領・対象外XML件数`、`errors = etl_errors登録件数` として記録できる。
- `exam_item_values` 件数は `rows_inserted` に含めず、必要に応じて `etl_runs.notes` のサマリーへ記録できる。
- 正規化Phaseの正式名称、スクリプト名、ETL単位・再実行単位、`normalize_exam_item_value()` の正式引数・返却値、正規化完了後に更新する台帳・サマリー項目の詳細は後続Phase設計で決め、Phase4実装内で仮決定しない。

#### Codex 指示単位
Phase4 02_import_xml.pyのみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

### Phase5 dev_phr制度マスタ整備

#### 依存Phase
- Phase1 Core DDL

#### 目的
`03_check_exam_results.py` の項目別判定で利用する `dev_phr.exam_item_group_*` 系マスタを、制度チェック対象72項目に対応させる。

#### 新規作成ファイル
- 未定

#### 更新ファイル
- 未定

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`

#### 完了条件
- 共通72項目用グループの初期データ方針がSQL化されている。
- 法定健診判定用グループの初期データ方針がSQL化されている。
- 特定健診用グループは初期未投入でも動作可能な前提で整理されている。
- `exam_item_group_identity_members` への追加カラムは作成しない。

#### Codex 指示単位
Phase5 dev_phr制度マスタ整備のみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

### Phase6 exam_check_results DDL

#### 依存Phase
- Phase1 Core DDL
- Phase5 dev_phr制度マスタ整備

#### 目的
制度チェック結果を保持する `exam_check_results` のDDLを作成する。

#### 新規作成ファイル
- `sql/ddl/health_exam_result/0080_health_exam_result__exam_check_results.sql`

#### 更新ファイル
- 未定

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`

#### 完了条件
- 72項目の項目別 `status` / `reason` を同一性項目コード単位で横持ちするDDLになっている。
- 法定健診・特定健診で項目別 `status` / `reason` を二重に持たない。
- 制度チェック総合判定は `xml_ledger.check_status` に保持し、`exam_check_results` には72項目の項目別 `status` / `reason` のみを保持する。

#### Codex 指示単位
Phase6 exam_check_results DDLのみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

### Phase7 03_check_exam_results.py

#### 依存Phase
- Phase1 Core DDL
- Phase5 dev_phr制度マスタ整備
- Phase6 exam_check_results DDL

#### 目的
`exam_item_values` を入力に、72項目の項目別 `status` / `reason` を `exam_check_results` に記録し、制度単位の総合判定を `xml_ledger.check_status` へ集約する。

#### 新規作成ファイル
- `scripts/from_medical/03_check_exam_results.py`

#### 更新ファイル
- `scripts/from_medical/script_lib/` 配下の判定関数モジュール（必要に応じて）

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/11_v2_script_design_notes.md`
- `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md`

#### 完了条件
- 72項目の項目別 `status` / `reason` を生成できる。
- `ANY_NONEMPTY` はpresence判定のみを担当する。
- `CALCULATE` ルールは、対象同一性項目に有効値が存在しない場合のみ評価する。
- 対象同一性項目に有効値が存在する場合は、その値を採用し、項目別 `status = OK` とする。
- `CALCULATE` に必要な同一性項目がすべて揃う場合は、共通計算ライブラリを利用して値を生成し、項目別 `status = CALCULATED` とする。
- `CALCULATE` で値を確定できない場合のみ、`ALTERNATIVE` ルールを評価する。
- `ALTERNATIVE` が成立した場合は、対象項目を項目別 `status = ALTERNATIVE`、代替項目を項目別 `status = OK` とする。
- `CALCULATE` と `ALTERNATIVE` のいずれでも値を確定できない場合は、項目別 `status = MISSING` とする。
- 計算ロジックは共通ライブラリ `scripts/lib/examination/calc.py` へ実装し、制度チェック側は計算ライブラリを呼び出して `status` を決定する。
- `CALCULATE` と `ALTERNATIVE` は別ルールとして扱い、同一の処理フローへ混在させない。
- `ALTERNATIVE` は既存の identity 項目コードによる処理フローを利用する。
- `ALTERNATIVE` 共通処理は `scripts/lib/examination/alternative.py` に実装する。
- `ALTERNATIVE` 共通処理では、ケース判定と実処理関数を分離する。
- ルール種別をキーとして判定関数へディスパッチする。
- 法定健診・特定健診の総合判定は `exam_check_results` を唯一の入力として算出する。
- XMLや `exam_item_values` を総合判定時に直接参照しない。
- 共通72項目の項目別判定完了後、制度グループ単位で集計して `xml_ledger.check_status` を生成する。
- 法定OK・特定OKの場合は `xml_ledger.check_status = OK` とする。
- 法定OK・特定WARNINGの場合は `xml_ledger.check_status = WARNING` とする。
- 法定NGの場合は、特定健診の結果にかかわらず `xml_ledger.check_status = NG` とする。
- 特定健診不足は `WARNING`、法定健診不足は `NG` とする。

#### Codex 指示単位
Phase7 03_check_exam_results.pyのみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

### Phase8 04_export_hia_xml.py

#### 依存Phase
- Phase4 02_import_xml.py
- Phase7 03_check_exam_results.py

#### 目的
チェック結果と出力可否を参照して、HIA提出用XMLを生成する。

#### 新規作成ファイル
- `scripts/from_medical/04_export_hia_xml.py`

#### 更新ファイル
- 未定

#### 参照資料
- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/11_v2_script_design_notes.md`
- `docs/refactor/health_exam_result/19_implementation_ready_summary.md`

#### 完了条件
- 後続フェーズとして本実装する。
- 初期実装では設計・入れ物のみでも可とする。

#### Codex 指示単位
Phase8 04_export_hia_xml.pyのみを実装する。

#### コミット単位
当該Phase完了・レビュー完了後にコミットする。

#### レビュー観点
- 設計資料（03 / 11 / 12 / 19）との整合
- 命名規則
- 責務分離
- 初期実装スコープ逸脱がないこと

## 7. 実装ルール

- Phaseを跨いだ実装は行わない。
- Codexは、指示されたPhase以外のファイルを変更しない。
- 設計変更が必要になった場合は実装を停止し、`05_design_history.md` で協議した上で `03_decisions.md` を更新してから実装を再開する。
- 各Phaseは「実装 → レビュー → コミット」の順で完了させ、次Phaseへ進む。
- 設計資料（03 / 11 / 12 / 19）と20の内容に矛盾が見つかった場合は、実装を優先せず設計を同期してから進める。
