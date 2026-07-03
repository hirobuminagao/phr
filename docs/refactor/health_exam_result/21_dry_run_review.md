# 21 Dry-run Review

## 1. レビュー対象

対象資料:

1. `docs/refactor/health_exam_result/03_decisions.md`
2. `docs/refactor/health_exam_result/11_v2_script_design_notes.md`
3. `docs/refactor/health_exam_result/12_v2_ddl_design_notes.md`
4. `docs/refactor/health_exam_result/19_implementation_ready_summary.md`
5. `docs/refactor/health_exam_result/20_implementation_plan.md`
6. `docs/refactor/health_exam_result/21_dry_run_review.md`

正本は `03_decisions.md` とする。

## 2. Dry-run条件

今回の前提:

- 実装禁止
- コード生成禁止
- DDL生成禁止
- migration作成禁止
- スクリプト作成禁止
- レビューのみ
- `03_decisions.md` を正本として判断する
- `20_implementation_plan.md` のPhase構成に沿って再レビューする

## 3. Phase別レビュー

### Phase判定サマリー

| Phase | 判定 | ブロッカー / 後続課題 | 理由 |
| --- | --- | --- | --- |
| Phase1 Core DDL | 条件付きGO | Phase開始前確認あり | core 7テーブル、配置、命名規則は確定済み。`exam_check_results` を含めないことも明確。DDL詳細の型・制約・unique/indexは12/19の候補を実装時に突き合わせる必要がある。 |
| Phase2 medical_folder_aliases 初期データ / event migration | 条件付きGO | Phase開始前確認あり | `medical_folder_aliases` 初期データの参照資料と方針は明確。`event.result_root_path` の既存DDL / migration要否確認がPhase開始条件。 |
| Phase3 01_scan_files.py | 条件付きGO | 依存Phase待ち | 責務・配置・処理順は明確。Phase1/2完了後に着手可能。設定ファイル正式名、重複判定キーの細部は実装前確認。 |
| Phase4 02_import_xml.py | 条件付きGO | 実装中判断あり | XML取込、`xml_ledger`、`xml_file_links`、`exam_item_values` の責務は明確。`xml_status` / `xml_reason` の保持方針も解消済み。`validation_status` 正式値、既存XML再受領時の細部は未決。 |
| Phase5 dev_phr制度マスタ整備 | 保留 | ブロッカーあり | 72項目対応方針、追加カラムなし方針は明確。実際に投入するgroup_code、初期データ、差分SQL範囲が未確定。 |
| Phase6 exam_check_results DDL | 条件付きGO | Phase5待ち | `check_result` の物理保持先は `xml_ledger.check_status` として解消済み。`exam_check_results` は72項目の項目別 `status` / `reason` のみ保持する。Phase5のマスタ整備完了後に着手可能。 |
| Phase7 03_check_exam_results.py | 条件付きGO | Phase5/6待ち | CALCULATE / ALTERNATIVE評価順、総合判定条件、`xml_ledger.check_status` 生成方針は解消済み。reason集約詳細、`INVALID` 詳細、マスタ初期データは残る。 |
| Phase8 04_export_hia_xml.py | 初期実装対象外 | 後続Phase | 03/11/19では後続フェーズ扱い。20でも「後続フェーズとして本実装」「初期実装では設計・入れ物のみでも可」とされるため、本実装は初期GO判定外。 |

### 前回ブロッカーの再判定

| 前回ブロッカー | 最新判定 | 根拠 |
| --- | --- | --- |
| `check_result` の物理保持先 | 解消済み | 制度チェック総合判定は `xml_ledger.check_status` に保持する方針へ同期済み。 |
| 制度単位の `OK / WARNING / NG` 集計条件 | 解消済み | 法定OK・特定OKは `OK`、法定OK・特定WARNINGは `WARNING`、法定NGは `NG` と決定済み。 |
| 法定健診・特定健診の判定優先順位 | 解消済み | 法定健診不足は `NG`、特定健診不足は `WARNING` と決定済み。 |
| CALCULATE / ALTERNATIVE の評価順 | 解消済み | 対象値ありは `OK`、なければ `CALCULATE`、確定不可時のみ `ALTERNATIVE`、いずれも不可なら `MISSING`。 |
| XML単位の `xml_status` / `xml_reason` | 解消済み | `xml_ledger` に保持し、XML読込エラー等をまとめて扱う。詳細ステータスは初期では持たない。 |
| `xml_ledger.check_status` の生成方針 | 解消済み | `exam_check_results` の制度判定結果から生成する。 |

## 4. Phase詳細

### Phase1 Core DDL

実装時に迷う点:

- core DDL 7テーブルの対象、配置、ファイル名は明確。
- `exam_check_results` はこのPhaseでは作成しない。
- `xml_ledger` は `xml_status` / `xml_reason` / `check_status` / `xml_export_status` を持つ前提で扱える。
- DDLとしての型、NULL可否、unique制約、index、FK方針は12/19の記載を突き合わせる必要がある。

不足資料:

- core 7テーブルの最終カラム定義表。
- index / unique制約 / foreign key有無の最終判断。

判断できない事項:

- `xml_file_links` の一意制約。
- `exam_item_values.validation_status` の正式値。
- `file_receipts` の重複判定に対応するDB制約。

改善提案:

- Phase1実装前に、12の候補情報をもとにテーブル別DDL確定表を用意すると迷わない。

GO / 保留:

- 条件付きGO。
- Phase1着手自体は可能。ただしDDL詳細確定をPhase開始前に確認すること。

### Phase2 medical_folder_aliases 初期データ / event migration

実装時に迷う点:

- `medical_folder_aliases` 初期データの参照資料は明確。
- `event_id = 2` の医療機関フォルダ188件、原則 `src_folder_raw = dst_folder_norm` は明確。
- `event.result_root_path` の既存DDL / migration要否確認が残る。

不足資料:

- 初期データ投入SQLの配置とファイル名。
- `event.result_root_path` migration が必要な場合のファイル名。

判断できない事項:

- `202604開院_福岡労働衛生研究所　健診スクエア博多` の仮フォルダ名注意をSQLコメントに残すか、データ項目に持つか。

改善提案:

- Phase2前に、初期データSQLとevent migrationの想定ファイル名を20へ明記するとよい。

GO / 保留:

- 条件付きGO。
- `event.result_root_path` の確認をPhase開始前に行うこと。

### Phase3 01_scan_files.py

実装時に迷う点:

- スクリプト配置、入力、主要処理、`file_receipts.status = DISCOVERED` は明確。
- `event.result_root_path` と `medical_folder_aliases` を参照して対象フォルダを探索する流れは明確。
- 設定ファイル正式名は未定。

不足資料:

- `scripts/from_medical/config/` 配下の設定ファイル名と最小項目。
- scan対象ファイル種別の判定ルール。
- `file_receipts` の一意制約に合わせた重複判定仕様。

判断できない事項:

- 同一 `file_sha256` だがパスや医療機関フォルダが異なる場合の扱い。
- フォルダ参照不可時にRun全体を失敗にする条件の境界。

改善提案:

- Phase3前に、設定YAML例と重複判定キーを20へ追記するとよい。

GO / 保留:

- 条件付きGO。
- Phase1/2完了後なら実装可能。

### Phase4 02_import_xml.py

実装時に迷う点:

- `file_receipt` 単位transaction、work一時利用、XML SHA256、`xml_file_links`、`xml_ledger`、`exam_item_values` の責務は明確。
- XML単位の結果は `xml_ledger.xml_status` / `xml_reason` に保持する方針が明確になった。
- XML単位の詳細ステータスは初期実装では持たないため、細分化を実装者が追加してはいけない。
- `validation_status` の正式値は未決。
- 既存XML再受領時に既存 `xml_ledger.xml_status` を変更しないか、`SKIPPED` をどこに記録するかが未決。

不足資料:

- XML項目抽出仕様への参照。
- `exam_item_values` の値型別カラム利用ルール。
- duplicate XML時の `file_receipts` サマリー更新方針。

判断できない事項:

- 既存XMLの場合、Runサマリー上の扱い。
- XML基本情報不足時に `xml_ledger` を作るか、作らず `etl_errors` のみにするか。

改善提案:

- Phase4の参照資料に、XML項目抽出に必要な仕様資料を追加するとよい。

GO / 保留:

- 条件付きGO。
- `validation_status` と重複XML時の状態更新は、Phase4開始前または実装中の明示確認が必要。

### Phase5 dev_phr制度マスタ整備

実装時に迷う点:

- `dev_phr.exam_item_group_*` 系マスタを72項目対応にする方針は明確。
- v2初期で `exam_item_group_identity_members` へ追加カラムしない点も明確。
- 共通72項目用グループ、法定健診判定用グループ、特定健診判定用グループの具体的なgroup_code、投入対象レコード、差分SQLの粒度が未定。
- 特定健診用グループは初期未投入でも動作可能とされるが、Phase5でどこまでSQL化するかは未確定。

不足資料:

- dev_phrマスタ投入データの確定表。
- group_code命名。
- 既存 `LSIO_Legal_Item` との差分反映方針。

判断できない事項:

- 法定健診判定用グループに含める項目集合。
- `presence_value_mode` 等の既存カラムへどの値を投入するか。
- migrationファイル名と配置。

改善提案:

- Phase5前に、72項目と各グループの対応表を作り、SQL化対象を明示するとよい。

GO / 保留:

- 保留。
- マスタ投入内容が未確定であり、ここは実装者判断で補完できない。

### Phase6 exam_check_results DDL

実装時に迷う点:

- 72項目の項目別 `status` / `reason` を横持ちする方針は明確。
- 項目別statusの値、reasonのOK時NULLは明確。
- 制度チェック総合判定は `xml_ledger.check_status` に保持し、`exam_check_results` は項目別 `status` / `reason` のみ持つ方針が明確になった。
- 72項目カラム一覧のDDL生成元は確認が必要。

不足資料:

- 72項目カラム一覧のDDL生成元。
- `status_<item_code>` / `reason_<item_code>` の型・長さ・NULL可否。

判断できない事項:

- specから機械的にDDL生成するか、固定DDLとして手書きするか。

改善提案:

- Phase6前に、72項目カラム一覧を固定DDL表として扱うか、仕様書から生成するかを明記するとよい。

GO / 保留:

- 条件付きGO。
- 前回の `check_result` 物理保持先ブロッカーは解消済み。Phase5完了とカラム定義確定を条件とする。

### Phase7 03_check_exam_results.py

実装時に迷う点:

- 項目単位判定と制度単位総合判定の分離は明確。
- `ANY_NONEMPTY`、`CALCULATE`、`ALTERNATIVE` の評価順と責務は明確。
- `CALCULATE` 共通処理は `scripts/lib/examination/calc.py`、`ALTERNATIVE` 共通処理は `scripts/lib/examination/alternative.py` に置く方針が明確。
- `xml_ledger.check_status` 生成条件は明確。
- reason集約詳細と `INVALID` 詳細は未決。

不足資料:

- 各ルール種別の入力・出力・判定結果対応表。
- `INVALID` に入れる不正理由の詳細表現。
- reason集約の詳細。
- dev_phrマスタ投入データ。

判断できない事項:

- `CALCULATED` / `ALTERNATIVE` が制度グループ集計上OK相当かどうかは、03では明示されていない。項目別status生成は実装可能だが、総合集計の内部換算は実装時に確認が必要。
- 特定健診用グループ未投入時に総合判定をどう扱うか。

改善提案:

- Phase7前に、項目別statusから法定健診・特定健診の制度判定へ変換する最小表を用意するとよい。
- reason集約は初期では詳細未決のため、`reason IS NOT NULL` 集約に留めるなどスコープを明記するとよい。

GO / 保留:

- 条件付きGO。
- 前回の総合判定保持先・OK/WARNING/NG条件・判定優先順位ブロッカーは解消済み。残るのはマスタ初期データ、status換算、reason集約詳細。

### Phase8 04_export_hia_xml.py

実装時に迷う点:

- 03/11/19では後続フェーズ扱い。
- 20ではPhase8として入っているが、完了条件は「後続フェーズとして本実装」「初期実装では設計・入れ物のみでも可」とされている。
- 本実装の詳細仕様は不足している。

不足資料:

- HIA出力XML生成仕様。
- `xml_export_status` 遷移条件。
- 出力済み再出力ポリシー。

判断できない事項:

- Phase8でファイルだけ作るのか、本実装まで行うのか。

改善提案:

- 初期実装ではPhase8を本実装対象外とし、後続フェーズへ送る扱いを維持するのが自然。

GO / 保留:

- 初期実装対象外。
- 入れ物のみ作成する場合は、その旨を20で明記する必要がある。

## 5. 設計資料間の整合性

### 03

- DH-20260703-02〜05 の主要決定事項は反映済み。
- CALCULATE / ALTERNATIVE 評価順、`xml_ledger.check_status` 生成方針、XML単位の `xml_status` / `xml_reason` 方針が明記されている。
- 保留事項として reason code詳細、XML単位の詳細ステータス追加要否、`INVALID` 詳細などが残っている。

### 11

- `03_check_exam_results.py` の処理順と制度チェック総合判定が最新方針へ同期されている。
- XML単位の `xml_status` / `xml_reason` も初期詳細ステータスなしとして整理されている。

### 12

- `xml_ledger.check_status` を制度チェック総合判定の保持先とする方針へ更新済み。
- `exam_check_results` は72項目の項目別 `status` / `reason` を保持する台帳として整理されている。

### 19

- 前回の未決だった `check_result` 物理保持先、集計条件、判定優先順位は解消済みとして整理されている。
- GO判定も `check_result` 総合判定はOK寄りへ更新されている。

### 20

- Phase6 / Phase7 の完了条件に `xml_ledger.check_status` 方針が反映されている。
- Phase8は後続扱いのまま残っており、初期実装対象外として扱うのが自然。

## 6. 実装を止める事項

Phase開始前に必ず決める必要があるもの:

- Phase1前: core DDLの型、NULL可否、unique/index/FK方針。
- Phase2前: `event.result_root_path` の既存DDL / migration要否。
- Phase5前: dev_phr制度マスタのgroup_code、投入データ、差分SQL範囲。
- Phase6前: 72項目カラム一覧のDDL化方法、型、長さ、NULL可否。
- Phase7前: dev_phr制度マスタ投入完了、項目別statusから制度判定への最小換算表。

実装中判断でよいもの:

- Phase3のログ粒度、Runサマリーの文言。
- Phase4の軽微なエラー文言、`etl_errors` のメッセージ詳細。
- Phase7の内部関数分割。

後続Phaseへ送るもの:

- Phase8 `04_export_hia_xml.py` の本実装。
- HIA出力履歴台帳。
- 人＋イベント単位の状態管理台帳。
- XML単位の詳細ステータス（項目別・工程別）。

## 7. 実装しやすくする改善提案

- Phase1前に、core 7テーブルのDDL確定表を用意する。
- Phase2前に、初期データSQL / event migration のファイル名と配置先を20に明記する。
- Phase3前に、設定YAML例、設定ファイル名、重複判定キーを明記する。
- Phase4前に、XML項目抽出仕様への参照資料を20へ追加する。
- Phase5前に、共通72項目用グループ、法定健診判定用グループ、特定健診判定用グループの投入データ表を作る。
- Phase6前に、72項目カラム一覧のDDL化方法を決める。
- Phase7前に、項目別statusから制度判定への最小換算表を用意する。
- Phase8は、初期実装対象外か入れ物のみ作成かを20で明確にする。

## 8. 総合判定

Phase単位の判定:

- Phase1 Core DDL: 条件付きGO
- Phase2 medical_folder_aliases 初期データ / event migration: 条件付きGO
- Phase3 01_scan_files.py: 条件付きGO
- Phase4 02_import_xml.py: 条件付きGO
- Phase5 dev_phr制度マスタ整備: 保留
- Phase6 exam_check_results DDL: 条件付きGO
- Phase7 03_check_exam_results.py: 条件付きGO
- Phase8 04_export_hia_xml.py: 初期実装対象外

全体として、前回の大きな制度チェック系ブロッカーはかなり解消された。

ただし、Phase1のDDL詳細、Phase5のdev_phrマスタ投入内容、Phase6の72項目DDL化方法、Phase7のstatus換算・reason集約詳細はまだ実装者が補完してはいけない事項として残る。

したがって、初期実装は一括GOではなく、Phase1から順に条件付きGOで進める判定とする。
