# phr_master Design History

## このドキュメントの位置付け

- 本ドキュメントは `02_02_exam_result_csv_import` の前提となるマスタ整備、および `phr_master` 新設に関する協議内容と意思決定の経緯を記録する。
- 採用済みの決定事項は `03_decisions.md` へ反映する。
- `05_design_history.md` と `03_decisions.md` が異なる場合は `03_decisions.md` を優先する。
- ADRは方針が固まってから作成する。

## 実装ルール

- この段階ではDDL、migration、seed、スクリプト変更は行わない。
- まず `02_02_exam_result_csv_import` に必要なマスタ、DB境界、権限分離、CSV取込への影響を整理する。
- 実装者は `03_decisions.md` に反映された内容だけを実装前提とする。
- 未決事項は未決のまま残し、推測でDDL化しない。

---

## DH-20260723-01 / 2026-07-23 JST

### テーマ

`02_02_exam_result_csv_import` 前提のマスタ境界整理

### 背景

本来の目的は `02_02_exam_result_csv_import` の設計である。
その設計前調査で、CSV取込は単一フォーマットではなく、受領ファイル、健診機関、CSVフォーマット、列マッピング、結果値normalizeをつなぐ必要があることを確認した。

また、現行の `medical_folder_aliases` は健診機関そのものではなく、受領フォルダ名の揺れやイベント別配置を扱う性質が強い。
CSV取込を実装する前に、健診機関マスタを親として新設し、aliasを子として扱う方向を検討することになった。

同時に、将来的に複数人で利用する場合、マスタ更新権限と個人情報・健診結果データへのアクセス権限を分けやすくするため、CSV取込の前提マスタを `phr_master` として分離する案を検討した。

### 当初の考え

- 健診機関マスタを新たに作り、現行aliasを子にする方がよい。
- `medical_folder_aliases` をどのDBへ置くかは別途検討する。
- `exam_item_master` や保険者系マスタも将来的にマスタ用DBへ移す可能性がある。

### 議論

- マスタ用DB名は `phr_master` が自然である。
- `phr_master` は、PHR全体で参照する共通マスタの置き場とする。
- `dev_phr` にマスタと機微情報が混在すると、複数人運用時に「マスタだけ編集できる人」を作りにくい。
- `phr_master` を分けることで、マスタ管理者、業務処理者、機微情報参照者などのロール分離がしやすくなる。
- `medical_folder_aliases` はイベント別の受領配置・フォルダ名揺れを扱うため、少なくとも当面は `health_exam_result` 側に残す案が有力。
- 健診機関そのものは `phr_master` に置き、`health_exam_result.medical_folder_aliases` が親を参照する構成がよい。
- CSVフォーマット、CSV列マッピング、結果値変換辞書は健診機関マスタや項目マスタと関係が深いため、`phr_master` 側の候補とする。
- ADRはこの時点では書かず、まずspec配下で協議内容と決定事項を分けて管理する。

### 現時点の考え

`phr_master` は本丸ではなく、`02_02_exam_result_csv_import` に必要なマスタを整備するための境界である。
加えて、将来的な共同運用における権限境界としても設計する。

健診結果CSV取込では、受領ファイルから健診機関を特定し、健診機関に紐づくCSVフォーマット・マッピングを選択する必要がある。
そのため、健診機関マスタはCSV取込設計の前提マスタとして扱う。

### 決定事項

- マスタ用DB名は `phr_master` とする。
- 本specの主目的は `02_02_exam_result_csv_import` の前提マスタ整備とする。
- `phr_master` はPHR全体の共通マスタDBとする。
- `phr_master` は個人情報・健診結果値・受領ファイルなどの業務トランザクションを保持しない。
- 健診機関そのものを表す親マスタは `phr_master` 側に作成する方針とする。
- `medical_folder_aliases` は直ちに移動せず、当面は `health_exam_result` 側に残す案を第一候補とする。
- CSVフォーマット・CSV列マッピングは `phr_master` 側の候補とする。
- ADRは方針が固まってから作成する。

### 保留事項

- `phr_master` 初期DDLの詳細。
- 初期作成テーブルの正式一覧。
- `exam_item_master`、`exam_item_groups`、`norm_variants`、`norm_rules` の移動時期。
- 保険者系マスタの移動時期。
- `medical_folder_aliases` の最終配置。
- `file_receipts` に `medical_institution_id` を追加するかどうか。
- cross schema FKを張らない既存方針との整合をどう表現するか。
- マスタ管理ロールの具体設計。
- 既存SQL・スクリプトの参照先変更範囲。

### 根拠

- `02_02_exam_result_csv_import` 設計前調査。
- 現行 `01_scan_files.py` と `file_receipts` の調査結果。
- 既存 `medical_folder_aliases` の役割確認。
- 将来的な複数人運用と権限分離の要件。

### 次回検討

- `phr_master` に置く候補テーブルを現行DDLから棚卸しする。
- すぐ作るテーブル、将来移すテーブル、移さないテーブルを分類する。
- 健診機関マスタの最小カラム候補を整理する。
- `medical_folder_aliases` との接続方法を整理する。
- CSVフォーマット・CSV列マッピングのテーブル候補を整理する。
- `03_decisions.md` に反映する正式決定を追加する。

---

## DH-20260723-02 / 2026-07-23 JST

### テーマ

`02_02_exam_result_csv_import` の全体フローと健診機関マスタ境界

### 背景

CSV取込設計の入口として、`01_scan_files.py`、フォルダalias、健診機関マスタ、CSVフォーマット、normalize、`exam_item_values` 登録までの責務分担を確認した。

前回時点では `phr_master` 新設とマスタ境界を中心に整理したが、今回の本丸は `02_02_exam_result_csv_import` であり、マスタ整備はその前提であることを再確認した。

### 当初の考え

- `01_scan_files.py` はCSVファイルを `file_receipts` に登録し、CSV取込側が後続で解釈する。
- 健診機関マスタを `phr_master` に作り、現行aliasを子にする。
- `medical_folder_aliases` は当面 `health_exam_result` 側に残す案もあった。
- CSV取込時のnormalizeをどこで実施するかは未確定だった。

### 議論

- `01_scan_files.py` で健診機関そのものを解析するというより、フォルダaliasを通じて新しい健診機関マスタのIDを確定し、後続処理へ引き継ぐイメージとした。
- 手順としては、健診機関マスタを先に整備し、alias側に健診機関IDを持たせ、そのaliasから `file_receipts` へ健診機関情報を渡す。
- 「医療機関」と「健診機関」は明確に分ける。
- 今回は健診結果CSV取込に必要な健診機関なので、テーブル名・仕様名は健診機関寄りにする。
- `medical_folder_aliases` の配置は改修範囲の問題であり、このタイミングで `phr_master` 側へ移す方向とする。
- 移行時は、現行の登録状況を原則そのまま入れる。
- CSVフォーマット定義、CSV列マッピングは `phr_master` 側に置く。
- CSV取込では、`exam_item_values` 登録時にnormalize処理まで組み込む。

### 現時点の考え

CSV取込の前提として、`01_scan_files.py` はCSVファイルを単に見つけるだけでなく、フォルダaliasを介して健診機関IDを確定し、`file_receipts` から後続処理が健診機関を辿れる状態にする。

CSV取込本体は、`file_receipts` から健診機関を取得し、`phr_master` のCSVフォーマット・列マッピング・normalize辞書を参照して、`exam_item_values` へ正規化済み情報を含めて登録する。

### 決定事項

- 医療機関と健診機関は明確に分ける。
- 今回の主対象は健診機関である。
- 健診機関マスタは `phr_master` 側に作成する。
- alias側に健診機関マスタのIDを追加する。
- `01_scan_files.py` は、フォルダaliasから健診機関IDを確定し、後続処理へ引き継げる状態で `file_receipts` へ登録する。
- `medical_folder_aliases` はこのタイミングで `phr_master` 側へ移す方向とする。
- 現行のalias登録状況は原則そのまま移行する。
- CSVフォーマット定義とCSV列マッピングは `phr_master` 側に置く。
- `02_02_exam_result_csv_import` は `exam_item_values` 登録時にnormalize処理まで含める。

### 保留事項

- 健診機関マスタの正式テーブル名。
- aliasテーブルの正式テーブル名。
- 既存 `medical_folder_aliases` 名を維持するか、健診機関寄りの名前へ変更するか。
- `file_receipts` に健診機関IDを直接持たせるか、既存 `facility_code` / `facility_name` とalias参照で引き継ぐか。
- `01_scan_files.py` のCSV対応をどの範囲で実装するか。
- normalize処理をCSV取込内でどのモジュールとして実装し、将来XML由来にも共通化するか。
- `health_exam_result` から `phr_master` へaliasを移すmigration手順。

### 根拠

- CSVは健診機関ごとにフォーマット・列マッピングが異なる。
- フォルダaliasは既に受領フォルダと健診機関を結びつける実務上の入口になっている。
- `file_receipts` 起点で後続処理を行う方針と整合する。
- 複数人運用では、マスタ編集権限と健診結果・個人情報アクセス権限を分けやすい。

### 次回検討

- 健診機関マスタの命名候補を整理する。
- aliasテーブルの命名候補を整理する。
- `file_receipts` への健診機関ID引き渡し方式を比較する。
- CSVフォーマット・列マッピングの最小テーブル構成を整理する。
- CSV取込時normalizeの入力・出力項目を整理する。

---

## DH-20260723-03 / 2026-07-23 JST

### テーマ

健診機関マスタ命名候補とCSV取込前提カラム

### 背景

前回協議で、CSV取込ではフォルダaliasから健診機関IDを確定し、CSVフォーマット・列マッピング・normalizeへつなぐ方針を決めた。
今回はその続きとして、健診機関マスタ名、aliasテーブル名、`file_receipts` への引き継ぎ情報、CSVマッピング最小要素、normalize入出力を整理した。

### 議論

- 健診機関マスタ名は、医療機関全般と混ざらない名前にする必要がある。
- `medical_institutions` は医療機関一般に見えやすく、今回の「健診機関」としては広すぎる。
- 候補は以下とする。
  - `exam_facilities`
  - `health_exam_facilities`
  - `checkup_facilities`
  - `kenshin_facilities`
  - `exam_providers`
- 候補比較の結果、正式テーブル名は `exam_facilities` とする。
- aliasテーブル名は既存の `medical_folder_aliases` をそのまま使う。
- `file_receipts` には健診機関IDと健診機関名称を持たせる。
- CSVマッピングは、健診機関ID、mapping version、CSV項目、変換ルールを基本要素とする。
- CSV取込時のnormalizeは、raw値を入力し、normalize結果を `exam_item_values` のnormalize系カラムへ反映する。

### 現時点の考え

`01_scan_files.py` はフォルダaliasから健診機関IDを確定し、`file_receipts` に健診機関IDと健診機関名称を保持する。
`02_02_exam_result_csv_import` はその健診機関IDを起点にCSVマッピングを選択し、raw値を登録すると同時にnormalize系カラムへ結果を反映する。

### 決定事項

- aliasテーブル名は既存の `medical_folder_aliases` をそのまま使う。
- `file_receipts` には健診機関IDと健診機関名称を持たせる。
- CSVマッピングは健診機関ID、mapping version、CSV項目、変換ルールを持つ。
- CSV取込時のnormalizeはraw値を入力し、normalize系カラムへ反映する。

### 保留事項

- 健診機関マスタの正式テーブル名は `exam_facilities` に確定した。
- `exam_facilities` の初期カラム構成。
- 健診機関IDカラムの正式名。
- 健診機関名称カラムの正式名。
- CSVマッピングテーブルの正式名。
- mapping version の管理単位。
- CSV項目をヘッダー名で持つか、列番号も持つか。
- 変換ルールを文字列コードで持つか、別マスタ化するか。

### 根拠

- CSVフォーマットは健診機関ごとに異なる。
- 後続処理は `file_receipts` 起点で処理対象を選ぶ。
- raw値とnormalize結果を同時に保持することで、取込後の調査と再normalizeの導線を残せる。

### 次回検討

- 健診機関マスタの最小カラムを整理する。
- `file_receipts` 追加カラム名を整理する。
- CSVマッピングテーブルの最小DDL候補を整理する。

---

## DH-20260723-04 / 2026-07-23 JST

### テーマ

健診機関マスタ最小構成、`file_receipts` 引き渡しカラム、CSVマッピング最小構成

### 背景

前回までに、CSV取込の入口として健診機関マスタ、フォルダalias、`file_receipts`、CSVフォーマット・列マッピング、normalizeをつなぐ方針を決めた。
今回は実装前の設計粒度として、初期DDLへ落とせる程度の名前と責務を整理した。

既存DDLでは `file_receipts` に `facility_code` / `facility_name` があり、XML抽出やフォルダ由来の値としても同名概念が使われている。
そのため、新しい親マスタのIDは既存の `facility_code` とは混ぜず、CSV取込のマスタ選択キーとして明確に分ける必要がある。

### 議論

- 親マスタの主キー名は、テーブル名と揃えて `exam_facility_id` とする。
- 健診機関側の業務コードは `exam_facility_code` とする。
- 表示名は `exam_facility_name` とする。
- 既存の `facility_code` / `facility_name` はXML抽出値・フォルダ由来値として残し、親マスタIDとは別物として扱う。
- `file_receipts` には、後続CSV取込が迷わずマッピングを選べるように `exam_facility_id` を持たせる。
- `file_receipts.exam_facility_name` は、取込時点の表示名スナップショットとして持つ。
- `medical_folder_aliases` は `phr_master` へ移す方向で進めるが、既存の `event_id + src_folder_raw` の一意性は維持する。
- cross schema FK は張らない既存方針に合わせ、整合性は移行SQL・検査SQL・アプリケーション側で確認する。
- CSVフォーマット定義は、健診機関ごと・バージョンごとの親情報として `csv_format_versions` に分ける。
- CSV列マッピングは、実際のCSV列と `exam_item_values` 登録先namecodeをつなぐため `csv_column_mappings` とする。
- CSV列はヘッダー名だけでは不足する可能性があるため、列番号も保持できる構成にする。
- 変換ルールは最初から過度に別マスタ化せず、文字列コードとして持ち、必要になった段階で辞書化する。

### 現時点の考え

`01_scan_files.py` は、`phr_master.medical_folder_aliases` から `exam_facility_id` を確定し、`health_exam_result.file_receipts` に `exam_facility_id` と `exam_facility_name` をスナップショットとして登録する。

`02_02_exam_result_csv_import` は、`file_receipts.exam_facility_id` から `phr_master.csv_format_versions` を選択し、さらに `csv_column_mappings` に従ってCSV列を `exam_item_values` のnamecodeへ変換する。
登録時にはraw値を残しつつ、normalize結果をnormalize系カラムへ入れる。

### 決定事項

- `exam_facilities` の主キー名は `exam_facility_id` とする。
- 健診機関コードは `exam_facility_code` とする。
- 健診機関表示名は `exam_facility_name` とする。
- `exam_facilities` の初期候補カラムは `exam_facility_id`, `exam_facility_code`, `exam_facility_name`, `exam_facility_name_kana`, `note`, `is_active`, `created_at`, `updated_at` とする。
- `file_receipts` には `exam_facility_id` と `exam_facility_name` を追加する方針とする。
- `file_receipts.exam_facility_name` は表示名スナップショットとして扱う。
- 既存の `facility_code` / `facility_name` は当面残す。
- cross schema FK は張らない。
- CSVフォーマット定義テーブル名は `csv_format_versions` とする。
- CSV列マッピングテーブル名は `csv_column_mappings` とする。
- CSV列識別はヘッダー名と列番号の両方を保持できる構成にする。
- 変換ルールは初期段階では文字列コードとして保持する。

### 保留事項

- `exam_facility_code` の採番元・一意性範囲。
- `csv_format_versions` / `csv_column_mappings` の正式DDL詳細。
- mapping version の命名規則と有効期間切替ルール。
- CSV取込時normalize処理のモジュール境界。
- `01_scan_files.py` が `phr_master` を読むためのDB接続設定名。
- 既存 `health_exam_result.medical_folder_aliases` から `phr_master.medical_folder_aliases` への移行SQL。
- `file_receipts` 既存行へのバックフィル方針。

### 根拠

- 既存 `file_receipts` の `facility_code` / `facility_name` は、親マスタの安定IDとして使うには由来が曖昧である。
- CSV取込は健診機関ごとのフォーマット・列マッピング選択が必要である。
- フォルダaliasは既に受領フォルダを識別する入口になっているため、親マスタIDへの接続点として使いやすい。
- cross schema FK を避けることで、既存のDB分離方針と運用の柔軟性を保てる。

### 次回検討

- `phr_master` 初期DDL案を作成する。
- `health_exam_result.file_receipts` のmigration案を作成する。
- `medical_folder_aliases` 移行SQL案を作成する。
- `01_scan_files.py` の設定・参照先変更範囲を整理する。

---

## DH-20260723-05 / 2026-07-23 JST

### テーマ

支払基金CSV確認と `phr_master` 初期DDL案

### 背景

`02_02_exam_result_csv_import` の設計前調査として、健診機関マスタの外部コード候補を確認するため、社会保険診療報酬支払基金の全国CSV `Pref_00.csv` を project 配下へ配置し、文字コード、カラム、内容を確認した。

あわせて、`phr_master` 初期DDL案として、健診機関、フォルダalias、CSVフォーマット、CSV列マッピング、CSV結果値変換ルールの最小構成を整理した。

### 確認結果

- `docs/spec/exam_result_csv_import/downloads/Pref_00.csv` は `/Users/hiro/Downloads/Pref_00.csv` と同一内容である。
- `Pref_00.csv` はCP932として読込可能であり、UTF-8としては読込不可である。
- ヘッダーは `機関コード`, `機関種別`, `機関名`, `郵便番号`, `電話番号`, `機関所在地`, `ホームページ`, `経営主体` の8列である。
- データ行は54,712行で、全データ行が8列である。
- `機関種別` は `特定健診`, `特定健診・指導`, `特定保健指導` が存在する。

### 決定事項

- `phr_master` 初期DDL案は `10_phr_master_initial_ddl_draft.md` に記録する。
- 初期DDL案の対象は `exam_facilities`, `medical_folder_aliases`, `csv_format_versions`, `csv_column_mappings`, `csv_value_transform_rules` とする。
- `exam_facilities` は、支払基金CSVなどの外部コードを保持できるように `medical_institution_code` を持つ案とする。
- `exam_facilities` は、予約システム由来の医療機関コードを保持できるように `reservation_system_medical_institution_code` を持つ案とする。
- `exam_facilities` は、支払基金CSV由来の住所・電話番号を取り込めるように `postal_code`, `address`, `phone_number` を持つ案とする。
- CSV結果値変換ルールのテーブル名案は `csv_value_transform_rules` とする。
- 現時点ではDDL適用、migration作成、seed作成、スクリプト変更は行わない。

### 保留事項

- `medical_institution_code` に支払基金CSVの `機関コード` をそのまま入れるか、支払基金専用カラムへ分けるか。
- `exam_facility_code` の採番元と一意性範囲。
- `reservation_system_medical_institution_code` の実データ由来と桁数。
- CSVフォーマット有効期間の選択基準。
- `csv_value_transform_rules` の正式なルール種別とパラメータ仕様。
- `csv_column_mappings` と `csv_value_transform_rules` にFKを張るかどうか。

### 根拠

- 支払基金CSVは健診機関の外部コード、名称、住所、電話番号を持つため、`exam_facilities` 初期整備の入力候補になる。
- CSV取込では、健診機関ごとのフォーマット・列マッピング・結果値変換ルールを選択する必要がある。
- DDL案をspecに先に置くことで、実装変更前にテーブル責務と未決事項を確認できる。

---

## DH-20260723-06 / 2026-07-23 JST

### テーマ

CSV取込処理台帳、`csv_row_ledger`、`exam_item_values` 接続

### 背景

`phr_master` 初期DDL案では、CSVフォーマットと列マッピングのマスタ側を整理した。
一方で、`02_02_exam_result_csv_import` を実装するには、CSVファイル単位・CSV行単位の処理状態と、`exam_item_values.ledger_type` / `ledger_id` の接続先を決める必要がある。

既存の `health_exam_result` 決定事項には、将来的にCSV直取込へ対応する場合は `csv_row_ledger` を追加し、基本情報Ledgerと健診結果値を分離する方針が記録されている。

### 議論

- `phr_master` はマスタDBであり、受領ファイル、CSV行、取込エラー、健診結果値などの業務トランザクションは保持しない。
- CSV取込の処理状態は `health_exam_result` 側で管理する。
- `exam_item_values` は由来を `ledger_type` / `ledger_id` で表す既存方針があるため、CSV由来でも同じ構造を使う。
- CSV由来の場合は `ledger_type = 'CSV'`, `ledger_id = csv_row_ledger.csv_row_ledger_id` とする案が自然である。
- CSVファイル単位の処理状態は `file_receipts`、実行履歴は既存の `etl_runs` を根にする。
- 再取込やdry-runを理由にCSV専用の `csv_import_batches` を分ける案も検討したが、既存のETL履歴思想を変えるため初期設計では採用しない。
- CSV列マッピングは、受診者識別・健診日などの基本情報と、健診結果値を区別する必要がある。
- 既存 `csv_loader` spec では、CSV読込、文字コード、delimiter、ヘッダー処理までが共通部品の責務であり、mapping適用、rule実行、normalize、identity生成、加入者照合は責務外とされている。

### 決定事項

- `02_02_exam_result_csv_import` の処理側設計案は `11_csv_import_processing_design_draft.md` に記録する。
- CSV直取込では、CSVデータ行単位の台帳として `health_exam_result.csv_row_ledger` を追加する案を基本とする。
- CSV由来の `exam_item_values` は `ledger_type = 'CSV'`, `ledger_id = csv_row_ledger.csv_row_ledger_id` で由来を表す案を基本とする。
- `csv_loader` はCSV読込の共通部品として使い、mapping適用、rule実行、normalize、identity生成、加入者照合は `02_02_exam_result_csv_import` 側の責務とする。

### 保留事項

- 将来、業務画面上で1ファイルに対する複数試行履歴を詳細表示する必要が出た場合、`etl_runs` を親にした補助テーブルを追加するか。
- `csv_column_mappings` に `target_kind` を追加するか、基本情報マッピングと結果値マッピングを別テーブルに分けるか。
- `csv_row_ledger.row_sha256` の算出対象。
- 同一CSV再取込時に既存 `csv_row_ledger` 行を再利用するか、同一 `file_receipts` 配下で再処理し `etl_runs` / `etl_errors` に実行証跡を残すか。
- CSV取込後の `file_receipts.status` 集約ルール。

### 根拠

- 既存設計で `exam_item_values` はCSV由来も同一構造で扱う方針がある。
- CSV行には受診者識別、健診日、加入者照合状態など、結果値そのものではない基本情報が含まれる。
- `csv_loader` の責務をI/Oに限定することで、健診結果CSV固有のmappingやnormalizeを業務処理側へ閉じ込められる。

---

## DH-20260723-07 / 2026-07-23 JST

### テーマ

`exam_facilities` の支払基金CSV由来属性追加

### 背景

`exam_facilities` の初期カラム案について、支払基金CSVのうち未保持だった `機関種別`, `ホームページ`, `経営主体` を保持するか確認した。

### 決定事項

- `exam_facilities` に `exam_facility_type` を追加する案とする。
- `exam_facilities` に `website_url` を追加する案とする。
- `exam_facilities` に `management_entity` を追加する案とする。
- 支払基金CSVの `機関種別`, `ホームページ`, `経営主体` は、それぞれ `exam_facility_type`, `website_url`, `management_entity` へ保持する案を基本とする。
- 支払基金CSVの `機関コード` は、支払基金専用カラムではなく汎用的な `medical_institution_code` へ保持する案を基本とする。
- データソースが支払基金CSVであることを表す専用カラムは、初期DDL案には含めない。

### 保留事項

- `exam_facility_type` の正式コード体系。
- `management_entity` を将来コード化するか、当面raw文字列として扱うか。

### 根拠

- `機関種別` は `特定健診`, `特定健診・指導`, `特定保健指導` の区分を持ち、健診機関確認や将来の絞り込みに使える。
- `ホームページ` と `経営主体` はCSV取込処理の必須条件ではないが、健診機関マスタとして確認用属性にできる。
- `medical_institution_code` は支払基金に縛られない汎用外部コードとして扱う方が、将来の別ソース追加と衝突しにくい。

---

## DH-20260723-08 / 2026-07-23 JST

### テーマ

フォルダaliasから健診機関を解決する共通lookup lib

### 背景

`medical_folder_aliases` を `phr_master` 側へ移設し、`exam_facility_id` を持たせる方針を整理した。
次に、`01_scan_files.py` がフォルダ名から健診機関IDを確定する処理を、スクリプト内SQLではなく共通libとして切り出すかを確認した。

### 決定事項

- フォルダaliasから健診機関を解決する処理は、共通lookup libとして追加する案を基本とする。
- 配置案は `scripts/lib/db/lookup/exam_facility.py` とする。
- lookupの入力は、マスタDB名、イベントID、フォルダ名を基本とする。
- lookupの返却値は、少なくとも `exam_facility_id`, `exam_facility_code`, `exam_facility_name` を含む案とする。
- 同じlookup libに、`exam_facility_id` から健診機関handleを返す関数も追加する案とする。
- 同じlookup libに、`exam_facility_code` から健診機関handleを返す関数も追加する案とする。
- 各lookup入口は、返却キーとして `exam_facility_id`, `exam_facility_code`, `exam_facility_name`, `exam_facility_display_name`, `medical_institution_code` を揃える案とする。
- lookupは `phr_master.medical_folder_aliases` と `phr_master.exam_facilities` をJOINし、有効なaliasと有効な健診機関だけを返す。
- lookup libはSELECT専用とし、DB接続、INSERT/UPDATE、file_receipts状態更新、CSV処理、normalizeは担当しない。
- `01_scan_files.py` はこのlookupを使って、`file_receipts` へ健診機関情報を引き渡す案とする。

### 保留事項

- `file_receipts` に `exam_facility_code` もスナップショットとして追加するか。
- alias未登録、空フォルダ名、無効施設の場合の `file_receipts.status`。
- `exam_facility_display_name` がある場合、lookup返却の `exam_facility_name` を正式名にするか表示名にするか。

### 根拠

- フォルダalias解決SQLを共通化すると、`01_scan_files.py`、CSV取込、バックフィル、検査SQL補助で同じ解決ルールを使える。
- lookup層は既存方針として `scripts/lib/db/lookup/` に配置され、参照処理だけを持つことになっている。
- フォルダ名を渡して健診機関のID・コード・名称を返すAPIにすると、呼び出し元はDB構造を意識せずに後続処理へ渡せる。

---

## DH-20260723-09 / 2026-07-23 JST

### テーマ

健診結果値normalize共通libと `exam_item_master` lookup

### 背景

CSV取込では `exam_item_values` 登録時にnormalizeまで行う方針を決めている。
そのnormalize処理をCSV取込専用に閉じず、XML由来値の後続normalizeや再normalizeでも利用できる共通libとして整理する必要がある。

あわせて、`namecode` を渡したら型や単位を返すlookupが必要という観点で、既存libを確認した。

### 確認結果

- 既存に `scripts/lib/db/lookup/exam_item_master.py` が存在する。
- 既存APIとして `get_exam_item(cur, namecode)` と `get_exam_items(cur, namecodes)` がある。
- 既存lookupは `data_type`, `unit`, `display_unit`, `ucum_unit`, `identity_item_code`, `jun_no` などを返す。
- `dev_phr.norm_variants` は存在するが、現時点では `scripts/lib/db/lookup/` 配下の共通lookupは見当たらない。
- 旧スクリプトでは `norm_variants` を `result_code_oid + raw_value_utf8` の完全一致で利用している。
- 旧「紙→Excel→DB 2テーブル直接投入→normalize→export」フローでは、`normalize_item_values.py` が `norm_variants` を参照し、`medi_export_xml.py` がnormalize済み結果からXMLを作成する。
- 既存の `health_exam_result` 決定事項には、共通libとして `scripts/lib/examination/value_normalizer.py` を作成し、公開APIを `normalize_exam_item_value()` とする方針が記録されている。

### 決定事項

- 健診結果値normalizeはCSV取込専用実装にせず、共通libとして作成する案を基本とする。
- 配置案は `scripts/lib/examination/value_normalizer.py` とする。
- 主API案は `normalize_exam_item_value()` とする。
- `normalize_exam_item_value()` は、`namecode`, raw値、raw単位、raw値型、項目メタデータ、変換ルールを入力し、normalize結果dictを返す。
- normalize結果dictは、`exam_item_values` の `normalized_value`, `normalized_unit`, `normalize_status`, `normalize_reason`, `validation_status`, `validation_reason` へ写しやすい形にする。
- `namecode` から型・単位を返すlookupは、新規作成ではなく既存 `scripts/lib/db/lookup/exam_item_master.py` を利用する案を基本とする。
- CD/CO系の結果値名寄せは `dev_phr.norm_variants` を利用する案を基本とする。
- `norm_variants` 参照は `scripts/lib/db/lookup/norm_variant.py` として共通lookup化する案を基本とする。
- 初期案では、CD/CO系かつ `exam_item_master.result_code_oid` が存在する場合のみ、`result_code_oid + raw_value_utf8` の完全一致で `norm_variants` を引く。
- `norm_variants` は `phr_master` への移設候補とする。
- `norm_variants` を `phr_master` へ移設した場合、運用が安定した後に `dev_phr.norm_variants` の廃止を検討する。
- CSV由来raw値に含まれうる `未実施`, `測定不能`, `未受診` などの値ノイズは、CD/CO系 `norm_variants` とは別に、型に依存しない共通前処理として `value_normalizer` 側で扱う案を基本とする。
- raw値ノイズに一致した場合は、元値を `exam_item_values.raw_value` に残し、normalize結果側では `RAW_VALUE_NOISE` として扱う案とする。
- `なし` をノイズ扱いにすると、対になる `あり` も存在し、既往歴・自覚症状・所見などでは意味を持つ結果値になりうる。
- そのため `あり` / `なし` は初期の共通ノイズ辞書には含めず、項目別ルール、CD/CO辞書、または変換ルールで扱う案とする。
- CSV取込では、対象 `namecode` をまとめて `get_exam_items()` で取得し、行・項目ごとのDB問い合わせを避ける案とする。

### 保留事項

- `normalize_exam_item_value()` の正式引数と戻り値型。
- `norm_variant` lookupの正式API。
- raw値空欄時に `exam_item_values` 行を作るかどうか。
- 数値系、文字列系、コード系の正式な型判定。
- 単位不一致時に単位変換まで行うか、初期はwarning止まりにするか。
- CD/CO系で `norm_variants` をCSV取込初期から使うか。
- `norm_variants` を `phr_master` 初期DDLに含める時期。
- `dev_phr.norm_variants` の廃止タイミング。
- raw値ノイズ辞書をコード定数で始めるか、`phr_master` に辞書テーブルとして持つか。
- raw値ノイズを `nullflavor` へ写像するかどうか。
- `あり` / `なし` の項目別扱い。

### 根拠

- normalizeはCSV由来・XML由来で本質的に共通の処理であり、入力経路ごとに実装すると判定差分が出やすい。
- `exam_item_master` lookupは既に存在し、型・単位取得の土台として使える。
- CD/CO系の名寄せは既存 `norm_variants` に役割があり、共通lookup化すればCSV取込とXML後続normalizeで同じ辞書参照を使える。
- CSVでは数値・文字列・コードいずれの列にも `未実施` などの値ノイズが入りうるため、型別処理の前段で共通判定する方がスクリプトごとの差分を避けやすい。
- CSV取込でnormalize結果まで登録する方針と、後続再normalizeの導線を両立できる。

---

## DH-20260723-10 / 2026-07-23 JST

### テーマ

CSVヘッダー読取共通lib確認

### 背景

CSV取込では、CSVヘッダーを読み取り、`csv_column_mappings` のヘッダー名または列番号と照合する必要がある。
既にCSV読取共通libがあるはず、という観点で現物を確認した。

### 確認結果

- 既存に `scripts/lib/csv/csv_loader.py` が存在する。
- `CSVLoader` と `load_csv()` が実装済みである。
- 現実装は、UTF-8 BOM / UTF-8 / CP932 の簡易エンコーディング判定、BOM除去、delimiter指定、複数行ヘッダー読込、最終ヘッダー行による `header -> index` 辞書、行イテレータ、行数カウントに対応している。
- ヘッダー取得は `get_headers()`、ヘッダー辞書取得は `get_header_dict()` で行える。
- `docs/spec/common_lib/csv_loader.md` は `CsvLoadResult` / `CsvHeaderSet`、`disp_mode`、delimiter自動判定などを想定しているが、現実装とは一部差分がある。

### 決定事項

- CSVヘッダー読取は既存 `scripts/lib/csv/csv_loader.py` を利用する案を基本とする。
- `csv_loader` はCSV読込、文字コード、BOM、delimiter、ヘッダー、行イテレータまでを担当する。
- mapping適用、rule実行、normalize、identity生成、加入者照合は `csv_loader` の責務外とする。
- 既存 `csv_loader` 利用スクリプトに影響を与えない形で、`CsvLoadResult` / `CsvHeaderSet` 形式を追加APIとして拡張する案を基本とする。
- 既存 `load_csv()` の戻り値は `CSVLoader` のまま維持する。
- 構造化されたCSV読込結果が必要な処理向けに、`load_csv_result()` などの追加APIを作る案を基本とする。
- CSV取込本体スクリプトを太らせないため、encoding、delimiter、BOM、ヘッダー、行数などのI/Oメタ情報は共通lib側で扱う。

### 保留事項

- `csv_loader` 追加APIの正式名。
- delimiter自動判定をCSV取込前に実装するか。
- header validationを `csv_loader` 側へ追加するか、CSV取込側で行うか。

### 根拠

- ヘッダー読取、CP932対応、BOM除去はCSV取込で共通化したいI/O処理であり、既存libの責務と一致する。
- 既存libを使うことで、CSV取込本体はマッピング・normalize・行台帳登録に集中できる。

---

## DH-20260723-11 / 2026-07-23 JST

### テーマ

加入者CSV取込に合わせたCSVマッピング方針

### 背景

健診結果CSVマッピングで列番号を0始まりにするか1始まりにするか、ヘッダー名と列番号のどちらを正にするかを検討するため、加入者CSV取込の実装とDDLを確認した。

### 確認結果

- 健保加入者CSV取込では `dev_phr.templates` / `dev_phr.template_mappings` を使っている。
- `template_mappings` は `csv_header`, `target_column`, `rule`, `required`, `col_order` を持つ。
- `col_order` は1始まりの定義順としてseedされている。
- 処理実装は `loader.iter_dict_rows()` と `source_row.get(m.csv_header)` を使い、値取得は列番号ではなくヘッダー名を正としている。
- 取込前に `validate_mapping_headers()` で `template_mappings.csv_header` がCSVヘッダーに存在するか確認している。
- 同一CSVヘッダーから複数targetを生成する実データがある。

### 決定事項

- 健診結果CSVマッピングも、加入者CSV取込に合わせてヘッダー名を主たる照合キーとする案を基本とする。
- 列順は `csv_column_order` として保持し、1始まりの定義順・検査補助として扱う案を基本とする。
- `csv_column_order` はPythonの配列indexではなく、人間が見るCSV列順に合わせる。
- 実処理では `csv_loader` の `header -> index` 辞書やdict行を使い、`csv_header_name` で値を取得する案を基本とする。
- 取込前に、required mapping の `csv_header_name` がCSVヘッダーに存在するか検証する案を基本とする。
- 健診結果CSVは任意項目の増減、施設別・健保別テンプレート差分、健診基幹システムの出力設定差分により列位置が変わりやすいため、`csv_column_order` を処理上の値取得キーとして使わない案を基本とする。
- CSVヘッダー構造の解釈方式は、健診機関単位ではなく `csv_format_versions.header_structure_type` としてformat version単位に保持する案を基本とする。
- CSVヘッダーcontextの作り方も、`csv_format_versions.header_context_rule` としてformat version単位に保持する案を基本とする。
- 初期の `header_structure_type` は `SIMPLE_HEADER` と `GROUPED_VALUE_METHOD` を候補とする。
- 初期の `header_context_rule` は `NONE`, `UPPER_HEADER`, `CARRY_FORWARD_ITEM` を候補とする。
- `SIMPLE_HEADER` は単純な1行ヘッダーを対象とし、`csv_header_name` を主キー的に使う。
- `GROUPED_VALUE_METHOD` は検査項目名や上段ヘッダーの配下に `値`, `方式` などが繰り返される形式を対象とする。
- `UPPER_HEADER` は2行ヘッダーなどで上段ヘッダーをcontext、下段ヘッダーをnameとして扱う。
- `CARRY_FORWARD_ITEM` は `血圧, 値, 方式, 血糖, 値, 方式` のように、直前の検査項目名をcontextとして持ち回る。
- 同一ヘッダー名が繰り返されるCSVに備え、`csv_column_mappings` は `csv_header_context`, `csv_header_name`, `csv_header_occurrence` を持つ案を基本とする。

### 保留事項

- `csv_column_order` を必須にするか任意にするか。
- ヘッダーなしCSVを対象にする場合の例外ルール。
- 同一ヘッダーから複数 `EXAM_ITEM_VALUE` を生成するケースを許容するか。
- 同一ヘッダー名が複数列に出るCSVを対象にするか。
- ヘッダー名の表記ゆれを `csv_column_mappings` 複数行で吸収するか、別aliasテーブルへ分けるか。
- `header_structure_type` の正式コード体系。
- `header_context_rule` の正式コード体系。
- `CARRY_FORWARD_ITEM` で検査項目名と判断する条件。

### 根拠

- 加入者CSV取込ではヘッダー名ベースのmappingが既に運用されている。
- ヘッダー名ベースにすると、CSV列順が変わってもヘッダーが同じなら処理できる。
- `csv_column_order` を1始まりの定義順にすると、ExcelやCSV実物との突合がしやすい。
- 健診結果CSVは横持ち項目が増減しやすく、列番号を処理キーにすると値ズレを検知しにくい。

---

## DH-20260723-12 / 2026-07-23 JST

### テーマ

健診結果CSVマッピングをnamecode中心のルールモデルに寄せる案

### 背景

健診結果CSVでは、単純な `CSVヘッダー名 -> namecode` だけでは表現しきれないケースがある。
同一ヘッダー名として `値` / `方式` が繰り返されるCSV、2行ヘッダー、1行ヘッダー内で検査項目名を持ち回るCSV、方式列の値によって採用可否が変わるCSVが想定される。

また、運用画面としては「CSV列から登録先を選ぶ」よりも、「対象の `namecode` に対して、このCSVではどのヘッダー条件で値を拾うか」を追加していく形の方が自然である。

### 決定事項

- 健診結果値のマッピングは、単純な「CSV列 -> namecode」ではなく、`namecode` を親にしたルールモデルを優先候補とする。
- `csv_exam_item_mapping_rules` はCSVフォーマット内の `target_namecode` 単位の親ルールとする案を検討する。
- `csv_exam_item_mapping_conditions` は、親ルールに対するCSV上の値取得条件、方式条件、補助条件を表す子ルールとする案を検討する。
- 同一rule内の `condition_group_no` はOR条件の単位、同一group内の複数conditionはAND条件として評価する案を基本とする。
- 単純ヘッダー、2行ヘッダー、`値` / `方式` 分解、表記ゆれ、施設テンプレート差分は、親rule配下のcondition追加で吸収する方向とする。
- 複数候補が成立した場合は `priority` で優先し、それでも一意に決まらない場合はwarning/errorとして扱う案を基本とする。

### 保留事項

- `csv_column_mappings` を最終DDLとして残すか、`csv_exam_item_mapping_rules` / `csv_exam_item_mapping_conditions` に置き換えるか。
- `condition_type` の正式コード体系。
- `source_role` の正式コード体系。
- `operator` の正式コード体系。
- 管理画面上で、`namecode` とCSVヘッダー条件をどの粒度で入力させるか。
- `方式` 列が複数ある場合や、方式なしでも値を採用する場合の既定ルール。

### 根拠

- 健診結果CSVは施設やテンプレートごとの差分が大きく、列中心のmappingだけでは例外が増えやすい。
- `namecode` 中心にすると、最終的に登録したい健診項目を起点に値取得条件を確認できる。
- 方式列や補助列をconditionとして扱うと、実CSVで見られる `値` / `方式` 分解に対応しやすい。

---

## DH-20260723-13 / 2026-07-23 JST

### テーマ

CSVテンプレート登録画面の上位設定と同一性項目起点の候補namecode選択

### 背景

健診結果CSVマッピングは、個別の `namecode` 条件だけでなく、CSVファイル自体のベース設定が先に必要である。
ヘッダーなし、単一行ヘッダー、contextありヘッダー、データ開始行の違いにより、値列や条件列の解釈が変わる。

また、管理画面ではいきなり `namecode` を手入力するよりも、同一性項目を選択し、その同一性項目に紐づく候補 `namecode` 一覧から使用する項目をチェックする流れが自然である。

### 決定事項

- CSVベース設定は、健診機関、mapping version、ヘッダー設定、context生成方式、データ開始行を上位に持つ案を基本とする。
- `csv_format_versions` に `header_mode` と `data_start_row_no` を追加する案を基本とする。
- 初期の `header_mode` は `NONE`, `SINGLE`, `WITH_CONTEXT` を候補とする。
- `data_start_row_no` は1始まりのCSV行番号とする。
- テンプレート登録画面は、同一性項目を選択し、候補 `namecode` 一覧からCSVで値を受け取る項目にチェックを付ける流れを候補とする。
- チェックされた `namecode` ごとに、値列、検査方法列、検査方法条件、優先順位を設定する案とする。
- テンプレート登録は、将来的にFastAPIベースの管理APIとして実装する案を検討する。
- 現時点ではFastAPI実装は行わず、仕様整理に留める。

### 保留事項

- 同一性項目と候補 `namecode` の取得元。
- 候補 `namecode` 一覧の表示順。
- チェックを外した候補 `namecode` の未使用理由を保持するか。
- FastAPI管理APIを今回のスコープに含めるか、別タスクに切るか。
- 管理APIの認証・権限設計。

### 根拠

- CSVベース設定を上位に置くと、ヘッダー解釈とデータ行解釈がmapping ruleから分離できる。
- 同一性項目起点にすると、方式違いなどで複数 `namecode` に分岐する項目を画面上で確認しやすい。
- 候補 `namecode` をチェック式にすると、「このCSVでは受け取る値があるか」を運用者が明示できる。

---

## DH-20260723-14 / 2026-07-23 JST

### テーマ

初回検証候補施設メモ

### 内容

- CSV健診結果取込の初回検証候補施設として、ヒロオカクリニックを記録する。
- 理由は、担当から「CSVしか来ていない施設」と聞いたため。
- 現時点では仕様決定ではなく、実データ確認時に忘れないための運用メモとして扱う。

---

## DH-20260723-15 / 2026-07-23 JST

### テーマ

テンプレート登録画面モックとFastAPI化のスコープ整理

### 背景

CSVテンプレート登録は、同一性項目、候補 `namecode`、CSV由来項目列、条件列を扱うため、手SQL seedだけでは長期運用が難しくなる可能性が高い。
一方で、現時点でFastAPI管理APIや画面実装まで進めると、まだ揺れているマッピング構造を早く固定しすぎる懸念がある。

### 決定事項

- `20_mapping_rule_screen_mock.html` は画面実装ではなく、テンプレート登録構造を把握するためのサンプルモックとして扱う。
- 今回スコープでは、CSV取込を成立させるための初期テンプレート登録はseed前提とする。
- 一通りのCSV取込設計・初期seed・ヒロオカクリニック試験方針が固まった後、次タスクでFastAPIベースのテンプレート登録管理API化を検討する。
- FastAPI化では、上位に健診機関とmapping versionを持ち、その配下に同一性項目別のmapping ruleを登録する方向を候補とする。

### 根拠

- 取込処理を先に成立させるには、最初はseedでテンプレートを固定する方が早い。
- seedで一度通すことで、FastAPI化すべき入力項目とvalidationが具体化する。
- 画面モックにより、マッピング構造はかなり明確になったが、実装UIとしてはまだ作らない方がスコープを保ちやすい。

---

## DH-20260723-16 / 2026-07-23 JST

### テーマ

CSVテンプレート登録内の基本情報マッピングと検査結果値マッピングの分離

### 背景

CSVテンプレート登録という入口は1つだが、CSVから取り込む列には、受診者識別・健診日などの基本情報と、`namecode` に紐づく検査結果値が混在する。
基本情報は `csv_row_ledger` 作成と加入者照合に使い、検査結果値は `exam_item_values` 登録とnormalizeに使うため、内部では分けて扱う方が自然である。

### 決定事項

- CSVテンプレート登録という入口は1つにする。
- 内部では、基本情報マッピングと検査結果値マッピングを分ける案を基本とする。
- 基本情報マッピングは `csv_row_ledger` の基本情報カラムへ反映する。
- 検査結果値マッピングは `exam_item_values` の `namecode` ベース登録へ反映する。
- 暫定案として `csv_column_mappings.target_kind` による `LEDGER_FIELD` / `EXAM_ITEM_VALUE` 区別を残す。
- 最終案では、基本情報を `csv_ledger_field_mappings`、検査結果値を `csv_exam_item_mapping_rules` / `csv_exam_item_mapping_conditions` に分ける方向を優先候補とする。

### 根拠

- 基本情報は加入者照合前に必要であり、検査結果値登録より前段の処理で使う。
- 検査結果値は `namecode`、CSV由来項目列、検査方法条件、normalizeと密接に結びつく。
- 登録入口は1つでも、内部モデルを分けることで処理順と責務が明確になる。

---

## DH-20260723-17 / 2026-07-23 JST

### テーマ

CSV取込時の抽出・upsert処理単位

### 背景

CSVテンプレート内では基本情報マッピングと検査結果値マッピングを分ける方針とした。
処理実装では、基本情報と検査結果値を1行ごとにまとめて抽出してupsertする案と、基本情報だけを先に一括処理し、検査結果値だけを後から一括処理する案がある。

### 比較

- Option A: 行単位処理
  - CSV 1行ごとに基本情報と検査結果値を抽出する。
  - `csv_row_ledger` upsert、加入者照合、`exam_item_values` upsert、normalizeを同じ行処理内で行う。
  - 行単位status/errorを扱いやすい。
- Option B: フェーズ分離バッチ処理
  - 基本情報をCSV全体で一括抽出・upsertする。
  - 加入者照合後、検査結果値をCSV全体で一括抽出・upsertする。
  - 大量CSVでは性能面で有利になりやすいが、中間状態とエラー紐づけが複雑になる。

### 決定事項

- 初期案では Option A の行単位処理を優先候補とする。
- 理由は、1行単位で基本情報、加入者照合、検査結果値、normalize、行statusを閉じられるため。
- ただし、マッピング適用処理は基本情報抽出と検査結果値抽出の関数境界を分け、将来Option Bのバッチ処理へ転用できる形にする。

### 根拠

- 初回のヒロオカクリニック試験では、性能最適化よりも行単位の追跡性とエラー確認を優先したい。
- 行単位処理でも、内部の抽出関数を分けておけば、後からバルクupsertやフェーズ分離へ進める。

---

## DH-20260723-18 / 2026-07-23 JST

### テーマ

基本情報と検査結果値の抽出マッピング形式の共通化

### 背景

前段では、CSVテンプレート登録内で基本情報マッピングと検査結果値マッピングを分ける方針を整理した。
ただし、1人ずつ処理する前提では、CSVから値を抽出するルール形式そのものは基本情報と検査結果値で共通化できる。
基本情報は条件なしで値を取り、登録先が `csv_row_ledger` のfieldになるだけである。

### 決定事項

- CSVテンプレート登録という入口は1つにする。
- 基本情報と検査結果値は登録先としては分ける。
- ただし、CSVから値を抽出するマッピング形式は共通化する案を基本とする。
- 基本情報は `target_kind = LEDGER_FIELD`, `source_role = VALUE`, 条件なしのruleとして表現する。
- 検査結果値は `target_kind = EXAM_ITEM_VALUE`, `target_namecode` または `target_identity_item_code`, `source_role`, 必要に応じたconditionを持つruleとして表現する。
- `csv_ledger_field_mappings` のような基本情報専用テーブルは代替案として残すが、主案では共通rule形式を優先する。

### 根拠

- 抽出形式を共通化すると、seed、validation、CSV行からの値取得処理を再利用できる。
- 基本情報は条件なしの単純ruleとして扱えるため、別形式にする必要性が低い。
- 登録先と後続処理は分けつつ、抽出処理は共通化する方が初期実装を小さくできる。

---

## DH-20260723-19 / 2026-07-23 JST

### テーマ

CSV取込初期実装に向けた未決事項の整理

### 決定事項

- 共通rule形式の正式候補テーブル名は `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` とする。
- rule/conditionの正式カラムは未確定だが、`20_mapping_rule_screen_mock.html` を再現できる情報を最小候補とする。
- 条件数は固定せず、子conditionを複数行持てる構成を基本とする。
- 初期処理はリアルタイム性を求めず、1人/1行ずつ処理する方針とする。
- CSV由来の `VALUE` が空の場合は `exam_item_values` 行を作らない。
- `LOWER_LIMIT` / `UPPER_LIMIT` / `JUDGEMENT` だけが存在し、`VALUE` が空の場合も `exam_item_values` 行を作らない。
- 健診日など基本情報が不足していてもCSV取込段階ではskipしない。
- 基本情報不足の評価は、将来 `check_result` 側で追加するスコープとして扱う。
- ヒロオカクリニックの実CSV確認は、機微情報に関わるため現時点では保留する。

### 確認結果

- `work_other.medi_exam_result_ledger` は旧紙/Excel系の1人=1件の基本情報台帳である。
- `health_exam_result.file_receipts` はファイル単位台帳であり、人/行単位の基本情報台帳としては共用しない。
- `health_exam_result.exam_item_values` には `interpretation_code` / `interpretation_name` は存在する。
- `health_exam_result.exam_item_values` にはCSV由来の下限/上限専用カラムは現状ない。

### 保留事項

- `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` の正式カラム構成。
- CSV由来の下限/上限/判定の保存先。
- `exam_item_values` へのCSV由来下限/上限カラム追加migration要否。
- `csv_row_ledger` の正式カラム構成。旧 `work_other.medi_exam_result_ledger` を参照しつつ、v2の加入者照合に必要なカラムへ整理する。

---

## DH-20260723-20 / 2026-07-23 JST

### テーマ

CSV由来の基準下限・上限の保存先

### 背景

CSVテンプレート登録では、健診機関から提出されたCSV原本に `下限` / `上限` 列が含まれる場合がある。
これはマスタとしての基準範囲ではなく、健診機関側が提出した原本由来情報である。
既存 `health_exam_result.exam_item_values` には `interpretation_code` / `interpretation_name` はあるが、CSV由来の下限/上限専用カラムは存在しない。

### 決定事項

- CSV由来の下限/上限は、マスタ基準値ではなく原本由来情報として `exam_item_values` に保持する。
- `exam_item_values` に `source_reference_lower` / `source_reference_upper` を追加するmigration候補を作成する。
- CSV由来の下限/上限の単位は、結果値の `raw_unit` と同じ前提で扱う。
- 下限/上限だけ別単位で提出されるケースはかなり特殊であり、初期設計では専用単位カラムを持たない。
- CSV由来の判定は、XML由来の `interpretationCode` と同じく `exam_item_values.interpretation_code` / `interpretation_code_system` / `interpretation_name` に寄せる。

### 後続補足

上記のCSV由来判定をXML由来 `interpretationCode` と同じカラムへ寄せる決定は、後続協議で初期実装から外した。
現在の採用方針は `03_decisions.md` を正とし、CSV由来の検査別判定・カテゴリ総合判定は原本証跡として扱い、初期実装ではPHR側判定ロジックや納品判定には利用しない。

---

## DH-20260723-21 / 2026-07-23 JST

### テーマ

残協議事項の整理と重複制御・skip方針

### 決定事項

- `csv_row_ledger` は `xml_ledger` と対になるCSV行台帳として扱い、名称は `csv_row_ledger` を維持する案を基本とする。
- CSVファイル全体の重複抑制には `file_receipts.file_sha256` を使う。
- CSV行単位の重複抑制には `csv_row_ledger.row_sha256` を使う。
- `row_sha256` は列順込みのセル配列から算出し、ヘッダー名sort済みkey-value hashにはしない。
- check済みでOK扱いの同一 `row_sha256` は再取込時にskip候補とする。
- 完全空行はCSV取込段階でskipする。
- その他の基本情報不足行、フッター行、メモ行は原則skipせず、行台帳と後続checkで扱う。
- CSV由来判定は証跡として原本値を保持し、必要に応じてnormalize側で正規化する。
- ヒロオカクリニック実CSV確認は機微情報に関わるため保留し、現時点では構造・seed設計の話として扱う。
- `file_receipts` には既存 `facility_code` / `facility_name` があるため、scan時lookupの健診機関コード・名称スナップショットには既存カラムを活用する案とする。
- `file_receipts` に別途 `exam_facility_code` / `exam_facility_name` は追加しない方向とする。

### 追加資料

- `21_mapping_rule_structure_examples.md`
  - `20_mapping_rule_screen_mock.html` を `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` の候補データとして見える化した。

### 保留事項

- `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` の正式カラム構成。
- `csv_row_ledger` の正式カラム構成。
- ヒロオカクリニック実CSVを元にした初期seed内容。

### 追加ファイル

- `sql/migrations/health_exam_result/20260723_001_health_exam_result_add_csv_reference_bounds_to_exam_item_values.sql`

---

## DH-20260724-01 / 2026-07-24 JST

### テーマ

CSVテンプレート登録支援用の上位グループ初期セット

### 背景

`identity_item_code` は候補探索には使えるが、それだけでは排他候補か複数entry候補か判断できない。
一方で、全検査項目について医療分類を手作業で設計する運用は重い。
CSVテンプレート登録では、血糖・脂質・血圧・視力・聴力など、実CSVで迷いやすい項目の候補探索支援が必要である。
ただし、それだけでは付属2掲載項目の候補表示が漏れる可能性があるため、少なくとも付属2由来の `identity_item_code` は網羅する必要がある。

### 決定事項

- CSVテンプレート登録支援として、上位グループ初期セットを用意する案を採用する。
- `exam_item_master.identity_item_code` ごとの `ANNEX2_IDENTITY` グループを最小網羅単位とし、付属2項目の候補表示漏れを防ぐ。
- 現時点の `sql/export_sql/exam_item_master.sql` では、distinct `identity_item_code` が197件、対象 `namecode` が322件である。
- 血糖・脂質・血圧などは、複数の `ANNEX2_IDENTITY` を束ねる入力支援bundleとして扱う。
- 上位グループは全検査の医学分類を手作業で設計するものではなく、候補 `namecode` を探しやすくするための支援レイヤーとする。
- 労安法チェック用 `exam_item_group_*` とは共用しないが、初期seed候補の材料として参照する。
- 入力支援bundleの初期セットは、身体計測、血圧、視力、聴力、尿一般、血算、肝機能、血糖、脂質、腎機能、胸部X線、心電図、既往歴・症状、医師判定・意見を候補とする。
- 上位グループ配下でも、保存先は必ず `target_namecode` 単位で明示する。
- 初期グループの多くは、候補探索用途として `MULTI_ENTRY` を既定にする。
- `EXCLUSIVE_ONE` は、同じCSV値を検査方法条件などでどれか1つの `namecode` に寄せる場合だけ使う。

### 追加資料

- `22_exam_item_concept_group_initial_set_draft.md`
  - CSVテンプレート登録で候補 `namecode` を探しやすくするための上位グループ初期案。

---

## DH-20260724-02 / 2026-07-24 JST

### テーマ

CSV取込の行単位rule評価とcommit粒度

### 背景

CSVテンプレートに補助列や方式条件を持たせる場合、同じCSV列でも行ごとの補助列値によって保存先 `target_namecode` が変わる。
たとえば中性脂肪の値列が同じでも、1行目は空腹時TG方法1、2行目は随時TG方法3として `exam_item_values` を作る可能性がある。

### 決定事項

- CSVテンプレートは候補ruleを定義し、実際に作成する `exam_item_values.namecode` はCSVデータ行単位で決定する。
- 初期実装は、1行処理の中で抽出対象ruleを取得し、各項目ruleに従って値を1個ずつ作成する。
- 1行分の `csv_row_ledger` と `exam_item_values` のinsert/updateを組み立て、1行単位でcommitする。
- `VALUE` が空のruleは `exam_item_values` を作らない。
- 途中で失敗した場合は、その行の変更をrollbackし、行単位errorとして記録する。
- 将来バッチ化する場合も、rule評価の意味上の単位はCSVデータ行とする。

---

## DH-20260724-03 / 2026-07-24 JST

### テーマ

CSVヘッダー指紋によるテンプレート同一性確認

### 背景

健診基幹システム側のテンプレート変更により増減した項目は、何もしないとCSVから取り込まれず静かに欠落する可能性がある。
列位置が変わりやすい健診結果CSVでは列番号だけに依存しない方針だが、ヘッダー構造そのものの同一性確認は必要である。
一方で、テンプレート登録時に確認済みのヘッダーに含まれる未マッピング列は、不要と判断した意図的な非取込列として扱う。

### 決定事項

- `csv_format_versions` に `header_sha256` / `header_snapshot_json` / `header_hash_status` を持たせる案を採用する。
- `header_sha256` は、context/occurrence解決後の正規化済みヘッダー構造を列順込みでhash化する。
- `header_snapshot_json` には、ヘッダー行、context、列番号、occurrenceなど、hash元を確認できる情報を保持する。
- 実取込時は、実CSVから算出した `header_sha256` とformat versionの `header_sha256` を照合する。
- ヘッダー不一致の場合、初期実装では通常取込を停止し、dry-runで差分確認できるようにする。
- ヘッダー一致の場合、登録済みヘッダー内の未マッピング列は意図的な非取込列として扱い、coverage不足エラーにはしない。

---

## DH-20260724-04 / 2026-07-24 JST

### テーマ

concept物理保存、bundle精査、ヘッダー不一致時制御

### 決定事項

- `ANNEX2_IDENTITY` 197件は `phr_master` に物理seedとして保存する。
- `exam_item_concept_groups` / `exam_item_concept_group_members` は正式テーブル化する。
- 入力支援bundleも初期セットとして決める方針とする。
- `BLOOD_GLUCOSE` は、血糖とHbA1cを同一bundleにするか、別bundleにするかを具体例込みで精査して決める。
- `LIPID` は、TG/HDL/LDL/non-HDL/総コレステロールの範囲と、直接法/計算法などの方式条件との分担を精査して決める。
- `RENAL_FUNCTION` は、何を意味するbundleなのか、対象identityを確認してから決める。
- ヘッダー不一致時は原則停止する。
- 列番号指定ruleが含まれる場合は、列ズレリスクが高いため停止を絶対寄りに扱う。
- `SIMPLE_HEADER` かつ全ruleがヘッダー名指定の場合は、確認後Goの例外制御を検討する。
- 例外制御は、format側で常時不一致スルーにするより、file/batch側に確認済みoverrideを持たせる案を優先候補とする。
- `header_snapshot_json` はJSONで保持する案を基本とするが、検索・制御に使う値は通常カラムに出す。

### 検討観点

- bundleは保存先や排他性を決める本体ではなく、候補探索を助ける入力支援単位である。
- bundleが広すぎると候補が増えすぎ、狭すぎると実CSV登録時に探しにくくなる。
- ヘッダー不一致時の例外許可は、誰が、どのファイルに対して、何を確認したかを証跡として残す必要がある。

---

## DH-20260724-05 / 2026-07-24 JST

### テーマ

入力支援bundleの階層型採用

### 背景

bundleを大きくすると画面で探しやすいが、意味単位が粗くなり、細かい設定や人の確認が難しくなる。
bundleを小さくすると意味は明確になるが、CSVテンプレート登録時に候補を探す回数が増える。
メンテナンス対象とシステム上の関連項目は増えるが、画面上の探しやすさと内部の意味単位を両立するため、階層型bundleを採用する。

### 決定事項

- 入力支援bundleは、画面では大きい親bundleで探し、内部では小さい意味単位の子bundleに分ける階層型を採用する。
- `exam_item_concept_groups` に `parent_concept_group_id` / `parent_concept_group_code` / `concept_group_depth` を持たせる案を基本とする。
- `GLUCOSE_RELATED` 配下に `GLUCOSE` / `HBA1C` を置く。
- `LIPID_RELATED` 配下に `TRIGLYCERIDE` / `HDL_CHOLESTEROL` / `LDL_CHOLESTEROL` / `NON_HDL_CHOLESTEROL` / `TOTAL_CHOLESTEROL` を置く。
- `RENAL_RELATED` 配下に `CREATININE` / `EGFR` / `URIC_ACID` / `URINE_ALBUMIN` を置く。
- `CREATININE` は `3C015`、`EGFR` は `8A065` として分ける。
- 入力支援bundleは検体別分類より意味別分類を優先し、腎機能関連では血清尿酸・尿中アルブミンも近くに置く。
- LDL直接法/計算法、中性脂肪の空腹時/随時、血糖の空腹時/随時などはbundleではなくmapping rule条件で扱う。

---

## DH-20260724-06 / 2026-07-24 JST

### テーマ

取込優先方針、format policy、file_receipts確認Go、XML import準拠の加入者突合

### 背景

CSV健診結果取込の目的は、最初から完全な事前停止を増やすことではなく、まず取り込めるものを取り込み、エラー・不足・警告を明確にして後続確認へ渡すことである。
一方で、列ズレや必要列未解決のように誤登録につながるケースは停止候補として扱う必要がある。
また、将来的にCSV由来とXML由来の台帳を統合・共通化しやすくするため、加入者突合と健診結果値処理のカラム名・意味は `xml_ledger` に寄せる。

### 決定事項

- `csv_format_versions.header_mismatch_policy` の初期値は `IMPORT_RESOLVABLE_WITH_WARNING` とする。
- ヘッダー不一致でも、必要なmapping列を安全に解決できる場合はwarningとして取込を進める。
- 停止候補は、必要なmapping列を解決できない場合、列番号指定ruleがあり列ズレリスクが高い場合など、誤登録リスクが高いケースに絞る。
- `csv_format_versions` に `allow_column_no_rules`, `duplicate_row_policy`, `missing_basic_info_policy` を持たせる。
- 血糖の `区分列で分岐` / `空腹時・随時別列` などの選択はformat本体カラムにはせず、seed/FastAPI入力支援側の補助設定として扱う。
- `file_receipts` には `actual_header_sha256` / `matched_csv_format_version_id` を持たせ、実CSVのヘッダー照合情報を保存する。
- `file_receipts` の加入者突合カラムは `xml_ledger` と同名・同義に寄せ、`subscriber_match_status` / `subscriber_match_method` / `subscriber_match_reason` とする。
- CSV行単位の加入者突合は、XML import と同じく、基本情報抽出、`generate_identity_bundle()`, `resolve_subscriber_identity()` の流れに揃える。
- `file_receipts` の健診結果値処理カラムは `xml_ledger` と同名・同義に寄せ、`exam_item_status` / `exam_item_reason` とする。
- `file_receipts` のCSV取込全体状態は、`xml_ledger.xml_status` / `xml_reason` と対になる `csv_status` / `csv_reason` とする。
- `file_receipts` には停止後Goの証跡として `import_resume_approved` / `import_resume_approved_at` / `import_resume_approved_by` / `import_resume_approved_reason` / `import_resume_scope` を持たせる。
- 加入者突合・健診結果値以外の停止理由、たとえばヘッダー関連や文字コードなどは、`csv_status` / `csv_reason` と停止後Go項目で扱う。

補足: 後続協議で、加入者突合・健診結果値処理の状態は `file_receipts` ではなく `csv_row_ledger` に寄せる方針へ修正した。
`file_receipts` には `subscriber_match_*` / `exam_item_*` / `csv_status` / `csv_reason` を追加しない。
CSVファイル単位の現在状態は既存 `status` / `summary_message` と、CSV固有のheader照合・確認Goカラムで表す。

---

## Current Decision Reference / 2026-07-27 JST

この履歴ファイルには、検討途中の旧案として `csv_column_mappings`、`dev_phr.norm_variants` 参照、`IMPORT_RESOLVABLE_WITH_WARNING` などの記述が残っている。
現在の採用方針は `03_decisions.md`、`10_phr_master_initial_ddl_draft.md`、`11_csv_import_processing_design_draft.md`、`13_exam_value_normalize_lib_draft.md`、`30_pre_implementation_review.md` を正とする。

現時点の主な採用方針は以下。

- `csv_column_mappings` は旧暫定案であり、初期DDLの主対象にはしない。
- マッピング本体は `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` とする。
- CD/CO系の結果値名寄せは `phr_master.norm_variants` を参照する。
- `norm_variants` は `phr_master` 初期DDLに含める。
- 旧 `dev_phr.norm_variants` の廃止タイミングは今回決めず、CSV取込実装後に参照切替と運用影響を確認してから別途判断する。
- `csv_format_versions.header_mismatch_policy` の初期値は `ALLOW_AFTER_CONFIRM` とし、人の確認なしにヘッダー不一致を自動続行しない。
- CSV専用の `csv_import_batches` は採用せず、実行履歴は既存思想どおり `etl_runs` を使う。

---

## DH-20260727-01 / 2026-07-27 JST

### テーマ

`namecode` とCSV実値の意味不整合を、実装吸収ではなく健診機関確認事項として扱う

### 背景

ハートクロスサンプルの検証で、2行目に `namecode` が入っている列でも、その列の実値が `exam_item_master` 上の型やコード体系と合わないケースが見つかった。
具体例として、CD項目 `9N066000000000011` に `心雑音 要受診` のような自由記載/医師判断相当の文字列が入っていた。

### 決定事項

- CSVに `namecode` が付与されていても、実値が `xml_value_type` や `result_code_oid` と整合しない場合は、システム側で別項目へ推測振替しない。
- CD項目に自由記載や医師判断相当の文字列が入るケースは、`norm_variants` を追加して救済する対象ではない。
- この種の不整合は、CSV取込マッピング仕様で吸収する課題ではなく、健診機関へのフォーマット確認事項として記録する。
- 取込実装上はnormalizeエラーとして検知し、原本行は `csv_row_ledger.raw_row_json` に残す。

### 補足

これは健診結果そのものの品質問題ではなく、健診機関システムから出力されるCSV仕様、namecode付与、列内容の整合性に関する確認事項である。
システム側が過剰に推測して救済すると、別項目への誤登録につながるため、初期実装では明示的にエラー・確認事項として扱う。

---

## DH-20260728-01 / 2026-07-28 JST

### テーマ

`exam_facilities` のデータソース明示

### 背景

`exam_facilities` 初期データは、社内作業データや受領CSVから独自作成したものではなく、社会保険診療報酬支払基金の公開CSV `Pref_00.csv` を元に作成する。
この前提がDB上で見えないと、後続レビューや運用時に「会社で作った独自マスタなのか」「機微情報を含む作業データなのか」が判別しづらくなる。

### 決定事項

- `exam_facilities` に `data_source_name`, `data_source_file_name`, `data_source_file_sha256`, `data_source_note` を追加する。
- 支払基金CSV由来の初期データには、全行に同じsource情報を入れる。
- `data_source_name` は `社会保険診療報酬支払基金 全国特定健診・特定保健指導機関CSV` とする。
- `data_source_file_name` は `Pref_00.csv` とする。
- `data_source_file_sha256` は project 配下に保存した `docs/spec/exam_result_csv_import/downloads/Pref_00.csv` のsha256を入れる。
- 現時点のsha256は `6fd3348a13da4a0f6143ba6ace7a9646e1684d6af070b6f29125e98ec0b8915e`。
- `data_source_note` には `公開CSV由来。社内作業データ、受領CSV、機微情報を含まない。` を入れる。

### 補足

DH-20260723-05 では「データソースが支払基金CSVであることを表す専用カラムは、初期DDL案には含めない」としていたが、後続協議でsourceをDB上でも明示する方針へ変更した。

---

## DH-20260728-02 / 2026-07-28 JST

### テーマ

CSV format照合をscan直後の共通処理にする

### 背景

CSV取込本体で初めてmapping未登録が判明すると、初回受領時の状況把握が遅れる。
一方で、初回scan時にmappingが未登録だったCSVは、mapping登録後にformat照合だけを再適用できる必要がある。
また、同じ健診機関に複数のmapping versionがある場合、どれを使うかを人がdefault設定できる余地が必要である。

### 決定事項

- CSV format照合の共通処理は `scripts/lib/csv/exam_result_format_matcher.py` に置く。
- `01_scan_files.py` はCSVを `file_receipts` に登録する際、この共通処理で `actual_header_sha256` と `matched_csv_format_version_id` を設定する。
- formatが1件に確定したCSVは `READY` とする。
- format未登録、header不一致、複数候補は `WAITING_CONFIRM` とする。
- mapping登録後の再照合入口として `01_01_match_csv_format.py` を追加する。
- `01_01_match_csv_format.py` はフォルダscanをやり直さず、既存 `file_receipts` のCSVに対してformat照合だけを再適用する。
- `csv_format_versions` に `is_default_for_facility` を追加し、同一施設・同一header shaで複数候補がある場合にdefaultを1件だけ選べるようにする。
- defaultも一意でない場合は、自動選択せず `WAITING_CONFIRM` とする。
- `02_02_exam_result_csv_import` は `file_receipts.matched_csv_format_version_id` があればそれを優先し、実CSVのheader shaが登録formatと一致することを再確認してから取込む。

---

## DH-20260728-03 / 2026-07-28 JST

### テーマ

CSV文字コードfallbackとquote設定の実装方針

### 決定事項

- `csv_format_versions.character_encoding` は想定文字コードとして維持する。
- `encoding_fallback_policy` を追加し、`STRICT` と `ALLOW_COMMON_ENCODINGS` を扱う。
- 初期値は `ALLOW_COMMON_ENCODINGS` とし、登録文字コードを第一候補に、UTF-8 BOM、UTF-8、CP932を重複除外して試す。
- 別文字コードでdecodeできただけでは採用せず、登録済み `header_sha256` と一致した場合だけformat一致とする。
- 採用文字コードは `file_receipts.actual_character_encoding` に保存し、scan、再照合、CSV取込で同じ共通処理を使う。
- `quote_char` は共通CSV loaderから `csv.reader` へ渡す。引用符がないCSVも同じ設定で読めるため、引用符有無だけで停止しない。
- delimiterは初期実装では登録値固定とし、自動fallbackしない。

---

## DH-20260730-01 / 2026-07-30 JST

### テーマ

報告区分・プログラムコードのCSV年齢補完とXML明示値保存

### 決定事項

- CSVで正しい厚生労働省コードを受領できない場合は、年度末時点の満年齢により、40～74歳を `10/010`、それ以外を `40/990` としてledgerへ補完する。
- 現行正はevent年度の年度末日を使う。`event_year = 2026` なら `2027-03-31` を年齢判定日とする。
- `dev_phr.event.age_reference_date` は予約/運用上の年齢換算日として使われる可能性があり、特定健診対象判定には使わない。
- CSVに正しい明示値がある場合は受領値を優先し、コース名称、検査構成、施設内コードからは推測しない。
- XML取込では `ClinicalDocument/code` と `documentationOf/serviceEvent/code` の明示値を `xml_ledger` に保存する。
- XML由来コードには年齢補完を適用しない。
- 既存XMLは取込済みfile receiptを明示的に再処理してbackfillできるようにする。

---

## DH-20260730-02 / 2026-07-30 JST

### テーマ

CSV健診結果から厚生労働省V08 XMLを出力する初期実装

### 決定・実装内容

- `04_export_hia_xml.py` を業務順の出力入口とする。
- 出力候補は報告区分・プログラムコード確定済み、加入者突合MATCHED、法定OKまたはMISSINGのみを理由として承認証跡つきで手動許可されたCSV行に限定する。
- `VALID` の検査値だけを型別の正規化済みカラムからXMLへ出力する。
- 個人CDAと `ix08_V08.xml` はリポジトリ内の公式V08 XSD bundleで検証し、1人でも失敗した健診機関・保険者単位のZIPは出力しない。
- 正常ZIPと個人XMLは `etl_runs` 配下の `xml_export_zips` / `xml_export_members` へ追記する。
- XML基本情報は既存identity共通libを使用し、郵便番号・住所・電話番号の不足していた出力処理も共通field層へ置く。
- 実行環境では `20260730_009_health_exam_result_create_xml_export_history.sql` を適用してからdry-runを行う。

---

## DH-20260730-03 / 2026-07-30 JST

### テーマ

支払基金特定健診XMLサンプルと付属2に基づくXML構造の補完

### 調査結果

- 支払基金サンプル `kensin_kihon_tokutei.xml` は、基本、詳細、任意の各項目を含み、リポジトリ内V08 XSDへ適合した。
- 詳細健診の貧血、心電図、眼底、血清クレアチニンは、親observation配下へ `COMP` / `RSON` で束ねられている。
- 付属2 `001082795.xlsx` には、この構造の正となる `一連検査グループ識別` / `一連検査グループ関係コード` があり、53 namecodeに定義されている。
- サンプルは原本判定を `interpretationCode`、原本基準値を `referenceRange`、実施なしを `negationInd` または `nullFlavor` で表現している。

### 決定・実装内容

- 一連検査グループは健診機関別ruleやnamecode文字列から推測せず、付属2マスタを正とする。
- `exam_item_master` に付属2由来のグループ識別値と関係コードを追加し、初期seedへ53 namecodeを登録する。
- CSVから出力する `interpretationCode` と基準範囲は、原本に明示されmappingされた場合だけ使用する。値からの自動判定、単位変換、施設固有ABC判定の流用は行わない。
- XML builderはグループ項目を親observationの下へ構造化し、非グループ項目は従来どおりsection直下へ出力する。

---

## DH-20260730-04 / 2026-07-30 JST

### テーマ

同一パス差替えreceiptの対象外化とM4 CSV→XML一連検証

### 調査結果

- 同じ相対パスのCSVが別shaへ差し替わると、新receiptは追加される一方、旧内容の未処理receiptが `READY` のまま残り、後続Runでhash不一致を繰り返す状態があった。
- ヒロオカの機微情報除去済みfixtureは基本情報が空だったため、ローカルsubscriber seedと一致する5人分だけテスト基本情報を設定し、残り2人を不足停止確認用に残した。

### 決定・実装内容

- 新shaのreceipt登録時、同一event・同一相対パスの旧 `DISCOVERED` / `READY` / `WAITING_CONFIRM` を `SUPERSEDED` とする。
- 取込済みreceiptは受領・処理履歴であるためstatusを変更しない。
- M4では5人が加入者MATCHED・法定OKとなり、2人は基本情報不足で停止した。
- 5人分のV08個人XML、IX08、XSD bundleを公式命名ZIPへ出力し、XSD適合、付属2グループ構造、DB出力履歴、業務向け出力履歴CSVを確認した。

---

## DH-20260730-05 / 2026-07-30 JST

### テーマ

未受領aliasフォルダをscan errorにしない

### 調査結果

- event 2のalias seed 187件は、現在フォルダが存在する施設一覧ではなく、eventで受領し得る既知施設・フォルダ名の一覧である。
- 旧scanはalias全件に `02_健診結果（編集）` の存在を要求し、M4で実在2施設以外の185施設を `EDIT_FOLDER_NOT_FOUND` としていた。
- alias登録不備ではなく、alias masterと物理受領フォルダの責務をscanが混同していた。

### 決定・実装内容

- alias対応施設フォルダ自体が存在しない場合は未受領として正常skipする。
- 実在する施設フォルダだけをscanし、その配下に編集フォルダがない場合は運用配置エラーとする。
- 実在する未知フォルダ、無効alias、手動判断alias、健診機関未解決aliasは従来どおり明示エラーとする。

---

## DH-20260730-06 / 2026-07-30 JST

### テーマ

実機event 2フォルダ一覧とalias masterの再同期

### 調査結果

- 実機ルート直下には198施設フォルダがあり、初期alias seed 187件との完全一致は185件だった。
- 実機のみの13件は、11件の追加施設フォルダと2件の名称変更だった。
- 13件はすべて先頭コードまたは確認済み採用コードにより、既存 `exam_facilities` へ確定できた。

### 決定・実装内容

- 実機名13件を `0004_add_event2_actual_machine_folder_aliases.sql` で追加する。
- 名称類似による自動名寄せは行わない。
- 既存の旧名称aliasは削除せず、過去フォルダ名の再受領にも対応できるよう残す。
- 実機一覧ファイルはrepositoryへ保存せず、差分seedと調査結果だけを保存する。

---

## DH-20260730-07 / 2026-07-30 JST

### テーマ

健診機関別の受診者エラー率VIEW

### 決定・実装内容

- `exam_result_ledger_report` を元に、健診機関別の人数と法定チェックエラー率を返す `exam_result_facility_error_rate` VIEWを追加する。
- エラーは最終法定チェック結果 `check_status = 'NG'` と定義し、取込途中の `PENDING` はエラーに混ぜず別人数で表示する。
- 全人数比 `error_rate_percent` と、判定済み人数比 `checked_error_rate_percent` の両方を表示する。
- XML/CSVの重複計上を抑えるため、同一施設内で `subscriber_id`、次に `identity_hash` を使って受診者を数え、どちらもない行だけledger単位で数える。
- 名寄せ前のledger行数は `source_result_count` として残し、人数との差を確認できるようにする。

---

## DH-20260731-01 / 2026-07-31 JST

### テーマ

HIA XML出力時の受診者住所・郵便番号補完

### 調査結果

- 厚生労働省V08 XSD上、受診者 `patientRole/addr` は `minOccurs=0` であり、XSD検証だけでは住所欠落をエラーにできない。
- HIA受付では受診者住所・郵便番号が必須扱いとなるため、予約・受診・検査キット送付のいずれにも住所情報がないケースをXML出力側で扱う必要がある。
- 日本郵便は公式に郵便番号データを公開しており、郵便番号から都道府県・市区町村・町域までの補完マスタを作成できる。

### 決定・実装内容

- 住所補完はXML出力時の基本情報projectionで行い、原本CSV/XML値は上書きしない。
- 補完順は、原本値、業務上利用許可された加入者住所等、日本郵便郵便番号マスタ、最終fallbackの順とする。
- 郵便番号から住所を補完する場合、丁目、番地、建物名など個人住所の詳細は推測しない。
- 日本郵便データの「以下に掲載がない場合」等はXMLへそのまま出さず、住所文字列として使える表記へ整備し、元表記と整備理由を記録する。
- 郵便番号でも住所を解決できない場合は、郵便番号 `000-0000`、住所 `－` をHIA提出用の代替値として使用できる。
- 補完または代替値使用を行った場合は、XMLに出した値、元値、補完元、補完理由、処理Run、処理日時を記帳する。
- 基本情報補正はCSVだけでなくXML取込にも必要なため、`xml_ledger` と `csv_row_ledger` の両方に現在XML出力で使う補正値と項目別の最新変更履歴IDを持たせる。
- 変更履歴は `ledger_type` / `ledger_id` でXML/CSVを区別する共通テーブルで保持する。
- 履歴は `active` flagではなく、`previous_correction_history_id` による変更チェーンと、ledger側の最新履歴IDで現在値を示す。
- 初期の補正対象は、保険者番号、保険証記号、保険証番号、枝番、受診券番号、受診券有効期限、氏名カナ、郵便番号、住所とする。
- 修正画面では、加入者突合済みの行に対して `subscribers` の値を補正候補として表示する。採用した場合も通常の補正履歴として記録し、由来を `SUBSCRIBER` とする。

---

## DH-20260731-02 / 2026-07-31 JST

### テーマ

XML/CSV個別ledgerから統合台帳 `exam_ledgers` への方針転換

### 背景

- CSV取込、法定check、CSV→HIA XML出力まで一通り動作確認できた。
- その後、住所補完、基本情報補正、加入者再突合、出力画面、XML+CSV結合、HIAアップロード状態管理を検討すると、`xml_ledger` / `csv_row_ledger` の二重管理が今後の改修の負担になることが明確になった。
- XML由来の基本情報にも誤りや欠落があり、CSVだけを補正対象にする設計では足りない。

### 決定・実装内容

- 今後の改修は統合台帳 `exam_ledgers` を中核にする。
- 既存 `xml_ledger` / `csv_row_ledger` は直ちに廃止せず、移行元、原本証跡、既存処理の後方互換として残す。
- check、基本情報補正、加入者再突合、XML出力、出力履歴、HIAアップロード状態管理、画面表示は原則として `exam_ledgers.exam_ledger_id` を参照する。
- `exam_ledgers` はXML/CSV共通の1人1健診結果台帳とし、source種別、source ledger ID、file_receipt、event、加入者突合情報、基本情報、check/export状態、補正現在値を持つ。
- 移行は、既存個別ledgerからのデータ移行、または再scan/再importによる再構築のどちらも許容する。
- 個別ledgerの廃止タイミングは、統合台帳ベースの取込、check、補正、XML出力が実行環境で安定してから決める。

---

## DH-20260803-01 / 2026-08-03 JST

### テーマ

人×イベント状態管理を既存 `person_event` と縦持ち状態項目へ寄せる

### 背景

- 統合ledgerの上位に人単位の状態管理が必要になった。
- 当初は `health_exam_result.event_person_statuses` のような横持ち集約テーブルを検討した。
- ただし既に `dev_phr.person_event` が人×イベント単位の親として存在しており、別テーブルを増やすと既存コンセプトと二重化する。
- 今後、check項目、HIAアップロード状態、再提出対象、補正待ちなどの管理項目は増減する見込みがあるため、横持ち列を増やし続けると変更影響が大きい。

### 決定・実装内容

- 人単位の親は既存 `dev_phr.person_event` を使う。
- 増減しやすい状態項目は `dev_phr.person_event_status_items` に `person_event_id + item_code` で縦持ちする。
- `health_exam_result.event_person_statuses` 案は採用しない。
- 未突合ledgerはまだ人として確定していないため `person_event` を作らず、`exam_ledgers` 側の未突合状態として残す。
- `exam_ledgers` から `person_event` / `person_event_status_items` へ同期する `scripts/health_exam_event/sync_person_event_status_items.py` を追加する。
- 初期item_codeは、代表状態、受領件数、check件数、XML出力可能件数、XML出力済み件数、最新ledger参照、最新健診日、最新健診機関、補正待ち、手動出力許可有無とする。

---

## DH-20260803-02 / 2026-08-03 JST

### テーマ

source値と清書値の二層化

### 背景

- 複数の結果ファイルから1つの論理健診結果を作る場合、原本ファイルに紐づく値と、最終的にXMLへ出す採用済み値の責務が異なる。
- source値だけから毎回XML出力値を組み立てると、結合、補正、再出力時にJOINと採用判定が重くなり、状態も揺れやすい。
- 一方で、出力都合でsource値を上書きすると、受領ファイルのraw値、normalize結果、エラー理由、健診機関確認の証跡を失う。

### 決定・実装方針

- 検査値は、ファイル由来のsource value layerと、納品・XML出力用のadopted value layerに分ける。
- source value layerは、raw値、normalize値、validation、由来、処理run、エラー理由を保持する処理・証跡層とする。
- adopted value layerは、出力に必要な最小限の採用済み値、採用元source値への参照、採用状態、採用理由を保持する業務・納品層とする。
- 原本値は不変寄り、清書値は再生成可能と位置づける。
- 初期実装では既存 `exam_item_values` を使い、`ledger_type = XML / CSV` をsource値、`ledger_type = EXAM` を清書値として扱う案を基本とする。
- 清書値の責務が大きくなった場合のみ、後続で `adopted_exam_item_values` 等の専用テーブル分離を検討する。

---

## DH-20260803-03 / 2026-08-03 JST

### テーマ

CSV→XML正式出力済み状態の保護

### 背景

- 実行環境ではCSV→XML出力を既に実行しており、生成済みXML/ZIPを正式出力として扱う。
- 再出力は混乱の元になるため、scan/import/checkを再実行しても、出力済み事実を未出力へ戻してはならない。
- `csv_row_ledger.xml_export_status` や `exam_ledgers.xml_export_status` は技術状態として再同期で揺れる可能性がある。

### 決定・実装内容

- 正常完成した出力の正本は `xml_export_zips` / `xml_export_members` とする。
- `sync_exam_ledgers.py` は、`xml_export_members` に該当source ledgerの出力履歴があれば `exam_ledgers.xml_export_status = 'EXPORTED'` として復元する。
- 既存 `exam_ledgers.xml_export_status = 'EXPORTED'` は、source ledger側が `PENDING` でも上書きして戻さない。
- 正式出力済みXML/ZIPは再出力しない運用を基本とする。

### 追加実装

- `sync_exam_ledgers.py` を既存個別ledgerから統合ledgerへのbackfillとして使う。
- CSVは `csv_row_ledger.file_receipt_id`、XMLは `xml_file_links.file_receipt_id` から `exam_ledgers.file_receipt_id` と `exam_ledger_sources.file_receipt_id` を復元する。

---

## DH-20260803-04 / 2026-08-03 JST

### テーマ

健診イベントに特化した人単位チェックスコープ

### 背景

- `event_ledger` まで含めた汎用イベント管理へ広げると、予約、請求、HIA、納品などの抽象化が強くなりすぎ、今回の健診結果処理で必要な判断がぼやける。
- 一方で、現場で見たいのは「この健診イベントで、この人が今どこまで進んでいるか」である。
- event_id=2 は、トランス・コスモス健康保険組合の2026年度定期健診として登録されている。
- 年度内に複数受診がありうること、今後特殊健診系が追加されることを想定する必要がある。

### 決定・実装方針

- `person_event` は汎用イベント管理ではなく、健診イベントに対する人単位の進捗・確認状態に限定して使う。
- 対象視点は、予約申込日、受診日、結果ファイル受領、加入者・基本情報チェック、法定健診項目チェック、HIAアップロード用XML出力、HIAステータス、資格状況、HIAからのダウンロードXML有無、健保納品、事業所納品とする。
- `person_event` は人×eventの親であり、年度内複数受診や特殊健診の個別結果は `exam_ledgers` の複数件として保持する。
- 予約、HIA、納品の各詳細テーブルを `person_event` に統合するのではなく、必要な現在状態を `person_event_status_items` に集約する。
- `event_ledger` は今回の健診人チェックの正本にはしない。旧構想・source event台帳として残し、必要になった場合に別途再評価する。

---

## DH-20260803-05 / 2026-08-03 JST

### テーマ

HIAダッシュボード状態と健診人チェックの接続

### 背景

- HIAダッシュボードCSV取込の既存スクリプトとDBは存在する。
- 新フォーマットでは先頭にHIA加入者IDが追加されており、旧来の漢字氏名照合だけではなくHIA加入者IDを使った照合へ寄せる必要がある。
- 2025年度のHIA年度最終状態は、最新状態テーブルとは別のスナップショットテーブルへ保管済みである。
- 健診eventの人チェックではHIA状態を見たいが、HIAダッシュボードCSV取込そのものを健診ledgerの正本に混ぜると責務が曖昧になる。

### 決定・実装方針

- HIAダッシュボードCSVの最新観測状態は `work_other.hia_dashboard_status` に保持する。
- 年度末または年度最終状態は `work_other.hia_dashboard_year_end_status` に保持する。
- 健診eventに対する人単位の確認状態は `dev_phr.person_event` / `person_event_status_items` へ集約する。
- 進行中年度は `hia_dashboard_status` をHIA現在状態の入力として使う。
- 過年度eventの状態判定に `hia_dashboard_status` の最新値を直接使わず、必要に応じて年度スナップショットを参照する。
- HIAダッシュボードCSV新フォーマットでは、HIA加入者IDが存在する場合は加入者照合の第一候補とする。
- HIA取込の新フォーマット対応は、健診結果CSV取込とは別責務として実装する。

---

## DH-20260803-06 / 2026-08-03 JST

### テーマ

person_event母集団の作成元

### 背景

- `person_event` を `exam_ledgers` の突合済み結果から作るだけでは、結果未受領者が人チェック一覧に出ない。
- 予約データはまだ取り込めていないため、予約を母集団の正本にすることもできない。
- event_id=2 は特定の保険者・年度の健診eventであり、まずはeventの保険者番号に属する加入者全員を確認対象にする必要がある。
- 資格喪失者も、event上でどう扱うべきか確認が必要であり、母集団から除外すると状況確認から漏れる。

### 決定・実装内容

- `person_event` の母集団は、結果受領者ではなく、`dev_phr.event.insurer_number` に一致する `dev_phr.subscribers` 全員とする。
- 資格喪失者も母集団から除外しない。
- 資格喪失日は除外条件ではなく、`person_event_status_items` の `QUALIFICATION_STATUS` / `QUALIFICATION_LOST_DATE` として保持する。
- 母集団作成は `scripts/health_exam_event/sync_person_event_population.py` として独立させる。
- `scripts/health_exam_event/sync_person_event_status_items.py` は、母集団作成済みの `person_event` に対して、健診結果受領、check、XML出力状態を埋める役割へ寄せる。

---

## DH-20260803-07 / 2026-08-03 JST

### テーマ

加入者基本情報の出力用projection

### 背景

- `subscribers` は初期から使われている中心テーブルであり、列名だけではraw/norm/match/exportの責務が明確でない箇所がある。
- 健診XML出力では、保険証記号、保険証番号、氏名カナをHIA/XML用の値として扱う必要がある。
- 一方、`insurance_symbol_match`、`insurance_number_match`、`name_kana_full_match` は照合用であり、HIA/XML出力値として使ってはならない。

### 決定・実装内容

- `scripts/lib/db/lookup/subscriber_export_projection.py` を追加し、`subscribers` からHIA/XML出力候補値を取り出す責務を共通libへ閉じ込める。
- ledger側には `insurance_symbol_export_value`、`insurance_number_export_value`、`name_kana_export_value` と、それぞれの `source` / `reason` を保持する。
- CSV取込時は、まずCSV原本値から `SOURCE` として出力値を作り、加入者突合が `MATCHED` の場合は `subscribers` 登録値から `SUBSCRIBER` として出力値を作り直す。
- XML出力では、ledgerの `*_export_value` がある場合はそれを優先し、なければ従来どおりraw値をidentity共通libで正規化する。

---

## DH-20260805-01 / 2026-08-05 JST

### テーマ

統合ledger、結合出力case、出力可否summaryの責務整理

### 背景

- XML/CSV importを `exam_ledgers` へ寄せた後、`exam_ledgers` がsource ledgerなのか、人単位の結合済みledgerなのかが文書上で混在していた。
- ファイル単位の法定checkと、人単位に複数sourceを結合した後の法定checkは役割が違う。
- 実行環境では `03_00_check_imported_exam_ledgers.py`、`03_01_build_exam_export_cases.py`、`03_02_build_exam_export_case_values.py`、`03_04_check_exam_export_cases.py` まで動作し、case単位のOK/NGが見える状態になった。
- ただし人が見る「出力してよいか」は `check_status` だけでは分かりづらく、後続の出力画面やHIAアップロード依頼にも不十分だった。

### 決定・実装内容

- `exam_ledgers` は、XML/CSV/紙入力から取り込んだsource 1件を表す統合台帳とする。
- XMLならXML内の1人分、CSVならCSV 1行、紙入力なら入力1人分を `exam_ledgers` 1件として扱う。
- 複数sourceをまとめた人単位のXML出力候補は `exam_export_cases` とする。
- 構成元sourceは `exam_export_case_sources`、XML出力用の採用済み整値は `exam_export_case_values` に保持する。
- `exam_item_values` はsource値のraw/normalize/validation証跡に集中させ、清書値として兼用しない。
- source単位の法定check入口は `03_00_check_imported_exam_ledgers.py`、case単位の法定check入口は `03_04_check_exam_export_cases.py` とする。
- 人が見る出力可否summaryとして、`exam_export_cases.export_readiness_status` / `export_readiness_reason` を追加した。
- `export_readiness_status` は `EXPORT_READY`, `APPROVED_WITH_REASON`, `BLOCKED`, `WAITING_VALUES`, `WAITING_CHECK`, `EXPORTED`, `EXPORT_ERROR` を扱う。
- 出力後証跡として `exam_export_cases` に `output_zip_path`, `output_zip_file_name`, `output_xml_file_name`, `xml_exported_at`, `xml_export_etl_run_id` を追加した。
- `04_export_hia_xml.py` は後続実装で `exam_export_cases` / `exam_export_case_values` 起点へ切り替える方針とした。旧CSV行台帳起点の出力経路は今後の正にはしない。

---

## DH-20260805-02 / 2026-08-05 JST

### テーマ

XML importの基本情報backfillと受診者住所抽出

### 背景

- XML由来ledgerのcase化を進めたところ、CSVでは保持できている出力用基本情報がXML側で不足していた。
- XMLには受診者住所と医療機関住所の両方が現れうるため、誤って医療機関住所を受診者住所として扱うリスクがあった。
- 既存のXML抽出資産では、受診者住所は `recordTarget/patientRole/addr` を見る思想になっていた。

### 決定・実装内容

- XML importも `exam_ledgers` を通常保存先とし、XML由来の `exam_item_values` は `ledger_type = EXAM`, `ledger_id = exam_ledgers.exam_ledger_id` で登録する。
- XML ledgerの `exam_facility_id` は `file_receipts.exam_facility_id` から引き継ぐ。
- XML本文から施設コード・名称が抽出できない場合は、`file_receipts.facility_code` / `facility_name` のscan時スナップショットを補完表示値として使う。
- 受診者住所は `recordTarget/patientRole/addr` だけから抽出し、医療機関住所は使わない。
- 住所は `state + city + streetAddressLine` を優先し、タグ外mixed contentが住所として入るXMLでは `postalCode` を除いた本文をfallbackとして扱う。
- `02_import_xml.py --include-imported` は取込済み、`WARNING`、既存XML ledgerが `READY/PENDING` のもののbackfillにも使う。

---

## DH-20260806-01 / 2026-08-06 JST

### テーマ

HIA XML出力のcase起点化

### 背景

- 取込と法定checkは `exam_ledgers`、人単位の結合出力候補は `exam_export_cases` / `exam_export_case_values` へ寄せる方針に整理済みだった。
- しかし `04_export_hia_xml.py` は旧CSV行台帳起点の候補取得、値取得、出力済み更新が残っていた。
- XML+CSV結合後の人単位caseを出力するには、04もcase起点に揃える必要があった。

### 決定・実装内容

- `04_export_hia_xml.py` の出力候補取得を `exam_export_cases` 起点に変更した。
- 出力対象は `export_readiness_status` が `EXPORT_READY` または `APPROVED_WITH_REASON` のcaseとする。
- XML出力値は `exam_export_case_values` から取得し、XML項目メタ情報は `exam_item_master` で補う。
- 出力履歴 `xml_export_members` は `ledger_type = CASE`, `ledger_id = exam_export_cases.exam_export_case_id` として記録する。
- 出力成功時は `exam_export_cases.xml_export_status`, `output_zip_path`, `output_zip_file_name`, `output_xml_file_name`, `xml_exported_at`, `xml_export_etl_run_id` を更新する。
- 出力失敗時は該当caseを `EXPORT_ERROR` に寄せ、`etl_errors` に理由を残す。

---

## DH-20260806-02 / 2026-08-06 JST

### テーマ

HIAアップロード作業リストのDB履歴化

### 背景

- HIAアップロードは健診機関ごとの画面からZIPを手作業でアップロードする。
- 出力CSVログだけでは、出力リスト、健診機関、ZIP、個人XML、アップロード完了、個人単位エラーを後から追いにくい。
- 画面から出力リストを見て、エクスプローラーで対象ZIPを開き、HIAへアップロードし、完了やエラー内容を記帳する作業が必要になる。

### 決定・実装内容

- `xml_export_zips` にZIP単位のHIAアップロード作業状態を追加する。
- `xml_export_members` に個人XML単位のHIAアップロード結果、エラーコード、エラー内容、メモを追加する。
- 初期状態はZIP単位、個人XML単位ともに `PENDING` とする。
- 画面・確認SQL向けに `v_xml_export_hia_upload_worklist` を追加する。
- このVIEWで、病院毎、出力リスト毎、ZIP毎、元ファイル毎、個人毎に出力履歴とHIAアップロード状態を確認できるようにする。

---

## DH-20260806-03 / 2026-08-06 JST

### テーマ

HIA XML出力の出力リスト方式

### 背景

- 画面からXML出力する場合、条件を指定して即出力すると、対象者の確認、個別修正、理由ありOK、再出力対象の混在を扱いにくい。
- `etl_runs` は処理実行ログとして既に使っており、人が作る出力対象の作業箱まで `run` と呼ぶと混乱する。
- HIAアップロード作業では、出力前の候補確認と、出力後のZIP・個人XML履歴をつなげて見たい。

### 決定

- 画面運用では「出力リスト」を作成し、検索した `exam_export_cases` を追加・確認してからXML出力する方式を正とする。
- 出力リストは人が操作する作業箱、`etl_runs` は実行ログ、`xml_export_zips` / `xml_export_members` は実際に出力された履歴として責務を分ける。
- 初期DDL名は `ops_xml_export_lists` / `ops_xml_export_list_cases` とする。
- `run_id` という名前はETL実行履歴と混同しやすいため、出力対象の作業箱には使わない。
- `xml_export_zips` へ `xml_export_list_id` を追加し、HIAアップロード作業リストから元の出力リストへたどれるようにする。
- CLIの直接条件指定出力は当面残すが、画面運用の正ルートは出力リスト方式とする。

### 想定フロー

```text
1. 出力リストを作成する。
2. event、健診機関、受診月、受領ファイル、個人、再出力条件でcaseを検索する。
3. case詳細を確認し、必要に応じて基本情報補正や理由ありOKを行う。
4. 出力するcaseを出力リストへ追加する。
5. 出力リストを確定し、出力可能人数、出力不可人数、予定ZIP数を確認する。
6. 出力実行時にetl_runsを作成し、ZIP単位でXMLを生成する。
7. 成功したZIPと個人XMLは xml_export_zips / xml_export_members に記録する。
8. HIAアップロード作業リストでZIP単位・個人単位のアップロード結果を管理する。
```

### 保留

- 出力リストから除外したcaseの履歴をどこまで残すか。
- 同じcaseを複数の未出力リストへ同時追加できるかどうか。
- 出力済みcaseを再出力リストへ追加する時の警告・承認項目。

---

## DH-20260806-05 / 2026-08-06 JST

### テーマ

HIA XML出力リストと出力履歴連携の実装

### 背景

- 画面モック確認により、出力対象を検索して一時的な作業箱へ追加し、その作業箱からXML出力する流れが必要と整理した。
- HIAアップロード作業では、出力ZIPと個人XMLだけでなく、どの出力リストから作られたものかを追える必要がある。

### 決定

- `ops_xml_export_lists` / `ops_xml_export_list_cases` を正式テーブルとして追加する。
- `xml_export_zips` に `xml_export_list_id` を追加し、ZIP履歴から出力リストへ辿れるようにする。
- `v_xml_export_hia_upload_worklist` に出力リストID、リスト名、リスト状態を追加する。
- CLI運用として、`03_05_create_xml_export_list.py` で出力リストを作成し、`04_export_hia_xml.py --xml-export-list-id` で対象リストを出力できるようにする。
- `xml_export_members` は引き続き個人XML単位の出力履歴正本とし、出力成功時に `ops_xml_export_list_cases` も `EXPORTED` へ更新する。

---

## DH-20260806-06 / 2026-08-06 JST

### テーマ

画面実装前のXML出力リスト作成CLIを正式順序へ採番

### 背景

- 出力リスト方式を採用したが、画面が未実装の期間もスクリプトだけで運用できる入口が必要である。
- `dev_tools/create_xml_export_list.py` のままだと暫定ツールに見え、通常実行順に組み込みにくい。

### 決定

- 出力リスト作成の正式CLI入口を `03_05_create_xml_export_list.py` とする。
- 通常順序は `03_04_check_exam_export_cases.py` の後に `03_05_create_xml_export_list.py`、その後 `04_export_hia_xml.py --xml-export-list-id ...` とする。
- `03_05_create_xml_export_list.py` は標準でREADYな出力リストを作成する。下書き確認したい場合だけ `--draft` を使う。
- リスト名を省略した場合は、event、受診月、実行日時から自動生成する。
- 既存の `dev_tools/create_xml_export_list.py` は互換ラッパーとして残す。

---

## DH-20260806-07 / 2026-08-06 JST

### テーマ

画面未実装期間のXML出力を最新READY出力リストへ寄せる

### 背景

- `03_05_create_xml_export_list.py` で出力リストを作っても、`04_export_hia_xml.py` のYAML既定が直接全施設出力のままだと、順番にRunした時に「最後に作ったリスト」が使われない。
- 画面がない期間も、スクリプトだけで `03_05 -> 04` の運用を安全に回したい。

### 決定

- `export_hia_xml.yml` の既定を `use_latest_xml_export_list: true`, `all_facilities: false` とする。
- `04_export_hia_xml.py` は、明示条件がない場合に最新のREADY/PARTIAL/ERROR出力リストを自動選択する。
- `--xml-export-list-id`, `--all-facilities`, `--facility-code`, `--case-id` 等を明示した場合は、その明示条件を優先する。

---

## DH-20260806-04 / 2026-08-06 JST

### テーマ

加入者未突合・一部合致の修正を後続範囲へ追加

### 背景

- 受領したCSV/XMLの検査値が取れていても、加入者情報が当たっていない場合はHIA XML出力対象にできない。
- 一部項目だけ合致している場合、機械的にMATCHEDへ寄せるのは危険だが、人が確認して正しい加入者へ紐付け直せる口がないと、受領から出力までの作業が閉じない。
- 出力リスト画面では、検索候補を出力リストへ追加する前に、誰として出力するかを確定できる必要がある。

### 決定

- 後続実装に「加入者未突合・一部合致の修正」を追加する。
- 未突合・要確認の `exam_ledgers` を検索し、`subscribers` 候補から正しい加入者を選べるようにする。
- 正しい加入者を選択した場合、`exam_ledgers.subscriber_id`, `subscriber_match_status`, `subscriber_match_method`, `subscriber_match_reason` を更新する。
- 修正操作は履歴に残し、CSV/XML原本値は上書きしない。
- 加入者修正後は、該当sourceの `person_event` 反映、`exam_export_cases` 再構築、`exam_export_case_values` 再構築、case単位checkを再実行できるようにする。
- 手動確定では、確定理由、確定者、確定日時を必須にする。

### 位置付け

- これは基本情報補正より前段の「誰の健診結果か」を直す機能である。
- この機能が解決すれば、受領、突合修正、基本情報補正、case作成、XML出力、HIAアップロード管理までの作業導線を画面でカバーできる。

---

## DH-20260806-08 / 2026-08-06 JST

### テーマ

HIA XML出力リスト作成画面モックの状態整理

### 背景

- 出力リスト方式はDB/CLIとして実装済みだが、画面で人が操作する手順はまだ固まっていなかった。
- 初期モックでは、箱作成前の基本情報入力、初期投入条件、人追加検索、追加済みリスト、出力実行が同じ画面内に混在し、箱を作る前と作った後の責務が分かりにくかった。
- 画面実装に入る前に、まず「箱作成前」「箱作成後」「人追加モーダル」の役割を分ける必要があった。

### 決定

- `39_hia_xml_export_run_mock.html` を、HIA XML出力リスト作成画面の現時点の確認済みモックとする。
- 箱作成前の状態は `未作成` とし、リスト名、対象event、提出日、出力番号、作成時に含める対象だけを扱う。
- 箱作成後は `DRAFT` とし、作成時の初期追加ブロックは表示しない。
- 作成時に含める対象は `READY` と `理由ありOK` の2チェックとする。
- チェック対象を初期投入して作る場合は `選択した人を追加して作成`、箱を作ってから個別検索へ進む場合は `リスト作成して人を追加` とする。
- 受診月、健診機関、氏名カナ、HIA加入者IDなどの一時検索条件は箱作成画面では扱わず、人追加モーダル側で扱う。
- 人追加モーダルは、検索条件、検索結果、追加済みリストを分けて表示する。
- 健診機関は名称/コードの部分一致サジェストで選択する。
- 検索結果は、状態タグの下に `この人を追加` / `追加済み` / `追加不可` を表示し、追加済みリストは状態タグの下に `外す` を表示する。
- `基本情報補正` は出力リスト画面に入口だけ置き、補正本体は後続画面/APIとして扱う。

### 後続

- 出力リスト作成、出力候補case検索、case追加/削除、出力実行のFastAPI最小実装を検討する。
- 画面状態は、未作成、DRAFT、READY、EXPORTED、ERROR等のDB状態と同期して出し分ける。
- 基本情報補正画面、加入者未突合修正画面、HIAアップロード記帳画面は別導線として順次設計する。

---

## DH-20260810-01 / 2026-08-10 JST

### テーマ

FastAPI管理画面の現状同期と健診機関・alias管理の実装区切り

### 背景

- CSV取込、統合ledger、case作成、XML出力リストなどのCLI/DB実装が進み、運用者が見る画面も段階的に増えてきた。
- 出力リスト、受領ファイル一覧、統合ledger一覧、event設定に続き、scanエラー解消や画面絞り込みで必要になる健診機関マスタとフォルダaliasも画面から扱えるようにした。
- 画面実装の議論が広がっているため、いったん現在の到達点をドキュメントへ同期し、後続のHIAアップロード記帳、個人case詳細、基本情報補正、加入者突合NG修正、CSVマッピング管理と混ざらないようにする必要があった。

### 決定

- `apps/health_exam_admin` のFastAPI管理画面を、社内ローカル運用の確認・編集・台帳化入口として継続する。
- 2026-08-10時点の実装済み画面は、HOME、ログイン/ユーザー管理、権限設定、セキュリティ、監査ログ、受領ファイル一覧、統合ledger一覧、出力リスト一覧/詳細、event設定、健診機関・alias管理とする。
- 健診機関・alias管理では `phr_master.exam_facilities` と `phr_master.medical_folder_aliases` を作成・更新できる。
- `medical_folder_aliases` の紐づけ先健診機関は、5万件超の健診機関マスタを巨大プルダウンにせず、健診機関IDまたは健診機関コード入力で解決する。
- 健診機関名/コードの部分一致サジェスト、CSVフォーマット/マッピング管理、紙健診テンプレート管理、健診機関確認事項管理は後続とする。

### ドキュメント反映

- `38_health_exam_remaining_implementation_summary.md` にFastAPI管理画面の実装済み範囲と残範囲を追記する。
- `42_admin_screen_scope_and_priority.md` に健診機関・alias管理画面の実装済み内容と後続範囲を追記する。
- `README.md` の現在正ドキュメントと画面スコープ説明を2026-08-10時点へ更新する。

---

## DH-20260810-02 / 2026-08-10 JST

### テーマ

出力対象リストをops系テーブル名へ寄せる

### 背景

- `health_exam_result` に、取込結果、検査値、清書case、XML出力履歴に加えて、画面で人が操作する出力対象リストが増えた。
- `xml_export_lists` / `xml_export_list_cases` という名前だと、実際に出力されたXML/ZIP履歴なのか、人が作る作業箱なのかが分かりにくい。
- 当初は画面DBが実行環境へまだ反映されていない前提でDDLと既存migration名を更新したが、実行環境では旧名migrationが適用済みだったため、duplicateや旧名/新名混在を招いた。

### 決定

- 出力対象リストの物理テーブル名を `ops_xml_export_lists` / `ops_xml_export_list_cases` とする。
- `ops_` は、人が画面や運用で操作する作業台帳を示す接頭辞として扱う。
- `xml_export_zips` / `xml_export_members` は、実際に出力されたXML/ZIP履歴なので現行名を維持する。
- `exam_export_cases` / `exam_export_case_values` は、清書XMLを作るための人単位データなので現行名を維持する。
- 主キー・参照カラム名の `xml_export_list_id` / `xml_export_list_case_id` は、既存CLI引数、URL、ZIP履歴参照との互換性を優先して今回は維持する。
- 既に実行環境へ適用された可能性があるmigrationは変更してはならない。
- DDLはfresh構築用の最新版として更新し、既存環境向けには新規repair migrationを追加する。
- 旧名 `xml_export_lists` / `xml_export_list_cases` が存在する環境は、`20260810_001_health_exam_result_fix_ops_xml_export_list_table_names.sql` で `ops_` 名へ寄せる。

---

## DH-20260810-03 / 2026-08-10 JST

### テーマ

DDL / migration運用ルールとスクリプト配置責務の明文化

### 背景

- 画面実装、出力リスト、HIAアップロード作業、健診機関管理が進み、`health_exam_result` / `phr_master` / `phr_app` / `dev_phr` にまたがる変更が増えている。
- `20260806_002` の出力リストテーブル名変更で、DDL更新とmigration履歴更新を混同し、実行環境でduplicateが発生した。
- 今後、HIAアップロード後の健保納品、HIAダッシュボード、予約、紙健診、基本情報補正など画面/スクリプトが増えるため、配置責務も明確にしておく必要がある。

### 決定

- DDLは最新版スキーマとして更新してよい。
- migrationは既存環境への差分履歴であり、実行環境に一度でも適用された可能性がある時点で変更禁止とする。
- migrationを変更できるのは、実行環境にまだ適用していないことを明確に確認できている場合だけとする。
- 適用済みまたは適用済みの可能性があるmigrationに不備、命名変更、カラム不足、view差分が見つかった場合は、既存migrationを編集せず、新しい日付・連番のrepair/add/change migrationを追加する。
- duplicateが出た場合は、同名migrationを編集して再実行するのではなく、`information_schema` で現状を確認し、必要に応じて救済migrationを作る。
- 人が直接実行する健診結果取込・チェック・出力CLIは `scripts/from_medical/` 直下に置く。
- 医療機関受領物処理に閉じた共通ロジックは `scripts/from_medical/script_lib/`、設定は `scripts/from_medical/config/` に置く。
- HIA側から取得する情報の取込・同期は `scripts/hia/` に置く。
- eventに対する人単位の母集団、状態同期、HIA/予約/納品状態の集約は `scripts/health_exam_event/` に置く。
- PHR内で共通利用するlookup、identity、normalize、CSV loader、住所補完は `scripts/lib/` に置く。
- 保守・検証・マスタ更新用ツールは `scripts/dev_tools/` に置く。
- FastAPI社内管理画面は `apps/health_exam_admin/` に置き、画面は確認、補正、実行指示、状態記帳、監査ログの入口とする。

### 後続

- HIAから健保へのXML納品部分は、HIA後工程としてリファクタリングする。
- 既存の同一人物重複納品除外スクリプトは、2025年度後半時点の「古い出力を残して新しい方から取り除く」用途が中心だった。
- 今回は、整理前にHIAへアップロード済みのXMLがあり、その後に修正済みの新しいXMLを健保へ納品したいケースがある。
- そのため、後続リファクタリングでは「旧出力優先で新出力を落とす」だけでなく、「修正版を正として納品対象にする」モードを設計する。
- 判定キーは、event、HIA加入者IDまたはsubscriber、健診日、健診機関、出力run、修正版/正式版の区別を候補とする。

---

## DH-20260817-01 / 2026-08-17 JST

### テーマ

FastAPI管理画面の出力リスト/確認用出力の実装状態同期

### 背景

- 出力リスト方式はDB/CLIだけでなく、FastAPI管理画面からも運用できる状態に近づいた。
- 画面から出力リストを作成し、確認用出力と本番03フォルダ出力を実行できるようになった。
- 確認用出力は正式フォルダを汚さない一方、確認ファイルを人が取得できないと実運用で使いにくい。
- 健診機関コードの手入力だけでは施設選択が難しいため、受領フォルダalias一覧から検索して追加する導線を追加した。
- 健保からの指摘では、法定健診チェックだけでは拾いきれない特定健診項目側の構造、コード体系、単位、セクションの問題が見えてきた。

### 決定

- 出力リスト一覧/詳細画面を、HIA XML出力の画面入口として扱う。
- 出力リスト作成では、健診機関コード手入力に加え、受領フォルダalias一覧から検索して健診機関コードを追加できるようにする。
- 出力リスト詳細から `review` 確認用出力と `official` 本番03フォルダ出力を実行できるようにする。
- `review` 確認用出力は `<repo>/data/hia_xml_review_exports/event_<event_id>` 配下へ作成し、画面からダウンロードできるようにする。
- 確認用ZIPをダウンロードした場合、`phr_app.app_audit_logs` に `HIA_XML_REVIEW_DOWNLOAD` として記録し、ダウンロード後に確認用ZIPを削除する。
- `review_output_root` に相対パスを指定した場合は、実行カレントディレクトリではなく `<repo>` からの相対パスとして解決する。
- 確認用出力では正式履歴を進めないため、正式出力済みcaseも確認用には再生成できるようにする。
- 確認用出力でZIPが1件も作られない場合は、画面上で完了扱いにしない。

### 特定健診チェックの後続方針

- 法定健診チェックとは別に、特定健診チェックを追加する。
- 値の有無チェックは全員分の横持ちデータとして作成する。
- 法定健診チェックと重なる項目は、同一性項目やnamecodeの意味を確認したうえで、重複チェックにならないよう整理する。
- 法定側にない特定健診項目は、特定健診チェック側の横持ち項目として追加する。
- event年度の年度末年齢で対象者を選別し、対象外、チェックOK、チェックNGとして扱う。
- 報告区分とプログラムコードが、年度末年齢による特定健診対象判定と矛盾しないかも確認する。

---

## DH-20260817-02 / 2026-08-17 JST

### テーマ

特定健診チェック初版の実装

### 背景

- HIA/健保からのXMLエラー指摘で、法定健診チェックだけでは拾いきれない特定健診項目の不足が見えてきた。
- ただし初版で解釈や健診機関ごとの判定ロジックまで持つと、医療機関判定やメタボ判定と混同する。
- まずは、取込済みのnamecodeとnormalize結果をもとに、特定健診として機械的に確認できる事実だけを記帳する必要がある。

### 決定

- `03_00_check_imported_exam_ledgers.py` と `03_04_check_exam_export_cases.py` の共通チェック処理で、法定チェック後に特定健診チェックを実行する。
- 結果は既存の `exam_check_results.specific_check_result` / `specific_reason_summary` に保存する。
- 年齢判定はevent年度の年度末日を使う。event=2は `event_year = 2026` の年度末 `2027-03-31` 時点の満年齢で40-74歳を対象とする。
- `dev_phr.event.age_reference_date` は予約/運用上の年齢換算日と混同しないため、特定健診チェックでは参照しない。
- 対象外は `specific_check_result = OK` とし、summaryに対象外理由を残す。
- 法定健診チェックと重なる項目は、法定チェックがOKなら満たしているものとして扱う。
- 初版で確認する特定健診固有項目は、採血時間、メタボ判定、保健指導レベル、医師の診断（判定）、医師名、服薬1-3、既往歴1-3、貧血、喫煙、20歳からの体重変化、運動習慣、身体活動、歩行速度、食べ方2、食習慣、睡眠、特定保健指導の受診歴とする。
- CD/CO系はnormalize後のcode値、ST系はnormalize後の文字列が存在することを確認する。
- 初版では `specific_check_result` を出力停止条件にはしない。出力可否への反映は、実データでの不足傾向と健保/HIAエラー結果を見て後続で決める。

### 後続

- 報告区分、プログラムコードと年度末年齢の矛盾チェックを追加する。
- 問診項目の必須範囲を整理し、健保/HIAエラーで必要性が確定した項目から追加する。
- 理由ありOKや妊娠等の条件付き不足許可は、既存の理由ありOK枠と統合して管理する。

---

## DH-20260818-01 / 2026-08-18 JST

### テーマ

制度チェックと任意チェックの保持方針

### 背景

- 則44チェックは `4401001001` などの法令項目詳細Noを使い、横持ち結果として高速に確認できる形で整備している。
- 特定健診も健保/HIA/支払基金向けに継続的に必要になるため、則44と同じ形で扱いたい。
- 一方で、健保、事業所、納品先ごとの「この項目も確認してほしい」という任意チェックは、今後増減しやすく、制度チェックと同じ横持ちへ混ぜると破綻しやすい。

### 決定

- 定期健診の通常運用では、法定健診チェックと特定健診チェックを両方実行する。
- 法律・制度に根拠があり、全体共通で継続的に確認するチェックは `exam_check_results` の横持ち項目として保持する。
- 横持ちの基本範囲は、法定健診チェック（まずは労安則44。後続で他法定区分を追加可能）と特定健診チェックまでとする。
- 横持ちにする理由は、一覧、集計、出力可否判定で毎回高速に参照するためである。
- 特定健診チェックも、則44と同じく「制度detail code -> namecode候補 -> OK/MISSING/INVALID」の形へ寄せる。
- 特定健診用detail codeは、則44の `4401001001` などと衝突しない体系で定義する。
- 必要namecode群は `dev_phr.exam_item_group_members` 等のルールマスタで管理する。
- 健保、事業所、納品先、運用都合で追加したい任意チェックは、横持ち制度チェックへ混ぜない。
- 任意チェックは後続でルールセット型の柔軟な仕組みに分ける。対象が限定されるため、多少重くなっても出力前・納品前の確認処理として許容する。

### 後続

- 特定健診用detail code体系を決める。
- 特定健診用の `exam_item_group_members` seedを作成する。
- source単位とcase単位の両方で、特定健診detail code別のチェック結果を横持ちへ反映する。
- 健保独自、事業所独自、納品先独自の任意チェック用ルールセット/結果テーブルを別途設計する。
