# CSVマッピング解析支援

## 目的

健診結果CSVのマッピング作業を、本番取込処理の前段で支援する。

この仕組みは、CSVを本番の `exam_ledgers` / `exam_item_values` に入れるものではない。目検で列を確認し、ヘッダー、サンプル値、型、関連列、候補項目、判断メモを残すための解析用ワークベンチとして扱う。

## 位置づけ

- 本番DBとは別DBにする。
- 初版DB名は `csv_mapping_lab` とする。
- `phr_master` / `health_exam_result` への外部キーは張らない。
- 医療機関との紐付けは、支払基金等の公開コードやファイル名由来の文字列として保持する。
- 本番seed生成時だけ、必要に応じて本体DBの `exam_facilities.medical_institution_code` と照合する。

## 初版スコープ

1. CSVファイルを指定して解析する。
2. 解析ファイル単位の情報を保存する。
3. ヘッダー列ごとの情報を保存する。
4. 各列のサンプル値、空欄率、推定型を保存する。
5. 作業者が列ごとに解析内容、関連列、候補namecode、候補ledger field、判断状態を記録できるようにする。

初版では、LLM連携、候補ランキング、seed自動生成履歴はテーブル分割しない。必要になった段階で追加する。

## テーブル

### `analysis_files`

CSV解析1回分、または1ファイル分を表す親テーブル。

主な項目:

- `analysis_file_id`: 解析ファイルID。
- `source_file_name`: 元CSVファイル名。
- `source_file_path`: 元CSVの配置パス。ローカル作業用。
- `source_file_size_bytes`: 元CSVのファイルサイズ。
- `source_file_sha256`: ファイル内容のハッシュ。
- `facility_code`: ファイル名や支払基金公開データ由来の健診機関コード。
- `facility_name`: 健診機関名。
- `source_folder_name`: 受領フォルダ名など、ファイルの由来。
- `encoding`: 判定または指定した文字コード。
- `delimiter`: 区切り文字。
- `quote_char`: quote文字。
- `header_row_no`: ヘッダー行。
- `data_start_row_no`: データ開始行。
- `row_count`: データ行数。
- `column_count`: 列数。
- `header_sha256`: ヘッダー構造のハッシュ。
- `header_snapshot_json`: ヘッダー行と正規化列のスナップショット。
- `sample_row_count`: サンプル値抽出に使った最大行数。
- `parse_status`: `PENDING` / `OK` / `WARNING` / `ERROR`。
- `parse_error_message`: CSV読込時のエラーや警告。
- `analysis_status`: `NEW` / `ANALYZED` / `REVIEWING` / `READY_FOR_SEED` / `SEED_CREATED` / `ARCHIVED`。
- `memo`: ファイル全体のメモ。

### `analysis_columns`

CSVの1列ごとの解析結果を表す子テーブル。

主な項目:

- `analysis_column_id`: 解析列ID。
- `analysis_file_id`: 親ファイルID。
- `column_no`: CSV列番。1始まり。
- `header_occurrence`: 同名ヘッダーの出現順。
- `header_name`: 元ヘッダー名。
- `normalized_header_name`: 比較用に正規化したヘッダー名。
- `analysis_note`: 解析内容。人間の判断メモやLLM候補メモを含められる。
- `sample_values_json`: サンプル値の配列JSON。
- `sample_value_counts_json`: 値の出現数上位JSON。
- `distinct_value_count`: 値種類数。
- `blank_count`: 空欄件数。
- `non_blank_count`: 非空欄件数。
- `blank_rate`: 空欄率。
- `min_numeric_value` / `max_numeric_value`: 数値推定時の範囲。
- `min_text_length` / `max_text_length`: 非空値の文字数範囲。
- `first_non_blank_row_no` / `last_non_blank_row_no`: 値が出る行範囲。
- `inferred_value_type`: `EMPTY` / `NUMERIC` / `DATE` / `CODE` / `TEXT` / `MIXED` / `UNKNOWN`。
- `inferred_format`: `YYYYMMDD`、`YYYY/MM/DD`、`integer`、`decimal` などの補助推定。
- `sensitive_hint`: 氏名、記号番号、生年月日など、個人特定情報っぽい列の印。
- `value_profile_json`: 型推定やLLM投入用の補助プロファイル。
- `related_column_nos_json`: 関連しそうな列番JSON。
- `candidate_target_kind`: `LEDGER_FIELD` / `EXAM_ITEM_VALUE` / `IGNORE` / `REVIEW` など。
- `candidate_namecode`: 検査項目候補。
- `candidate_ledger_field`: 基本情報候補。
- `candidate_confidence`: 機械候補またはLLM候補の信頼度。
- `decision_status`: `UNREVIEWED` / `ADOPT` / `IGNORE` / `NEEDS_CONFIRMATION` / `DEFERRED`。
- `decision_note`: 最終判断のメモ。
- `seed_target`: seed化対象か。
- `seed_exported`: seedへ反映済みか。

## 初版で分けないもの

以下は初版ではJSONやメモに入れ、運用で必要性が見えたら別テーブル化する。

- LLMへの依頼履歴。
- LLMの候補ランキング。
- 複数候補namecodeの履歴。
- seed生成履歴。
- 施設マスタとの照合履歴。

## 状態の使い分け

`parse_status` は、CSVを機械的に読めたかを表す。

- `PENDING`: まだ解析していない。
- `OK`: ヘッダーと行を読み込めた。
- `WARNING`: 読めたが、行ごとの列数差や文字化け疑いがある。
- `ERROR`: 読めなかった。

`analysis_status` は、人間の解析作業の進行を表す。

- `NEW`: ファイル登録直後。
- `ANALYZED`: ヘッダー/サンプル値/型推定まで完了。
- `REVIEWING`: 目検確認中。
- `READY_FOR_SEED`: seed化してよい。
- `SEED_CREATED`: seed化済み。
- `ARCHIVED`: 今後使わない、または古い解析。

`decision_status` は、列ごとの判断を表す。

- `UNREVIEWED`: 未確認。
- `ADOPT`: 取り込む。
- `IGNORE`: 使わない。
- `NEEDS_CONFIRMATION`: 健診機関や仕様確認が必要。
- `DEFERRED`: 今回は見送る。

## LLM連携の将来形

ローカルLLMには、元CSV行全体ではなく、列単位の要約JSONを渡す。

渡す候補:

- ファイル概要。
- 列番。
- ヘッダー名。
- サンプル値。
- 推定型。
- 周辺列。
- 既知の `exam_item_master` 候補。

返す候補:

- `candidate_target_kind`
- `candidate_namecode`
- `candidate_ledger_field`
- `confidence`
- `analysis_note`
- `needs_human_review`

最終判断は必ず作業者が行い、LLM出力をそのままseed化しない。

## DDL

- `sql/ddl/csv_mapping_lab/0010_csv_mapping_lab__analysis_files.sql`
- `sql/ddl/csv_mapping_lab/0020_csv_mapping_lab__analysis_columns.sql`
