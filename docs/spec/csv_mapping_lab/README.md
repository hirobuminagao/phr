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

## サンプル値の保存レギュレーション

CSVマッピングラボはm4ローカル専用の解析DBとして扱うが、誤って実データCSVをアップロードした場合でも、機微情報の実値をDBへ残さない方針にする。

アップロード解析時点で、以下を機械的に行う。

- 元CSVファイルは一時領域に保存して解析し、解析後に削除する。
- `analysis_columns.sample_values_json` には、機微列の実値を保存しない。
- `analysis_columns.sample_value_counts_json` にも、機微列の実値を保存しない。
- 型、桁数、文字種、空欄率、値種類数、日付形式などの解析に必要な特徴は保持する。
- `value_profile_json.sample_values_masked` と `value_profile_json.sensitive_category` で、サンプル化済みかを確認できるようにする。

初期の機微列判定:

- 氏名系: `氏名`、`名前`、`受診者`、`被保険者`、`カナ`、`かな`、`フリガナ`、`ふりがな`
- 健康保険証番号/ID系: `保険証`、`被保険者証`、`記号`、`番号`、`枝番`、`社員番号`、`職員番号`、`従業員番号`、`加入者ID`、`HIA`、`受診券`、`利用券`
- 生年月日系: `生年月日`、`誕生日`、`生年`
- 連絡先系: `住所`、`電話`、`郵便`、`メール`
- 既存のledger候補が `name_full_raw`、`name_kana_raw`、`insurance_symbol_raw`、`insurance_number_raw`、`insurance_branch_number_raw`、`person_id_custom`、`birthdate`、`address`、`postal_code` の場合も機微列として扱う。

初期のサンプル化:

- 氏名: `サンプル太郎`、`サンプル タロウ`、半角カナなら `ｻﾝﾌﾟﾙ ﾀﾛｳ`
- 生年月日: `19750115`、`1975-01-15`、`1975/01/15` など入力形式を維持したダミー日付
- 健康保険証番号/ID系: 桁数と区切り記号を維持した決定的なダミー数字
- 住所/電話/郵便/メール: 形式に応じて `サンプル住所`、`sample@example.local`、または桁数維持のダミー数字

これは完全な匿名化を保証するものではなく、解析補助DBに実値を残さないための安全寄りガードである。列名が曖昧なCSVでは漏れが起こり得るため、必要に応じて辞書を追加する。

## 初版スコープ

1. CSVファイルを指定して解析する。
2. 解析ファイル単位の情報を保存する。
3. ヘッダー列ごとの情報を保存する。
4. 各列のサンプル値、空欄率、推定型を保存する。
5. 作業者が列ごとに解析内容、関連列、候補namecode、候補ledger field、判断状態を記録できるようにする。

初版では、LLM連携、候補ランキング、seed自動生成履歴はテーブル分割しない。必要になった段階で追加する。

## 同じ形式の再解析

同じ健診機関で同じヘッダー構造のCSVを追加で解析する場合は、別フォーマットとして増やすより、既存解析にサンプル値と値分布を追加して育てる運用を基本にする。

判定キー:

- `facility_code`
- `header_sha256`
- `encoding`
- `delimiter`
- `header_row_no`
- `data_start_row_no`

同じ形式と判断できる場合:

- `analysis_files` には解析履歴としてファイル単位の行を残す。
- `analysis_columns` はファイルごとに作る。
- 画面や集計では、同じ `facility_code` + `header_sha256` の列を束ね、サンプル値や出現値の追加分を確認できるようにする。
- 将来的には「代表解析」または「形式グループ」を追加し、複数ファイルのサンプル値を統合表示できるようにする。

現在のCLIは、同一ファイルSHAの再解析を `--replace-source-sha` で入れ替えできる。次の改修では、同一施設・同一ヘッダーの追加解析を「履歴追加」として扱う。

## 候補推定

候補推定は、列ヘッダーと値の特徴から、作業者が見る前の初期候補を入れる補助機能。

現時点のCLIでは以下を行う。

- ヘッダー名をNFKC正規化し、空白を除去して比較する。
- `社員番号`、`保険証記号`、`カナ氏名`、`生年月日` などの明示的なヘッダーは `LEDGER_FIELD` 候補にする。
- `身長`、`体重`、`HbA1c`、`eGFR` などの明示的なヘッダーは `EXAM_ITEM_VALUE` 候補にする。
- `安静心電図1`、`胸部X線1`、`他覚症状(1)` のような連番所見は、限定的にST候補にする。
- `判定`、`疑い`、`実施理由` を含む列は、所見本文と混ざりやすいため部分一致候補から外す。

候補推定は確定ではない。最終判断は `decision_status` と `decision_note` に作業者が記録する。

今後の候補推定候補:

- `exam_item_master` の表示名、namecode、OID情報との照合。
- 既存 `csv_exam_result_mapping_rules` の過去判断との照合。
- 値の型、単位、周辺列を含めた候補。
- ローカルLLMによる候補メモ生成。

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

### `csv_mapping_rules`

再利用するマッピングルール本体。

主な項目:

- `rule_id`: ルールID。
- `scope`: `global` / `facility` / `event`。
- `facility_code`: `scope=facility` の時の健診機関コード。
- `event_id`: `scope=event` の時のイベントID。初期画面では未使用。
- `condition_type`: `header_exact` / `normalized_header_exact` / `header_contains` / `sensitive_category`。
- `column_no_min` / `column_no_max`: 解析済み範囲だけに閉じるための列番制約。NULLなら制限なし。
- `header_pattern`: 元ヘッダー条件。
- `normalized_header_pattern`: 正規化ヘッダー条件。
- `value_type`: `NUMERIC` / `DATE` / `CODE` / `TEXT` / `MIXED` など。NULLなら型不問。
- `sensitive_category`: 個人系カテゴリ。NULLなら不問。
- `target_kind`: `LEDGER_FIELD` / `EXAM_ITEM_VALUE` / `IGNORE` / `REVIEW`。
- `target_namecode`: 検査項目値へ寄せる場合の `namecode`。
- `target_ledger_field`: 基本情報へ寄せる場合のledger field。
- `mapping_strategy`: `DIRECT` / `MULTI_COLUMN_JOIN` / `DERIVED_CODE` / `METHOD_SELECTION` / `IGNORE` / `NEEDS_CONFIRMATION`。
- `confidence`: ルール信頼度。
- `reason`: ルール根拠。
- `active`: 有効フラグ。

### `csv_mapping_rule_hits`

解析列にどのルールが当たったかを記録する。

主な項目:

- `analysis_column_id`: 対象の解析列。
- `rule_id`: ヒットしたルール。
- `score`: 適用スコア。
- `reason`: ヒット理由。

## ルール辞書の適用

- CSVアップロード直後に、登録済みルールを自動適用する。
- 画面から「ルール再適用」もできる。
- `decision_status` が `UNREVIEWED` の列だけ機械候補を書き換える。
- 複数ルールが近いスコアで別targetを返した場合は `REVIEW` に寄せる。
- ルール適用結果は `analysis_columns.candidate_*` と `csv_mapping_rule_hits` に残す。
- 画面から列ヘッダーを元にした初期ルールを登録できる。
- CSV全体を見ていない段階のseedは、必ず `column_no_min` / `column_no_max` で解析済み範囲に閉じる。

初期のルール登録は、作業者が画面で選んで保存する。Codexによるルール候補作成は次段階で、候補を `csv_mapping_rule_suggestions` のような別テーブルに受ける想定。

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

ローカルLLMや外部のAI作業には、元CSV行全体ではなく、列単位の要約JSONを渡す。

渡す候補:

- ファイル概要。
- 列番。
- ヘッダー名。
- サンプル値。
- 推定型。
- 周辺列。
- 既知の `exam_item_master` 候補。
- 登録済みルールのヒット結果。

返す候補:

- `candidate_target_kind`
- `candidate_namecode`
- `candidate_ledger_field`
- `confidence`
- `analysis_note`
- `needs_human_review`

最終判断は必ず作業者が行い、Codex出力をそのままseed化しない。

初版では、Codexへ直接POSTしない。`analysis_file_id` からJSONを出力し、そのJSONをCodexに渡して候補JSONを返す流れにする。

Codexへ渡す時は、画面またはCLIで作ったJSONを使う。

- 小さめの範囲なら、画面の「JSON表示」からコピーしてチャットへ貼る。
- 120列程度に分ける場合は、開始列/終了列を指定してJSONを作る。
- 全列や大きいJSONは「Codex確認ZIPをダウンロード」で保存し、そのZIPを添付するか、保存パスをCodexへ伝える。
- 実CSV全体を貼るより、解析済みJSONを渡す方が、列番、ヘッダー、サンプル値、型、周辺列を安定して見られる。
- 個人特定系の列は初期では除外する。必要な時だけ「個人系ヒント列も含める」を使う。
- ZIPには `REGULATION.md` と `analysis_prompt.json` を入れる。CodexにはZIPを解析させれば、毎回レギュレーションを説明し直さなくてよい。

軽量AI向けの仕分けモードは廃止する。現時点では、健診結果のマスタや既存ルールを正しく参照できないAIに任せると、存在しない `namecode` やヘッダーと無関係な候補が混ざるリスクが高い。

運用は、120列前後の範囲に区切ってCodexへ渡し、CodexがDB、DDL、seed、既存コードを確認しながら候補を返す形を標準にする。

## DDL

- `sql/ddl/csv_mapping_lab/0010_csv_mapping_lab__analysis_files.sql`
- `sql/ddl/csv_mapping_lab/0020_csv_mapping_lab__analysis_columns.sql`
- `sql/ddl/csv_mapping_lab/0030_csv_mapping_lab__mapping_rules.sql`
- `sql/migrations/csv_mapping_lab/20260825_001_csv_mapping_lab_add_rule_column_range.sql`

## 初回CLI

CSVを読み込み、`analysis_files` と `analysis_columns` に登録する。

- `scripts/csv_mapping_lab/analyze_csv.py`
- `scripts/csv_mapping_lab/export_llm_prompt.py`

例:

```bash
python3 scripts/csv_mapping_lab/analyze_csv.py \
  /path/to/sample.csv \
  --facility-code 0110119070 \
  --facility-name 円山クリニック \
  --created-by operator
```

確認だけ行いDBに登録しない場合:

```bash
python3 scripts/csv_mapping_lab/analyze_csv.py /path/to/sample.csv --dry-run
```

同じCSVを再解析して古い解析を置き換える場合:

```bash
python3 scripts/csv_mapping_lab/analyze_csv.py /path/to/sample.csv --replace-source-sha
```

Codex確認用JSONを出力する場合:

```bash
python3 scripts/csv_mapping_lab/export_llm_prompt.py 3 \
  --output /tmp/maruyama_prompt.json
```

列範囲を絞る場合:

```bash
python3 scripts/csv_mapping_lab/export_llm_prompt.py 3 \
  --column-start 1 \
  --column-end 120 \
  --output /tmp/maruyama_prompt_001_120.json
```

## 簡易画面

`/utilities/csv-mapping-lab` に解析用の簡易画面を追加した。

初版画面:

- CSVアップロード。
- 解析実行。
- 解析ファイル一覧。
- 解析結果の列プレビュー。
- AI確認用JSONの画面表示。
- AI確認用JSONのコピー。
- AI確認用ZIPのダウンロード。

画面でまだ扱わないもの:

- 配置済みファイルパス指定。
- 列ごとの最終判断メモ編集。
- AI回答JSONの取り込み。
- seed生成。

これらは、CSVマッピング判断の運用が見えてから追加する。
- 列ごとの `ADOPT` / `IGNORE` / `NEEDS_CONFIRMATION` / `DEFERRED` 更新。

画面はPHR本番処理のstepには混ぜない。CSVマッピング解析支援の入口として、独立した画面にする。
