# Legal Check Investigation by Codex

## 調査日時

2026-06-26 JST

## 調査対象

- `scripts/kenshin_list_pydir/`
- `sql/ddl/`
- `sql/migrations/`

## 検索キーワード

- `法定`
- `legal`
- `lsio`
- `annex2`
- `judge_`
- `legal_report`
- `annex2_legal_report_flag`
- `定期健診`
- `胸部X線`
- `聴力`
- `視力`
- `心電図`
- `赤血球数`
- `血色素量`

## 結論

現行参照実装内では、法定健診項目の不足チェック、または法定健診成立判定を実際に実行している Python スクリプトは確認できなかった。

DDL上は、法定/LSIOチェック結果を保存するための候補テーブル・候補カラムが存在する。しかし、今回対象範囲のスクリプトおよび migration では、それらを生成・更新する実装は確認できない。

確認できたのは主に以下である。

- `dev_phr.exam_item_master` に付属2由来の法定/任意フラグを持たせる migration。
- `medi_export_xml.py` が `annex2_legal_report_flag` を参照して CDA セクションを法定側/任意側へ振り分ける処理。
- `medi_xml_ledger` に `judge_*` / `lsio_legal_*` 系カラムが存在すること。
- `medi_lsio_identity_presence` / `medi_lsio_missing_items` という LSIO 欠損チェック用らしいテーブル DDL が存在すること。
- `medi_xml_item_extract.py` 自身が、法定健診成立判定は「別工程」と明記していること。

## 1. 法定健診項目の不足チェックを実施しているスクリプトがあるか

確認できなかった。

`scripts/kenshin_list_pydir/scripts/medi_xml_item_extract.py` は、XML observation から値を抽出して `work_other.medi_xml_item_values` へ UPSERT するスクリプトだが、docstring で「法定健診の成立判定（LSIOなどの充足判定）は別工程」と明記している。

根拠:

- `scripts/kenshin_list_pydir/scripts/medi_xml_item_extract.py:10-13`
- `scripts/kenshin_list_pydir/scripts/medi_xml_item_extract.py:24-33`

また、`medi_shared_files_auto_judge.py` に `judge` という名前はあるが、これは ZIP 内 XML 有無を見て `medi_shared_files.auto_judgement` を `KENSHIN` / `UNKNOWN` にする処理であり、法定健診項目の充足判定ではない。

根拠:

- `scripts/kenshin_list_pydir/scripts/medi_shared_files_auto_judge.py:6-16`

## 2. `medi_xml_ledger` の `judge_*` / `lsio_legal_*` 系カラムを更新しているスクリプトがあるか

確認できなかった。

`work_other.medi_xml_ledger` の DDL には以下の列がある。

- `judge_status`
- `is_exam_result`
- `is_legal_exam`
- `judge_score`
- `judge_note`
- `judged_run_id`
- `judged_at`
- `lsio_legal_required_count`
- `lsio_legal_present_count`
- `lsio_legal_is_complete`
- `lsio_legal_missing_methods`
- `lsio_legal_judged_run_id`
- `lsio_legal_judged_at`

ただし `judge_*` / `is_*` 系は DDL コメント上も `【未使用】` とされている。`lsio_legal_*` 系はコメント上は LSIO 判定結果用に見えるが、今回対象のスクリプト内では更新箇所を確認できなかった。

根拠:

- `sql/ddl/work_other/0020_work_other__medi_xml_ledger.sql:46-61`
- `scripts/kenshin_list_pydir/kenshin_lib/medi/db_medi.py:780-810`

`db_medi.py` の `INSERT INTO medi_xml_ledger ... ON DUPLICATE KEY UPDATE` は、受診者識別、施設、健診日、XSD結果などを更新しているが、`judge_*` / `lsio_legal_*` 系列は INSERT/UPDATE 対象に含まれていない。

## 3. `annex2_legal_report_flag` をどこで設定しているか

`annex2_legal_report_flag` は `dev_phr.exam_item_master` に対して migration で設定されている。

### カラム追加

`20260213_001_dev_phr_add_annex2_flags_to_exam_item_master.sql` が `dev_phr.exam_item_master` に以下を追加している。

- `annex2_exec_requirement`
- `annex2_legal_report_flag`
- `cda_section_code_default`

根拠:

- `sql/migrations/dev_phr/20260213_001_dev_phr_add_annex2_flags_to_exam_item_master.sql:29-43`

### 付属2/MHLW Excel由来の初期反映

`20260213_002_dev_phr_update_exam_item_master_annex2_from_mhlw.sql` が `docs/mhlw/phase4_v08/001082795.xlsx` を source として、`namecode` 単位で `annex2_exec_requirement`、`annex2_legal_report_flag`、`cda_section_code_default` を UPDATE している。

根拠:

- `sql/migrations/dev_phr/20260213_002_dev_phr_update_exam_item_master_annex2_from_mhlw.sql:1-7`
- `sql/migrations/dev_phr/20260213_002_dev_phr_update_exam_item_master_annex2_from_mhlw.sql:30-35`
- `sql/migrations/dev_phr/20260213_002_dev_phr_update_exam_item_master_annex2_from_mhlw.sql:108-114`

### 未設定の付属2掲載項目を任意扱いに補完

`20260213_003_dev_phr_set_optional_annex2_flags_for_remaining_items.sql` が、`jun_no IS NOT NULL` かつ `annex2_legal_report_flag IS NULL` の項目を `annex2_legal_report_flag = 0`、`cda_section_code_default = '01990'` に更新している。

根拠:

- `sql/migrations/dev_phr/20260213_003_dev_phr_set_optional_annex2_flags_for_remaining_items.sql:4-9`
- `sql/migrations/dev_phr/20260213_003_dev_phr_set_optional_annex2_flags_for_remaining_items.sql:23-28`

### 利用箇所

`medi_export_xml.py` は `work_other.medi_exam_result_item_values` と `dev_phr.exam_item_master` を JOIN し、`annex2_legal_report_flag` と `cda_section_code_default` を取得している。

その後、`annex2_legal_report_flag in (1, 2)` なら法定側セクション、そうでなければ任意側セクション `01990` に振り分ける。

根拠:

- `scripts/kenshin_list_pydir/scripts/medi_export_xml.py:528-540`
- `scripts/kenshin_list_pydir/scripts/medi_export_xml.py:774-780`

注意: これは XML 出力時の法定/任意セクション振り分けであり、法定健診として成立するか、必須項目が足りているかの判定ではない。

## 4. 法定健診項目チェック結果がどのテーブル・カラムに保存されるか

現行コードで実際に保存している箇所は確認できなかった。

DDL上の保存先候補は以下である。

### `work_other.medi_xml_ledger`

XML単位の横持ちサマリとして、以下が候補になる。

- `is_legal_exam`
- `judge_status`
- `judge_score`
- `judge_note`
- `judged_run_id`
- `judged_at`
- `lsio_legal_required_count`
- `lsio_legal_present_count`
- `lsio_legal_is_complete`
- `lsio_legal_missing_methods`
- `lsio_legal_judged_run_id`
- `lsio_legal_judged_at`

ただし、`judge_*` 系は DDL コメントで未使用扱い。`lsio_legal_*` 系も、今回対象コードでは更新実装を確認できない。

根拠:

- `sql/ddl/work_other/0020_work_other__medi_xml_ledger.sql:46-61`

### `work_other.medi_lsio_identity_presence`

LSIO identity presence の中間生成テーブルらしい。

主な列:

- `xml_sha256`
- `group_code`
- `identity_item_code`
- `present_flag`

根拠:

- `sql/ddl/work_other/0021_work_other_medi_lsio_identity_presence.sql:1-15`

### `work_other.medi_lsio_missing_items`

法定健診の必要項目について、XMLごとの欠損を縦持ちするテーブルらしい。

主な列:

- `xml_sha256`
- `group_code`
- `identity_item_code`
- `required_flag`
- `missing_flag`

DDLコメントは「労基（法定）健診の必要項目：XMLごとの欠損（縦持ち）」。

根拠:

- `sql/ddl/work_other/0022_work_other_medi_lsio_missing_items.sql:1-23`

## 5. チェックロジックが未実装・別フォルダ・手作業由来の可能性があるか

可能性は高い。

今回の対象範囲では、以下の状態だった。

- DDL上はチェック結果保存用に見えるテーブル・カラムが存在する。
- `medi_xml_item_extract.py` は法定健診成立判定を「別工程」と明記している。
- `medi_xml_ledger` の `judge_*` 系は DDLコメントで未使用扱い。
- `medi_xml_ledger` の `lsio_legal_*` 系を更新する Python/SQL は対象範囲で確認できない。
- `medi_lsio_identity_presence` / `medi_lsio_missing_items` を INSERT/UPDATE/DELETE する Python/SQL は対象範囲で確認できない。
- `annex2_legal_report_flag` は migration でマスタに設定され、XML出力時のセクション振り分けには使われているが、不足チェックには使われていない。

したがって、法定健診項目チェックは以下のいずれかと考えるのが自然である。

- 未実装。
- 今回対象外フォルダに存在する。
- DBに対して手作業SQLまたは一時SQLで実行していた。
- 過去に実験的に作ったテーブル/カラムだけが残っている。
- LSIOチェックの設計途中で、実行スクリプトがまだ現行参照実装に取り込まれていない。

## 補足: 項目名キーワード検索結果

`定期健診`、`胸部X線`、`聴力`、`視力`、`心電図`、`赤血球数`、`血色素量` は、今回対象範囲内では法定健診不足チェックの実装箇所としては確認できなかった。

`20260213_002_dev_phr_update_exam_item_master_annex2_from_mhlw.sql` には、これらに相当し得る `namecode` への `annex2_legal_report_flag` 設定が含まれるが、項目名ベースのチェックロジックではなく、`exam_item_master` へのマスタ反映である。

## v2設計への示唆

現行の `medi_xml_item_values` / `medi_exam_result_item_values` は値の保存が主目的であり、不足項目や制度判定結果を持つ実装は確認できない。v2では、既存の DDL候補を流用する場合でも、少なくとも以下を明示した専用チェック処理が必要になる。

- 法定健診チェック対象XML/対象者の選定条件。
- 必須項目のルールソース。
- 同一性項目、methodCode、namecode のどれを充足判定キーにするか。
- EITHER_OK、OPTIONAL_BY_DOCTOR、CONDITIONAL、REPORT_IF_AVAILABLE の扱い。
- XML単位の総合判定と、項目単位の不足明細の保存先。
- 再実行時の冪等性と run_id / judged_at の扱い。
