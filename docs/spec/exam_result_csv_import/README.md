# Exam Result CSV Import

## Purpose

`phr_master` は、`02_02_exam_result_csv_import` を設計・実装する前提として、健診機関、CSVフォーマット、項目マッピング、結果値normalize辞書などの共通マスタを整理するためのマスタ用DBである。

このディレクトリでは、CSV健診結果取込に必要なマスタ境界を整理する。
`phr_master` 新設そのものは目的ではなく、受領CSVを健診機関別に解釈して `exam_item_values` へ登録するための前提整備として扱う。

## Pilot Note

- 初回検証候補施設: ヒロオカクリニック
- 既存alias seed: `event_id = 2`, `src_folder_raw = 1310438796_ヒロオカクリニック`, `dst_folder_norm = 1310438796_ヒロオカクリニック`
- メモ: 担当から「CSVしか来ていない施設」と聞いたため、CSV健診結果取込の最初の試験対象候補として忘れないように記録する。

## Documents

実装前レビューでは、現在正は `03_decisions.md` と `30_pre_implementation_review.md` とする。
`05_design_history.md` は協議履歴であり、旧案や却下済み案が残っていても現在正とは限らない。

- `03_decisions.md`
  - 採用済みの決定事項。
  - DDL、seed、migration、実装へ進む際の基準とする。
- `05_design_history.md`
  - 協議内容と意思決定の経緯。
  - `03_decisions.md` と差分がある場合は `03_decisions.md` を優先する。
- `10_phr_master_initial_ddl_draft.md`
  - `phr_master` 初期DDL案と、支払基金CSVの調査結果。
  - 現時点では検討用であり、DDL適用・migration・seed・スクリプト変更は行わない。
- `11_csv_import_processing_design_draft.md`
  - `02_02_exam_result_csv_import` の処理側設計案。
  - `file_receipts`、CSV行台帳、`exam_item_values` 登録までの接続を整理する。
- `12_exam_facility_lookup_lib_draft.md`
  - 受領フォルダ名から健診機関を解決する共通lookup lib案。
  - `scripts/lib/db/lookup/exam_facility.py` の入出力と責務を整理する。
- `13_exam_value_normalize_lib_draft.md`
  - `namecode` とraw値から健診結果値をnormalizeする共通lib案。
  - 既存 `exam_item_master` lookupとの責務分担を整理する。
- `20_mapping_rule_screen_mock.html`
  - テンプレート登録構造を把握するための画面モック。
  - 実装画面ではなく、入力粒度確認用のサンプルとして扱う。
- `21_mapping_rule_structure_examples.md`
  - 画面モックの内容を `csv_exam_result_mapping_rules` / `csv_exam_result_mapping_conditions` の候補データとして見える化する。
- `22_exam_item_concept_group_initial_set_draft.md`
  - CSVテンプレート登録で候補 `namecode` を探しやすくするための上位グループ初期案。
  - 全検査の医学分類ではなく、CSVで迷いやすい項目の登録支援レイヤーとして扱う。
- `23_hirooka_clinic_pattern_a_review.md`
  - ヒロオカクリニックCSVサンプルをパターンA代表として調査したメモ。
  - 単一ヘッダー830列、基本情報、主要検査列、確認項目を整理する。
- `24_heartcross_akasaka_pattern_b_review.md`
  - ハートクロス健診プラザ赤坂駅前CSVサンプルをパターンB代表として調査したメモ。
  - 2行ヘッダー、2行目コード/namecode指定、健診日未含有の扱いを整理する。
- `30_pre_implementation_review.md`
  - 実装前レビュー用の集約ドキュメント。
  - 決定済み事項、実装範囲、テーブル方針、処理方針、残レビュー項目をまとめる。
- `31_seed_data_preparation.md`
  - CSVフォーマット関連seedの整備メモ。
  - レビュー用SQL、含める初期データ、実投入前の判断点を整理する。
- `32_exam_facility_master_data_check.md`
  - 支払基金CSVと既存 `medical_folder_aliases` seed の突合結果。
  - ヒロオカ/ハートクロスの存在確認、alias全体のコード一致状況、未一致施設を整理する。

## ADR Policy

ADRは、`02_02_exam_result_csv_import` に必要なマスタ境界とDB分離方針が固まった後に作成する。

初期段階ではADRへ直接書かず、まず `05_design_history.md` に協議を積み、採用済みの内容だけを `03_decisions.md` へ反映する。
