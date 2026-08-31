# Exam Result CSV Import

## Purpose

`phr_master` は、`02_02_exam_result_csv_import` の健診機関、CSVフォーマット、項目マッピング、結果値normalize辞書などを管理するマスタ用DBである。

このディレクトリでは、CSV健診結果取込に必要なマスタ境界を整理する。
`phr_master` 新設そのものは目的ではなく、受領CSVを健診機関別に解釈して `exam_item_values` へ登録するための前提整備として扱う。

## Pilot Note

- 初回検証候補施設: ヒロオカクリニック
- 既存alias seed: `event_id = 2`, `src_folder_raw = 1310438796_ヒロオカクリニック`, `dst_folder_norm = 1310438796_ヒロオカクリニック`
- メモ: 担当から「CSVしか来ていない施設」と聞いたため、CSV健診結果取込の最初の試験対象候補として忘れないように記録する。

## Documents

2026-08-10時点の現在正は、決定事項が `03_decisions.md`、実装到達点と未実装範囲が `38_health_exam_remaining_implementation_summary.md`、管理画面スコープと優先度が `42_admin_screen_scope_and_priority.md` である。
実際のカラム・処理挙動は適用対象のDDL、migration、seed、コードを優先する。
`05_design_history.md` は協議履歴、`30_pre_implementation_review.md` は実装着手時の基準点であり、旧案や当時の未決事項が残っていても現在正とは限らない。

- `03_decisions.md`
  - 採用済みの決定事項。
  - DDL、seed、migration、実装へ進む際の基準とする。
- `05_design_history.md`
  - 協議内容と意思決定の経緯。
  - `03_decisions.md` と差分がある場合は `03_decisions.md` を優先する。
- `10_phr_master_initial_ddl_draft.md`
  - `phr_master` 初期DDLの設計基準と、支払基金CSVの調査結果。
  - 実DDLは `sql/ddl/phr_master/` と `sql/migrations/phr_master/` を正とする。
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
- `csv_mapping/README.md`
  - 健診機関ごとのCSVマッピング解析結果と、既存施設別レビュー資料への入口。
  - 今後のCSVマッピング解析メモはこの配下を起点に整理する。
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
  - 実装着手時点のレビュー基準を残す履歴ドキュメント。
  - 現在の実装状況や残課題の判定には単独で使用しない。
- `31_seed_data_preparation.md`
  - CSVフォーマット関連seedの整備メモ。
  - レビュー用SQL、含める初期データ、実投入前の判断点を整理する。
- `32_exam_facility_master_data_check.md`
  - 支払基金CSVと既存 `medical_folder_aliases` seed の突合結果。
  - ヒロオカ/ハートクロスの存在確認、alias全体のコード一致状況、未一致施設を整理する。
- `33_implementation_status_and_xml_handoff.md`
  - 2026-07-29時点の実装、DDL、seed、テストと決定事項の同期結果。
  - 実装済み範囲、未実装の汎用機能、施設確認待ち、CSVからXML作成へ渡せる情報と次の決定事項を整理する。
- `34_csv_to_hia_xml_export_design_draft.md`
  - CSV行台帳と正規化済み検査値から厚生労働省指定XMLを生成する設計と初期実装の現在正。
  - 出力条件、基本情報norm、XSD検証、公式ZIP構成、出力履歴、残る後続項目を整理する。
- `35_social_insurance_fund_xml_sample_review.md`
  - 社会保険診療報酬支払基金の特定健診XMLサンプルをV08 XSD・付属2と照合した結果。
  - 基本、詳細、任意項目と、一連検査グループ・基準範囲・判定の出力基準を整理する。
- `37_event_person_status_and_ledger_layers.md`
  - 複数結果結合、統合台帳、eventに対する人単位の状況管理レイヤーを整理する。
  - `exam_ledgers` と `person_event` / `person_event_status_items` の責務分離、出力制御の置き場所、実装順をまとめる。
- `38_health_exam_remaining_implementation_summary.md`
  - `exam_ledgers` へ取込を集約した後の、結合出力case、人単位event状態、HIA連携、基本情報補正、出力画面の残タスクを整理する。
  - 2026-08-10時点では、source単位check、case作成、case値作成、case単位check、出力可否summary、case基点XML exporter、出力リスト、受領ファイル一覧、統合ledger一覧、event設定、健診機関・alias管理まで実装済みである。
- `39_hia_xml_export_run_mock.html`
  - HIA XML出力リスト画面のモック。
  - 箱作成前は出力リストの基本情報と初期追加対象だけを扱い、箱作成後は人追加モーダルで施設、受診月、氏名カナ、HIA加入者ID等からcaseを検索・追加してXML出力する画面イメージとして扱う。
- `40_hia_upload_worklist_mock.html`
  - HIAアップロード作業リストの画面モック。
  - 出力リスト、健診機関別ZIP、個人XML単位エラー記帳、アップロード完了記帳の操作イメージ確認用として扱う。
- `41_exam_export_case_detail_mock.html`
  - 個人出力case詳細画面のモック。
  - 構成元source、採用済み整値、法定チェック、理由ありOK、出力/HIA履歴を1人単位で確認する画面イメージとして扱う。
- `42_admin_screen_scope_and_priority.md`
  - 健診管理画面全体のスコープ、優先度、マスタ管理画面候補、個人別作業権限方針を整理する。
  - 出力リスト、受領ファイル一覧、統合ledger一覧、event設定、健診機関・alias管理などの実装済み画面と、HIAアップロード、加入者アップロード、予約CSV、紙健診入力、マスタ編集などの後続画面を分けて扱う。
- `43_manual_exam_entry_draft_to_official_design.md`
  - 健診結果手入力の仮登録、本データ反映、仮登録削除、法定チェック、特定健診チェックへの接続方針を整理する。
  - 手入力値をいきなり `exam_ledgers` / `exam_item_values` に作らず、仮登録を経由して正式sourceへ昇格する設計を正とする。
  - 2026-08-24時点では、仮登録ID発行、仮登録リスト、手入力画面での下書き保存・自動保存、仮登録削除、本データ反映まで実装済み。各チェックstepで手入力sourceが期待どおり扱われるかの試走確認は後続とする。
- `44_case_scoped_rebuild_screen.md`
  - eventと加入者を検索し、既存caseの限定再実行、またはledgerからのcase作成を行う管理画面の現在仕様。
  - 加入者条件、健診機関・受診月のcase/ledger横断検索、処理範囲、複数加入者処理を保留する理由を整理する。

## ADR Policy

ADRは、`02_02_exam_result_csv_import` に必要なマスタ境界とDB分離方針が固まった後に作成する。

初期段階ではADRへ直接書かず、まず `05_design_history.md` に協議を積み、採用済みの内容だけを `03_decisions.md` へ反映する。
