# Health Exam Remaining Implementation Summary

## Status

Current as of 2026-08-05.

CSV健診結果取込、法定チェック、CSVからHIA向けXML出力までは一通り動作確認済みである。
次の実装は、取込そのものを増やす段階ではなく、運用で人ごとに管理するための台帳、補正、結合、HIA状態連携を整える段階である。

## Already Working

### CSV/XML Source Import

- 受領フォルダscan。
- 健診機関alias解決。
- CSV format照合。
- CSV mapping適用。
- 統合台帳 `exam_ledgers` 作成。CSVは1行1件、XMLはXML内の1人分1件として登録する。
- 加入者identity生成、加入者突合。
- `exam_item_values` への検査値登録。
- CD/CO/PQ/ST系のnormalize。
- 辞書不足、型不正、未実施などのエラー・警告保持。

### Check and XML Export

- XML/CSV取込単位 `exam_ledgers` を対象にした法定チェック。
- `exam_check_results` 作成。
- `exam_ledgers` へのsource単位check結果反映。
- 人単位の `exam_export_cases` 作成。
- 採用済み整値 `exam_export_case_values` 作成。
- `exam_export_cases` を対象にした清書後法定チェック。
- `exam_export_cases.export_readiness_status` / `export_readiness_reason` に、人が見る出力可否summaryを反映。
- `exam_export_cases` に出力ZIPパス、ZIP名、個人XML名、出力日時、出力run IDを保持する器を追加。
- CSV由来結果からHIAアップロード用XML/ZIP出力。これは旧CSV行台帳起点の経路が残っており、次段階でcase起点へ切り替える。
- XSD検証。
- XML出力履歴 `xml_export_zips` / `xml_export_members`。
- 既存個別ledgerから統合ledger `exam_ledgers` へのbackfill。
- `exam_ledgers` 起点の報告用 `exam_result_ledger_report` 更新。

### HIA Dashboard Side

- HIAダッシュボードCSV取込系の既存スクリプトは存在する。
- 最新状態は `work_other.hia_dashboard_status` に保持する。
- 2025年度の年度最終状態は `work_other.hia_dashboard_year_end_status` にスナップショット保管済み。
- 新フォーマットでは先頭にHIA加入者IDが追加されているため、取込・照合ロジックの追従が必要である。

## Main Remaining Implementations

### 1. Unified Exam Ledger Import Flow

今後の取込単位の業務処理は、XML/CSV個別ledgerではなく `health_exam_result.exam_ledgers` を中心にする。
`exam_ledgers` は `xml_ledger` / `csv_row_ledger` の統合版であり、XMLならXML内の1人分、CSVならCSV 1行、紙入力なら紙入力1人分を表す。

必要なこと:

- scan/importでXML/CSV/紙入力を `exam_ledgers` へ登録する。
- `exam_item_values` は `exam_ledgers.exam_ledger_id` を親にして、source値、raw、normalize、validationを保持する。
- source単位の法定checkは `exam_ledgers + exam_item_values` に対して実行する。
- check結果は `exam_check_results` に保存し、`exam_ledgers.check_status` / `check_reason` へ戻す。
- `exam_result_ledger_report` は個別ledgerではなく `exam_ledgers` から作る。
- XML/CSV個別ledgerは移行元、原本証跡、後方互換として当面残す。
- 再scan/再importで正式出力済み状態を戻さない。

現状:

- `sync_exam_ledgers.py` によるbackfillは実装済み。
- CSV import本体は、通常取込の保存先を `exam_ledgers` とし、CSV由来の `exam_item_values` を `ledger_type = EXAM` / `ledger_id = exam_ledgers.exam_ledger_id` として登録する。
- XML import本体も、通常取込の保存先を `exam_ledgers` とし、XML由来の `exam_item_values` を `ledger_type = EXAM` / `ledger_id = exam_ledgers.exam_ledger_id` として登録する。
- XML importでは `file_receipts.exam_facility_id` を引き継ぎ、受診者住所は `recordTarget/patientRole/addr` だけから抽出する。
- 既存個別ledger向けのcheck関数は明示指定時の後方互換として残すが、通常運用では使わない。
- 通常運用のcheck入口は、source単位が `03_00_check_imported_exam_ledgers.py`、結合出力用case単位が `03_04_check_exam_export_cases.py` とする。
- `03_check_exam_results.py` は廃止し、人が実行する入口は `03_00_check_imported_exam_ledgers.py` と `03_04_check_exam_export_cases.py` に分ける。
- `sync_exam_ledgers.py` は通常運用の必須手順ではなく、初回移行、復旧、再構築用へ下げる方針。

通常実行順:

```text
01_scan_files.py
02_import_xml.py
02_02_exam_result_csv_import.py
03_00_check_imported_exam_ledgers.py
03_01_build_exam_export_cases.py
03_02_build_exam_export_case_values.py
03_04_check_exam_export_cases.py
04_export_hia_xml.py  # 次段階でcase基点へ切替
```

`03_00` はファイル/row sourceとして正しいかのcheck、`03_04` は複数sourceを束ねたcaseとしてXMLに出せるかのcheckであり、両方必要である。

### 2. Person Event Population and Status Sync

健診eventに対する人の状況管理を `dev_phr.person_event` / `person_event_status_items` に寄せる。

必要なこと:

- `event_id` から `dev_phr.event.insurer_number` を取得し、同じ保険者番号の `dev_phr.subscribers` を全件抽出する。
- `event_id + subscriber_id` で `person_event` の母集団を作る。
- 資格喪失者も除外しない。資格喪失日は状況確認用の状態として保持する。
- `subscribers` の追加・氏名・資格喪失日・identity系情報更新に追従できるよう、母集団同期は再実行可能なupsertにする。
- 結果状態同期は母集団系itemを削除せず、結果受領、check、XML出力状態だけを更新する。
- `exam_ledgers` から人単位の受領件数、source check件数、要補正件数を同期する。
- 結合出力用caseからXML出力可否、case check状態、出力済み件数を同期する。
- HIAダッシュボード状態、資格状態、HIAダウンロードXML有無、健保納品、事業所納品をitemとして同期できるようにする。
- 未突合ledgerは `person_event` にしない。

現状:

- `person_event_status_items` DDLと同期スクリプトの初期版は実装済み。
- 現同期スクリプトは `exam_ledgers` に存在する突合済み加入者だけから `person_event` を作るため、未受領者を含むevent全体の母集団作成としては不足している。
- HIA状態、予約、納品系sourceとの接続は未実装。

### 3. HIA Dashboard CSV New Format

HIAダッシュボードCSVの新フォーマット対応を行う。

必要なこと:

- 先頭に追加されたHIA加入者IDを取り込む。
- HIA加入者IDがある場合は照合の第一候補にする。
- 旧漢字氏名照合は必要な場合のfallbackまたは確認用に下げる。
- `hia_dashboard_status` 最新値と `hia_dashboard_year_end_status` 年度スナップの責務を崩さない。
- 健診person_event側へは必要な状態だけ同期する。

現状:

- 既存取込スクリプトはある。
- 新フォーマットで実運用可能かの確認と改修が必要。

### 4. Basic Info Correction

CSV/XML由来の基本情報に不足や誤りがある場合、画面から補正できる構造が必要である。

対象候補:

- 保険者番号。
- 記号。
- 番号。
- 枝番。
- 受診券番号。
- 受診券有効期限。
- 氏名かな。
- 郵便番号。
- 住所。

必要なこと:

- ledgerに補正後の現在値を持つ。
- 補正履歴を別テーブルに保持する。
- 項目ごとに変更履歴IDを追えるようにする。
- 修正時に `subscribers` から候補を検索できるようにする。
- 郵便番号から住所候補を補完できるようにする。

現状:

- 郵便番号マスタ設計はある。
- 補正画面と補正履歴は未実装。

### 5. Postal Code Master

住所がどこにもないケースがHIA必須項目で問題になるため、郵便番号から住所候補を補えるようにする。

必要なこと:

- 日本郵便 `utf_ken_all.csv` を取り込むマスタDB/DDL。
- 郵便番号から都道府県、市区町村、町域を引けるlookup。
- どうしても住所が作れない場合の暫定値を記帳する。

方針:

- 郵便番号がある場合は郵便番号マスタから補完候補を出す。
- 郵便番号も住所も不明な場合は、初期案として郵便番号 `000-0000`、住所 `－` を使い、補正・代替処理であることを履歴に残す。

現状:

- `36_postal_code_master_design.md` に設計あり。
- DDL/loaderの実装確認と運用投入は未完了。

### 6. Export Case and Multi-Source Merge

XMLとCSV、または複数CSVで不足項目を補い、1つの論理健診結果としてXML出力する。

必要なこと:

- 同一人物、同一健診日、同一健診機関、同一eventの `exam_ledgers` 候補をまとめる。
- 1人1回分のXML出力候補を結合出力用caseとして作る。
- caseを構成する `exam_ledgers` をsource tableとして持つ。
- XMLをCSVより優先する。
- CSV同士、XML同士で同じ値が競合した場合は止める。
- 片方にしかない検査値は補完値として採用する。
- 採用済み値は結合出力用case valuesとして持つ。
- case valuesはrawを持たず、XML出力に必要な正規化済み値だけを持つ。
- 型、単位、OID、section、methodCode、順番、一連検査グループは `exam_item_master` から参照する。
- 採用元 `exam_item_values.id` を保持し、raw証跡へ戻れるようにする。
- case values + `exam_item_master` に対してcase単位の法定チェックを行い、出力OK/NGを結合出力用caseに反映する。
- 理由ありOKはsource単位checkを書き換えず、結合出力用caseに承認者、理由、承認日時を持つ。
- ledgerが増えるたびに、該当者の結合出力用caseと `person_event_status_items` を更新できるようにする。

現状:

- 設計方針はある。
- XML出力候補の命名は `exam_export_cases` / `exam_export_case_sources` / `exam_export_case_values` に寄せる。
- `build_combined_exam_ledgers.py` は試作として残すが、本流にはしない。
- 結合出力用case DDL、case作成、case value採用、case単位checkは `exam_ledgers + exam_item_values` 起点で整備する。
- case起点exportは未実装。

### 7. Export Control UI

XML出力条件を画面から指定できるようにする。

想定条件:

- event。
- 健診機関。
- 受領ファイル。
- 健診年月。
- 個人または結合出力用case。
- 既出力者を含める/含めない。
- 同日分割送信回数の自動/手動指定。

必要なこと:

- 実行前に出力可能人数、不可人数、不可理由を見せる。
- ZIP内でOK/NG混在の場合、OKのみ抽出出力するかを選べるようにする。
- HIAアップロード作業に必要な出力履歴ログを見せる。

現状:

- CLI/YAMLによる出力は実装済み。
- local FastAPIなどの画面は未実装。

### 8. HIA Upload and Delivery Status

XML出力後、人がHIAへアップロードしたか、その後健保・事業所へ納品したかを管理する。

必要なこと:

- HIAアップロード済み/未済。
- HIAアップロード失敗理由。
- HIAからダウンロードしたXML有無。
- 健保納品状態。
- 事業所納品状態。
- 人が行うアクションの履歴。

現状:

- XML出力履歴はある。
- HIAアップロード以降の人手ステータス管理は未実装。

### 9. Paper Input

紙から作成した健診結果も、今回のフォーマットに沿って登録できるようにする。

責務:

- 基本情報入力。
- 検査結果登録。
- normalize。
- `exam_item_values` までの格納。

責務外:

- XML出力はCSV/XML取込結果と同じ出力タスクで扱う。

現状:

- 旧「紙→Excel→DB→normalize→export」資産はある。
- 新統合ledger前提の画面入力は未実装。

### 10. Standardization and Mapping Intelligence Layer

CSV取込で蓄積したサンプル、マッピング、名寄せ辞書、エラー判断を、納品処理とは別の標準化資産として扱う。

目的:

- 健診機関ごとのCSVヘッダー、項目名、値、判定列、検査方法列の傾向を見えるようにする。
- `raw値 -> namecode / OID / code` の寄せ方と、あえて寄せずにエラーにした判断を蓄積する。
- 新しいCSVフォーマット登録時に、人が確認すべき候補や過去類似例を出せるようにする。
- 標準化の議論や健診機関との調整に使える根拠を残す。
- 医療機関へ返す是正項目まとめを作れるようにする。

集計候補:

- 健診機関。
- CSVヘッダー名。
- 項目種別: 値、判定、検査方法、所見、基本情報、独自項目。
- raw値の種類と出現回数。
- 寄せ先namecode、結果コードOID、code。
- normalize結果、validation結果。
- 初出ファイル、初出日、確認待ち、確認結果。
- 是正カテゴリ: 標準コード不一致、項目内容不一致、値型不一致、施設独自コード、必須基本情報不足、列名/値意味不明、未回答確認事項。
- 医療機関確認用の要約文、対象ヘッダー、対象raw値例、出現件数、影響範囲。

方針:

- 機微情報を含む実データは保持しない。
- 機微情報を除去・加工したサンプル、format seed、mapping seed、`norm_variants` seed、判断メモは保持する。
- このレイヤーは自動推測で本番取込を変えるためのものではなく、人が標準化判断を行うための参照材料とする。
- 是正項目まとめは、内部エラー名をそのまま出すのではなく、健診機関や取引先に説明できる業務用カテゴリと文章に変換する。
- 健診機関からデータ抽出費用が発生する場合でも、受領データが用途に合わないときは、PHR側で行った名寄せ・変換・補正作業と、健診機関へ是正依頼すべき内容を分けて記録する。
- 後続で、省庁、標準化団体、医療情報標準、特定健診・労安法健診の法体系、政策動向を調査する。

## Proposed Priority

1. `event_id` の保険者番号から `subscribers` 全員を抽出し、`person_event` 母集団を作る。
2. HIAダッシュボードCSV新フォーマット対応。
3. `exam_ledgers` / `person_event_status_items` の同期を通常運用に近づける。
4. 基本情報補正のDB構造と履歴。
5. 郵便番号マスタと住所補完。
6. `exam_export_cases` / `exam_export_case_sources` / `exam_export_case_values` のDDL。
7. case作成、case value採用、case単位check。
8. case起点のXML出力。
9. XML出力制御UI。
10. HIAアップロード、HIAダウンロード、健保・事業所納品ステータス。
11. 紙入力画面。
12. 標準化・マッピング知見レイヤー。

納品が迫る場合は、1、2、4、5を先に進める。
XMLとCSVを合わせて法定を満たす必要が出た時点で、6、7、8を優先する。

## Current Risk

- 住所がHIAで必須となるため、CSV/XML/予約/加入者台帳のどこにも住所がない人の扱いを決める必要がある。
- HIAダッシュボードCSVは年度で状態が上書きされるため、過年度eventの状態を最新テーブルだけで判定すると誤る。
- 基本情報補正をDB直接修正で続けると、誰が何を直したか追えなくなる。
- 複数ファイル結合を取込単位の `exam_ledgers` 上で直接処理すると、原本証跡と清書値が混ざる。
- HIAアップロード後の人手状態を持たないと、正式出力済みXMLと実際のアップロード済みがズレる。
