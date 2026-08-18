# Health Exam Remaining Implementation Summary

## Status

Current as of 2026-08-10.

CSV健診結果取込、法定チェック、CSVからHIA向けXML出力までは一通り動作確認済みである。
次の実装は、取込そのものを増やす段階ではなく、運用で人ごとに管理するための台帳、補正、結合、HIA状態連携を整える段階である。
FastAPI管理画面は、スクリプト運用を置き換える完成版ではなく、まず確認・編集・台帳化しやすい入口として段階的に拡張している。

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
- `exam_export_cases` に出力ZIPパス、ZIP名、個人XML名、出力日時、XML出力のETL実行IDを保持する器を追加。
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

### FastAPI Admin Screen

- `apps/health_exam_admin` に社内ローカル向け管理画面を実装中。
- ログイン、登録申請、承認、無効化、パスワード初期化、本人情報変更、ロール変更、個人別作業権限ON/OFFは実装済み。
- セキュリティ設定、自動ログアウト、IP制限、個人情報監査ログの設定/参照入口は実装済み。
- HOMEには、今すぐ使える作業と準備中の作業を分けて表示する。
- 受領ファイル一覧、統合ledger一覧、出力リスト一覧/詳細は実装済み。
- イベント設定画面は実装済み。`dev_phr.event` のevent名、年度、保険者番号、年齢基準日、結果ルート等を管理する。
- 健診機関・alias管理画面は実装済み。`phr_master.exam_facilities` と `phr_master.medical_folder_aliases` を作成・更新できる。
- 健診機関・alias管理では、5万件超の健診機関マスタを巨大プルダウンにせず、aliasの紐づけ先は健診機関IDまたは健診機関コード入力で解決する。
- 出力実行、HIAアップロード記帳、個人case詳細、基本情報補正、加入者突合NG修正、CSVマッピング管理、紙健診入力は後続。

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
03_05_create_xml_export_list.py
04_export_hia_xml.py
```

`03_00` はファイル/row sourceとして正しいかのcheck、`03_04` は複数sourceを束ねたcaseとしてXMLに出せるかのcheckであり、両方必要である。
`03_05` は画面実装前の正式CLI入口として、出力可能なcaseをREADYな出力リストへまとめる。
`04_export_hia_xml.py` は通常 `--xml-export-list-id` で確定済みリストを指定して実行する。

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

### 4. Subscriber Match Correction

CSV/XML受領後、加入者情報が当たっていない、または一部項目だけ合致しているcaseを、人が確認して正しい加入者へ紐付け直せる構造が必要である。
これがないと、検査値が取れていても `subscriber_match_status != MATCHED` のまま出力対象にできず、受領からHIA XML出力までの作業が閉じない。

対象:

- `exam_ledgers.subscriber_match_status` が未突合、不一致、複数候補、要確認のsource。
- 氏名、生年月日、性別、記号、番号、枝番、HIA加入者IDなどの一部だけが合致しているsource。
- CSV/XML原本値は正しそうだが `subscribers` 側の登録値と表記や履歴がずれているsource。
- `exam_export_cases` 作成前、またはcase作成後に加入者が変わる可能性があるsource/case。

必要なこと:

- 未突合・要確認の `exam_ledgers` を検索できるようにする。
- `subscribers` から候補者を検索し、候補の続柄、資格喪失日、HIA加入者ID、記号番号等を並べて比較できるようにする。
- 正しい加入者を選択した場合、`exam_ledgers.subscriber_id`, `subscriber_match_status`, `subscriber_match_method`, `subscriber_match_reason` を更新する。
- 修正操作は履歴に残し、原本CSV/XML値は上書きしない。
- 加入者修正後は、該当sourceの `person_event` 反映、`exam_export_cases` 再構築、`exam_export_case_values` 再構築、case単位checkを再実行できるようにする。
- 一部合致のまま手動で紐付ける場合は、手動確定理由、確定者、確定日時を必須にする。
- 誤った加入者へ出力済みの場合の扱いは別途手順化する。初期版では正式出力済みcaseの加入者修正は警告・停止対象とする。

現状:

- 取込時の自動加入者突合は実装済み。
- HIA加入者IDを使う方向性は整理済み。
- 人が未突合・一部合致を修正する画面/API/履歴テーブルは未実装。

### 5. Basic Info Correction

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

### 6. Postal Code Master

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

### 7. Export Case and Multi-Source Merge

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
- 旧COMBINED ledger方式の試作 `build_combined_exam_ledgers.py` は削除済み。本流にはしない。
- 結合出力用case DDL、case作成、case value採用、case単位checkは `exam_ledgers + exam_item_values` 起点で整備済み。
- case起点exportは `04_export_hia_xml.py` とFastAPI管理画面の出力リスト詳細から実行できる。

### 8. Export Control UI

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
- 出力リストの作成、一覧、詳細確認はFastAPI管理画面に実装済み。
- 出力リスト作成画面では、健診機関コード手入力に加えて、受領フォルダalias一覧から健診機関を検索して追加できる。
- 出力リスト詳細画面から、同じリストを対象に `review` 確認用出力と `official` 本番03フォルダ出力を実行できる。
- `review` 確認用出力では、ZIPを `data/hia_xml_review_exports/event_<event_id>` 配下へ作成し、詳細画面の確認用ZIP一覧からダウンロードできる。ダウンロード時は監査ログへ記録し、ダウンロード後に確認用ZIPを削除する。
- `official` 本番出力では、従来どおり健診機関フォルダ配下の `03_健診結果（アップロードデータ）` に出力し、正式出力履歴とcase/list状態を更新する。
- 出力結果のHIAアップロード作業記帳、個人case詳細からの理由ありOK操作は未実装。

#### 8.1 HIA XML出力リスト画面モック確定メモ

`39_hia_xml_export_run_mock.html` は、出力リスト作成画面の現時点の確認済みモックである。

画面状態は、箱作成前と箱作成後で分ける。

- 箱作成前:
  - リスト状態は `未作成`。
  - 入力する基本情報は、リスト名、対象event、提出日、出力番号。
  - 作成時の初期追加として、`READY`、`理由ありOK` をチェックで選べる。
  - 一時検索条件は置かない。受診月、健診機関、個人指定などは人追加モーダル側で扱う。
  - 作成操作は以下の2つに分ける。
    - `選択した人を追加して作成`: チェックした状態のcaseを初期投入してリストを作る。
    - `リスト作成して人を追加`: 空に近いリストを作り、人追加モーダルへ進む。

- 箱作成後:
  - リスト状態は `DRAFT`。
  - 作成時の初期追加ブロックは非表示にする。
  - 右側に追加済みcase一覧を表示する。
  - `人を追加` からモーダルを開き、検索条件を指定してcaseを追加する。
  - `基本情報補正` は入口だけ置く。補正画面/API/履歴は後続実装とする。

人追加モーダルの操作は以下とする。

- 健診機関名または健診機関コードを部分一致で検索し、サジェスト候補から選択する。
- 氏名カナ、HIA加入者ID、受診月、状態でも検索できる。
- 状態集計カードは追加済みリストではなく検索条件側に置き、`READY`、`理由ありOK`、`ZIP予定`、`確認待ち`、`BLOCKED` などで候補を絞れるようにする。
- 検索結果行の状態タグ直下に `この人を追加`、`追加済み`、`追加不可` を表示する。
- 追加済みリスト側も状態タグ直下に `外す` を表示する。
- 追加済みcaseは検索結果側でボタン非活性にする。ドラッグ追加を実装する場合も、追加済みであることが分かる状態にする。
- 本人確認情報は、保険証記号-番号、氏名カナ、生年月日を縦並びにする。
- 健診機関名、受診日、HIA加入者IDも1セル内に縦並びにする。長い健診機関名は省略表示し、hoverで全体名を確認できるようにする。

画面実装時の最小API候補:

- 出力リスト作成API。
- 出力リスト取得API。
- 出力リスト更新API。
- 出力候補case検索API。
- 出力リストへcase追加API。
- 出力リストからcase削除API。
- 出力リスト確定またはREADY化API。
- 出力リスト指定のXML出力実行API。初期版は実装済み。
- 確認用ZIPダウンロードAPI。初期版は実装済みで、ダウンロード後に確認用ZIPを削除する。

初期画面実装では、テンプレート登録や基本情報補正の本体処理は含めない。
テンプレート登録は別画面/API、基本情報補正は出力リストまたはcase詳細から遷移する後続画面/APIとして扱う。

### 8.2 特定健診チェック

`exam_check_results` は法定健診チェックに加え、特定健診チェック結果を保持できる。
健保からのXMLエラー指摘では、特定健診項目としての構造、コード体系、単位、セクションが問題になるケースが見えてきている。

定期健診の通常運用では、法定健診チェックと特定健診チェックを両方実行する。
事業所向けには法定健診、健保/HIA/支払基金向けには特定健診が必要になるため、どちらか一方だけを標準にはしない。

制度チェックの保持方針:

- 法律・制度に根拠があり、全体共通で継続的に確認するチェックは横持ちで保持する。
- 横持ち対象は、法定健診チェック（まずは労安則44。後続で他法定区分を追加可能）と特定健診チェックまでを基本範囲とする。
- 横持ちにする理由は、一覧、集計、出力可否判定で毎回高速に参照するためである。
- 制度改正など全体ルールが変わる場合は、migration/seed/共通チェック処理の更新で追従する。
- 健保、事業所、納品先、運用都合で追加したい任意チェックは横持ち制度チェックへ混ぜない。
- 任意チェックは後続でルールセット型の柔軟な仕組みに分ける。対象が限定されるため、多少重くなっても出力前・納品前の確認処理として許容する。

初版では、法定チェックとは別に `specific_check_result` / `specific_reason_summary` を更新する。
判断は解釈ではなく、取込済み値の事実確認に限定する。
`specific_*` は元々特定健診チェックを想定していた枠であるため、初期実装では新しい結果カラムを増やさず、この既存枠を正として使う。

初版の対象:

- `03_00_check_imported_exam_ledgers.py` で、受領source単位の特定健診チェックを `exam_check_results` に保存する。
- `03_04_check_exam_export_cases.py` で、出力case単位の特定健診チェックを `exam_check_results` に保存する。
- 年齢判定はevent年度の年度末日を使う。event=2では `event_year = 2026` の年度末 `2027-03-31` 時点の満年齢で40-74歳を特定健診対象とする。
- `dev_phr.event.age_reference_date` は予約/運用上の年齢換算日と混同しないため、特定健診チェックでは参照しない。
- 対象外は `specific_check_result = OK` とし、summaryに対象外理由を残す。
- 法定健診チェックと重なる項目は、法定チェックがOKなら満たしているものとして扱い、特定健診側では重複チェックしない。
- 法定側にない特定健診項目は、namecodeの存在、normalize後のCD/ST値としての妥当性を確認する。
- 特定健診チェックも則44と同じく「制度detail code -> namecode候補 -> OK/MISSING/INVALID」の形へ寄せる。
- 特定健診用detail codeは、則44の `4401001001` などと衝突しないよう先頭を `10` とする。
- 特定健診用detail codeは `10` + チェックカテゴリ + 項番の体系で採番する。具体的な桁配分は初期seed作成時に確定するが、カテゴリ単位で一覧・集計・追加ができる形を優先する。
- 必要namecode群は `dev_phr.exam_item_group_members` 等のルールマスタで管理する。

初版で確認する特定健診側の主なnamecode:

- `9N141000000000011` 採血時間（食後）
- `9N501000000000011` メタボリックシンドローム判定
- `9N506000000000011` 保健指導レベル
- `9N511000000000049` 医師の診断（判定）
- `9N516000000000049` 医師名
- `9N701000000000011` 服薬1（血圧）
- `9N706000000000011` 服薬2（血糖）
- `9N711000000000011` 服薬3（脂質）
- `9N716000000000011` 既往歴1（脳血管）
- `9N721000000000011` 既往歴2（心血管）
- `9N726000000000011` 既往歴3（腎不全・人工透析）
- `9N731000000000011` 貧血
- `9N736000000000011` 喫煙
- `9N741000000000011` 20歳からの体重変化
- `9N746000000000011` 30分以上の運動習慣
- `9N751000000000011` 歩行又は身体活動
- `9N756000000000011` 歩行速度
- `9N771000000000011` 食べ方2（就寝前）
- `9N781000000000011` 食習慣
- `9N796000000000011` 睡眠
- `9N808000000000011` 特定保健指導の受診歴

残る判断:

- `specific_check_result` は初版では記帳に留め、`exam_export_cases.export_readiness_status` にはまだ反映しない。
- 報告区分とプログラムコードが年齢判定と矛盾している場合の停止/警告方針は後続で決める。
- 特定健診の問診項目をどこまで必須チェックに含めるかは、健保/HIAエラー実績を見ながら追加する。
- 理由ありOKや妊娠等の条件付き不足許可は、既存の理由ありOK枠へ統合する。

### 9. Master and Facility Admin UI

健診機関、フォルダalias、eventなど、取込・出力・画面絞り込みに必要な運用マスタを画面から扱う。

現状:

- event設定画面は実装済み。
- 健診機関・alias管理画面は実装済み。
- `exam_facilities` の新規作成/更新、キーワード絞り込みに対応済み。
- `medical_folder_aliases` の新規作成/更新、キーワード絞り込みに対応済み。
- 操作は監査ログへ記録する。

未実装:

- 健診機関名/コードの部分一致サジェスト。
- CSVフォーマット/マッピング管理。
- 紙健診テンプレート管理。
- normalize辞書、検査項目、出力ポリシー、郵便番号住所マスタなどの共通マスタ管理。

### 10. HIA Upload and Delivery Status

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

### 11. Paper Input

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

### 12. Standardization and Mapping Intelligence Layer

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
4. 加入者未突合・一部合致の修正構造と履歴。
5. 基本情報補正のDB構造と履歴。
6. 郵便番号マスタと住所補完。
7. `exam_export_cases` / `exam_export_case_sources` / `exam_export_case_values` のDDL。
8. case作成、case value採用、case単位check。
9. case起点のXML出力。
10. XML出力制御UI。
11. HIAアップロード、HIAダウンロード、健保・事業所納品ステータス。
12. 紙入力画面。
13. 標準化・マッピング知見レイヤー。

納品が迫る場合は、1、2、4、5、6を先に進める。
XMLとCSVを合わせて法定を満たす必要が出た時点で、7、8、9を優先する。

## Current Risk

- 住所がHIAで必須となるため、CSV/XML/予約/加入者台帳のどこにも住所がない人の扱いを決める必要がある。
- 加入者未突合や一部合致を直す口がないと、受領済みの結果を正しい人に寄せられず、出力リストへ追加できない。
- HIAダッシュボードCSVは年度で状態が上書きされるため、過年度eventの状態を最新テーブルだけで判定すると誤る。
- 基本情報補正をDB直接修正で続けると、誰が何を直したか追えなくなる。
- 複数ファイル結合を取込単位の `exam_ledgers` 上で直接処理すると、原本証跡と清書値が混ざる。
- HIAアップロード後の人手状態を持たないと、正式出力済みXMLと実際のアップロード済みがズレる。
