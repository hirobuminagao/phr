# 健診結果手入力 仮登録から本データ反映までの設計

## 目的

紙健診、XML/CSV不足分の補足、再提出補正を、画面から安全に登録できるようにする。

手入力は入力ミスの影響が大きいため、画面入力後にいきなり `exam_ledgers` / `exam_item_values` の本データを作らない。
まず仮登録として保存し、確認後に本データへ反映する。

本資料では、以下を設計範囲とする。

- 仮データ登録
- 仮登録リストからの削除
- 仮登録から本データへの反映
- 本データ反映後の法定チェック
- 本データ反映後の特定健診チェック
- 必要なDB、画面、スクリプト改修範囲

## 前提

- 手入力画面は `apps/health_exam_admin` に初期実装済み。
- 手入力画面は仮登録への下書き保存まで接続済みである。
- 仮登録リストから本データ反映できる。反映時に `exam_ledgers` / `exam_item_values` を作成し、draftを `APPLIED` に更新する。
- 本データの受け皿は既存の `health_exam_result.exam_ledgers` と `health_exam_result.exam_item_values` を使う。
- case作成、case採用値作成、caseチェックは既存の以下stepへ乗せる。
  - `03_00_check_imported_exam_ledgers.py`
  - `03_01_build_exam_export_cases.py`
  - `03_02_build_exam_export_case_values.py`
  - `03_04_check_exam_export_cases.py`
- 法定健診・特定健診の不足判定結果は `exam_item_values` に持たず、`exam_check_results` および `exam_case_check_review_items` 側で管理する既存方針を維持する。
- 手入力値も、正式反映後はCSV/XML由来値と同じ `exam_item_values` として扱う。

## レイヤー方針

### 仮登録レイヤー

仮登録は、まだ正式な受領sourceではない。
そのため、仮登録中は `exam_ledgers` / `exam_item_values` へ直接書かない。

理由:

- 入力途中や確認前の値がcase作成・出力候補へ混ざる事故を避ける。
- 「入力中」「確認中」「削除済み」を本データと分離する。
- 仮登録リストから削除しても、正式sourceや既存caseへ影響させない。
- 将来、入力者と承認者を分ける余地を残す。

### 本データレイヤー

仮登録を確定した時点で、正式な手入力sourceとして以下を作る。

- `exam_ledgers.source_type = 'PAPER'` または `'MANUAL'`
- `exam_item_values.ledger_type = exam_ledgers.source_type`
- `exam_item_values.ledger_id = exam_ledgers.exam_ledger_id`

現時点の画面・運用名称としては「紙」「補足」「再提出補正」がある。
DB上のsource_type候補は以下とする。

| source_type | 用途 |
|---|---|
| `PAPER` | 紙健診を起点に手入力したsource |
| `MANUAL` | XML/CSVの不足補足、再提出補正など紙以外の手入力source |

ただし、既存の `exam_ledgers.source_type` が `XML/CSV/PAPER` 想定で整理されているため、初期実装では `PAPER` を優先し、補足・再提出補正は別カラムで目的を持つ案を第一候補とする。

## 新規テーブル案

### `manual_exam_entry_drafts`

手入力1件分の仮登録ヘッダー。
正式反映前の入力sourceの箱である。

主なカラム案:

| カラム | 内容 |
|---|---|
| `manual_exam_entry_draft_id` | PK |
| `event_id` | 対象event |
| `draft_status` | `DRAFT` / `READY` / `APPLIED` / `ERROR` |
| `entry_purpose` | `PAPER_ONLY` / `SUPPLEMENT` / `RESUBMISSION_FIX` |
| `exam_export_case_id` | 補足元case。任意 |
| `subscriber_id` | 加入者。任意だが、本反映時は原則必要 |
| `hia_subscriber_id` | HIA加入者ID |
| `person_id_custom` | PHR内person key |
| `insurer_number` | 保険者番号 |
| `insurance_symbol` | 記号 |
| `insurance_number` | 番号 |
| `insurance_branch_number` | 枝番 |
| `name_full` | 氏名 |
| `name_kana` | 氏名カナ |
| `birthdate` | 生年月日 |
| `gender_code` | 性別 |
| `exam_facility_id` | 健診機関ID |
| `facility_code` | 健診機関コード |
| `facility_name` | 健診機関名 |
| `facility_document_id` | 健診機関側ドキュメントID |
| `exam_date` | 健診実施日 |
| `note` | 備考 |
| `created_by_app_user_id` | 作成者 |
| `updated_by_app_user_id` | 更新者 |
| `applied_by_app_user_id` | 本反映者 |
| `applied_at` | 本反映日時 |
| `applied_exam_ledger_id` | 本反映後の `exam_ledgers.exam_ledger_id` |
| `created_at` / `updated_at` | 作成・更新日時 |

制約案:

- `draft_status = APPLIED` の場合、`applied_exam_ledger_id` を保持する。
- `DELETED` は物理削除ではなく論理削除を基本とする。
- 同じ仮登録を二重反映しないため、反映処理は `draft_status` と `applied_exam_ledger_id` を確認する。

### `manual_exam_entry_draft_values`

手入力された検査値の仮登録明細。

主なカラム案:

| カラム | 内容 |
|---|---|
| `manual_exam_entry_draft_value_id` | PK |
| `manual_exam_entry_draft_id` | 親draft |
| `namecode` | JLAC/namecode |
| `namecode_display_name` | 項目名 |
| `occurrence_no` | 同namecode複数時の連番 |
| `raw_value_type` | `PQ` / `CD` / `CO` / `ST` |
| `raw_value` | 入力値 |
| `unit` | 単位 |
| `code_system` | CD/COのコード体系 |
| `code_value` | CD/COのコード |
| `code_display` | CD/COの表示名 |
| `method_code` | 検査方法コード |
| `method_name` | 検査方法名 |
| `identity_item_code` | 法定/特定の横持ち同一性項目コード等 |
| `identity_item_name` | 同一性項目名 |
| `include_flag` | 本反映対象か |
| `input_status` | `ENTERED` / `EMPTY` / `SKIPPED` |
| `note` | 値単位の備考 |
| `created_at` / `updated_at` | 作成・更新日時 |

方針:

- 入力欄が空の項目は、初期実装では明細を作らない。
- 「値なしだが確認済み」を表現する必要が出た場合は、`input_status = SKIPPED` として明細を作る。
- 本反映時は `include_flag = 1` かつ値がある行を `exam_item_values` へ作成する。

### `manual_exam_entry_draft_audit_logs`

仮登録の状態変更履歴。

最低限、以下を保持する。

- draft作成
- 値更新
- 仮登録削除
- 本データ反映
- 本反映失敗

監査ログは `phr_app.app_audit_logs` にも残すが、業務データ側でdraftの履歴を追えるよう、draft専用auditも持つ。

## 仮登録画面フロー

### 1. 入力

画面で以下を入力する。

- イベント
- 健診機関ドキュメントID
- 入力目的
- 健診機関コード、健診機関名
- 健診実施日
- HIA加入者ID、記号、番号、枝番、氏名、氏名カナ、生年月日、性別
- 検査項目値

caseから選択した場合:

- caseの基本情報を入力補助として反映する。
- case側の採用値は変更しない。
- 手入力値は、あくまで新しい手入力sourceの仮登録として扱う。

### 2. 下書き作成

手入力は入力時間が長く、項目数も多いため、最初に下書きを作る。
下書きは `manual_exam_entry_drafts.draft_status = 'DRAFT'` として保存する。

下書き作成時は、必須情報がすべて揃っていなくても保存できる。
ただし、画面がどのeventの下書きかを失わないよう、最低限 `event_id` は保持する。

初期実装のボタン:

- `下書き保存`
- `登録チェックして仮登録`

`下書き保存` は `DRAFT` のまま保存する。
`登録チェックして仮登録` は、保存後にバリデーションを実行し、問題なければ `READY` へ進める。

### 3. 自動保存

下書き作成後は、自動保存を有効にする。

目的:

- 入力途中のブラウザ終了、画面遷移、通信切断による入力喪失を防ぐ。
- 手入力中の事故を本データに混ぜず、下書きだけに閉じ込める。
- 項目数が多い画面でも、作業者が保存操作を意識しすぎずに入力できるようにする。

基本仕様:

- 自動保存は `DRAFT` のみに行う。
- 自動保存では `READY` / `APPLIED` へは進めない。
- 自動保存では `exam_ledgers` / `exam_item_values` を作らない。
- 入力変更後、2〜3秒操作が止まったらまとめて保存する。
- 健診機関選択、加入者選択、case選択など大きな状態変更時は即保存してよい。
- ページ離脱時に未保存があれば警告する。
- 画面には `保存済み HH:MM` / `未保存あり` / `保存中` / `保存失敗` を表示する。

実装方式:

- debounce保存とする。
- 入力のたびにDBへ書かず、最後の入力から一定時間後にまとめて保存する。
- 自動保存APIはdraftヘッダーと入力済み値を受け取り、同一draftへ上書き保存する。
- 空欄になった検査値は、初期実装では `manual_exam_entry_draft_values` から削除する。
- 将来「値なし確認済み」を表現する必要が出た場合だけ、`input_status = 'SKIPPED'` または `EMPTY` の行を保存する。

同時編集:

- 初期実装では、1つのdraftを1人が開いて編集する前提とする。
- 後続で複数人編集を扱う場合は、`updated_at` または `version` による楽観ロックを追加する。
- 自動保存時にサーバ側の `updated_at` が画面保持値より新しければ、保存せず衝突警告を出す。

### 4. 登録前チェック

`登録チェックして仮登録` を押した時は、必ず直前の入力内容を保存した上でバリデーションする。

バリデーションで確認すること:

- `event_id`
- `entry_purpose`
- `exam_date`
- `facility_code` または `exam_facility_id`
- 加入者特定情報
  - `subscriber_id` または `hia_subscriber_id`
  - もしくは、記号・番号・氏名カナ・生年月日等から加入者候補を選択済み
- 1件以上の検査値
- CD/CO系のコード値が候補として成立していること
- PQ系の数値が数値として成立していること
- 日付が `YYYY-MM-DD` として成立していること
- 随時中性脂肪など、採血時間が必要な値を入力した場合に採血時間があること

バリデーションNGの場合:

- `draft_status` は `DRAFT` のままにする。
- エラー一覧を画面へ戻す。
- 該当項目へスクロールできるようにする。
- 本データ反映は不可。

バリデーションOKの場合:

- `draft_status = READY` にする。
- 仮登録リストから本データ反映できる状態にする。

### 5. 仮登録リスト

仮登録済み一覧を表示する。

主な表示項目:

- 状態
- 入力目的
- 健診機関
- 健診実施日
- HIA加入者ID
- 氏名カナ
- 検査値件数
- 作成者
- 作成日時
- 本反映済みledger

操作:

- 詳細表示
- 編集
- 仮登録削除
- 本データ反映

### 6. 仮登録リストから削除

本反映前のdraftは削除できる。
削除は物理削除ではなく `draft_status = DELETED` とする。

削除条件:

- `draft_status IN ('DRAFT', 'READY', 'ERROR')`
- `APPLIED` は削除不可

削除時:

- draft auditへ削除理由を記録する。
- `phr_app.app_audit_logs` にも操作ログを残す。
- `exam_ledgers` / `exam_item_values` へは影響しない。

### 7. 本データ反映

`READY` のdraftを正式sourceへ反映する。

反映処理:

1. draft状態を再確認する。
2. `exam_ledgers` を1件作成する。
3. draft valuesから `exam_item_values` を作成する。
4. draftを `APPLIED` に更新し、`applied_exam_ledger_id` を保持する。
5. draft audit logを残す。

`exam_ledgers` への主な設定:

- `source_type = 'PAPER'` または `'MANUAL'`
- `event_id`
- `subscriber_id`
- `hia_subscriber_id`
- `person_id_custom`
- `facility_code`
- `facility_name`
- `exam_facility_id`
- `facility_document_id`
- `exam_date`
- 基本情報raw/export値
- `subscriber_match_status`
- `subscriber_match_method = 'manual_entry'`
- `xml_export_status = 'PENDING'`
- `merge_status = 'SOURCE_SINGLE'`

`exam_item_values` への主な設定:

- `ledger_type = exam_ledgers.source_type`
- `ledger_id = exam_ledgers.exam_ledger_id`
- `event_id`
- `subscriber_id`
- `hia_subscriber_id`
- `namecode`
- `raw_value_type`
- `raw_value`
- `unit`
- `code_system`
- `code_value`
- `code_display`
- `method_code`
- `method_name`
- `occurrence_no`
- `source_ledger_type = exam_ledgers.source_type`
- `source_ledger_id = exam_ledgers.exam_ledger_id`
- `value_source_role = 'PRIMARY'`
- `normalize_status = 'OK'`
- `normalize_reason = 'MANUAL_ENTRY'`
- `validation_status = 'VALID'`

実装上の入口:

- 画面: `/manual-exam-entry-drafts` の `本データ反映` ボタン。
- API: `POST /api/manual-exam-entry-drafts/{draft_id}/apply`
- 権限: `manual_exam_entry.manage`
- 反映対象: `draft_status IN ('DRAFT', 'READY', 'ERROR')` かつ入力値が1件以上あるdraft。
- 反映後: `draft_status = 'APPLIED'` とし、同じdraftの再反映を禁止する。

二重反映防止:

- `manual_exam_entry_drafts.applied_exam_ledger_id` がある場合は再反映不可。
- 反映処理はトランザクションで行う。
- 反映中に失敗した場合はrollbackし、draftを `ERROR` にするか、エラーをauditに残して `READY` のまま再実行可能にするかを実装時に決める。

正式反映後の戻し:

- 通常運用では、正式反映済みの `exam_ledgers` / `exam_item_values` を物理削除しない。
- 本番運用で取り消す場合は、削除ではなく無効化・再作成で扱う。
- 開発/試走時だけ、現場管理者以上の画面から「手入力正式ledgerをdraftへ戻す」導線を用意する。
- 戻し対象は、手入力由来の `exam_ledgers.source_type IN ('PAPER', 'MANUAL')` とし、`manual_exam_entry_drafts.applied_exam_ledger_id` でdraftへ辿れるものを基本にする。
- 戻し操作は通常の個人case一覧には置かない。caseは正式ledgerから作られる派生物であり、戻し判断の責務はledger側に寄せる。
- 戻し後は `03_01`〜`03_04` 相当のcase再生成・case値再作成・caseチェックを再実行する。
- 戻し実行時は、正式ledgerを物理削除せず `exam_ledgers.row_status = 'REVERTED_TO_DRAFT'` として残す。
- 紐づく `manual_exam_entry_drafts` は `draft_status = 'DRAFT'`、`applied_*` をNULLに戻し、再編集・再反映できる状態へ戻す。
- 既に出力リストへ掲載されている正式ledgerは、初期版では戻し不可とする。
- case構成sourceやcase採用値に使われていても戻し可能とする。戻し時にcase sourceを無効化し、該当source由来の採用値を削除する。
- 戻し時は該当 `exam_export_case_sources.source_status` を `REVERTED_TO_DRAFT` にし、該当source由来の `exam_export_case_values` は削除する。
- 戻し操作は `manual_exam_entry_draft_audit_logs` と個人情報監査ログへ記録する。

初期実装:

- DB書き込み失敗時はrollbackし、draft状態は変更しない。
- 失敗内容は画面エラーとして返す。
- `ERROR -> READY` の明示操作は後続とする。

## チェック処理への接続

本反映後は、既存の処理順へ乗せる。

推奨手順:

1. 仮登録を本データ反映
2. `03_00_check_imported_exam_ledgers.py`
3. `03_01_build_exam_export_cases.py`
4. `03_02_build_exam_export_case_values.py`
5. `03_04_check_exam_export_cases.py`

画面上では、本データ反映完了後に以下を案内する。

> 本データへ反映しました。出力caseへ反映するには、健診結果処理実行の step5〜7 を実行してください。

画面の番号表記に合わせる場合:

- `03_00 受領単位チェック`
- `03_01 case更新`
- `03_02 case値更新`
- `03_04 case単位チェック`

### 法定チェック

source単位:

- `03_00` で `exam_ledgers` 単位の法定チェックを行う。
- 手入力sourceも `exam_ledgers` に入っているため、XML/CSVと同じ入口で扱う。

case単位:

- `03_04` でcase採用値に対して法定チェックを行う。
- 不足があれば `exam_case_check_review_items` に確認対象を作る。
- 理由ありOKは既存のcase確認事項記帳フローで管理する。

### 特定健診チェック

source単位:

- 初期実装では、source単位の特定健診チェックは必須にしない。
- source単位は「このファイル/sourceが取込として成立しているか」を見る位置づけとし、制度上の出力可否はcase単位を正とする。

case単位:

- `03_04` でcase採用値に対して特定健診チェックを行う。
- 対象判定は年度末年齢を基準とする。
- 年齢対象外はOKとは別表示にし、`対象外` として扱う。
- 年齢や生年月日が不足して対象判定できない場合は `判定不能` とする。

## 既存処理の改修範囲

### DB

新規:

- `manual_exam_entry_drafts`
- `manual_exam_entry_draft_values`
- `manual_exam_entry_draft_audit_logs`

DDL:

- `sql/ddl/health_exam_result/0250_health_exam_result__manual_exam_entry_drafts.sql`

Migration:

- `sql/migrations/health_exam_result/20260824_001_health_exam_result_create_manual_exam_entry_drafts.sql`
- `sql/migrations/health_exam_result/20260824_002_health_exam_result_fix_manual_exam_item_value_ledger_type.sql`

既存テーブル追加検討:

- `exam_ledgers.source_type` の値として `MANUAL` を許容するか確認。
- `exam_item_values.value_source_role` の値として `MANUAL_PRIMARY` 等を追加するか確認。
- enum制約は現状varcharのため、DB制約追加は不要の見込み。

### 画面

対象:

- `apps/health_exam_admin/templates/manual_exam_entry_drafts.html`
- `apps/health_exam_admin/templates/manual_exam_entry.html`
- `apps/health_exam_admin/main.py`

初期画面:

- `/manual-exam-entry-drafts` を「健診結果仮登録リスト」とする。
- HOMEには仮登録リストと手入力画面の導線を置く。通常作業は仮登録リストを起点にする。
- 新規登録は、仮登録リスト上で「人を選んで仮登録」または「caseを選んで仮登録」を選ぶ。
- 人/caseの選択は遷移先の手入力画面ではなく、仮登録リスト画面上のモーダルで行う。
- 選択した時点で `manual_exam_entry_drafts` にレコードを作成し、仮登録リストへ表示する。
- 手入力画面へは、仮登録リスト行の `入力へ` から `draft_id` 指定で遷移する。
- 手入力画面は `draft_id` の基本情報を読み込み、加入者・case・健診機関・受診日などを反映する。
- リストは人ごとに表示し、登録者、更新者、入力値数、反映状態を確認できるようにする。
- 仮登録リスト上部のサマリカードは、全件、DRAFT、READY、APPLIED、ERRORで一覧を絞り込む。
- 仮登録リスト上では、健診機関未設定の `未設定` テキストおよび `選択` ボタンから健診機関選択モーダルを開ける。
- 健診機関と健診実施日は、仮登録リスト上で後から更新できる。
- DDL/migration未適用環境では、画面は表示しつつ「仮登録テーブル未適用」として落ちないようにする。
- `apps/health_exam_admin/static/app.js`
- `apps/health_exam_admin/static/app.css`

追加する画面/操作:

- 仮登録保存ボタン
- 仮登録リスト
- 手入力画面での仮登録読み込み
- 仮登録削除
- 本データ反映
- 本反映後の処理実行案内

実装済みの画面構成:

1. 仮登録リストで人/caseを選んでdraft IDを発行する。
2. 仮登録リストから `draft_id` 付きで手入力画面を開く。
3. 手入力画面で基本情報、健診機関、受診日、検査値を入力する。
4. 手入力画面で下書き保存する。
5. 基本情報が入っている場合、検査値入力時に自動で下書き保存する。
6. 仮登録リストでDRAFT/READY/APPLIED/ERRORを確認し、必要に応じて削除する。

未実装:

- `READY` 化する登録前チェック。
- ページ離脱時の未保存警告。
- `ERROR -> READY` の明示操作。
- `03_00`〜`03_04` が手入力sourceを期待どおり処理するかの試走確認。

### 手入力画面の実装済み仕様

- 左下の入力対象フロートに `draft ID`、HIA ID、記号番号、健診機関/受診日を表示する。
- `下書き保存` ボタンを持つ。
- 検査値入力は `manual_exam_entry_draft_values` へ保存する。
- 空欄に戻した検査値は、保存時に `manual_exam_entry_draft_values` から削除する。
- 検査項目全体は `入力済み数/総項目数` で表示する。
- 検査カテゴリ見出しも、例: `身体計測 1/9項目` のようにカテゴリ単位の `入力済み数/総項目数` を表示する。
- 下書き読込時は、保存済み検査値も画面に復元する。
- 検査値全クリア時は確認ダイアログを出し、保存済みdraftがある場合は空の値として下書きへ反映する。

### 仮登録削除の実装済み仕様

- `manual_exam_entry.edit` を持つ利用者は、仮登録作成、基本情報変更、検査値下書き保存ができる。
- `manual_exam_entry.manage` を持つ現場管理者以上は、`APPLIED` 以外のdraftを削除できる。
- 削除は画面内モーダルで確認する。ブラウザ標準confirmは使わない。
- 削除確認モーダルは画面中央に表示する。
- 削除完了メッセージには氏名、カナ等の機微情報を出さず、`draft {id}` のみを表示する。
- 実装上は `manual_exam_entry_drafts` を物理削除する。関連する `manual_exam_entry_draft_values` / `manual_exam_entry_draft_audit_logs` はFKのCASCADEで削除される。

### 手入力正式ledger管理の実装済み仕様

- 現場管理者以上の画面 `/admin/manual-exam-ledgers` を追加する。
- 通常の個人case一覧には危険操作を追加しない。
- `source_type IN ('PAPER', 'MANUAL')` の正式ledgerを対象にする。
- `manual_exam_entry_drafts.applied_exam_ledger_id` でdraftとの紐づきを表示する。
- 1行で、正式 `exam_item_values` 件数、draft値件数、case source件数、case採用値件数、出力リスト掲載件数を確認できる。
- draftの作成者、更新者、正式反映者を表示し、作業担当者単位で絞り込める。
- 作業担当者による絞り込みは、作成者・更新者・正式反映者のいずれかに一致するものを対象にする。
- 画面上では「戻し候補」または「要確認」を表示する。
- 安全条件を満たす行だけ「戻す」ボタンを表示する。
- 戻し不可条件は、draft紐づきなし、出力リスト掲載ありなど。
- case採用値ありの行は戻し可能だが、戻し実行時に採用値が解除されることを画面に表示する。
- 戻し後の導線として、健診結果処理画面へのリンクを表示し、step5〜7の再実行を促す。
- この画面の個人情報閲覧は監査ログに記録する。

### スクリプト

初期実装では、画面API内の共通関数からdraftを本データ反映する。
処理が重くなった場合は、後続で `script_lib` へ分離し、既存の処理実行画面と同様にjob化する。

### 既存step

確認/改修対象:

- `03_00_check_imported_exam_ledgers.py`
  - `source_type = PAPER/MANUAL` を対象に含める。
  - `row_status = REVERTED_TO_DRAFT` は対象外にする。
- `03_01_build_exam_export_cases.py`
  - 手入力sourceをcase構成sourceに含める。
  - `row_status = REVERTED_TO_DRAFT` は対象外にする。
- `03_02_build_exam_export_case_values.py`
  - XML/CSV/手入力sourceの優先順位を扱う。
  - 基本方針はXML優先、CSV補足、手入力補足。デフォルト採用は XML -> CSV -> PAPER/MANUAL とする。
  - 紙のみの場合は手入力sourceがprimaryになる。
- `03_04_check_exam_export_cases.py`
  - 手入力値もcase採用値としてチェックする。
  - 法定チェック、特定健診チェックの両方へ接続する。

## source優先順位

目的別に優先順位を分ける。

### 紙のみ

- 手入力sourceをprimaryとする。
- 既存XML/CSVがない前提。

### XML/CSV補足

- XMLがある場合はXMLをprimary。
- CSVがある場合はCSVをsupplement。
- 手入力は不足補足としてsupplement。
- 手入力値が既存値を上書きする場合は、明示的な補正理由を必要とする。

### 再提出補正

- 再提出補正は、既存値より優先する可能性がある。
- 初期実装では、自動上書きせず、case値作成時のprecedence ruleまたは手入力source優先フラグで扱う。
- どの項目を置き換えたかをauditで追える必要がある。

## 未決事項

| 論点 | 推奨初期方針 |
|---|---|
| `source_type` を `PAPER` のみで始めるか、`MANUAL` も追加するか | まず `PAPER` を正式採用し、補足/再提出は `entry_purpose` で表現する |
| 仮登録保存時にnormalizeまで行うか | 保存時は軽い入力チェックのみ。本反映時または反映直後にnormalize |
| 本反映失敗時のdraft状態 | `ERROR` にして、再実行には `READY` 戻し操作を必要にする |
| 手入力値が既存case値と競合した場合 | 初期は自動上書きしない。case値更新側のprecedence設計で制御 |
| 仮登録削除を物理削除にするか | 初期実装は物理削除。APPLIEDは削除不可 |
| 承認者を分けるか | 初期は分けない。auditだけ残す |
| 自動保存を最初から入れるか | 実装済み。基本情報があり、検査値が入力された場合に自動保存する |
| 自動保存間隔 | 入力停止後およそ0.9秒のdebounce保存 |
| 自動保存失敗時 | 画面に保存エラーを表示する。READY化・本反映は未実装 |
| 下書き保存時の必須チェック | 入力値があること、かつ基本情報のいずれかが入っていることを最低条件にする |
| 空欄に戻した検査値 | 保存時にdraft_valuesから削除する |

## 実装順

1. DDL/migration作成 済
   - `manual_exam_entry_drafts`
   - `manual_exam_entry_draft_values`
   - `manual_exam_entry_draft_audit_logs`
2. 仮登録リストで人/case選択時にdraftを作成 済
3. 仮登録リストから `draft_id` で手入力画面を開く 済
4. 手入力画面に `下書き保存` を接続 済
5. 下書き作成後の自動保存APIを接続 済
6. 保存状態表示を追加 済
   - `未保存`
   - `保存中`
   - `保存済 n件`
   - `保存エラー`
   - `入力値なし`
   - `基本情報不足`
7. 仮登録リスト表示 済
8. 仮登録削除 済
9. 仮登録から本データ反映 済
10. 本反映後の処理実行案内 済

未実装:

- `READY` 化する登録前チェック。
- ページ離脱時の未保存警告。
- `ERROR -> READY` の明示操作。
- `03_00`〜`03_04` が手入力sourceを期待どおり処理するかの試走確認。

### 本データ反映の実装済み仕様

- `manual_exam_entry.manage` を持つ利用者だけが本データ反映できる。
- 反映前に画面内モーダルで確認する。ブラウザ標準confirmは使わない。
- 入力値が0件のdraftは反映できない。
- 反映時に `exam_ledgers.source_type` を `PAPER` または `MANUAL` にする。
- `entry_purpose = PAPER_ONLY` は `PAPER`、それ以外は `MANUAL` とする。
- `exam_item_values` は手入力済み値だけを作成する。
- 手入力値は `normalize_status = OK`、`normalize_reason = MANUAL_ENTRY`、`validation_status = VALID` とする。
- `manual_exam_entry_drafts.applied_exam_ledger_id` で正式ledgerへ辿れる。
- 反映完了メッセージでは `ledger id` と入力値件数を表示する。

後続:

9. ページ離脱時の未保存警告を追加 後続
10. `登録チェックして仮登録` を追加 後続
   - 直前保存
   - バリデーション
   - NG時は該当項目へ戻す
   - OK時は `READY`
11. `03_00` が手入力sourceを対象にすることを確認/修正 後続
12. `03_01` が手入力sourceからcaseを作ることを確認/修正 後続
13. `03_02` が手入力sourceを採用値候補に入れることを確認/修正 後続
14. `03_04` で法定チェック・特定健診チェックへ反映 後続

## この設計で守ること

- 仮登録は本データへ混ぜない。
- 自動保存は下書きだけに閉じる。
- 登録前チェックを通らない限り `READY` にしない。
- 本データ反映は明示操作にする。
- 本反映後は既存の `exam_ledgers` / `exam_item_values` / case / check に乗せる。
- 制度チェック結果を `exam_item_values` に混ぜない。
- 理由ありOKや確認事項は `exam_case_check_review_items` 側で扱う。
- 入力値の由来を追えるように、draft、反映ledger、反映item_valuesを紐付ける。
