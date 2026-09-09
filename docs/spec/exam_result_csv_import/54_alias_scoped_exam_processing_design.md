# 受領alias単位の健診結果処理実行 設計

## 1. 目的と範囲

`event + 受領フォルダalias`を単位として、次を一連で再実行できるようにする。

1. `01_scan_files.py`
2. `02_import_xml.py`
3. `02_02_exam_result_csv_import.py`
4. `03_00_check_imported_exam_ledgers.py`
5. `03_01_build_exam_export_cases.py`
6. `03_02_build_exam_export_case_values.py`
7. `03_04_check_exam_export_cases.py`

alias未指定時のevent全体実行は維持する。`03_05`以降の出力リスト作成・XML出力は自動実行しない。

### 1.1 1回の実行内で扱う4つの対象集合

画面操作は1回でも、内部の対象集合は次の4つに分ける。

1. **CSV受領処理**: 選択alias配下のCSVをscanし、対象CSVをimportする。
2. **XML受領処理**: 選択alias配下のXML/ZIPをscanし、対象XMLをimportする。
3. **case成立データ処理**: 今回または過去に取り込まれ、選択alias由来sourceを持つ既存case、および今回case成立条件を満たしたledgerを全sourceで再結合し、値作成・caseチェックまで行う。
4. **case未成立ledger再判定**: 選択aliasで取り込み済みだがcaseに属していないledgerを再判定する。加入者突合等が解決済みなら新規caseを作り、未解決なら理由付きでスルーする。

3で既存caseと今回成立caseが重複した場合はcase IDで一意化し、1回だけ後続処理する。4から新規作成されたcaseも同じ後続集合へ追加する。CSVとXMLはファイル種別・status・再取込判断が異なるため対象集合を分けるが、scan自体はaliasフォルダに対して1回でよい。

加入者未突合などcase成立条件を満たさないledgerは、import成功として保持し、case工程では対象外にする。この対象外は処理全体の失敗にせず、理由別件数とledger IDを結果へ残す。

## 2. スコープの正

画面とCLIは`event_id`と`medical_folder_alias_id`を渡す。健診機関IDだけでは絞らない。

- 同一施設に複数aliasが存在し得る。
- alias名・施設名は変更され得る。
- 1 ZIP内に複数施設のXMLがある既存仕様を維持する。
- フォルダ名の曖昧一致は誤処理になる。

`health_exam_result.file_receipts`へnullableな`medical_folder_alias_id`と`(event_id, medical_folder_alias_id)`索引を追加する。クロススキーマ外部キーは設定しない。scan時に必ずalias IDを記帳し、alias無効化後も履歴として保持する。

既存receiptは専用backfillで、event一致かつ`relative_path`の先頭フォルダと`src_folder_raw`が完全一致し、候補が1件の行だけ更新する。0件・複数候補は更新せず報告する。健診機関IDだけで補完しない。

## 3. 工程別仕様

### 3.1 scan

`--medical-folder-alias-id`指定時は、そのeventに属する当該aliasフォルダだけをscanする。別event、無効、手動判断中、施設未解決なら更新前に停止する。未知フォルダ全体の探索はalias指定時に行わない。

同一event・同一相対パス・同一SHAの既存receiptを検出した場合、新規receiptは作らない。既存の`medical_folder_alias_id`がNULLなら、現在走査中のalias IDを補完する。既にalias IDがある場合は、同じ値でも別の値でもscanから上書きしない。この補完はevent全体scanでもalias指定scanでも行うため、migration後に全体scanを1回実行すれば、現在も同じaliasフォルダに存在する既存ファイルは順次補完される。削除・移動済みファイルと曖昧な過去パスは専用backfillの確認対象とする。

### 3.2 XML/CSV import

両importは`file_receipts.event_id + medical_folder_alias_id`で対象を選ぶ。status、`--include-imported`、dry-runはこの条件とANDで適用する。複数施設XMLを含むZIPは受領元aliasのファイルとして従来どおり取り込み、XML内施設の検証は緩和しない。

### 3.3 ledger check

`03_00`は対象aliasのreceiptへ紐づく`exam_ledgers.file_receipt_id`だけを確認する。直接receiptを持たないMANUAL/PAPERはこの工程の対象外とする。

### 3.4 case作成

`03_01`は二段階で処理する。

1. alias由来ledgerから`subscriber_id + exam_date + exam_facility_id`の対象受診キーを抽出する。
2. その受診キーに属するevent内の全有効sourceを読み直してcaseを構築する。

第2段階ではMANUAL/PAPER等も含める。alias由来sourceだけでcaseを再構築し、既存の補完値を落としてはならない。複数ACTIVE caseや保険者番号不整合は既存どおり停止し、この機能で自動統合・自動補完しない。

case工程へ渡す入口は次の2集合とする。

- **今回取込集合**: 今回のXML/CSV import runで作成・更新対象になったalias由来ledger。
- **既存再処理集合**: 過去分を含め、alias由来receiptへ紐づく有効ledgerをsourceとして持つACTIVE case。
- **case未成立再判定集合**: alias由来receiptへ紐づくが、現在ACTIVE caseのsourceになっていない取り込み済みledger。

3集合から得た対象受診キーを統合し、同一受診の全sourceを読み直す。既存再処理集合を含めることで、マッピング・normalize・case結合ロジック変更後に、ファイルを再importしなくても対象aliasのcaseだけを再評価できる。case未成立再判定集合を含めることで、加入者突合等の修正後にファイルを再importせずcase作成へ進める。

### 3.5 case未成立ledger

次はcase工程でスキップし、処理全体を停止しない。

- 加入者未突合、候補複数、加入者情報不正
- case作成に必要な受診日または健診機関が未解決
- importは完了したが、既存のcase成立条件を満たさないledger

これらは`case_not_eligible`として、理由、ledger ID、file receipt ID、source typeを実行結果へ記録する。加入者突合や基本情報修正後、同じaliasの「既存取込分からcase再処理」を実行すれば再び判定対象になる。

ただし、case成立条件を満たして処理を開始した後に次が発生した場合は、安全上のエラーとしてそのcaseを停止対象にする。

- 許可外保険者番号
- 同一自然キーの複数ACTIVE case
- sourceの別case所属競合
- eventとaliasの不整合

一つのcaseエラーでalias全体を止めるか、当該caseだけを隔離して継続するかは既存CLIのトランザクション境界に影響する。初期実装では既存どおりstepを停止し、対象caseと理由を明示する。

### 3.6 後続case ID

`03_01`は今回対象にした全有効case IDをJSONへ原子的に出力する。対象には新規caseだけでなく、既存の更新caseと、値が同じでも今回の対象受診キーとして再評価したcaseを含める。統合済み・無効・別event caseは除く。

`03_02`と`03_04`はこのcase IDファイルだけを処理する。aliasや施設IDから再検索しない。Windowsのコマンド長を避けるため、大量の`--case-id`展開ではなく`--case-id-file`を正式経路にする。

case IDファイルがない、形式不正、event不一致、対象groupがあるのに0件の場合は後続を開始しない。`03_02`後は必ず`03_04`まで行い、途中失敗時は停止する。

## 4. 管理画面

`/exam-processing`のevent選択の隣に「対象受領フォルダ」を追加し、既存alias検索モーダルを流用する。

- alias ID、フォルダ名、施設名・コード、想定受領形式を表示する。
- 初期値は未選択の「event全体」とする。
- event変更時はaliasを解除する。
- POST時と各CLI開始時に、aliasがeventに所属するかDBで再検証する。
- alias指定中の個別`03_02`・`03_04`実行は許可しない。直前の`03_01`が確定したcase集合が必要なためである。
- 実行確認にevent、alias、再取込、対象stepを表示する。

画面上は4集合をそのまま4回の操作にはせず、実行範囲として次を選べるようにする。

- **新しい受領から処理**: CSV/XMLのscan・import、今回取込case処理を行う。
- **取り込み済みデータを再処理**: 既存alias caseの再結合に加え、case未成立ledgerを再判定し、成立したものはcase作成、値作成、caseチェックまで行う。
- **両方**: 上記を同じ実行内で行い、重複caseを一意化する。

CSVのみ・XMLのみを調査する既存の個別step実行は残す。通常運用ではファイル種別を意識せず「新しい受領から処理」を使える構成とする。

現行のevent単位排他は維持し、alias同士の並列実行は初期実装で許可しない。

## 5. 履歴・制限・復旧

各`etl_runs.input_base`へ`event_id`、alias ID、`src_folder_raw`を記録する。結果画面にはCSV receipt数、XML receipt数、取込ledger数、case未成立ledger数、新規case数、既存更新case数、既存再処理case数、後続case数を表示する。

まとめ実行で工程ごとに件数制限を再適用すると集合がずれるため、alias指定の一括実行では件数制限を使用不可とする。調査用dry-run・個別stepでは許可する。

`03_01`失敗時はcase IDファイルを完成させない。`03_02`成功後に`03_04`が失敗した場合は、同じcase IDファイルでチェックだけ再実行できる保守手順を結果に出す。

## 6. 実装範囲

### 必須

1. receiptへのalias ID追加migration・DDL同期
2. 既存receipt用dry-run/backfill
3. 7工程と共通チェック処理へのスコープ引数
4. `03_01`の対象受診キー方式とcase IDファイル出力
5. `03_02`・`03_04`のcase IDファイル入力
6. 画面のalias選択・検証・引き渡し
7. 実行履歴表示
8. event全体実行の回帰テスト

### 対象外

- alias単位並列実行
- `03_05`以降
- 複数施設XML仕様の変更
- case自動統合・分割
- 受領原本値の変更

## 7. 主要テスト

1. alias未指定で従来のevent全体件数になる。
2. alias指定scanで他フォルダを登録しない。
3. 同一施設の別aliasをimportしない。
4. 取り込み済み再取込でもalias範囲を維持する。
5. ZIP内複数施設XMLを既存仕様どおり扱う。
6. alias由来sourceと既存MANUAL/PAPERが同じcaseへ再結合される。
7. 新規・更新・再評価caseがすべて後続対象になる。
8. 統合済み・無効・別event caseを除く。
9. event変更後の古いalias IDを拒否する。
10. backfillの曖昧行を更新しない。
11. `03_02`失敗時に`03_04`を実行しない。
12. 理由ありOK・補正・監査履歴がcase限定再実行で維持される。
13. 加入者未突合ledgerがあってもimport成功を保持し、case工程では理由付きでスキップされる。
14. 今回取込caseと既存alias caseが重複しても後続処理は1回だけ行う。
15. 取り込み済みcase再処理だけを選んだ場合、scan/importを行わず既存caseを再結合できる。
