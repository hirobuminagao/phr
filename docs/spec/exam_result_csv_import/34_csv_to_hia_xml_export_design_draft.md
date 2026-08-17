# CSV to HIA XML Export Design Draft

## Status

Current implementation note as of 2026-08-06. `exam_ledgers`、`exam_export_cases`、`exam_export_case_sources`、`exam_export_case_values`、case単位check、出力可否summaryは実装済みである。`04_export_hia_xml.py` も `exam_export_cases` / `exam_export_case_values` 起点へ切り替え済みである。

この文書は、統合取込ledger `exam_ledgers`、結合出力用case、出力用整値から厚生労働省指定の健診結果XMLを生成する処理の叩き台である。
確定済み条件と、実装前に確認が必要な不足情報を分けて記載する。

## Unified Ledger, Export Case, and Person Event Direction

CSV→XML出力まで一通り動作確認できたため、今後の改修はXML/CSV個別ledgerを直接拡張し続けるのではなく、統合取込ledger、結合出力用case、人単位event状態を分けて扱う。

`exam_ledgers` は、XML由来・CSV由来・紙入力由来を問わず、取込結果1件を表す統合ledgerとする。
XMLならXML内の1人分、CSVならCSV 1行、紙入力なら紙入力1人分を `exam_ledgers` 1件として扱う。
加入者突合、基本情報、検査値処理、source単位の法定check、補正現在値は `exam_ledgers.exam_ledger_id` を参照する。

既存 `xml_ledger` / `csv_row_ledger` は直ちに廃止せず、移行元、原本証跡、後方互換のsource tableとして残す。
通常運用ではimport時に `exam_ledgers` へ登録し、既存個別ledgerからの `sync_exam_ledgers.py` は初回移行、復旧、再構築用に下げる。

結合出力用caseは、人単位・1回分健診・XML出力候補を表す。
複数の `exam_ledgers` を組み合わせる場合、case sourcesで構成元ledgerを保持し、case valuesでXMLへ出す採用済み整値を保持する。
XMLのみ、CSVのみ、XML+CSV、手修正込みのいずれの場合も、最終的なXML出力可否は結合出力用caseで判断する。

`person_event` / `person_event_status_items` は、eventに対する人単位の業務状態を管理する。
結果受領件数、source check状態、出力case状態、XML出力状態、HIAアップロード状態、納品状態などは、`exam_ledgers` や結合出力用caseから同期する。

初期移行では以下を許容する。

- 既存 `xml_ledger` / `csv_row_ledger` から `exam_ledgers` へ移行する。
- 必要に応じて全ファイルを再scan/再importし、`exam_ledgers` を作り直す。
- 個別ledgerの廃止タイミングは、実行環境で `exam_ledgers` ベースの取込、check、補正、XML出力が安定してから決める。

## Existing Exporter

以前のXML出力スクリプトは以下である。

```text
scripts/kenshin_list_pydir/scripts/medi_export_xml.py
```

旧スクリプトは以下を一体で処理する。

- `work_other.medi_exam_result_ledger` の基本情報取得
- `work_other.medi_exam_result_item_values` の検査値取得
- 個人CDA XML生成
- 旧名称 `ix08.xml` の交換用基本情報ファイル生成
- `DATA` / `XSD` 構成作成
- 旧実装時点のルートフォルダ名、個人XML名、ZIP名生成
- XSDコピーとZIP作成

旧スクリプトは運用挙動をfreezeした資産であるため、直接変更せず参照元として残す。

## Confirmed Export Eligibility

結合出力用caseは以下をすべて満たす場合にXML出力候補とする。

1. 報告区分が空でない。
2. プログラムコードが空でない。
3. 加入者突合が `MATCHED`。
4. 次のいずれかを満たす。
   - case単位の法定checkが `OK`。
   - 法定チェックNGの原因が `MISSING` のみで、`manual_export_approved = 1`、理由、承認者、承認日時が設定されている。

`03_04_check_exam_export_cases.py` の `check_status` は現状、法定チェック結果を基準に `OK` / `NG` を設定している。
したがって4は、法定チェックOKまたはMISSINGだけを理由として明示的に手動許可された状態を意味する。

手動出力許可は、妊娠中等の確認済み理由により法定検査値がMISSINGとなる場合の例外とする。
`exam_check_results` とcase側 `check_status` は書き換えず、架空の検査値を作らない。MISSINGの該当entryはXMLへ出力しない。
`INVALID`、`PARSE_ERROR`、加入者不一致、報告区分・プログラムコード不足、健診機関不一致は手動許可の対象外とする。
`manual_export_approved` / `manual_export_reason` / `manual_export_approved_at` / `manual_export_approved_by` は結合出力用caseへ持たせる。手動許可時は理由、承認者、承認日時を必須とする。

上記4条件は業務上の出力候補条件とする。
XML生成時に必須値のnorm失敗、健診機関番号不正、XSD不一致などが発生した場合は、出力候補であっても生成エラーとして扱う。

## Export Selection and Logical Exam Records

画面およびCLIの対象選択は、同じ選択modelと対象抽出処理を使用する。

| selector | initial behavior |
| --- | --- |
| event | 必須。画面を開いたeventを固定する |
| 健診機関 | 必須。複数選択可。全施設も明示選択とする |
| 受領ファイル | 任意。`file_receipt_id` の複数選択可。未指定時は選択施設内の全対象CSV |
| 健診年月 | 任意。結合出力用caseの健診日に対する単月 `YYYY-MM` 指定 |
| 個人 | 任意。画面で表示された論理健診結果を複数選択可 |

各selectorはAND条件で適用する。
受領ファイルは対象行の抽出条件であり、ZIPの分割単位にはしない。
同じ健診機関・保険者に属する複数ファイルの結果は、同じ出力リストでは1つのZIPへまとめる。
ファイル別にZIPを分ける場合は別の出力リストとして実行する。
複数ファイル結合でも、選択されていない受領ファイルをexporterが暗黙に追加しない。
結合に使う受領ファイルは画面またはCLIで明示選択し、画面は同一人物の補完候補ファイルを案内できる形とする。

画面では、対象件数、出力可能件数、出力不可件数と理由を実行前に表示する。
既出力者の扱いは、受診年月等と同じ対象抽出条件として `含めない` / `含める` の2モードを持つ。
通常運用の既定値は `含めない` とし、初期検証や再出力では `含める` を明示選択できるようにする。
`含める` は未出力者と既出力者の両方を対象とし、既出力者だけを抽出する第3モードは初期版では設けない。

### Export Case

将来、同一人物の結果が複数の受領CSVに分かれ、一方のCSVにしか存在しない検査項目を組み合わせて1件のXMLを作る可能性がある。
そのため、外部仕様およびexporter内部では `exam_ledgers` 1件をそのまま1件の出力単位にしない。

1件の個人XMLに対応する出力単位を結合出力用caseとし、次の構造で扱う。

```text
exam_export_cases
  exam_export_case_id
  event_id
  person_event_id
  exam_facility_id
  insurer_number
  subscriber_id
  exam_date
  health_exam_report_category
  program_code
  case_status
  merge_status
  check_status
  xml_export_status
  export_readiness_status
  export_readiness_reason
  output_zip_path
  output_zip_file_name
  output_xml_file_name
  xml_exported_at
  xml_export_etl_run_id

exam_export_case_sources
  exam_export_case_id
  source_exam_ledger_id
  source_type
  source_role
  source_status

exam_export_case_values
  exam_export_case_id
  namecode
  occurrence_no
  adopted normalized/code value
  adopted_source_exam_item_value_id
  adopted_reason
```

画面の個人選択は加入者単位ではなく、この論理健診結果単位とする。
表示項目には氏名、カナ、生年月日、健診日、健診機関、構成元受領ファイルを含める。
同一人物に別日または別プログラムの健診結果がある場合は、別の候補として表示する。

caseは出力ボタンを押した時に初めて仮想生成するのではなく、取込後またはsource check後に事前作成し、出力OK/NGをXML出力候補単位で管理する。
複数sourceを組み合わせた過不足判定は、`exam_export_case_values + exam_item_master` に対して法定チェックを行った時点で確定する。
XML exporterは case checkがOK、または理由ありOKのcaseだけを読み、結合や採用判断を行わない。

case単位の法定チェック結果は、source単位checkとは分けて保持する。
実装入口は `03_04_check_exam_export_cases.py` とし、保存先は結合出力用caseを参照する。

### Item Output Policy

CSV/XMLから受け取った検査値は、標準コード不一致や施設独自項目であっても証跡として `exam_item_values` に残す。
一方で、HIA提出用XMLへ出すかどうかは別判断であるため、`phr_master.exam_item_output_policies` で `namecode` 単位に出力制御する。

| policy | behavior |
| --- | --- |
| `INCLUDE` | XML出力候補に含める。policy未登録時の既定値 |
| `EXCLUDE` | 取込証跡は残すが、case値作成およびXML出力から除外する |
| `REVIEW_REQUIRED` | 医療機関確認または運用判断が完了するまでcase値作成を停止する |

`exam_facility_id = 0` は全施設共通、施設ID指定は施設別上書きとする。
例えば `ZG...` 指導区分系のように検査結果XMLへ出すべきでない施設独自項目は `EXCLUDE`、`5C120 BNP` のように標準コードとの同一視が危ない項目は `REVIEW_REQUIRED` として扱える。

制御点は2段に分ける。
`03_02_build_exam_export_case_values.py` は `EXCLUDE` を採用済み整値に入れず、`REVIEW_REQUIRED` が含まれるcaseは `value_build_status = REVIEW_REQUIRED` として停止する。
`04_export_hia_xml.py` は出力直前にも `INCLUDE` の項目だけを読む。
これにより、取り込み後にpolicyを変更して再出力しても、証跡を壊さず出力対象だけを切り替えられる。

### Export Readiness Status

人が「この人は出力してよいか」を見る列は、`exam_export_cases.export_readiness_status` / `export_readiness_reason` とする。
これは後続処理や画面向けのsummaryであり、個別の原因は `subscriber_match_status`, `merge_status`, `case_status`, `value_build_status`, `check_status`, `manual_export_approved`, `xml_export_status` に残す。

| status | meaning |
| --- | --- |
| `EXPORT_READY` | case値作成済み、case check OK、未出力 |
| `APPROVED_WITH_REASON` | MISSING等を理由に手動出力許可済み、未出力 |
| `BLOCKED` | 加入者不一致、結合停止、case不備、法定check NGなどで出力不可 |
| `WAITING_VALUES` | caseはあるが採用済み整値が未作成 |
| `WAITING_CHECK` | 採用済み整値はあるがcase単位checkが未実行 |
| `EXPORTED` | XML/ZIP出力済み |
| `EXPORT_ERROR` | XML生成、XSD検証、ZIP作成など出力処理で失敗 |

`03_01_build_exam_export_cases.py`、`03_02_build_exam_export_case_values.py`、`03_04_check_exam_export_cases.py` は、それぞれの処理後にこのsummaryを再計算する。
XML exporterは、初期版では `EXPORT_READY` と `APPROVED_WITH_REASON` のcaseだけを対象にする。
出力成功後は、`output_zip_path`, `output_zip_file_name`, `output_xml_file_name`, `xml_exported_at`, `xml_export_etl_run_id` をcaseへ記録し、`xml_export_zips` / `xml_export_members` にも出力事実を残す。

### Export List Concept

画面運用では、条件を指定して即XML出力するだけではなく、先に「出力リスト」を作成し、対象caseを追加・確認してから出力する方式を正とする。

出力リストは人が操作する作業箱であり、`etl_runs` とは役割を分ける。
`etl_runs` はスクリプト実行履歴、エラー、処理件数を残す技術ログである。
出力リストは、どのcaseを今回HIAへ出す候補にしたか、誰が確認したか、どのZIP出力につながったかを管理する業務用リストである。

初期DDLは `20260806_002_health_exam_result_create_ops_xml_export_lists.sql` で追加する。
既存 `xml_export_zips` / `xml_export_members` とは責務を分け、出力リストは作業選択、ZIP/memberは出力事実の正本とする。

| table | role |
| --- | --- |
| `ops_xml_export_lists` | 出力対象を集める作業リスト。event、リスト名、状態、抽出条件、件数、出力実行結果を持つ |
| `ops_xml_export_list_cases` | 出力リストに追加された `exam_export_cases`。追加時点の出力可否snapshot、選択状態、除外理由を持つ |

状態の概念は以下とする。

| status | meaning |
| --- | --- |
| `DRAFT` | 対象を検索・追加している途中 |
| `READY` | 対象確認済みで出力実行可能 |
| `EXPORTING` | XML出力処理中 |
| `EXPORTED` | 対象ZIPが正常に出力された |
| `PARTIAL` | 一部グループのみ出力済み、残りにエラーあり |
| `ERROR` | 出力処理で失敗 |
| `CANCELLED` | 人が使用しないと判断して閉じた |

`xml_export_zips` には `xml_export_list_id` を持たせる。
これにより、HIAアップロード作業画面で「どの出力リストから作られたZIPか」をたどれる。
ただし、出力履歴の正本は引き続き `xml_export_zips` / `xml_export_members` であり、出力リストは作業選択の単位である。

CLI暫定運用では、画面の代わりに以下の2段階で実行できる。

```text
1. scripts/from_medical/03_05_create_xml_export_list.py
   条件に合う export_readiness_status OK相当のcaseを ops_xml_export_lists / ops_xml_export_list_cases へ登録する。
   標準ではREADYな出力リストを作成し、画面前のスクリプト運用ではこの入口を正式手順とする。
   リスト名を省略した場合は、event、受診月、実行日時から自動採番する。

2. scripts/from_medical/04_export_hia_xml.py --xml-export-list-id {xml_export_list_id}
   出力リストに含まれるcaseだけをZIP出力し、xml_export_zips / xml_export_members へ履歴を残す。
```

画面未実装期間の通常Runでは、`export_hia_xml.yml` の `use_latest_xml_export_list: true` を正とする。
そのため、`03_05_create_xml_export_list.py` でREADYリストを作成した直後に `04_export_hia_xml.py` を引数なしで実行すると、最新のREADY出力リストを自動選択して出力する。
直接条件指定で出す場合だけ、`--xml-export-list-id`, `--all-facilities`, `--facility-code`, `--case-id` 等を明示する。

### Export Processing Flow

画面運用での基本処理は、以下の順序とする。

```text
1. 出力リストを作成する。
   event、リスト名、受診月、健診機関、再出力可否などの検索条件を設定する。

2. 出力対象caseを検索する。
   exam_export_cases から、event、健診機関、受診月、受領ファイル、人単位、再出力条件に合うcaseを取得する。
   対象は export_readiness_status が EXPORT_READY または APPROVED_WITH_REASON のcaseだけとする。
   再出力を明示した場合だけ EXPORTED も候補に含める。
   検索ボタン押下では、出力リスト本体と追加済みcaseは保存変更せず、検索候補だけを再取得する。
   検索結果には、未追加、追加済み、追加不可を表示し、既に出力リストへ追加済みのcaseを重複追加しない。
   検索時点でcase状態が変わっていた場合は、検索結果側に最新の export_readiness_status と理由を表示する。

3. case詳細を確認し、必要なら加入者突合修正、基本情報補正、理由ありOK、検査値の再取込・再構築を行う。
   加入者情報が当たっていない、または一部項目だけ合致しているsource/caseは、`subscribers` 候補を検索して正しい加入者へ紐付け直す後続操作を用意する。
   加入者修正後は、該当sourceの `exam_ledgers`、`person_event`、`exam_export_cases`、`exam_export_case_values`、case単位checkを再同期・再構築する。

4. 出力するcaseを出力リストへ追加する。
   追加時点の export_readiness_status と理由はsnapshotとして保持する。
   BLOCKEDのcaseは原則追加しない。追加する場合は出力不可としてリスト内に残し、実行対象から除外する。

5. 出力リストを確定する。
   追加済みcaseの現在状態を再確認し、出力可能人数、理由ありOK人数、出力不可人数、予定ZIP数を計算する。

6. 出力対象をグルーピングする。
   健診機関 × 保険者番号 × 受診月でまとめる。
   受領ファイルは抽出条件であり、ZIP分割単位にはしない。

7. グループごとに処理を開始する。
   健診機関番号、保険者番号、提出日、同日出力番号から公式ZIP名を決める。
   出力先は イベントルート / 健診機関フォルダ / 03_健診結果（アップロードデータ） / 出力日時 / 受診月 とする。

8. グループ内のcaseを1人ずつXML化する。
   exam_export_cases の基本情報をXML出力用に整形する。
   exam_export_case_values から採用済み検査値を取得する。
   exam_item_master で型、単位、OID、methodCode等を補い、個人XMLを作る。
   個人XMLごとにXSD検証を行い、OKならDATA配下へ配置する。

9. グループ全員分のXML作成が終わったら、交換用基本情報 ix08_V08.xml を作る。
   XSD一式を同梱し、公式構成でZIP化する。

10. 出力履歴を保存する。
   xml_export_zips にZIP単位の履歴を登録する。
   xml_export_members に case単位の個人XML履歴を ledger_type = CASE で登録する。
   exam_export_cases を EXPORTED に更新し、ZIP/XMLファイル名、出力日時、etl_run_idを記録する。
   HIAアップロード作業状態は ZIP単位、個人XML単位ともに PENDING として開始する。

11. 次のグループを処理する。
```

グループ内の誰か1人でもXML生成またはXSD検証に失敗した場合、そのグループのZIPは作らない。
実装上はグループ単位でロールバックするため、同じグループのcaseを `EXPORT_ERROR` に寄せ、失敗理由を `etl_errors` と `exam_export_cases.export_readiness_reason` で追える状態にする。
他のグループは独立して処理を継続できる。

CLI運用では、当面は従来どおり条件指定から直接出力できる経路を残す。
ただし画面運用の正は出力リスト方式とし、後続で `04_export_hia_xml.py --xml-export-list-id ...` のように、確定済み出力リストを指定して出力する入口を追加する。

### Export Selectors

XML出力は、後続の画面操作を見据えて以下の条件を組み合わせて絞り込めるようにする。

| selector | purpose |
| --- | --- |
| 健診機関 | 健診機関ごとのHIAアップロードページに合わせてZIPを作る |
| 受診月 | 既にアップロードした月と、これからアップロードする月を分ける |
| 受領ファイル | 特定の受領物だけを再確認・出力する |
| 人単位 | 個別修正後の1人または少人数だけを出力する |
| 再出力 | 既に出力済みのcaseを、明示指定した時だけ再度出力する |

人単位の指定は `exam_export_case_id`、`subscriber_id`、`hia_subscriber_id`、`person_id_custom` を受け付ける。
通常運用では `facility_codes + exam_month`、個別修正後は `hia_subscriber_id` または `exam_export_case_id`、再出力時は `include_exported = true` を使う。
再出力時も過去の出力履歴は削除せず、`EXPORTED` のcaseを明示的に対象へ戻す。
同日出力回数 `split_no` は健診機関フォルダ配下の既存ZIP名から自動採番するか、明示指定して別ZIPとして残す。

### HIA Upload Worklist

HIAアップロード作業では、出力されたZIPを健診機関ごとのHIA画面から手作業でアップロードする。
そのため、出力履歴は単なるCSVログだけでなく、DB上の作業リストとしても保持する。

作業リストの正本は以下とする。

| level | table | role |
| --- | --- | --- |
| ZIP単位 | `xml_export_zips` | 健診機関、出力リスト、ZIPパス、HIAアップロード作業状態を持つ |
| 個人XML単位 | `xml_export_members` | ZIP内の個人XML、case、加入者、個人単位アップロード結果・エラーを持つ |
| 一覧表示 | `v_xml_export_hia_upload_worklist` | 画面・確認SQL向けにZIP、個人、case、元ファイルを横断して表示する |

想定作業は以下である。

```text
1. 出力リスト、健診機関、受診月でアップロード対象ZIPを一覧する。
2. 一覧の zip_path またはフォルダパスをコピーしてエクスプローラーで開く。
3. HIAの健診機関ページで対象ZIPをアップロードする。
4. ZIP単位でアップロード完了、エラー、確認者、確認日時、メモを記帳する。
5. HIAが個人単位エラーを返す場合は、xml_export_members に個人単位エラー内容を記帳する。
6. 後続の再出力、修正、再アップロード対象は、この履歴から抽出する。
```

`xml_export_zips.hia_upload_status` はZIP単位の作業状態を表す。
初期値は `PENDING` とし、画面または運用スクリプトで `UPLOADED`, `UPLOAD_ERROR`, `PARTIAL`, `CONFIRMED` 等へ更新する。

`xml_export_members.hia_upload_status` は個人XML単位の結果を表す。
初期値は `PENDING` とし、ZIP全体が問題なくアップロードできた場合は対象memberを `UPLOADED` へ更新する。
HIAが個人単位の取込エラーを返した場合は、該当memberを `UPLOAD_ERROR` とし、`hia_upload_error_code`, `hia_upload_error_message`, `hia_upload_note` に内容を記録する。

画面実装前の暫定運用では、`scripts/from_medical/dev_tools/update_hia_xml_upload_status.py` でZIP単位・個人XML単位のHIAアップロード状態を更新できる。

```text
ZIP単位でアップロード完了:
  update_hia_xml_upload_status.py --zip-id {xml_export_zip_id} --zip-status UPLOADED --member-status UPLOADED --apply-to-members --by {operator}

個人XML単位でHIAエラーを記帳:
  update_hia_xml_upload_status.py --member-id {xml_export_member_id} --member-status UPLOAD_ERROR --error-code {code} --error-message {message} --by {operator}
```

### Future Multi-Source Merge Rules

複数受領ファイルからの結合は、自動推測ではなく、加入者突合済みの行と明示的な結合条件を使う。
同一の論理健診結果にXML由来とCSV由来の値がある場合、正常なXMLを正常なCSVより優先する。
XMLは厚生労働省標準様式によって構造、namecode、型、単位が明示されているため、canonical sourceとして扱う。
基本の同一候補判定には以下を使用する。

- `event_id`
- `insurer_number`
- `subscriber_id`
- `exam_date`
- 健診機関コード。XMLの `facility_code` と、CSVの `exam_facility_id` から解決した `exam_facilities.exam_facility_code` を同じcanonical codeへ揃える

`health_exam_report_category` と `program_code` は同一候補判定keyには含めず、候補内で値を集約する。
片方のsourceだけに値があればその値を採用し、複数の異なる有効値があれば `MERGE_BASE_FIELD_CONFLICT` として出力を停止する。
健診日またはcanonical健診機関コードを解決できない行は、安全に同一候補と確定できる別ルールが設定されるまで自動結合しない。

`exam_ledgers` はXMLの `ClinicalDocument/code` を `report_category_code`、`documentationOf/serviceEvent/code` を `program_type_code` として保存する。
XML由来コードは元XMLの明示値を正とし、event年齢規則で上書きしない。
既存XMLは `02_import_xml.py --include-imported` で再取込し、追加カラムをbackfillする。

検査値の結合keyは、原則として `namecode + occurrence_no` とする。

- XMLにしか存在しないnamecodeはXML値を採用する。
- CSVにしか存在しないnamecodeは、不足項目の補完値としてCSV値を採用する。
- XMLとCSVに同じkeyがあり、正規化後の値、単位、型が一致する場合はXMLを採用し、CSVをduplicateとして残す。
- XMLとCSVに同じkeyがあり、有効値が異なる場合もXMLを採用する。CSVは削除せず `SUPERSEDED_BY_XML` とし、値の差異を確認できる状態にする。この差異だけではXML出力を停止しない。
- CSV同士に同じkeyの異なる有効値がある場合は自動優先せず、`MERGE_VALUE_CONFLICT` として停止する。
- XML同士に同じkeyの異なる有効値がある場合も自動優先せず、`MERGE_VALUE_CONFLICT` として停止する。
- 一方が空値・未実施、他方が有効値の場合の採用可否は、未実施表現のXML方針と合わせて決定する。
- XMLとCSVの基本情報が異なる場合も正常なXMLを優先し、CSV側の差異を警告として残す。
- CSV同士またはXML同士で基本情報に異なる有効値がある場合は、`MERGE_BASE_FIELD_CONFLICT` とする。

XML由来であっても、parse、normalize、validationに失敗した値は優先対象にしない。
正常なXML値がなく正常なCSV値がある場合は、CSV値をactiveにできる。

結合後も証跡を失わないよう、個人XMLごとに全 `source_exam_ledger_ids` と `source_file_receipt_ids` を追跡する。
出力成功時は、構成元となった全 `exam_ledgers` に同じexport Runと出力先を関連付ける。
出力履歴CSVと `ix08_V08.xml.totalRecordCount` の人数は、構成元CSV行数ではなく、生成した論理健診結果数とする。

## Output Path

### Official File Transfer Rules

本設計で参照する公式仕様は、厚生労働省が公開する以下の資料である。

- [特定健診・特定保健指導の電子的な標準様式](https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/xml_30799.html)
- [送付用ファイルアーカイブ仕様説明書 Version 4（2023-03-31）](https://www.mhlw.go.jp/content/12400000/8-1A.pdf)
- [交換用基本情報ファイル仕様説明書 Version 4](https://www.mhlw.go.jp/content/12400000/1-1A.pdf)

2026-07-30に上記を確認した。
公式仕様では、提出対象一式を所定構成の「送付用データルートフォルダ」に配置し、そのルートフォルダ全体をZIP形式で圧縮したものを送付用アーカイブファイルとする。
ZIPファイル名はルートフォルダ名に `.zip` を付けた名前でなければならない。

健診機関から保険者へ特定健診結果を送る今回のルートフォルダ名は、以下の公式規則を適用する。

```text
<提出元健診機関番号10桁>_<提出先保険者番号8桁>_<提出年月日YYYYMMDD><同日分割送信回数N>_<実施区分コードX>
```

| part | official meaning | current scope |
| --- | --- | --- |
| 提出元健診機関番号 | 健診機関番号10桁 | `exam_facilities.exam_facility_code` |
| 提出先保険者番号 | 8桁。8桁未満は先頭ゼロ埋め | 結合出力用caseで採用した `insurer_number` |
| 提出年月日 | ZIPを提出する年月日 | XML/ZIP作成日と同一とし、exporterのRun日を使う |
| 同日分割送信回数 `N` | 同一送付元・送付先の同日1回目は`0`。以後`1`から`9` | 既存ZIPから自動採番する。CLI/APIで`0`から`9`の明示指定も可能とする |
| 実施区分コード `X` | `1`: 特定健診情報 | 今回は`1` |

したがって、公式規則上の最終ZIP名は以下となる。

```text
<提出元健診機関番号10桁>_<提出先保険者番号8桁>_<提出年月日YYYYMMDD><N>_1.zip
```

ここで使う日付は個人XML内の健診実施日ではない。
今回の運用では提出日とXML/ZIP作成日を同日とし、exporterのRun日を `YYYYMMDD` で使用する。
同日分割送信回数は単なる任意の出力番号ではなく、同じ健診機関から同じ保険者へ同日に送信する回数を識別する番号である。

今回のHIAアップロードは健診結果のみを提出し、決済情報を含めない前提とする。
公式仕様では結果のみを提出する場合、交換用基本情報、健診結果データ、XMLスキーマで構成できるため、`su08_V08.xml` と `CLAIMS` は作成しない。
ZIP内部は以下の構成とする。

```text
<ZIP名から.zipを除いたルートフォルダ名>/
  ix08_V08.xml
  DATA/
    h<健診機関番号10桁><ファイル生成日YYYYMMDD><N><種別1桁><連番6桁>.xml
  XSD/
    ix08_V08.xsd
    hc08_V08.xsd
    co08_V08.xsd
    ...
    coreschemas/
      ...
```

交換用基本情報ファイルの公式ファイル名は `ix08_V08.xml` である。
旧 `medi_export_xml.py` は既定値に `ix08.xml` を使用しているが、新exporterではこの旧名称を引き継がない。

`ix08_V08.xml` には今回の交換経路に合わせ、少なくとも以下を設定する。

| XML field | value | meaning |
| --- | --- | --- |
| `interactionType/@code` | `6` | 健診機関から保険者 |
| `creationTime/@value` | exporterのRun日 `YYYYMMDD` | 提出用ファイル作成年月日 |
| `sender/id/@root` | `1.2.392.200119.6.102` | 健診機関番号OID |
| `sender/id/@extension` | 健診機関番号10桁 | 送付元 |
| `receiver/id/@root` | `1.2.392.200119.6.101` | 保険者番号OID |
| `receiver/id/@extension` | 保険者番号8桁 | 送付先 |
| `serviceEventType/@code` | `1` | 特定健診情報 |
| `totalRecordCount/@value` | `DATA`内の個人XML数 | ZIPへ収録した結果件数 |

`interactionType=6` とZIP名末尾の実施区分コード `_1` は別のコード体系である。
個人XMLファイル名21桁目の種別1桁も表2の実施区分コードであり、今回の特定健診情報では `1` 固定とする。任意に選択する設定値にはしない。

### XSD Source and Versioning

新exporterは、実行時にXSDを外部サイトから取得しない。
リポジトリ内で内容を確認した固定bundleを選択し、個人XMLと `ix08_V08.xml` の検証および送付用ZIPへの同梱に使用する。

現行bundleは以下へ配置する。

```text
scripts/from_medical/source/XSD/
  mhlw_v4_20230331_v08/
    bundle.yml
    ix08_V08.xsd
    su08_V08.xsd
    cc08_V08.xsd
    gc08_V08.xsd
    co08_V08.xsd
    hc08_V08.xsd
    hg08_V08.xsd
    coreschemas/
      datatypes-base_hcgv08.xsd
      datatypes_hcgv08.xsd
      voc_hcgv08.xsd
      narrativeBlock_hcgv08.xsd
```

`mhlw_v4_20230331_v08` は、厚生労働省仕様書Version 4、公開日2023-03-31、XSDファイル版V08を表す。
2026-07-30に厚生労働省の公式配布ファイル11件と照合し、同一内容であることを確認した。
公式配布版のXML構造は変えず、リポジトリ格納時に改行をLFへ統一し、行末空白を除去した。
`bundle.yml` には配布元、仕様書版、確認日、格納ファイルのSHA-256を記録する。

厚生労働省仕様書の版と、ファイル名に含まれる `V08` は別に管理する。
将来の改訂では既存bundleを上書きせず、たとえば `mhlw_v5_<公開日>_<XSD版>` のような別ディレクトリを追加する。
exporter設定にはbundle IDを持たせ、選択したbundleのXSDファイルと `coreschemas/` だけを送付用ZIP内の `XSD/` へコピーする。
管理用の `bundle.yml` は送付用ZIPへ含めない。
これにより、過去Runの再現性を維持しつつ、新旧XSDを設定で切り替えられる。

出力先は以下とする。

### Official Output Mode

通常運用・HIAアップロード対象の正式出力は `output_mode = official` とする。
正式出力では、従来どおりイベントルート配下の健診機関フォルダへZIPを配置する。

```text
<event.result_root_path>/
  <健診機関フォルダ>/
    03_健診結果（アップロードデータ）/
      yyyymmdd_hhmmss/
        <健診実施月YYYYMM>/
          <健診機関番号>_<保険者番号>_<yyyymmdd><同日分割送信回数>_1.zip
```

健診機関フォルダは、CSVに対応する `file_receipts.relative_path` の先頭フォルダを第一候補とする。
これにより、実際に受領したCSVの健診機関フォルダへ出力できる。
`yyyymmdd_hhmmss` はXML/ZIPを作成した出力実行日時である。
HIAアップロード担当者への依頼は「`yyyymmdd_hhmmss` に出力した分をアップロードしてください」と伝える運用にするため、まず出力実行日時でフォルダを分ける。
その配下を健診実施月 `YYYYMM` ごとに分ける。
健診実施月は個人XMLの健診実施日 `exam_date` から算出する。

### Review Output Mode

確認用出力では `output_mode = review` を指定する。
このモードは、作成したXML/ZIPを人が確認するための確認用であり、HIAアップロード対象フォルダを汚さないことを目的とする。
ZIP内部のルートフォルダ名、個人XML名、XSD構成は正式出力と同じにする。
違いは外側の配置場所だけである。
ETL run と `etl_errors` は実行証跡として残すが、`xml_export_zips` / `xml_export_members`、`exam_export_cases.xml_export_status`、`ops_xml_export_lists` / `ops_xml_export_list_cases` の正式出力状態は更新しない。

```text
<repo>/data/hia_xml_review_exports/
  event_<event_id>/
    <健診機関フォルダ>/
      03_健診結果（アップロードデータ）/
        yyyymmdd_hhmmss/
          <健診実施月YYYYMM>/
            <健診機関番号>_<保険者番号>_<yyyymmdd><同日分割送信回数>_1.zip
```

`review_output_root` を指定した場合は、上記 `<repo>/data/hia_xml_review_exports` の代わりにそのパスを使う。
ただし相対パスを指定した場合は、実行カレントディレクトリではなく `<repo>` からの相対パスとして解決する。
確認用出力は、正式アップロード依頼には使用しない。
確認後に正式出力する場合は、同じ出力リストを `output_mode = official` で再実行する。

FastAPI管理画面からの確認用出力では、確認用ZIPを出力リスト詳細画面に表示する。
確認用ZIPは、画面からダウンロードした時点で `app_audit_logs` に `HIA_XML_REVIEW_DOWNLOAD` として記録し、ダウンロードレスポンス送信後に `data/hia_xml_review_exports` から削除する。
確認用ZIPは正式な `xml_export_zips` / `xml_export_members` には記録しない。
そのため、正式出力済みcaseであっても確認用には再生成できるようにし、確認用出力では既出力除外条件を適用しない。

最終出力物は以下の命名規則で作るZIPである。
個人XML名とZIP内部の構成は公式仕様を正とし、旧exporterの実装は補助的に参照する。

```text
ZIP:
  <健診機関番号10桁>_<保険者番号8桁>_<作成日YYYYMMDD><同日分割送信回数0-9>_1.zip

個人XML:
  h<健診機関番号10桁><作成日YYYYMMDD><同日分割送信回数0-9><種別1桁><連番6桁>.xml
```

例:

```text
1310438796_06139463_202607300_1.zip
```

- `20260730`: 提出日と同日にしたXML/ZIP作成日。
- 末尾直前の `0`: 同日分割送信回数。初回は `0`、同じ提出元・提出先に同日再送する場合は最大 `9` まで順に増やす。
- 最後の `_1`: 固定値。
- ZIP内部のトップフォルダ名は `.zip` を除いた同名とする。

ZIP内部:

```text
1310438796_06139463_202607300_1/
  ix08_V08.xml
  DATA/
    <個人XML>.xml
  XSD/
    ...
```

展開済みフォルダは作業用一時ディレクトリにだけ作り、ZIP作成と検証後に削除する。
`03_健診結果（アップロードデータ）/yyyymmdd_hhmmss/YYYYMM/` にはアップロード対象ZIPだけを残す。

同じ出力リストに複数の健診実施月が含まれる場合、同じ `yyyymmdd_hhmmss` 配下に月別フォルダを作る。
ZIPは健診実施月ごとに分ける。
同一ZIPに複数の健診実施月を混在させない。
同日分割送信回数は、同じ出力先月フォルダ内に既存の同一提出元・提出先・提出日ZIPがあるかを見て採番する。
これにより、同じ出力リスト内でも健診実施月ごとに `0-9` を使える。

既存出力は上書きしない。
同一秒の出力先がすでに存在する場合は、既存内容へ追記せず停止する。

## Operator Export Log

HIAへのアップロードは健診機関ごとのページから行い、同じページで全施設分を一括アップロードできない。
そのため、出力リストごとにアップロード作業用の一覧ログをイベントルートへ出力する。

```text
<event.result_root_path>/
  xml作成_出力履歴/
    yyyymmdd_hhmmss/
      健診結果XML出力履歴.csv
```

ログは健診機関単位の出力ZIPごとに1行を記録する。

| column | content |
| --- | --- |
| 健診機関コード | XML/ZIPの提出元として使用した健診機関コード |
| 健診機関名 | XML/ZIPの提出元として使用した健診機関名 |
| 健診機関フォルダ名 | `event.result_root_path` 直下の健診機関フォルダ名 |
| 出力フォルダ | `03_健診結果（アップロードデータ）` 配下の `yyyymmdd_hhmmss` |
| 健診実施月 | ZIPを配置した健診実施月フォルダ `YYYYMM` |
| ZIP名 | アップロード対象ZIPファイル名 |
| 人数 | 対応するZIPへ格納し、IX08件数へ反映した個人XML数 |

CSVヘッダーは以下とする。

```text
健診機関コード,健診機関名,健診機関フォルダ名,出力フォルダ,健診実施月,ZIP名,人数
```

ログはWindows/Excelでの確認を考慮し、UTF-8 BOM付きCSVを第一候補とする。
同じ出力リストの出力実行では、各健診機関フォルダ側の出力ディレクトリとログ側ディレクトリに同じ `yyyymmdd_hhmmss` を使う。
XSD検証とZIP作成まで成功した健診機関だけをログへ記載し、人数は最終的なIX08の `totalRecordCount` と一致させる。
ログは一時ファイルへ書き、全施設の処理終了後に確定ファイル名へ置き換える。

XML、XSD、ZIP本体は `xml作成_出力履歴` へコピーしない。
ZIPは各健診機関フォルダの `03_健診結果（アップロードデータ）/yyyymmdd_hhmmss/YYYYMM/` にだけ配置し、一覧ログから健診機関フォルダ名、出力フォルダ、健診実施月、ZIP名を確認してアップロードする。

アップロード依頼時は以下のように伝えられる状態を目標とする。

```text
20260803_153000 に出力した分をアップロードしてください。
中は健診実施月ごとに 202604 / 202605 / 202606 に分かれています。
各健診機関ページで、出力履歴CSVに記載されたZIPをアップロードしてください。
```

## Basic Information Normalization

XML基本情報は照合用 `match` ではなく、格納・出力用のnorm値を使う。
元値は `exam_ledgers` の原本由来値とし、`subscribers` の値へ直接置き換えない。
`subscribers` で使用している共通identity libと同じ関数を利用する。
`export_fields.py` はこれらの既存関数を呼び出し、XML出力値のprojectionと必須項目エラーの集約だけを行う。同じ値生成・妥当性確認を再実装しない。

| XML field | source | common function | output |
| --- | --- | --- | --- |
| 保険者番号 | `insurer_number` | `normalize_insurer_number()` | 数字半角、XML用に8桁化 |
| 保険証記号 | `insurance_symbol_raw` | `normalize_insurance_symbol()` | `export` |
| 保険証番号 | `insurance_number_raw` | `normalize_insurance_number()` | `field_norm` |
| 氏名カナ | `name_kana_raw` | `normalize_name_kana_full()` | `field_norm` |
| 生年月日 | `birthdate` | `normalize_birthdate()` | XMLでは `match` のYYYYMMDD表現 |
| 性別 | `gender_raw` / `gender_code` | `normalize_gender_code()` | `field_norm` |
| 郵便番号 | `postal_code` | `normalize_postal_code_export()` | `###-####` |
| 住所 | `address` | `normalize_address_export()` | 空白なし、英数字・記号は全角寄せ、CP932換算80バイト以内 |

保険者番号の8桁化には既存 `zero_pad()` を使用する。
郵便番号、住所、電話番号は、旧exporterにはXML出力処理がある一方、現在のidentity共通libには同一仕様の公開関数がない。
この3項目は旧exporterから必要な規則を共通field層へ移し、CSV XML exporterから直接再実装しない。

- 郵便番号と住所は既存 `scripts/lib/identity/field/address.py` を拡張する。
- 電話番号は `scripts/lib/identity/field/phone_number.py` を共通fieldとして追加する。
- 既存の `build_postal_code_match()` / `build_address_match()` は照合用であり、XML出力用の値として流用しない。

保険証記号の出力方針は既存 `normalize_insurance_symbol()` の `export` を共通入口として使う。

- 数字のみは半角。
- 元値に全角文字を含む場合は全体を全角へ寄せる。ただし数字だけの記号は半角へ寄せる。
- `match` や `person_id_custom` 用の値はXMLへ使用しない。

現行 `normalize_insurance_symbol()` は `base_normalize()` のNFKC変換後に全角判定するため、元値の全角ASCIIが半角化されたことを判別できない。
たとえば全角英字を含む記号で、今回の出力条件と異なる結果になる可能性がある。
exporter側に別ロジックを重複実装せず、共通lib側の `export` 判定を修正し、数字のみ、半角英数字、全角英字混在、日本語混在のテストを追加する。

保険証番号は数字のみを半角で出力する。
氏名カナはひらがなをカタカナへ変換し、全角カタカナ・全角文字へ寄せた `field_norm` を使用する。

住所・郵便番号・電話番号のXML出力normは旧exporterに実装されている。
これらは旧ファイルから新しい共通export helperへ移し、CSV exporter固有処理として重複実装しない。

厚生労働省「特定健診情報ファイル仕様説明書 Version 4（2023-03-31）」では、最大バイト数は特に条件記載がない限り半角文字を1バイト、全角文字を2バイトとして換算するとされている。
同資料の受診者・作成機関・健診実施機関の住所説明では、住所本文は郵便番号を含まず、空白を含めない全角文字列で、最大80バイトとされている。
郵便番号は `###-####` 形式の半角文字列8バイト固定とされている。

現行実装では `scripts/lib/identity/field/address.py` の `normalize_address_export()` が住所本文を空白除去、英数字・ASCII記号の全角寄せ、CP932換算80バイト以内へ整形する。
80バイトを超える場合は、先頭から文字単位で足して80バイトを超えない位置で切り詰める。
原本値は変更せず、XML出力用のprojection値だけを整形する。

### Address Completion for HIA Export

厚生労働省V08 XSD上、受診者 `patientRole/addr` は `minOccurs=0` であり、XSD検証だけでは住所欠落をエラーにできない。
一方、HIA受付では受診者住所・郵便番号が必須扱いとなるため、XML出力前に住所・郵便番号の利用可否を解決する。
exporterは補完lookupや業務判断を行わず、importまたはledger同期までに準備された値をXMLへ詰めることに集中する。

住所補完は以下の順で行う。

1. CSV/XML原本に住所がある場合は原本値をXML出力normへ通す。
2. import時の加入者突合後、郵便番号があり住所がない場合は、日本郵便の郵便番号データ由来の住所マスタから都道府県・市区町村・町域を補完候補として保存する。
3. 郵便番号lookupで安全に住所を解決できない場合は、憶測補完せずXML生成時の基本情報不足として扱う。

業務上利用を許可された加入者住所等による補完、およびHIA提出用の最終代替値として郵便番号 `000-0000`、住所 `－` を使用する処理は、基本情報補正画面・補正履歴テーブルと合わせて後続で実装する。

現行実装では、CSV importが加入者突合直後に `scripts/from_medical/script_lib/basic_info_completion.py` を呼び出し、`exam_ledgers` に以下を保存する。
XML import側も同じ `exam_ledgers` へ保存する。XML原本に住所・郵便番号がない場合はNULLとし、後続の基本情報補正・住所補完で扱う。

| column | meaning |
| --- | --- |
| `basic_info_status` | XML出力に向けた基本情報状態。`OK` / `WARNING` / `NG` |
| `basic_info_reason` | 基本情報状態の理由 |
| `address_source` | `SOURCE` / `POSTAL_LOOKUP` / `NONE` |
| `address_completion_status` | `NOT_NEEDED` / `AVAILABLE` / `NEED_REVIEW` / `NOT_FOUND` / `INVALID` / `MISSING` |
| `address_completion_reason` | 郵便番号lookup結果などの理由 |
| `address_completed_value` | 原本住所がない場合のXML出力用補完候補住所 |
| `postal_code_completed_value` | XML出力用に整形済みの補完候補郵便番号 |

保険者番号は他の基本情報と異なり、eventとの整合性判定を行う。
初期実装では `event.insurer_number` を正とし、CSV/受領ファイル側に保険者番号がある場合は8桁正規化後にevent値と比較する。
event値と一致すれば `insurer_number_source = SOURCE`、`insurer_number_completion_status = NOT_NEEDED` とする。
CSV/受領ファイル側に保険者番号がない場合は `event.insurer_number` を採用し、`insurer_number_source = EVENT`、`insurer_number_completion_status = FILLED_FROM_EVENT` とする。
CSV/受領ファイル側に保険者番号がありevent値と異なる場合は `CONFLICT` とし、import行の `row_reason` にもエラーとして残す。

`dev_phr.fund_insurer_numbers` には同一健保の複数保険者番号が存在し得る。
特例退職者等の扱いは健保ごとの運用判断が必要なため、初期実装では `fund_insurer_numbers` に存在する別番号であっても自動許可しない。
複数保険者番号を許可するeventルールは後続で追加する。

XML exporterは原本住所を優先し、原本住所がない場合に `address_completed_value` を使う。
保険者番号は `insurer_number_export_value` があればそれを使い、なければ原本 `insurer_number` をXML出力normへ通す。
郵便番号は原本値を優先し、原本値がXML形式へ正規化できない場合に `postal_code_completed_value` を使う。
exporterは郵便番号masterを直接lookupしない。

郵便番号マスタは日本郵便公式の「住所の郵便番号（1レコード1行、UTF-8形式）」を元に作成する。
個人住所の丁目、番地、建物名は郵便番号から推測しない。
日本郵便データに含まれる「以下に掲載がない場合」等の表現はXMLへそのまま出力せず、住所文字列として使用できる表記へ整備する。

日本郵便は郵便番号・デジタルアドレスAPIも提供しているが、初期実装では採用しない。
APIはデータ更新の手間が少ない一方、ビジネスアカウント登録、API利用権限、通信可否、障害時の運用、呼び出し証跡管理が必要になる。
今回の用途はHIA XML出力時の住所補完であり、リアルタイム性は不要なため、公式CSVを定期取得してmaster DBへ取り込む方式を採用する。

採用する入力ファイルは「住所の郵便番号（1レコード1行、UTF-8形式）」とする。
従来形式CSVは、町域が複数行に分割されるケースがあり、住所補完masterとして扱うには取込時の結合・解釈が増える。
1レコード1行形式は文字コードがUTF-8で、郵便番号単位のmaster化に向くため、今回の補完処理の主入力とする。
事業所の個別郵便番号CSVは、加入者住所補完の初期対象には含めない。
会社・事業所宛の個別郵便番号が必要になった場合は、別masterとして追加し、通常住所masterより優先するかを後続で決める。
実ファイル `utf_ken_all.csv` の精査結果とDDL案は `36_postal_code_master_design.md` に記載する。

郵便番号マスタの初期候補は以下とする。

| column | meaning |
| --- | --- |
| `postal_code` | 7桁またはXML出力用ハイフン付き郵便番号の元値 |
| `prefecture` | 都道府県 |
| `city` | 市区町村 |
| `town_area_raw` | 日本郵便データの町域原文 |
| `town_area_normalized` | XML出力用に整備した町域 |
| `address_for_xml` | 都道府県・市区町村・整備済み町域を連結した補完住所 |
| `source_file_name` | 取り込み元ファイル名 |
| `source_file_updated_at` | 日本郵便データの更新日または取得確認日 |
| `source_row_sha256` | 元行の同一性確認用hash |
| `normalization_note` | 「以下に掲載がない場合」等を整備した理由 |
| `created_at` / `updated_at` | 管理日時 |

住所補完または代替値使用を行った場合、原本値は上書きせず、XMLに出した値、元値、補完元、補完理由、処理Run、処理日時を記帳する。

基本情報補正はCSV由来とXML由来の両方を対象にする。
現在XML出力で使う補正値は `exam_ledgers` に横持ちし、変更履歴は `exam_ledger_id` を起点にした共通テーブルへ残す。
補正対象は初期版では以下に限定する。

- `insurer_number`
- `insurance_symbol`
- `insurance_number`
- `insurance_branch_number`
- `exam_ticket_number`
- `exam_ticket_expires_on`
- `name_kana`
- `postal_code`
- `address`

`insurer_number` はCSVにない場合、取込時にeventまたはfile_receipt由来で自動補完する。
XMLでは原本XML内の保険者番号を優先するが、欠落や明確な誤りがある場合は同じ補正機構で扱う。
手修正対象というより、自動補完値と補完元を追跡する対象とする。

各ledger側には、各補正項目ごとに補正値と最新変更履歴IDを持たせる。
たとえば氏名カナなら以下のように扱う。

```text
name_kana_raw = 長尾
name_kana_corrected = 佐藤
name_kana_correction_history_id = 3
```

XML由来もCSV由来も同じ画面・同じ出力projectionで補正するため、補正対象項目の原本値または補正値を受け止めるカラムは `exam_ledgers` 側に揃える。
XMLに存在しない項目は原本値NULLとして保持し、補正値と履歴で補える状態にする。

履歴テーブルは、項目ごとの変更チェーンを保持する。
前回履歴IDを持つことで、`active` flagに依存せず、どの値からどの値へ変わったかを追えるようにする。

```text
exam_ledger_basic_info_correction_histories
  correction_history_id
  exam_ledger_id
  field_name
  before_value
  after_value
  correction_source
  correction_reason
  previous_correction_history_id
  etl_run_id
  corrected_by
  corrected_at
  created_at
```

例:

```text
row原本値: 氏名カナ = 長尾
履歴ID 1: field=name_kana, before=長尾, after=田中, previous=NULL
履歴ID 3: field=name_kana, before=田中, after=佐藤, previous=1
ledger現在値: name_kana_corrected=佐藤, name_kana_correction_history_id=3
```

XML出力projectionは、補正値がある項目は補正値を優先し、なければ原本値を使用する。
どちらを採用したかと、参照した変更履歴IDはXML出力履歴または出力時snapshotに残す。

修正画面では、加入者突合済みの行について `subscribers` テーブルの値を補正候補として表示する。
候補表示は手入力の代替ではなく、入力支援と確認材料とする。
利用者が `subscribers` 由来の候補を採用した場合も、直接ledger原本値を上書きせず、通常の補正操作として履歴テーブルへ記録する。
この場合の `correction_source` は `SUBSCRIBER` とし、候補値を採用した事実、採用者、採用日時、理由を残す。

`subscribers` から候補表示する初期項目は以下とする。

- 保険証記号
- 保険証番号
- 枝番
- 氏名カナ
- 郵便番号
- 住所

受診券番号と受診券有効期限は `subscribers` に正となる値がない場合があるため、初期候補には含めず、必要になった時点で予約・受診券管理側の参照元を別途決める。

## Exam Item Values

source検査値は `health_exam_result.exam_item_values` の `ledger_type = 'EXAM'`、`ledger_id = exam_ledgers.exam_ledger_id` を対象とする。
XML出力では、複数sourceを統合した `exam_export_case_values` の採用済み整値を使用する。

初期の値選択は以下を基本とする。

| XML type | value | metadata |
| --- | --- | --- |
| `PQ` / 数値 | `normalized_value` | `normalized_unit` |
| `CD` / `CO` | `code_value` | `code_system`, `code_display` |
| `ST` / `TX` | `normalized_value` | なし |

- `namecode`、`section_code`、`section_code_system`、`section_name` は `exam_item_values` の値を使う。
- 同一namecodeの複数値は `occurrence_no` の順で出力する。
- `namecode_display_name`、`jun_no` は表示名と出力順に利用する。
- `xml_method_code` など `exam_item_values` にないXMLメタデータだけ `dev_phr.exam_item_master` から補う。
- 施設別ABC判定や総合判定は出力しない。
- `interpretation_*` は原本CSVから明示的にマッピング・正規化された値がある場合だけ出力する。値から自動判定しない。
- `source_reference_lower` / `source_reference_upper` は原本CSVにある場合だけ `referenceRange` として出力する。単位は検査値と同じ `normalized_unit` を使用し、単位変換はしない。
- 付属2の `一連検査グループ識別` / `一連検査グループ関係コード` がある項目は、親observationの下へ `COMP` / `RSON` で構造化する。施設別ruleから推測しない。

## Reuse and Replacement

### Reuse from the Old Exporter

- HL7 CDA / IX08 namespace定義
- ClinicalDocumentの基本構造
- author、custodian、serviceEventの構造
- section / observation / valueの構造
- 交換用基本情報XMLの構造。出力ファイル名は公式どおり `ix08_V08.xml` とする
- 厚生労働省指定のルートフォルダ・個人XML・ZIP命名規則。ただし旧 `ix08.xml` 名は再利用しない
- `DATA` / `XSD` 配置とXSD一式
- XMLインデント、mixed content整形、ZIP作成の考え方

### Change from the Old Exporter

- 旧 `work_other` 2テーブルや個別ledgerではなく、`exam_ledgers` / `exam_export_cases` / `exam_export_case_values` / `exam_item_values` を読む。
- 全員一律の `10/010` 既定値は使わない。CSV取込時にevent年齢規則で40～74歳を `10/010`、それ以外を `40/990` として補完する。
- event年齢規則または生年月日を解決できずコードが空のままの場合は出力対象外とする。
- 健診機関番号・保険者番号の `000...` fallbackを廃止し、不正値はエラーにする。
- 基本情報は `match` 優先ではなく共通libのnorm/export値を使う。
- 検査値はrawではなく型別の正規化済みカラムを使う。
- sectionはAnnex2 flagから再判定せず、取込時に確定した `section_code` を使う。
- `occurrence_no` を反映する。
- 付属2の一連検査グループを `exam_item_master` から再現する。
- ETL run/errorと結合出力用caseの `xml_export_status` を更新する。
- 完成フォルダへ直接書かず、一時ディレクトリでXML生成・検証後に確定配置する。
- 個人XMLとIX08をXSD検証してから出力成功とする。

## Implemented Layout

```text
scripts/from_medical/04_export_hia_xml.py
  DB対象抽出、grouping、ETL、状態更新、出力先制御、作業用出力履歴CSV

scripts/from_medical/config/export_hia_xml.yml
  event_id、DB schema、XSD bundle ID、対象条件、既出力者指定、dry-run、limit、同日分割送信回数、提出日、出力モード

対象条件はYAMLで以下を指定できる。CLIで同じ条件を指定した場合はCLIを優先する。

- `use_latest_xml_export_list`: `true` の場合、明示条件がない通常Runでは最新READY出力リストを自動選択する。
- `all_facilities`: `true` の場合、指定event内の全施設を直接条件指定で対象にする。
- `facility_codes`: 複数の健診機関コードを指定する。手動運用では原則こちらを使用する。
- `facility_ids`: `exam_facilities.exam_facility_id` の内部IDを指定する。
- `file_receipt_ids`: 受領ファイル単位で指定する。
- `ledger_ids`: CSV行台帳単位で個人を指定する。
- `exam_month`: `YYYY-MM` で受診月を指定する。全施設月指定を行う場合は `all_facilities: true` も明示する。
- `output_mode`: `official` または `review`。`official` はHIAアップロード対象の正式出力、`review` はプロジェクト `data` 配下への確認用出力。
- `review_output_root`: `output_mode = review` の基点。未指定時は `<repo>/data/hia_xml_review_exports`。

誤出力防止のため、`use_latest_xml_export_list` も、`xml_export_list_id` / `all_facilities` / `facility_codes` / `facility_ids` / `file_receipt_ids` / `ledger_ids` 等の明示条件もない場合は停止する。

CLIの `--health-db` / `--dev-db` / `--master-db` でschema名を上書きできる。
M4 Dockerでは `--dev-db m4_dev_phr` を指定し、実行環境では既定の `dev_phr` を使用する。

scripts/from_medical/script_lib/hia_xml_export_loader.py
  exam_ledgers / exam_export_cases / exam_export_case_values / exam_item_values / exam_facilities の取得、論理健診結果候補の構築

scripts/lib/examination/mhlw_v08_xml.py
  個人CDA、IX08、命名、XML書込、XSD検証、ZIP構成

scripts/lib/identity/export_fields.py
  既存identity field関数を組み合わせたXML出力用基本情報projection。独自の正規化規則は持たない
```

2026-08-05時点では、`04_export_hia_xml.py` と `hia_xml_export_loader.py` に旧CSV行台帳起点の経路が残っている。
今後の実装正はこの文書のとおりcase起点であり、exporterは `exam_export_cases` / `exam_export_case_values` から個人XMLを作成する方向へ切り替える。

旧 `medi_export_xml.py` は変更しない。
新処理から旧スクリプトをimportするのではなく、確認済みのXML生成部分を共通moduleとして新設し、テストで旧出力構造との差分を管理する。

## Input Preconditions and Remaining Information

### Report and Program Code Input

現行5 formatでは、厚生労働省の報告区分・プログラムコードとして直接使用できる受領項目は確認できていない。

| format | report category | MHLW program code |
| --- | --- | --- |
| ヒロオカ | 未設定 | 未設定 |
| ヘルスケアクリニック厚木 | 未設定 | 未設定 |
| 渋谷ウェストヒルズ | 未設定 | 未設定 |
| ハートクロス | 未設定 | 未設定 |
| 小禄病院 | 未設定 | 未設定。CSVの施設内コースコード `1` / `2` は使用しない |

ヒロオカ、厚木、渋谷のサンプルには `コース名称` があるが、既存決定どおり名称からコードを自動推測しない。
小禄の `健診コースコード` も厚生労働省プログラムコードとの対応が確認できるまで直接使用しない。

報告区分と厚生労働省プログラムコードは、CSV mappingによって正しい値がledgerへ登録されている場合は、その値をXML出力に使用する。
mapping対象がない、またはmapping値がNULLの場合は、CSV取込時に `event.age_rule_type` と `event.age_reference_date` を参照して満年齢を求め、40～74歳を `10/010`、それ以外を `40/990` として不足値を補完する。
`event_id = 2` は2026年度の年齢基準日 `2026-11-30` を使用する。
コース名称、検査項目構成、施設内コードからは推測しない。

### Implemented Common Library Work

- `normalize_insurance_symbol()` は元値の全角有無をNFKC前に判定し、数字だけは半角、それ以外は元値に全角が1文字でもあれば全体を全角へ寄せるよう修正した。
- 郵便番号・住所のXML出力規則は `scripts/lib/identity/field/address.py`、電話番号は `scripts/lib/identity/field/phone_number.py` へ共通関数として移した。
- `export_fields.py` は既存identity関数と上記共通field関数を組み合わせ、XML必須値を確定する薄いprojectionとして実装した。
- 共通identity出力、候補判定、公式命名、個人CDAとIX08のV08 XSD検証をテストで固定した。
- 支払基金公開サンプルを基準に、詳細健診項目の一連検査グループ、原本判定、基準範囲、negationIndの出力をテストへ追加した。

### Applied Database Change

- `20260730_009_health_exam_result_create_xml_export_history.sql` を追加した。
- 結合出力用caseに手動許可の承認日時・承認者を追加した。
- `xml_export_zips` と `xml_export_members` を追加し、`etl_runs` を親に出力事実を追記する。
- 統合報告用snapshotにも手動許可の承認日時・承認者を引き継ぐ。

### Confirmed Export Decisions

1. 検査値のエラー状態
   - `validation_status = VALID` は出力する。
   - `WARNING/SKIPPED` の未実施等は初期版ではentryを省略し、nullFlavor変換は後続版で扱う。
   - `INVALID` は該当entryを出力しない。
   - 基本情報norm失敗、手動許可条件を満たさない法定項目NG、XSD不一致は個人XML生成失敗とする。
2. 伝送単位設定
   - 同日分割送信回数は、同じ提出元・提出先・作成日の既存ZIPから `0` から `9` を自動採番する。
   - CLI/APIから `0` から `9` の数値を明示指定することもできる。
   - 自動採番・明示指定とも、既存ZIPと衝突する場合は上書きせず停止する。
   - ルートフォルダ名およびZIP名末尾の `_1` は、今回の実施区分「特定健診情報」を表す。
   - 個人XMLファイル名21桁目の種別も、今回の特定健診情報を表す `1` 固定とする。
3. ZIP
   - 最終成果物は `<健診機関番号>_<保険者番号>_<yyyymmdd><同日分割送信回数>_1.zip` とし、展開済みフォルダは残さない方針で確定する。
   - ZIP単位は、健診機関、保険者、作成日、同日分割送信回数の組み合わせとする。
   - ZIP対象者のうち1人でも個人XML生成またはXSD検証に失敗した場合、そのZIP全体を出力しない。
   - 失敗したZIPと別のZIP単位は処理を継続できる。
4. 健診機関番号の正
   - `phr_master.exam_facilities.exam_facility_code` を正として出力する。
   - 結合出力用caseで採用した健診機関コードとmaster値が不一致の場合は、そのZIPを停止する。
5. 健診機関情報
   - export時に `exam_facilities` を参照し、名称、郵便番号、住所、電話番号を取得する。

### Export History

- `etl_runs` をXML出力処理の技術的な実行ログとし、出力対象を人が確認する作業単位は出力リストとして別に扱う。
- 正常完成したZIPを `xml_export_zips`、そのZIPへ収録した個人XMLを `xml_export_members` へ追記する。
- ZIP全体の生成とXSD検証が成功した後、ZIP履歴、個人XML履歴、技術的な出力済み状態を同一トランザクションで確定する。
- ZIPが失敗した場合は出力履歴を登録せず、失敗内容を `etl_runs` / `etl_errors` に残す。
- 再出力は過去履歴を更新・削除せず、別のZIP・個人XML履歴として追加する。
- 結合出力用caseの `xml_export_status` は既出力判定用の技術状態として使用できるが、出力履歴の正本にはしない。
- 再scan/importや統合ledger同期で構成元 `exam_ledgers` 側の `xml_export_status` が未出力へ戻っても、`xml_export_members` に出力事実がある場合は `exam_ledgers.xml_export_status = 'EXPORTED'` として復元する。
- 正式出力済みXML/ZIPは再出力しないことを基本とし、再出力が必要な場合は別途再出力理由と履歴管理を決める。
- `xml_export_members` には、出力時点の `manual_export_approved`、理由、承認者、承認日時をsnapshotとして保存する。
- 初回出力日時や出力回数は、専用履歴から取得する。
- 履歴テーブルの責務は、誰を、いつ、どのRun・ZIP・個人XMLとして出力したかという事実の保存までとする。
- 項目不足や誤りを修正して再出力した場合も、旧履歴は証跡として残す。どの出力を正本とするか、旧出力を無効化するかは初期版では判定しない。
- 個人単位の業務状態、HIAアップロード状態、納品状態、後続業務データへの反映時点は、業務フロー整備時に別途決める。

### Deferred Decisions

1. Runtime不足情報の見せ方
   - `etl_errors` とRunサマリーだけにするか。
   - 人が確認しやすい不足情報CSVを、送付用ZIPの外側へ併せて出すか。
   - ここでいう不足情報CSVは、確定済みの `健診結果XML出力履歴.csv` とは別物とする。
   - 不足情報CSVの追加は初期XML実装を止めず、後続で決める。
   - `manual_export_approved` / `manual_export_reason` は確認後の手動Goを表す列であり、不足情報CSVとは別概念である。

## Initial Recommendation

- 出力候補条件は確定済み4条件をそのまま使う。
- `VALID` の検査値だけを初期出力し、`INVALID` は出さない。
- 未実施等の `WARNING/SKIPPED` は初期版ではentryを出力しない。
- 同日分割送信回数は自動採番を既定とし、`0`から`9`の明示指定も許可する。個人XML種別は `1` 固定とする。
- 健診機関情報は `exam_facilities` を正とし、受領時snapshotとの差異を検証する。
- 出力は一時ディレクトリで作成し、個人XMLとIX08のXSD検証後に確定する。
- 1人でも生成・検証に失敗したZIP単位は成果物を作らず、別ZIP単位の処理は継続する。
- 不足情報は `etl_errors` に構造化して残し、Run終了時に項目別件数を表示する。
- イベントルートの `xml作成_出力履歴/yyyymmdd_hhmmss/健診結果XML出力履歴.csv` に、健診機関別のアップロード対象ZIP件数を一覧化する。
- XML/ZIPは `xml作成_出力履歴` 側へ複製しない。

## M4 Verification

2026-07-30に、ヒロオカfixtureを使用してscan、CSV import、加入者突合、法定チェック、XML出力を一連実行した。

- 7人中5人は `MATCHED / check OK` として1 ZIPへ出力した。
- 基本情報不足の2人は候補判定で停止し、ZIPへ含めなかった。
- 個人XML5件はV08 XSDへ適合した。
- 個人XML内で、付属2一連検査グループを親observation + `COMP` / `RSON` として確認した。
- ZIP履歴1件、個人履歴5件、当時のCSV台帳の出力済み状態5件を確認した。
- 業務向け `健診結果XML出力履歴.csv` に健診機関コード、名称、フォルダ名、出力フォルダ、人数5を出力した。

出力履歴は今回の実装対象とする。個人単位の業務状態や修正版の正本判定、不足情報CSVは後続判断とし、初期実装を止めない。
