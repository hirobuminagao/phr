# CSV to HIA XML Export Design Draft

## Status

Initial implementation completed as of 2026-07-30. The confirmed behavior in this document is implemented; deferred items remain explicitly marked.

この文書は、`csv_row_ledger` と `exam_item_values` から厚生労働省指定の健診結果XMLを生成する処理の叩き台である。
確定済み条件と、実装前に確認が必要な不足情報を分けて記載する。

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

CSV行は以下をすべて満たす場合にXML出力候補とする。

1. `csv_row_ledger.health_exam_report_category` が空でない。
2. `csv_row_ledger.program_code` が空でない。
3. `csv_row_ledger.subscriber_match_status = 'MATCHED'`。
4. 次のいずれかを満たす。
   - `csv_row_ledger.check_status = 'OK'`。
   - 法定チェックNGの原因が `MISSING` のみで、`manual_export_approved = 1`、理由、承認者、承認日時が設定されている。

`03_check_exam_results.py` の `check_status` は現状、法定チェック結果を基準に `OK` / `NG` を設定している。
したがって4は、法定チェックOKまたはMISSINGだけを理由として明示的に手動許可された状態を意味する。

手動出力許可は、妊娠中等の確認済み理由により法定検査値がMISSINGとなる場合の例外とする。
`exam_check_results` と `csv_row_ledger.check_status` は書き換えず、架空の検査値を作らない。MISSINGの該当entryはXMLへ出力しない。
`INVALID`、`PARSE_ERROR`、加入者不一致、報告区分・プログラムコード不足、健診機関不一致は手動許可の対象外とする。
既存の `manual_export_approved` / `manual_export_reason` に加え、`manual_export_approved_at` / `manual_export_approved_by` を `csv_row_ledger` へ追加する。手動許可時は理由、承認者、承認日時を必須とする。

上記4条件は業務上の出力候補条件とする。
XML生成時に必須値のnorm失敗、健診機関番号不正、XSD不一致などが発生した場合は、出力候補であっても生成エラーとして扱う。

## Export Selection and Logical Exam Records

画面およびCLIの対象選択は、同じ選択modelと対象抽出処理を使用する。

| selector | initial behavior |
| --- | --- |
| event | 必須。画面を開いたeventを固定する |
| 健診機関 | 必須。複数選択可。全施設も明示選択とする |
| 受領ファイル | 任意。`file_receipt_id` の複数選択可。未指定時は選択施設内の全対象CSV |
| 健診年月 | 任意。`csv_row_ledger.exam_date` に対する単月 `YYYY-MM` 指定 |
| 個人 | 任意。画面で表示された論理健診結果を複数選択可 |

各selectorはAND条件で適用する。
受領ファイルは対象行の抽出条件であり、ZIPの分割単位にはしない。
同じ健診機関・保険者に属する複数ファイルの結果は、同じ出力Runでは1つのZIPへまとめる。
ファイル別にZIPを分ける場合は別Runとして実行する。
複数ファイル結合でも、選択されていない受領ファイルをexporterが暗黙に追加しない。
結合に使う受領ファイルは画面またはCLIで明示選択し、画面は同一人物の補完候補ファイルを案内できる形とする。

画面では、対象件数、出力可能件数、出力不可件数と理由を実行前に表示する。
既出力者の扱いは、受診年月等と同じ対象抽出条件として `含めない` / `含める` の2モードを持つ。
通常運用の既定値は `含めない` とし、初期検証や再出力では `含める` を明示選択できるようにする。
`含める` は未出力者と既出力者の両方を対象とし、既出力者だけを抽出する第3モードは初期版では設けない。

### Logical Exam Record

将来、同一人物の結果が複数の受領CSVに分かれ、一方のCSVにしか存在しない検査項目を組み合わせて1件のXMLを作る可能性がある。
そのため、外部仕様およびexporter内部では `csv_row_ledger` 1行をそのまま1件の出力単位にしない。

1件の個人XMLに対応する出力単位を「論理健診結果」とし、次の構造で扱う。

```text
LogicalExamRecord
  candidate_key
  event_id
  exam_facility_id
  insurer_number
  subscriber_id
  exam_date
  health_exam_report_category
  program_code
  source_ledger_ids[]
  source_file_receipt_ids[]
  merged_exam_item_values[]
```

画面の個人選択は加入者単位ではなく、この論理健診結果単位とする。
表示項目には氏名、カナ、生年月日、健診日、健診機関、構成元受領ファイルを含める。
同一人物に別日または別プログラムの健診結果がある場合は、別の候補として表示する。

初期実装でも `source_ledger_ids` はlistとして扱う。
複数行結合をまだ有効にしない段階で同一候補に複数行が見つかった場合は、重複XMLを自動生成せず `MULTIPLE_SOURCE_ROWS_UNRESOLVED` として出力対象外にする。
将来の結合対応では同じ候補を `COMBINE` modeで処理できるようにし、画面/APIの選択単位を変更しない。

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

`xml_ledger` はXMLの `ClinicalDocument/code` を `report_category_code`、`documentationOf/serviceEvent/code` を `program_type_code` として保存する。
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

結合後も証跡を失わないよう、個人XMLごとに全 `source_ledger_ids` と `source_file_receipt_ids` を追跡する。
出力成功時は、構成元となった全 `csv_row_ledger` に同じexport Runと出力先を関連付ける。
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
| 提出先保険者番号 | 8桁。8桁未満は先頭ゼロ埋め | `csv_row_ledger.insurer_number` |
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

```text
<event.result_root_path>/
  <健診機関フォルダ>/
    03_健診結果（アップロードデータ）/
      yyyymmdd_hhmmss/
        <健診機関番号>_<保険者番号>_<yyyymmdd><同日分割送信回数>_1.zip
```

健診機関フォルダは、CSVに対応する `file_receipts.relative_path` の先頭フォルダを第一候補とする。
これにより、実際に受領したCSVの健診機関フォルダへ出力できる。

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
`03_健診結果（アップロードデータ）/yyyymmdd_hhmmss/` にはアップロード対象ZIPだけを残す。

既存出力は上書きしない。
同一秒の出力先がすでに存在する場合は、既存内容へ追記せず停止する。

## Operator Export Log

HIAへのアップロードは健診機関ごとのページから行い、同じページで全施設分を一括アップロードできない。
そのため、出力Runごとにアップロード作業用の一覧ログをイベントルートへ出力する。

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
| 人数 | 対応するZIPへ格納し、IX08件数へ反映した個人XML数 |

CSVヘッダーは以下とする。

```text
健診機関コード,健診機関名,健診機関フォルダ名,出力フォルダ,人数
```

ログはWindows/Excelでの確認を考慮し、UTF-8 BOM付きCSVを第一候補とする。
同じ出力Runでは、各健診機関フォルダ側の出力ディレクトリとログ側ディレクトリに同じ `yyyymmdd_hhmmss` を使う。
XSD検証とZIP作成まで成功した健診機関だけをログへ記載し、人数は最終的なIX08の `totalRecordCount` と一致させる。
ログは一時ファイルへ書き、全施設の処理終了後に確定ファイル名へ置き換える。

XML、XSD、ZIP本体は `xml作成_出力履歴` へコピーしない。
ZIPは各健診機関フォルダの `03_健診結果（アップロードデータ）/yyyymmdd_hhmmss/` にだけ配置し、一覧ログから健診機関フォルダ名と出力フォルダを確認してアップロードする。

## Basic Information Normalization

XML基本情報は照合用 `match` ではなく、格納・出力用のnorm値を使う。
元値は `csv_row_ledger` のCSV由来値とし、`subscribers` の値へ置き換えない。
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

## Exam Item Values

検査値は `health_exam_result.exam_item_values` の `ledger_type = 'CSV'`、`ledger_id = csv_row_ledger_id` を対象とする。

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

- 旧 `work_other` 2テーブルではなく `csv_row_ledger` / `exam_item_values` を読む。
- 全員一律の `10/010` 既定値は使わない。CSV取込時にevent年齢規則で40～74歳を `10/010`、それ以外を `40/990` として補完する。
- event年齢規則または生年月日を解決できずコードが空のままの場合は出力対象外とする。
- 健診機関番号・保険者番号の `000...` fallbackを廃止し、不正値はエラーにする。
- 基本情報は `match` 優先ではなく共通libのnorm/export値を使う。
- 検査値はrawではなく型別の正規化済みカラムを使う。
- sectionはAnnex2 flagから再判定せず、取込時に確定した `section_code` を使う。
- `occurrence_no` を反映する。
- 付属2の一連検査グループを `exam_item_master` から再現する。
- ETL run/errorと `csv_row_ledger.xml_export_status` を更新する。
- 完成フォルダへ直接書かず、一時ディレクトリでXML生成・検証後に確定配置する。
- 個人XMLとIX08をXSD検証してから出力成功とする。

## Implemented Layout

```text
scripts/from_medical/04_export_hia_xml.py
  DB対象抽出、grouping、ETL、状態更新、出力先制御、作業用出力履歴CSV

scripts/from_medical/config/export_hia_xml.yml
  event_id、DB schema、XSD bundle ID、対象条件、既出力者指定、dry-run、limit、同日分割送信回数、提出日

対象条件はYAMLで以下を指定できる。CLIで同じ条件を指定した場合はCLIを優先する。

- `all_facilities`: `true` の場合、指定event内の全施設を対象にする。
- `facility_ids`: 複数の健診機関IDを指定する。
- `file_receipt_ids`: 受領ファイル単位で指定する。
- `ledger_ids`: CSV行台帳単位で個人を指定する。
- `exam_month`: `YYYY-MM` で受診月を指定する。全施設月指定を行う場合は `all_facilities: true` も明示する。

誤出力防止のため、`all_facilities` / `facility_ids` / `file_receipt_ids` / `ledger_ids` のいずれもない場合は停止する。

CLIの `--health-db` / `--dev-db` / `--master-db` でschema名を上書きできる。
M4 Dockerでは `--dev-db m4_dev_phr` を指定し、実行環境では既定の `dev_phr` を使用する。

scripts/from_medical/script_lib/hia_xml_export_loader.py
  csv_row_ledger / exam_item_values / exam_facilities の取得、論理健診結果候補の構築

scripts/lib/examination/mhlw_v08_xml.py
  個人CDA、IX08、命名、XML書込、XSD検証、ZIP構成

scripts/lib/identity/export_fields.py
  既存identity field関数を組み合わせたXML出力用基本情報projection。独自の正規化規則は持たない
```

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
- `csv_row_ledger` に手動許可の承認日時・承認者を追加した。
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
   - `csv_row_ledger.facility_code` とmaster値が不一致の場合は、そのZIPを停止する。
5. 健診機関情報
   - export時に `exam_facilities` を参照し、名称、郵便番号、住所、電話番号を取得する。

### Export History

- `etl_runs` をXML出力処理の親とし、出力専用のRun管理テーブルは追加しない。
- 正常完成したZIPを `xml_export_zips`、そのZIPへ収録した個人XMLを `xml_export_members` へ追記する。
- ZIP全体の生成とXSD検証が成功した後、ZIP履歴、個人XML履歴、技術的な出力済み状態を同一トランザクションで確定する。
- ZIPが失敗した場合は出力履歴を登録せず、失敗内容を `etl_runs` / `etl_errors` に残す。
- 再出力は過去履歴を更新・削除せず、別のZIP・個人XML履歴として追加する。
- `csv_row_ledger.xml_export_status` は既出力判定用の技術状態として使用できるが、出力履歴の正本にはしない。
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
- ZIP履歴1件、個人履歴5件、CSV台帳の出力済み状態5件を確認した。
- 業務向け `健診結果XML出力履歴.csv` に健診機関コード、名称、フォルダ名、出力フォルダ、人数5を出力した。

出力履歴は今回の実装対象とする。個人単位の業務状態や修正版の正本判定、不足情報CSVは後続判断とし、初期実装を止めない。
