# Postal Code Master Design

## Status

Draft as of 2026-07-31.

HIA XML出力時、受診者住所がCSV/XML/加入者情報のいずれにもない場合に備え、日本郵便の郵便番号データから住所補完masterを作る。

## Source

初期入力は日本郵便公式の `utf_ken_all.csv` とする。

- 取得元: 日本郵便「住所の郵便番号（1レコード1行、UTF-8形式）」
- ローカル確認ファイル: `/Users/hiro/Downloads/utf_ken_all.csv`
- 文字コード: UTF-8
- ヘッダー: なし
- レコード区切り: CRLF
- 行数: 124,513
- 列数: 15
- 2026-07-31時点のローカルファイル更新日時: 2026-06-24 10:26

APIは初期実装では採用しない。
郵便番号・デジタルアドレスAPIは無料で公式提供されているが、ビジネスアカウント登録、API権限、通信可否、障害時運用、呼び出し証跡管理が必要になる。
今回の用途はHIA XML出力時の住所補完であり、リアルタイム性は不要なため、CSVをmaster DBへ取り込む方式を採用する。

## Column Mapping

日本郵便のUTF-8 1レコード1行形式は、以下の15列である。

| no | proposed column | meaning |
| --- | --- | --- |
| 1 | `jis_code` | 全国地方公共団体コード |
| 2 | `old_postal_code` | 旧郵便番号5桁 |
| 3 | `postal_code` | 郵便番号7桁 |
| 4 | `prefecture_kana` | 都道府県名カナ |
| 5 | `city_kana` | 市区町村名カナ |
| 6 | `town_area_kana` | 町域名カナ |
| 7 | `prefecture` | 都道府県名 |
| 8 | `city` | 市区町村名 |
| 9 | `town_area_raw` | 町域名原文 |
| 10 | `is_multi_postal_town` | 一町域が二以上の郵便番号で表される |
| 11 | `has_koaza_numbering` | 小字毎に番地が起番されている |
| 12 | `has_chome` | 丁目を有する町域 |
| 13 | `is_multi_town_postal` | 一つの郵便番号で二以上の町域を表す |
| 14 | `update_flag` | 更新の表示 |
| 15 | `change_reason_code` | 変更理由 |

## Observed Data

`/Users/hiro/Downloads/utf_ken_all.csv` を確認した結果は以下。

| item | value |
| --- | --- |
| rows | 124,513 |
| column count | all rows are 15 columns |
| unique postal codes | 120,682 |
| postal codes with multiple rows | 1,341 |
| maximum rows per postal code | 66 |
| `以下に掲載がない場合` rows | 1,870 |
| `の次に番地がくる場合` rows | 17 |
| `一円` rows | 23 |
| `その他` rows | 565 |
| rows with parentheses in town area | 6,504 |
| max `town_area_kana` length | 302 chars |
| max `town_area_raw` length | 297 chars |

郵便番号はユニークではない。
そのため、`postal_code` を主キーにした単純masterにはしない。
原本行は `jis_code + postal_code + town_area_raw + source_row_sha256` で保持し、補完時に使う代表住所を別に判断できる構造にする。

## Address Normalization

XML出力用住所は、原則として以下を連結する。

```text
prefecture + city + town_area_normalized
```

ただし、町域原文が以下の場合は町域として扱わない。

| raw town area | normalized behavior |
| --- | --- |
| `以下に掲載がない場合` | `town_area_normalized = ''`, `address_for_xml = prefecture + city` |
| `○○市の次に番地がくる場合` | `town_area_normalized = ''`, `address_for_xml = prefecture + city` |
| `○○町の次に番地がくる場合` | `town_area_normalized = ''`, `address_for_xml = prefecture + city` |
| `○○村の次に番地がくる場合` | `town_area_normalized = ''`, `address_for_xml = prefecture + city` |
| `○○市一円` / `○○村一円` | 原則 `town_area_normalized = ''`。市区町村名と重複する場合は二重出力しない |

`その他` や括弧つき町域は一律削除しない。
郵便番号の範囲情報として意味があるため、初期実装では原文を保持し、`address_for_xml` にも含める。
ただし、人が修正画面で確認できるよう `normalization_note` に特殊表記であることを残す。

## DDL

`phr_master.postal_code_addresses` を追加する。
正式DDLは `sql/ddl/phr_master/0090_phr_master__postal_code_addresses.sql` とする。
実行環境適用用migrationは `sql/migrations/phr_master/20260731_004_phr_master_create_postal_code_addresses.sql` とする。

`postal_code` は7桁数字、`postal_code_formatted` は `NNN-NNNN` 形式とする。
lookup入力はハイフンあり・なしの両方を受け、内部では7桁数字に寄せる。

## Lookup Behavior

郵便番号から住所を補完する共通libは `scripts/lib/db/lookup/postal_code_address.py` とする。
健診結果CSV/XML出力、基本情報補正画面、HIA系処理などから再利用できるよう、個別スクリプトへSQLを直書きしない。

主API:

```text
lookup_postal_code_address(cur, postal_code, master_db='phr_master')
lookup_postal_code_address_for_xml(cur, postal_code, master_db='phr_master')
```

`lookup_postal_code_address()` は以下を返す。

```text
PostalAddressLookupResult
  ok
  postal_code
  postal_code_formatted
  candidate_count
  selected_address_for_xml
  candidates[]
  reason
```

候補が1件の場合は自動採用できる。
候補が複数件の場合は、初期実装では以下の順で扱う。

1. `town_area_normalized = ''` の代表行が1件だけあれば、その市区町村住所を採用する。
2. 候補が複数残っても、すべて同じ都道府県・市区町村に属する場合は、`prefecture + city` までを代表補完住所として返す。
3. 都道府県・市区町村レベルでも一意にできない場合は、自動で詳細町域を推測しない。
4. 画面では候補一覧を表示し、人が正しい町域または手入力住所を選べるようにする。

郵便番号が不正、7桁に正規化できない、またはmasterに存在しない場合はlookup失敗とする。
HIA提出を止めない運用では、最終fallbackとして郵便番号 `000-0000`、住所 `－` を使い、理由を記録する。

lookup層は候補取得と代表候補の提示までを責務とする。
複数候補時に `prefecture + city` の代表住所を返す場合でも、それを採用するか、画面確認へ回すか、最終fallbackを使うかは呼び出し側の業務処理で決める。

現行CSV importは、加入者突合後に受診者住所が空で郵便番号がある場合のみlookupを使用し、補完候補と補完状態を `csv_row_ledger` へ保存する。
`sync_exam_ledgers.py` は補完候補と状態を `exam_ledgers` へコピーする。
XML exporterは郵便番号masterを直接lookupせず、原本住所または事前準備済みの補完候補を使う。
原本住所がある場合は原本住所をXML出力normへ通して使い、lookup候補で置き換えない。
lookup失敗時の `000-0000` / `－` 最終fallbackは、補正履歴テーブルまたは画面承認と合わせて後続実装とする。

## Loader

初期loaderは手動実行のdev/operation toolとして作る。

```text
scripts/from_medical/dev_tools/import_postal_code_addresses.py
```

入力:

```text
--source /Users/hiro/Downloads/utf_ken_all.csv
--master-db phr_master
```

処理:

1. UTF-8でCSVを読む。
2. 15列でない行はエラーにする。
3. 郵便番号を7桁数字に正規化する。
4. `town_area_normalized` と `address_for_xml` を作る。
5. `source_row_sha256` を作る。
6. 既存masterを全件置換するか、同一source_row_sha256をupsertする。

初期実装では全件置換を推奨する。
郵便番号masterは外部公開データのsnapshotであり、個別修正対象ではないため、差分mergeよりも「今回使用した公式CSVの内容」を再現しやすい。

## Open Items

- source CSVをrepositoryへ保存するか。18MB程度あるため、初期は保存せず、取得元、ファイル名、取込日時、source row hashをDBに記録する案を優先する。
- 複数候補時に `prefecture + city` だけで自動補完してよいか。HIA提出優先なら可、正確性優先なら画面確認に回す。
- 事業所個別郵便番号を後続で追加するか。
