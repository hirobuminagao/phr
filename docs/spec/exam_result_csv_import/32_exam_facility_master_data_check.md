# Exam Facility Master Data Check

## Status

Draft.

このドキュメントは、CSV健診結果取込の `exam_facilities` 初期整備に向けて、支払基金CSVと既存 `medical_folder_aliases` seed の突合状況を確認した結果を整理する。

## Source Files

支払基金CSV:

- `docs/spec/exam_result_csv_import/downloads/Pref_00.csv`
- 文字コード: CP932
- ファイルサイズ: 8,705,832 bytes
- SHA-256: `6fd3348a13da4a0f6143ba6ace7a9646e1684d6af070b6f29125e98ec0b8915e`
- データ行数: 54,712
- ヘッダー列数: 8

既存alias seed:

- `sql/seed/health_exam_result/0010_health_exam_result__medical_folder_aliases_event2.sql`
- `event_id = 2`
- alias件数: 188

## 支払基金CSV Columns

| column_no | column_name | `exam_facilities` mapping |
| ---: | --- | --- |
| 1 | 機関コード | `medical_institution_code` |
| 2 | 機関種別 | `exam_facility_type` |
| 3 | 機関名 | `exam_facility_name` |
| 4 | 郵便番号 | `postal_code` |
| 5 | 電話番号 | `phone_number` |
| 6 | 機関所在地 | `address` |
| 7 | ホームページ | `website_url` |
| 8 | 経営主体 | `management_entity` |

`exam_facility_code` は内部業務コードとして必要だが、初期seedでは `medical_institution_code` と同値で作成する案が自然である。
将来、予約システムや別ソース由来のコードを使う場合は `reservation_system_medical_institution_code` などへ分ける。

既存受領フォルダは医療機関番号を先頭10桁にして作成されている運用と考えられる。
したがって、alias先頭10桁は `medical_institution_code` 候補として扱う。
全国CSVに見つからない番号がある場合も、地方厚生局・都道府県単位のオープンデータや別年度/別区分の公開データに存在する可能性がある。
初期実装では地方局データまで探索して正規化しきることはせず、支払基金CSV、過去CSV/XML実績、受領データ内の健診機関番号で確認できた範囲だけを採用する。

## Pilot Facilities

### ヒロオカクリニック

支払基金CSVに存在する。

| item | value |
| --- | --- |
| 機関コード | `1310438796` |
| 機関種別 | `特定健診・指導` |
| 機関名 | `医療法人社団　順正会　ヒロオカクリニック` |
| 郵便番号 | `160-0022` |
| 電話番号 | `03-3225-1666` |
| 機関所在地 | `東京都新宿区新宿２－５－１２　ＦＯＲＥＣＡＳＴ新宿ＡＶＥＮＵＥ３Ｆ` |
| ホームページ | `http://www.h-c.l.org/` |
| 経営主体 | `診療所　医療法人` |
| alias src | `1310438796_ヒロオカクリニック` |

alias先頭10桁と支払基金CSVの `機関コード` は一致する。
alias名は短縮名、支払基金CSVの `機関名` は法人名込みであり、差分は許容する。

### ハートクロス健診プラザ赤坂駅前

支払基金CSVに存在する。

| item | value |
| --- | --- |
| 機関コード | `4011028133` |
| 機関種別 | `特定健診・指導` |
| 機関名 | `ハートクロス健診プラザ赤坂駅前` |
| 郵便番号 | `810-0041` |
| 電話番号 | `092-791-4292` |
| 機関所在地 | `福岡県福岡市中央区大名２－１２－８` |
| ホームページ | `http://ohga-prevention.com/` |
| 経営主体 | `診療所　医療法人` |
| alias src | `4011028133_ハートクロス健診プラザ赤坂駅前` |

alias先頭10桁と支払基金CSVの `機関コード` は一致する。

## Alias Coverage

既存 `medical_folder_aliases` seed 188件を、alias先頭10桁と支払基金CSV `機関コード` で突合した。

| result | count |
| --- | ---: |
| alias件数 | 188 |
| 支払基金コード一致 | 179 |
| 支払基金コード未一致 | 8 |
| 先頭10桁コードなし | 1 |

名前差分は156件ある。
ただし多くは法人名、全角半角、略称、施設表示名の違いであるため、`exam_facilities` 初期整備では名称一致を主キー的に使わず、機関コード一致を正とする。

## Unmatched Aliases

支払基金CSVの `機関コード` と一致しなかったaliasは以下である。

協議と過去CSV/XML確認により、8件中7件は採用コードを確定した。
浦和医師会 健診センターのみ、実績なしのため保留する。

| alias code | alias name | adopted code | status | note |
| --- | --- | --- | --- | --- |
| `0415312420` | `みやぎ健診プラザ` | `0421200015` | 確定 | CSV内健診機関番号。支払基金CSVに `0421200015 みやぎ健診プラザ` として存在 |
| `1010211041` | `伊勢崎健診プラザ` | `1020700017` | 確定 | 去年実績。支払基金CSVに `一般財団法人　日本健康管理協会　北関東支部` として存在 |
| `1110103762` | `浦和医師会　健診センター` | NULL | 保留 | 今年実績なし、去年実績なし。データ受領時にCSV/XML内の健診機関番号で紐付ける |
| `1210122986` | `IMS　Me-Lifeクリニック千葉` | `1220700072` | 確定 | XML確認。支払基金CSVに `明理会ＩＭＳＭｅ－Ｌｉｆｅクリニック千葉` として存在 |
| `2310607227` | `KKCウエルネス名古屋健診クリニック` | `2320700061` | 確定 | XML確認。支払基金CSVに `一般財団法人近畿健康管理センターＫＫＣウエルネス名古屋健診クリニック` として存在 |
| `2311301861` | `守山内科・小児科　守山健康管理センター` | `2320800200` | 確定 | 支払基金CSVに `医療法人順秀会守山内科・守山健康管理センター` として存在 |
| `2719109346` | `KKCウエルネ新大阪健診クリニック` | `2720700059` | 確定 | 昨年XML実績。支払基金CSVに `ＫＫＣウエルネス新大阪健診クリニック` として存在 |
| `4410121711` | `おおいた健診センター` | `4420700074` | 確定 | XML確認。支払基金CSVに `おおいた健診センター` として存在 |

先頭10桁コードではないalias:

| alias | note |
| --- | --- |
| `202604開院_福岡労働衛生研究所　健診スクエア博多` | 旧仮フォルダ名。正式採番済みフォルダがあるため、`exam_facilities` 対象外 |

## Decision-Oriented Findings

1. ヒロオカ、ハートクロスは支払基金CSVで確定できる。
2. 既存aliasの大半は、先頭10桁コードで支払基金CSVと突合できる。
3. 受領フォルダ先頭10桁は医療機関番号候補として扱い、名称の自動名寄せではなくコードを起点に `exam_facility_id` を決める。
4. `exam_facilities` 初期seedは、支払基金CSV全件を物理保存する。
5. 支払基金CSVに一致しない8件のうち、7件は過去CSV/XML実績または確認済みコードで支払基金CSVへ紐付ける。
6. 浦和医師会 健診センターは保留し、データ受領時にCSV/XML内の健診機関番号で紐付ける。
7. 旧仮フォルダ `202604開院_福岡労働衛生研究所　健診スクエア博多` は `exam_facilities` 対象外とする。
8. 名前差分は多いため、名称の自動名寄せで `exam_facility_id` を決めない。
9. `medical_folder_aliases.exam_facility_id` は、alias先頭10桁または確認済み採用コードと `exam_facilities.medical_institution_code` の対応で初期付与する。
10. `exam_facilities` 初期seedの全行に、支払基金公開CSV由来であることを示す `data_source_*` を入れる。
11. `data_source_note` で、社内作業データ、受領CSV、機微情報を含まないことを明示する。

## Future Medical Institution Master

医療機関番号の正規マスタ化は後続バージョンで扱う。
契約、請求、予約システムなどが医療機関番号を使用しているため、それらと連携する段階では `medical_institutions` のような医療機関マスタを別途持ち、`exam_facilities` と紐づける構成を検討する。
この論点は事業所単位の関係ではなく、健保、代行機関、医療施設の連携関係として扱う。
後続設計では、`exam_facilities` の一階層上に医療施設/医療機関マスタを置く案、または既存 `exam_facilities` に連携カラムを追加する案を比較する。

その後続設計では、全国CSVだけでなく地方厚生局・都道府県単位のオープンデータ、別年度/別区分の公開データ、契約/請求側の実データを突合し、番号ギャップを医療機関マスタ側で吸収する。
今回のCSV健診結果取込では、`exam_facilities.medical_institution_code` を暫定的な医療機関番号保持先とし、正規医療機関マスタへの分離は行わない。

## Recommended Next Step

次は、`exam_facilities` 初期seed作成方針を以下に固定する。

- 支払基金CSV全件から `exam_facilities` seedを作る。
- `exam_facility_code = medical_institution_code` とする。
- `exam_facility_display_name` はalias名を使うか、支払基金CSVの `機関名` を使うかを決める。
- `exam_facility_name` は支払基金CSVの正式名、`exam_facility_display_name` はalias側の短い名前とする。
- `medical_folder_aliases` 移設seedでは、コード一致または確認済み採用コードで `exam_facility_id` を付与する。
- `exam_facilities` seedには、支払基金公開CSV由来であることを示すsource情報を全行に入れる。
- 浦和医師会 健診センターは `exam_facility_id = NULL` の確認対象として扱う。
