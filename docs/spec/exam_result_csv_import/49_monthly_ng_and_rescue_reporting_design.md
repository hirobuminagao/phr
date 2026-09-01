# 月次NG・補完／救済実績レポート設計

## 1. 目的

受診月ごとに健診結果処理の品質と人手対応量を報告できるページを追加する。

- 月次レポート: 全体傾向、件数、率、前月差、対応成果を報告する。
- 健診機関サマリー詳細: 施設、ファイル、項目、caseまで掘り下げて原因を調査する。
- CSVマッピングテンプレート: NG傾向から実際のtargetルールを追加・修正する。

## 2. 現在利用できる実装とデータ

| 分類 | 主な参照元 | 利用できる内容 |
| --- | --- | --- |
| case全体 | `exam_export_cases` | event、受診月、健診機関、case状態、出力可否 |
| 法定・特定健診チェック | 最新の `exam_check_results` | OK、NG、対象外、判定不能、理由summary |
| 項目別確認 | `exam_case_check_review_items` | scope、項目コード、項目名、MISSING理由、レビュー状態 |
| 採用値 | `exam_export_case_values` | caseに採用された項目と採用元値 |
| source | `exam_export_case_sources` / `exam_ledgers` | XML、CSV、MANUAL、PAPER等のsource構成 |
| source値 | `exam_item_values` | namecode、値、normalize/validation、元ledger |
| 出力 | `xml_export_members` / `xml_export_zips` | 出力対象、出力済み、ZIP単位 |

既存の健診機関サマリー詳細では、XML/CSVのsource NG、値エラー、caseチェック理由、特定健診NG項目、聴力判定分布を確認できる。

CSVマッピングテンプレートでは、選択施設の特定健診MISSING 100%項目をtarget候補として表示し、既存マッピング状態、XML受領件数、CSVヘッダー検索結果を同じ作業画面で確認できる。

## 3. 画面の位置と権限

- 画面名: `月次NG・対応実績`。
- 配置: ユーティリティ。
- 閲覧権限: 作業管理者以上。
- 初期版は参照専用。
- 氏名、記号番号、住所、電話番号は表示しない。
- 詳細追跡にはcase ID、ledger ID、健診機関コードを使う。

想定URL:

```text
/reports/monthly-exam-ng-summary
```

## 4. 検索条件と集計基準

必須:

- event。
- 受診月。複数月を許可するが初期表示は単月とする。

任意:

- 健診機関。
- source構成。
- 出力可否。
- 法定チェック状態。
- 特定健診チェック状態。

集計月は `exam_export_cases.exam_date` の年月とする。受領月や処理実行月とは混ぜない。

## 5. ページ構成

### 5.1 全体指標

| 指標 | 定義 |
| --- | --- |
| case総数 | 対象月のdistinct case数 |
| 出力可能 | `EXPORT_READY` |
| 理由ありOK | `APPROVED_WITH_REASON` |
| BLOCKED | `BLOCKED` |
| 未判定 | 上記へ確定していないcase |
| 法定NG率 | 法定NG case数 / 法定判定対象case数 |
| 特定健診NG率 | 特定健診NG case数 / 特定健診判定対象case数。対象外は母数から除く |
| source NG率 | XML/CSVのNG ledger数 / XML/CSV ledger数 |
| 出力済み | 対象月caseのうち出力済みのcase数 |

case件数と項目NG件数は分ける。1caseに複数NG項目があるため、両者の合計は一致しない。

### 5.2 健診機関別

1行1施設で次を表示する。

- 健診機関コード、名称。
- case数。
- 法定NG数・率。
- 特定健診NG数・率。
- source NG数・率。
- MISSING項目数。
- CSV採用case数。
- 手入力採用case数。
- 未解決case数。

行から既存の健診機関サマリー詳細へ遷移する。

### 5.3 NG項目別

- scope。
- 業務チェックIDまたはnamecode。
- 項目名、NG理由。
- NG case数、判定対象case数、NG率。
- 該当施設数。
- XML受領件数、CSV受領件数。
- 理由ありOK、再提出待ち、除外、未確認の内訳。

`MISSING 100%`、`一部MISSING`、`値・コードNG`で絞り込めるようにする。

### 5.4 工程別

- SCAN・alias解決。
- XML/CSV import。
- 加入者突合。
- normalize/validation。
- case作成・採用値作成。
- 法定・特定健診check。
- 理由確認・再提出待ち。
- XML出力・XSD検証。

## 6. 「補完」と「救済」の定義

### 6.1 補完実績

既存DBだけで直接集計できる事実である。

| 指標 | 定義 |
| --- | --- |
| CSV採用case | case採用値にCSV由来が1件以上あるcase |
| CSV採用項目 | CSV由来で採用されたcase value数 |
| 手入力採用case | MANUAL/PAPER由来の採用値が1件以上あるcase |
| 手入力採用項目 | MANUAL/PAPER由来で採用されたcase value数 |
| 複数source補完case | 2種類以上のsourceから採用値を持つcase |

これは「そのsourceを利用した」実績であり、そのsourceがなければNGだったことまでは保証しない。

### 6.2 厳密な救済実績

次の反実仮想で判定する。

```text
通常の全sourceでは出力可能
AND
対象source種別を除外して同じ値選択・checkを行うとNG
```

- CSV救済case: CSVを除外するとNGになるが、CSV込みでは出力可能。
- 手入力救済case: MANUAL/PAPERを除外するとNGになるが、手入力込みでは出力可能。

厳密な救済判定はDBを更新せず、既存のcase値選択とcheckerをメモリ上で再評価する。CSV値や手入力値が混在しているだけのcaseは救済数へ含めない。

## 7. source構成の報告分類

重複しない主分類:

1. XMLのみで完成。
2. CSVのみで完成。
3. XML + CSVで完成。
4. XML/CSV + 手入力で完成。
5. 手入力のみで完成。
6. 理由ありOKで出力可能。
7. 未解決。

重複を許す別軸実績:

- CSV採用あり。
- 手入力採用あり。
- CSV救済。
- 手入力救済。
- CSVと手入力の両方で救済。

主分類の合計はcase総数と一致させる。別軸実績は重複するため合計をcase総数へ一致させない。

## 8. 実装フェーズ

### Phase 1: 既存DBの参照集計

- 全体、施設別、項目別、工程別NGサマリー。
- CSV/手入力の採用case数と採用項目数。
- source構成分類。
- 画面表示と報告用CSVダウンロード。

Phase 1では厳密な救済判定をしていないため、画面文言は「採用」「補完」とする。

### Phase 2: 厳密な救済再判定

- source種別を除外したメモリ上のcase値再構築。
- 同一checkerによる反実仮想check。
- CSV救済case、手入力救済case、救済項目の集計。
- 集計条件、checkerバージョン、実行日時の記録。

### Phase 3: 月次確定スナップショット

現行case、review、出力可否は再取込や再checkで更新される。過去月報を当時の数字で固定する必要が出た段階で、月次集計スナップショットを追加する。

初期版は「現在のDB状態を受診月で集計した値」と明示し、過去時点の再現値とは扱わない。

## 9. CSVダウンロード

1. 健診機関別月次サマリー。
2. NG項目別月次サマリー。

個人情報は含めない。個別追跡は画面遷移、または別権限の明細CSVへ分ける。

## 10. migration判断

- Phase 1: 既存DBの参照だけなので不要。
- Phase 2: 都度計算なら不要。救済判定結果を監査用に保存する場合は別途必要。
- Phase 3: 月次確定スナップショット用migrationが必要。

## 11. 実装前の確認事項

- 月次報告の基準を受診月で固定してよいか。
- MANUALとPAPERを「手入力」として合算してよいか。
- 理由ありOKを補完成果へ含めず、独立分類にしてよいか。
- 出力後に再取込・再checkしたcaseを現在状態で再集計してよいか。
- 前月比較を同一event内だけで行うか、年度をまたいで行うか。
- Phase 1を先に提供し、厳密な救済をPhase 2とするか。
