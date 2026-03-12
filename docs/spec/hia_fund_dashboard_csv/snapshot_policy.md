# スナップショットポリシー

このドキュメントは、`HIA Fund Dashboard CSV` の取り込み処理において
CSVスナップショットをどのように記録・比較するかを定義する。

HIA管理画面からエクスポートされるCSVは **必ずしも全件データではない**。
ユーザーは次のような条件でフィルタした結果をダウンロードできる。

- 「未予約のみ」
- 「特定企業のみ」
- その他任意の条件

そのため、本システムではCSVを **部分スナップショット（partial snapshot）**
として扱う。

この理由により、CSVの差分から **自動削除（DELETE）を推定することは禁止する**。

---

# 基本方針

## 1. スナップショット識別キー

CSVの各行は「1人の状態」を表すレコードである。

人物の識別には次の項目を使用する。

```
insurer_number
insurance_symbol
insurance_number
relationship
```

この4項目から次の論理キーを構築する。

```
snapshot_identity_key
```

補足:

- 枝番（branch number）はキーに含めない
- 氏名は表示用データでありキーとして信用しない

氏名は参照用フィールドとしてのみ保持する。

---

### 保険証記号・番号の正規化

人物識別および突合に使用する `*_match` 列は、共通正規化ルールを使用する。

#### insurance_symbol_match

`insurance_symbol_match` は **HIA export ZIP v1 で確定した正規化手順**を使用する。

このルールは HIA 系データ処理で共通とし、特段の指示がない限り変更しない。

目的:

- `hia_person_years`
- HIA XML
- HIA export CSV

などのデータと安定して join できるようにするため。

#### insurance_number_match

`insurance_number_match` は次の正規化を行う。

1. NFKC 正規化
2. 数字以外の文字を除去
3. 半角数字へ統一
4. 先頭の `0` を削除

例:

```
００１２３ → 123
000123 → 123
001-23 → 123
```

このルールも HIA 系データ処理の共通ルールとして扱う。

---

# 2. 行変更判定

CSVの各行は正規化された文字列に変換され、
次のハッシュを生成する。

```
row_sha256
```

このハッシュは **その行の意味的内容全体** を表す。

ハッシュ生成時には以下を含めない。

- 行順序
- ファイル名
- run_id

目的は **行内容が実際に変化したかどうかを判定すること**。

`row_sha256` は `snapshot_identity_key` によって特定される
人物単位で評価される。

ファイル全体のSHAは主比較には使用しない。
理由:

- 同じCSVを何度もダウンロードできる
- フィルタされた部分データである可能性がある

---

# 3. 変更判定結果

各runでは、現在CSVと既存データを
`snapshot_identity_key` を使って比較する。

結果は次の3種類。

```
INSERT
UPDATE
UNCHANGED
```

### INSERT

人物キーが既存テーブルに存在しない。

### UPDATE

人物キーは存在するが `row_sha256` が異なる。

### UNCHANGED

人物キーが存在し `row_sha256` も同じ。

ルール:

- 新規INSERTは履歴テーブルには記録しない
- UPDATEのみ履歴テーブルに記録する

---

# 4. 自動DELETE禁止

CSVに存在しないレコードを
**削除とみなしてはいけない**。

理由:

ダッシュボードCSVは次のようなフィルタ条件で
出力される可能性がある。

- 企業単位
- ステータス単位
- 任意条件

そのため、あるCSVに存在しないからといって
元システムから削除されたとは限らない。

DELETE判定は **手動分析のみ** とする。

---

# 5. 最終観測run

各人物レコードには
「最後に観測されたrun」を記録する。

```
last_seen_run_id
```

これにより

- 最近観測されていない人物

などの分析が可能になる。

現在状態テーブルには次を保持する。

```
last_seen_run_id
updated_at
```

---

# 6. 受診勧奨送信日時

CSVの次の列には複数の日時が格納されている。

```
受診勧奨送信日時
```

値は `|` 区切り。

例

```
2025-06-20 14:45:03|2025-06-24 10:25:03
```

これらは次のテーブルへ分解して保存する。

```
hia_dashboard_reminder_events

event_id
hia_dashboard_person_id
run_id
sent_at
created_at
```

1送信 = 1レコード。

重複登録を防ぐため

```
UNIQUE(hia_dashboard_person_id, sent_at)
```

制約を推奨する。

---

## 受診勧奨送信回数の扱い

CSVには次の列も存在する。

```
受診勧奨送信回数
```

この値は **CSVの値をそのまま保存する**。

保存先:

```
hia_dashboard_status.reminder_send_count
```

システムは

`hia_dashboard_reminder_events`

の件数と完全一致することを保証しない。

必要な場合のみ

```
COUNT(*)
```

との比較で整合性確認を行う。

---

# 7. ETL run管理

CSV取り込みは共通ETL管理テーブルを使用する。

```
work_other.etl_runs
work_other.etl_errors
```

推奨設定:

phase

```
import
```

source

```
hia_fund_dashboard_csv
```

ファイル情報は

```
etl_runs.notes
```

へ記録する。

例

```
filename=dashboard_06139463_20260312.csv file_mtime=2026-03-12 10:22:31 filter: status=未予約
```

---

# 8. RAW行保存

元CSVの行は次の形式で保存できる。

```
raw_row_json
```

これにより

- 元データの復元
- 監査

が可能になる。

現在状態テーブルは

**人物ごとの最新状態のみ保持する。**

変更履歴は別テーブルに保存する。

---

# 9. テーブル構成

本設計では次の2テーブルを使用する。

## 現在状態テーブル

```
hia_dashboard_status
```

1人物 = 1行。

主キー

```
hia_dashboard_person_id
```

論理キー

```
snapshot_identity_key
```

構成

```
insurer_number
insurance_symbol
insurance_number
relationship
```

枝番はキーに含めない。

氏名は参照用のみ。

---

## 履歴テーブル

```
hia_dashboard_status_history
```

列変更があった場合のみ記録する。

推奨カラム

```
hia_dashboard_status_history_id
hia_dashboard_person_id
run_id
changed_column
before_value
after_value
created_at
```

ルール

- 新規INSERTは履歴に書かない
- UPDATEのみ履歴に記録

---

# まとめ

この取り込みは

**部分スナップショット型データ処理**

として設計されている。

変更判定

```
snapshot_identity_key
row_sha256
```

最新状態

```
hia_dashboard_status
```

変更履歴

```
hia_dashboard_status_history
```

送信イベント

```
hia_dashboard_reminder_events
```

ETL管理

```
work_other.etl_runs
work_other.etl_errors
```

この構成により

- 最新状態の即時参照
- 変更履歴の追跡
- CSVフィルタによる誤削除防止

を実現する。