# scripts/from_fund/config

`from_fund` 系スクリプトで使う設定ファイル置き場。

設定ファイルは、実行対象の保険者番号・fund_id・import_run_id など、実行時に切り替える値を管理する。

---

## ファイル一覧

### `diff_status.yml`

`update_staging_subscriber_diff_status.py` 用の差分判定設定。

用途:

- `staging_subscribers_fund` と `subscribers` を比較する
- 対象 staging 行へ `diff_status` / `diff_status_method` / `diff_status_reason` を記録する
- `subscribers` 側に存在し、指定した staging import_run に存在しない人を `missing_from_new` CSV として出力する
- `diff_mode` が有効な場合、`staging_subscribers_fund.diff_status` 系カラムを更新し、`missing_from_new` CSV を出力する
- `export_mode` が有効な場合、`diff_status=add/update` のデータを HIA加入者情報登録用CSVとして出力する

設定例:

```yml
insurer_number: "06130256"
fund_id: 48
import_run_ids:
  - 70

diff_mode: true
export_mode: true
export_split_size: 1000
```

項目:

- `insurer_number`
  - 対象の保険者番号（8桁）
- `fund_id`
  - 対象の `fund_id`
  - 現時点では主に確認・識別用
- `import_run_ids`
  - 差分比較対象とする `staging_subscribers_fund.import_run_id` のリスト
  - 複数指定した場合は、複数runをまとめた比較対象として扱う
  - 例: `[70, 71]` の場合、run 70 と run 71 の両方を対象にして比較する
- `diff_mode`
  - true: diff_status 更新 + missing_from_new CSV出力を行う
  - false: diff_status 更新と missing_from_new CSV出力を行わない

- `export_mode`
  - true: HIA加入者情報登録用CSV（add/update）を出力する
  - false: HIA加入者情報登録用CSVを出力しない

- `export_split_size`
  - HIA加入者情報登録用CSVを何件ごとに分割するか
  - 出力ファイルは、分割単位で複数ファイルに分かれる

---

## 実行方法

### dry-run

DB更新・CSV出力を行わず、件数だけ確認する。

```bash
python -m scripts.from_fund.update_staging_subscriber_diff_status \
  --config scripts/from_fund/config/diff_status.yml \
  --dry-run
```

### 本実行

`staging_subscribers_fund` の `diff_status` 系カラムを更新し（diff_mode=trueの場合）、必要に応じて `missing_from_new` CSV および HIA加入者情報登録用CSV（export_mode=trueの場合）を出力する。

---

## 出力先

`missing_from_new` CSV は以下へ出力する。

```text
data/from_fund/diff_output/
```

ファイル名:

```text
yyyymmdd_hhmmss_[保険者番号]_missing_from_[import_run_ids].csv
```

例:

```text
20260428_153012_06130256_missing_from_70.csv
20260428_153012_06130256_missing_from_70-71.csv
```

---

## HIA加入者情報登録用CSV出力

`export_mode: true` の場合、以下にCSVを出力する。

```text
data/from_fund/export_staging_to_hia_subscribers/
  yyyymmdd_hhmmss_[保険者番号]_sort[import_run_ids]/
    add/
    update/
```

### ファイル名

```text
add_[保険者番号]_yyyymmdd_hhmmss_[start_row_No]-[last_row_No].csv
update_[保険者番号]_yyyymmdd_hhmmss_[start_row_No]-[last_row_No].csv
```

### 分割仕様

- `export_split_size` で指定した件数ごとにCSVを分割する
- 各ファイル名には、そのCSVに含まれる `src_row_no` の範囲を付与する

---

## import_run_ids 複数指定時の注意

`import_run_ids` を複数指定した場合、指定された複数runをまとめて今回受領分として扱う。

`missing_from_new` は、`subscribers` に存在し、指定されたすべての対象runに存在しない人を抽出する。

そのため、同一健保で複数CSVを分けて取り込んだ場合は、比較対象にしたいrunをすべて `import_run_ids` に含める。

---

## 注意

- `staging_subscribers_fund` は複数健保・複数runが混在しうる
- 差分判定では必ず `insurer_number` と `import_run_ids` で対象を絞る
- 同一健保の古いrunを削除する場合でも、健保単位ではなく `import_run_id` 単位で整理する
- 将来的に複数 `import_run_id` を束ねる ETL run group（RUNボタン単位）を導入する可能性がある