

# HIA Export Subscribers CSV – Import Phase

このドキュメントは **HIA export 加入者 CSV → staging_subscribers_hub 取込処理**の仕様を定義する。

対象スクリプト:

```
scripts/work_folder/scripts/import_subscribers_to_staging_hub.py
```

このフェーズの役割は **CSV正規化 + staging登録**であり、
subscriber master への反映は apply phase が担当する。

参照:

- apply 処理仕様 → `subscriber_apply.md`
- 正規化ルール → `phr/lib/normalize/`

---

# 1. Import Phase Overview

処理全体フロー:

```
HIA Export CSV
      │
      ▼
CSV reader
      │
      ▼
column mapping
      │
      ▼
normalize rules
      │
      ▼
staging_subscribers_hub
```

Import phase の責務:

- CSV 読み込み
- 列マッピング
- 正規化処理
- staging テーブル登録
- import_run_id 記録
- 行エラー記録

---

# 2. Input Source

入力ファイル:

```
data/input_subscribers_csv/<insurer_number>/*.csv
```

フォルダ名:

```
########
```

8桁数字

例:

```
12345678
87654321
```

フォルダ名から保険者番号を取得する。

使用関数:

```
normalize_insurer_folder_name_to_int()
```

---

# 3. Column Mapping

CSV列 → staging列

例:

| CSV Column | staging column |
|------------|----------------|
| 保険者番号 | insurer_number |
| 記号 | insurance_symbol |
| 番号 | insurance_number |
| 枝番 | insurance_branchnumber |
| 氏名漢字 | name_kanji_full |
| 氏名カナ | name_kana_full |
| 生年月日 | birth |
| 性別 | gender_code |
| 郵便番号 | postal_code |
| 住所 | address_line |
| 建物 | building |
| 電話番号 | phone |
| メール | email |

CSV列名の揺れは import 側で吸収する。

---

# 4. Normalize Rules

正規化は以下のモジュールを使用する。

```
phr/lib/normalize/
```

構成:

```
common.py
rules.py
subscriber.py
```

役割:

| module | role |
|------|------|
| common | 基本正規化 |
| rules | CSV列ルール |
| subscriber | 氏名 / ID |

---

# 5. Insurance Fields Normalize

処理:

```
normalize_insurance_symbol()
normalize_insurance_number_required()
normalize_branchnumber_optional()
```

出力:

```
insurance_symbol
insurance_symbol_digits
insurance_number
insurance_branchnumber
```

digits-only 強制は行わない。

---

# 6. Name Normalize

関数:

```
normalize_name_fields()
```

入力:

```
kanji_full
kana_full
```

出力:

```
name_kanji_family
name_kanji_middle
name_kanji_given

name_kana_family
name_kana_middle
name_kana_given

name_kana_full
```

ルール:

- カナ必須
- NFKC 正規化
- ひらがな → カタカナ
- 空白除去版 full を生成

---

# 7. Person ID Generation

関数:

```
generate_person_id_custom()
```

入力:

```
insurer_number
insurance_symbol
insurance_number
birth
```

生成ID:

```
person_id_custom
```

これは **加入者識別ID** として master 照合に使用される。

---

# 8. Date Normalize

関数:

```
normalize_date_iso()
```

出力形式:

```
YYYY-MM-DD
```

対象列:

```
qualification_start_date
qualification_end_date
```

空は

```
NULL
```

---

# 9. Staging Table

テーブル:

```
staging_subscribers_hub
```

DDL参照:

```
sql/ddl/dev_phr/0072_dev_phr__staging_subscribers_hub.sql
```

Insert列:

```
person_id_custom
insurer_number

insurance_symbol
insurance_symbol_digits
insurance_number
insurance_branchnumber

birth
gender_code

name_kanji_full
name_kana_full

name_kanji_family
name_kanji_middle
name_kanji_given

name_kana_family
name_kana_middle
name_kana_given

postal_code
address_line
building

phone
email

qualification_acquired_date
qualification_lost_date

src_file
src_row_no
src_line_no

import_run_id
loaded_at
```

---

# 10. Run Management

Import 実行は

```
etl_runs
```

で管理される。

phase:

```
import
```

保存情報:

```
run_id
rows_seen
rows_inserted
rows_skipped
errors
```

---

# 11. Error Handling

正規化エラー:

```
NormalizeError
```

行スキップ + 記録:

```
etl_errors
```

保存情報:

```
run_id
src_file
src_row_no
field
field_value
error_code
message
```

Import は **fail-fast しない**。

---

# 12. Import Output

成功した行は

```
staging_subscribers_hub
```

へ格納される。

次フェーズ:

```
apply_subscribers_from_staging_hub.py
```

---

# Summary

```
CSV
 ↓
column mapping
 ↓
normalize
 ↓
person_id generation
 ↓
staging_subscribers_hub
 ↓
apply phase
```