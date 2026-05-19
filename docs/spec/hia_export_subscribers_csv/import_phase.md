# HIA Export Subscribers CSV – Import Phase

このドキュメントは **HIA export 加入者 CSV → staging_subscribers_hub 取込処理**の仕様を定義する。

対象スクリプト:

```text
# 旧実装
scripts/work_folder/scripts/import_subscribers_to_staging_hub.py

# ADR-0021 以降の新構成予定
scripts/hia/import_subscribers_to_staging_hub.py
scripts/hia/script_lib/hub_subscriber_import.py
```

旧実装では import 後に apply phase を直接起動していたが、
ADR-0021 以降は:

```text
import
  ↓
current snapshot update
  ↓
prepare / compare
  ↓
apply
```

の段階へ分離する。

Import phase の役割は:

```text
CSV → raw / norm / match / identity_hash を生成し、
staging_subscribers_hub を構築すること
+
current subscriber state snapshot を staging 側へ更新すること
```

とする。

Import phase は本番 subscriber 系テーブルを参照するが、
本番 subscriber / address / contact は更新しない。

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
normalize / match / hash
      │
      ▼
staging_subscribers_hub
      │
      ▼
current snapshot update
      │
      ▼
prepare / compare phase
```

Import phase の責務:

- CSV 読み込み
- 列マッピング
- 正規化処理
- identity_hash 生成
- staging テーブル登録
- current subscriber state snapshot 更新
- import_run_id 記録
- 行エラー記録

Import phase は compare / apply 判定を行わない。

```text
insert
update
noop
identity_changed
review
```

などの action 判定は prepare / compare phase が担当する。

Import phase は current 状態の参照・snapshot 更新までは行う。

ただし、以下は行わない。

- subscribers 更新
- subscriber_addresses 更新
- subscriber_contacts 更新
- apply_action 決定
- audit 永続保存

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
scripts/lib/identity/
scripts/lib/normalize/
```

構成:

```
identity/
normalize/
subscriber/
```

役割:

| module | role |
|------|------|
| identity | identity_hash / person_id_custom |
| normalize | 基本 normalize |
| subscriber | 氏名 / subscriber normalize |

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

ADR-0021 以降は、以下を combine した:

```text
identity_hash
```

を compare / join 用 identity の中核として利用する。

---

# 8. identity_hash Generation

identity_hash は compare phase 用 join hash として生成する。

入力:

```text
person_id_custom
name_kana_full_match
gender_code
```

生成値:

```text
identity_hash
```

利用目的:

- compare phase join
- subscriber identity compare
- diff 判定
- identity change 検知
- apply_action 判定補助

---

# 9. Current Snapshot Update

import phase では、CSV import 後に本番 subscriber 系 current 状態を取得し、
staging 側へ snapshot として保持する。

目的:

- import 完了時点で本番に既存 subscriber が存在するか確認可能にする
- apply 前レビューを容易にする
- prepare / compare phase の入力を固定化する

snapshot 対象例:

```text
current_subscriber_id
current_identity_hash
current_name_kana_full_match
current_address_id
current_contact_id
current_lookup_status
current_lookup_checked_at
```

参照対象:

```text
subscribers
subscriber_addresses current row
subscriber_contacts current row
```

Import phase は current 状態を参照するが、本番側テーブルは更新しない。

---

# 10. Date Normalize

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

# 11. Staging Table

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
hia_subscriber_id
identity_hash
current_subscriber_id
current_identity_hash
current_name_kana_full_match
current_address_id
current_contact_id
current_lookup_status
current_lookup_checked_at
insurer_number

insurance_symbol
insurance_symbol_digits
insurance_number
insurance_branchnumber

birth
gender_code

name_kanji_full
name_kana_full
name_kanji_full_match
name_kana_full_match

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

# 12. Run Management

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

# 13. Error Handling

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

# 14. Import Output

成功した行は

```
staging_subscribers_hub
```

へ格納される。

Import output には、CSV由来の正規化済み値に加え、
本番 current snapshot が保持される。

prepare / compare phase により:

- snapshot と staging 値の compare
- identity_hash compare
- address/contact compare
- apply_action 作成

を実施する。

---

# Summary

```
CSV
 ↓
column mapping
 ↓
normalize / match
 ↓
person_id generation
 ↓
identity_hash generation
 ↓
staging_subscribers_hub
 ↓
current snapshot update
 ↓
prepare / compare phase
```