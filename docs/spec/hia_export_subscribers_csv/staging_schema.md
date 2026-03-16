

# Staging Schema

このドキュメントは **HIA export 加入者 CSV を一時受けする `staging_subscribers_hub` テーブルの役割と列仕様**を定義する。

対象テーブル:

```
staging_subscribers_hub
```

関連処理:

```
import_subscribers_to_staging_hub.py
apply_subscribers_from_staging_hub.py
```

この staging は **CSV import phase と subscriber apply phase の間に置かれる受け皿**であり、
正規化済みデータを保持して本番反映を安定化するための中間層である。

関連spec:

- `import_phase.md`
- `subscriber_apply.md`
- `identity_policy.md`

---

# 1. Purpose

`staging_subscribers_hub` の目的は以下の通り。

- HIA export CSV の正規化済みデータを保持する
- import と apply を分離する
- apply 前の確認・再実行を可能にする
- import run / apply run を追跡できるようにする
- 行単位エラー時の切り分けをしやすくする

このテーブルは **subscriber master 本体ではない**。

---

# 2. Role in Pipeline

全体フロー:

```
HIA Export CSV
      │
      ▼
import_subscribers_to_staging_hub.py
      │
      ▼
staging_subscribers_hub
      │
      ▼
apply_subscribers_from_staging_hub.py
      │
      ▼
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```

staging は

- import 済
- apply 未実行
- apply 済

を列で管理し、**未処理キュー**として使う。

---

# 3. Queue Semantics

apply 対象は次の条件。

```sql
SELECT *
FROM staging_subscribers_hub
WHERE processed_run_id IS NULL
ORDER BY rowid ASC;
```

意味:

| state | meaning |
|---|---|
| `processed_run_id IS NULL` | 未処理 |
| `processed_run_id IS NOT NULL` | apply 済 |

成功した apply 後に以下を刻印する。

```
processed_run_id
processed_at
```

---

# 4. Column Groups

`staging_subscribers_hub` の列は大きく以下に分かれる。

1. identity / core columns
2. insurance columns
3. name columns
4. address / contact columns
5. qualification / organization columns
6. source trace columns
7. run management columns

---

# 5. Identity / Core Columns

| column | type | meaning |
|---|---|---|
| `person_id_custom` | TEXT / VARCHAR | 加入者識別用の正規化ID |
| `birth` | DATE / TEXT | 生年月日 |
| `gender_code` | TEXT / INTEGER | 性別コード |

用途:

- subscriber identity 判定の中核
- apply 時の既存 subscriber 検索

参照:

- `identity_policy.md`

---

# 6. Insurance Columns

| column | meaning |
|---|---|
| `insurer_number` | 保険者番号 |
| `insurance_symbol` | 保険証記号 |
| `insurance_symbol_digits` | 記号数字抽出版 |
| `insurance_number` | 保険証番号 |
| `insurance_branchnumber` | 枝番 |

補足:

- import 時に正規化済みの値を格納する
- `insurance_symbol_digits` は照合補助用
- 元CSVの表記揺れはこの段階で吸収済みとする

---

# 7. Name Columns

## Full Name

| column | meaning |
|---|---|
| `name_kanji_full` | 氏名漢字全文 |
| `name_kana_full` | 氏名カナ全文（正規化済み） |

## Split Name

| column | meaning |
|---|---|
| `name_kanji_family` | 漢字姓 |
| `name_kanji_middle` | 漢字ミドル |
| `name_kanji_given` | 漢字名 |
| `name_kana_family` | カナ姓 |
| `name_kana_middle` | カナミドル |
| `name_kana_given` | カナ名 |

用途:

- identity 判定
- subscriber master 反映
- 後続の表示・帳票・検索補助

`name_kana_full` は identity key に使うため、
表記ゆれ吸収後の値を保持する。

---

# 8. Address / Contact Columns

## Address

| column | meaning |
|---|---|
| `postal_code` | 郵便番号 |
| `address_line` | 住所本体 |
| `building` | 建物名等 |

## Contact

| column | meaning |
|---|---|
| `phone` | 電話番号 |
| `email` | メールアドレス |

これらは identity には使用しない。

用途:

- apply phase で `subscriber_addresses` / `subscriber_contacts` に反映
- 履歴差分判定の入力

---

# 9. Qualification / Organization Columns

| column | meaning |
|---|---|
| `relationship_name` | 続柄名称 |
| `qualification_acquired_date` | 資格取得日 |
| `qualification_lost_date` | 資格喪失日 |
| `employer_code` | 事業所コード |
| `department_code` | 部門コード |
| `distribution_code` | 配送先コード等 |
| `employee_code` | 社員コード |
| `connect_id` | 外部連携ID |

用途:

- subscriber master 属性更新
- 後続の抽出条件や連携キー

これらは identity ではなく属性情報として扱う。

---

# 10. Source Trace Columns

| column | meaning |
|---|---|
| `src_file` | 元CSVファイル名 |
| `src_row_no` | CSV上の論理行番号 |
| `src_line_no` | 元ファイル上の実行番号 / 行番号 |
| `loaded_at` | staging 登録時刻 |
| `created_at` | 行作成時刻 |

目的:

- 元データ追跡
- エラー行特定
- 再取込確認
- 監査補助

---

# 11. Run Management Columns

| column | meaning |
|---|---|
| `import_run_id` | import 実行ID |
| `processed_run_id` | apply 実行ID |
| `processed_at` | apply 完了時刻 |

意味:

- `import_run_id` で「どの取込処理で入った行か」を追跡
- `processed_run_id` で「どの apply 実行で反映されたか」を追跡
- `processed_at` で apply 完了タイムスタンプを保持

これにより import と apply を独立管理できる。

---

# 12. Row Lifecycle

`staging_subscribers_hub` の1行は次のライフサイクルを持つ。

## 1. Import

CSV から読み込まれ、正規化後に INSERT される。

状態:

```
import_run_id = <run_id>
processed_run_id = NULL
```

## 2. Waiting

apply 待機状態。

意味:

```
未処理キュー
```

## 3. Applied

apply 成功時に刻印。

状態:

```
processed_run_id = <apply_run_id>
processed_at = now()
```

---

# 13. Constraints and Expectations

期待する性質:

- import 時点で列名揺れは吸収済み
- 正規化済みデータのみを保持する
- 1行は1加入者候補を表す
- apply は `processed_run_id IS NULL` を前提に走る

この staging は **長期マスタではなく処理中間層** である。

---

# 14. Design Notes

staging を設ける理由は、CSV 直反映を避けて処理を分層するためである。

利点:

- import 失敗と apply 失敗を分離できる
- 正規化後データを人が確認できる
- apply 再実行制御がしやすい
- run 管理が明確になる
- 行トレースが容易

PHR v1.0.1 では、この staging を subscriber ingest pipeline の中心中間層として扱う。

---

# Summary

`staging_subscribers_hub` は、HIA export 加入者 CSV の **正規化済み受け皿** であり、

- import と apply の分離
- subscriber identity 判定の入力
- 履歴反映の入力
- run / source trace の保持
- 未処理キュー管理

を担うテーブルである。