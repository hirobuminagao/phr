# Staging Schema

このドキュメントは **HIA export 加入者 CSV を一時受けする `staging_subscribers_hub` テーブルの役割と列仕様**を定義する。

対象テーブル:

```
staging_subscribers_hub
```

関連処理:

```
# 旧実装
import_subscribers_to_staging_hub.py
apply_subscribers_from_staging_hub.py

# ADR-0021 以降の新構成予定
scripts/hia/import_subscribers_to_staging_hub.py
scripts/hia/prepare_subscriber_apply_actions.py
scripts/hia/apply_subscribers_from_staging_hub.py
```

旧実装ではこの staging は **CSV import phase と subscriber apply phase の間に置かれる受け皿**として利用していた。

ADR-0021 以降は、`import → prepare / compare → apply` の3段階へ再整理し、
この staging は以下の2つを保持する中間層として扱う。

- import phase により生成された raw / norm / match / identity_hash
- prepare / compare phase により生成された apply_action / diff / compare status

関連spec:

- `import_phase.md`
- `subscriber_apply.md`
- `identity_policy.md`
- `compare_prepare_phase.md`

---

# 1. Purpose

`staging_subscribers_hub` の目的は以下の通り。

- HIA export CSV の正規化済みデータを保持する
- import と apply を分離する
- apply 前の確認・再実行を可能にする
- prepare / compare phase の結果を保持する
- apply_action / diff / compare status を保持する
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
import phase
      │
      ▼
staging_subscribers_hub
      │
      ▼
prepare / compare phase
      │
      ▼
apply_action / diff / compare status
      │
      ▼
apply phase
      │
      ▼
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```

staging は以下の状態を列で管理する。

- import 済
- compare 未実行
- compare 済
- apply 未実行
- apply 済

旧実装では `processed_run_id IS NULL` を apply 未処理キューとして扱っていた。
ADR-0021 以降は、prepare / compare phase により `apply_action` を生成し、apply phase は判定済み action を実行する。

---

# 3. Queue Semantics

旧実装での apply 対象は次の条件。

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

ADR-0021 以降の apply 対象は `apply_action` を基準とする。

例:

```sql
SELECT *
FROM staging_subscribers_hub
WHERE processed_run_id IS NULL
  AND apply_action IN ('insert', 'update')
ORDER BY rowid ASC;
```

`apply_action = 'noop'` または `apply_action = 'review'` は自動更新しない。

---

# 4. Column Groups

`staging_subscribers_hub` の列は大きく以下に分かれる。

1. identity / core columns
2. insurance columns
3. name columns
4. address / contact columns
5. qualification / organization columns
6. prepare / compare columns
7. source trace columns
8. run management columns

---

# 5. Identity / Core Columns

| column | type | meaning |
|---|---|---|
| `person_id_custom` | TEXT / VARCHAR | 加入者識別用の正規化ID |
| `hia_subscriber_id` | TEXT / VARCHAR | HIA加入者ID。HIA上の同一加入者を追跡する最優先外部ID |
| `identity_hash` | CHAR(64) | compare / join 用 identity hash |
| `birth` | DATE / TEXT | 生年月日 |
| `gender_code` | TEXT / INTEGER | 性別コード |

用途:

- import phase で identity を生成する
- prepare / compare phase で既存 subscriber を照合する
- HIA subscriber ID を最優先外部IDとして利用する
- identity_hash を compare / join 用 identity として利用する

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
| `name_kanji_full_match` | 氏名漢字全文の照合用match値 |
| `name_kana_full_match` | 氏名カナ全文の照合用match値 |

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

`name_kana_full_match` は identity_hash の構成要素として使用する。

prepare / compare phase で `name_kana_full_match` の変更を検知した場合、
既存 subscribers 側の name parts は clear 対象とする。

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

# 10. Prepare / Compare Columns

ADR-0021 以降、prepare / compare phase の結果を staging 側へ保持する。

| column | type | meaning |
|---|---|---|
| `apply_subscriber_id` | BIGINT / INTEGER | apply phase が更新対象とする subscribers.id |
| `apply_action` | TEXT / VARCHAR | apply phase の実行 action |
| `apply_diff_columns` | JSON / TEXT | 差分あり列の一覧 |
| `identity_match_status` | TEXT / VARCHAR | identity compare 結果 |
| `address_diff_status` | TEXT / VARCHAR | current address との差分状態 |
| `contact_diff_status` | TEXT / VARCHAR | current contact との差分状態 |
| `apply_checked_at` | DATETIME / TEXT | prepare / compare 実行時刻 |

想定 `apply_action`:

```text
insert
update
noop
review
```

想定 `identity_match_status`:

```text
hia_id_matched
identity_hash_matched
identity_hash_changed
identity_hash_not_found
identity_hash_multiple_match
review
```

用途:

- apply phase の判定済み入力
- diff確認
- 再実行制御
- audit生成補助

apply phase は compare 判定を行わず、これらの列を参照して実行する。

---

# 11. Source Trace Columns

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

# 12. Run Management Columns

| column | meaning |
|---|---|
| `import_run_id` | import 実行ID |
| `apply_checked_at` | prepare / compare 実行時刻 |
| `processed_run_id` | apply 実行ID |
| `processed_at` | apply 完了時刻 |

意味:

- `import_run_id` で「どの取込処理で入った行か」を追跡
- `processed_run_id` で「どの apply 実行で反映されたか」を追跡
- `processed_at` で apply 完了タイムスタンプを保持

これにより import と apply を独立管理できる。

---

# 13. Row Lifecycle

`staging_subscribers_hub` の1行は次のライフサイクルを持つ。

## 1. Import

CSV から読み込まれ、正規化後に INSERT される。

状態:

```
import_run_id = <run_id>
processed_run_id = NULL
```

## 2. Waiting for Compare

prepare / compare 待機状態。

意味:

```
compare未実行
```

## 3. Compared

prepare / compare phase により `apply_action` が生成される。

状態例:

```
apply_action = insert / update / noop / review
apply_checked_at = now()
```

## 4. Waiting for Apply

apply_action が `insert` または `update` の行は apply 対象となる。

```
processed_run_id = NULL
apply_action IN ('insert', 'update')
```

## 5. Applied

apply 成功時に刻印。

状態:

```
processed_run_id = <apply_run_id>
processed_at = now()
```

---

# 14. Constraints and Expectations

期待する性質:

- import 時点で列名揺れは吸収済み
- 正規化済みデータのみを保持する
- 1行は1加入者候補を表す
- apply は `processed_run_id IS NULL` かつ `apply_action IN ('insert', 'update')` を前提に走る

この staging は **長期マスタではなく処理中間層** である。

---

# 15. Design Notes

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

- import / prepare / apply の分離
- subscriber identity compare の入力
- apply_action / diff / compare status の保持
- 履歴反映の入力
- run / source trace の保持
- 未処理キュー管理

を担うテーブルである。