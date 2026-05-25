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

# ADR-0021 以降の新構成
scripts/hia/import_subscribers_to_staging_hub.py
scripts/hia/apply_hia_subscriber_sync.py
scripts/hia/script_lib/hub_subscriber_import.py
scripts/hia/script_lib/hub_subscriber_current_snapshot.py
scripts/hia/script_lib/hub_subscriber_prepare.py
scripts/hia/script_lib/hub_subscriber_compare.py
scripts/hia/script_lib/hub_subscriber_apply.py
scripts/hia/script_lib/hub_subscriber_audit.py
```

旧実装ではこの staging は **CSV import phase と subscriber apply phase の間に置かれる受け皿**として利用していた。

ADR-0021 以降は、実行単位を `import orchestration` と `apply orchestration` に分離する。

この staging は単なる CSV 一時置き場ではなく、以下を同一行に保持する compare workspace として扱う。

- import orchestration により生成された raw / norm / match / identity_hash / compare hash
- import orchestration により取得された current snapshot ID / hash / status
- apply orchestration により生成された apply_action / diff / compare status

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
- current snapshot の ID / hash / status を保持する
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
import orchestration
  - CSV import
  - compare hash generation
  - current snapshot update
      │
      ▼
staging_subscribers_hub
  - import values
  - current snapshot values
  - apply_action / diff / compare status
      │
      ▼
apply orchestration
  - prepare / compare
  - apply
  - audit
      │
      ▼
subscribers
subscriber_addresses
subscriber_contact_points
subscriber_contacts (legacy / backfill source)
subscriber_audit
```

staging は以下の状態を列で管理する。

- import 済
- compare 未実行
- compare 済
- apply 未実行
- apply 済

旧実装では `processed_run_id IS NULL` を apply 未処理キューとして扱っていた。
ADR-0021 以降は、apply orchestration が `import_run_id` 単位で prepare / compare を行い、`apply_action` を生成する。
apply 本体は判定済み action を実行する。

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
WHERE import_run_id = :import_run_id
  AND processed_run_id IS NULL
  AND apply_action IN ('insert', 'update')
ORDER BY staging_subscriber_hub_id ASC;
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
6. current snapshot columns
7. prepare / compare columns
8. source trace columns
9. run management columns

---

# 5. Identity / Core Columns

| column | type | meaning |
|---|---|---|
| `person_id_custom` | TEXT / VARCHAR | 加入者識別用の正規化ID |
| `hia_subscriber_id` | TEXT / VARCHAR | HIA加入者ID。HIA上の同一加入者を追跡する最優先外部ID |
| `identity_hash` | CHAR(64) | subscriber resolve / join 用 identity hash |
| `compare_identity_norm_hash` | CHAR(64) | identity登録値差分検知用 compare hash |
| `compare_other_hash` | CHAR(64) | identity以外の subscriber属性差分検知用 compare hash |
| `birth` | DATE / TEXT | 生年月日 |
| `gender_code` | TEXT / INTEGER | 性別コード |

用途:

- import phase で identity を生成する
- prepare / compare phase で既存 subscriber を照合する
- HIA subscriber ID を最優先外部IDとして利用する
- identity_hash を subscriber resolve / join 用 identity として利用する
- compare_identity_norm_hash を identity登録値差分検知に利用する
- compare_other_hash を subscriber属性差分検知に利用する

## compare_identity_norm_hash

`compare_identity_norm_hash` は:

```text
identity登録値の差分検知用 compare hash
```

として扱う。

対象値:

```text
insurance_symbol
insurance_number
name_kana_full
name_kanji_full
birth
gender_code
```

重要:

```text
identity_hash と compare_identity_norm_hash は別用途
```

である。

`identity_hash` は:

```text
subscriber resolve / join 用
```

compare_identity_norm_hash は:

```text
identity登録値差分検知用
```

として扱う。

compare hash は:

```text
scripts/lib/hash/compare_hash.py
```

の `build_compare_hash()` を利用して生成する。

固定手順:

```text
1. values list を受け取る
2. 各値を base_norm に通す
3. delimiter で連結する
4. sha256 を生成する
5. hex digest を返す
```

compare hash は標準用途として:

```text
match値ではなく norm値
```

を利用する。

---

## compare_other_hash

`compare_other_hash` は:

```text
identity以外の subscriber属性差分検知用 compare hash
```

として扱う。

対象候補:

```text
insured_attribute_name
relationship_name
qualification_acquired_date
qualification_lost_date
employer_code
department_code
distribution_code
employee_code
connect_id
```

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

`name_kana_full_match` は resolve / join 用 identity_hash の構成要素として扱う。
登録値差分検知は `compare_identity_norm_hash` で行う。

---

# 8. Address / Contact Columns

## Address

| column | meaning |
|---|---|
| `postal_code` | 郵便番号 |
| `address_line` | 住所本体 |
| `building` | 建物名等 |
| `address_hash` | 住所差分検知用 compare hash |

## Contact

| column | meaning |
|---|---|
| `phone` | 電話番号 |
| `email` | メールアドレス |

これらは identity には使用しない。

用途:

- apply orchestration で `subscriber_addresses` / `subscriber_contact_points` に反映
- address_hash により既存住所との比較を行う
- 履歴差分判定の入力

## address_hash

`address_hash` は:

```text
住所値の存在確認・差分検知用 compare hash
```

として扱う。

対象値:

```text
postal_code
address_line
building
```

compare では:

```text
staging_subscribers_hub.address_hash
```

と:

```text
subscriber_addresses.address_hash
```

を照合する。

注意:

```text
address_hash 一致 = current address 一致
```

ではない。

`subscriber_addresses` は 1:n の履歴型テーブルであり、
同一住所値が historical row として存在する可能性がある。

そのため compare では:

```text
same address_hash exists?
  yes:
    is_current = 1?
      yes -> noop
      no  -> current切替候補
  no:
    新住所 insert 候補
```

として扱う。

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

# 10. Current Snapshot Columns

import orchestration は、import_run_id 単位で staging 行に current snapshot を付与する。

staging は current 実データを大量に複製するのではなく、review と compare candidate filtering に必要な ID / hash / status に絞って保持する。

| column | type | meaning |
|---|---|---|
| `current_subscriber_id` | BIGINT / INTEGER | current subscribers 側 subscriber_id |
| `current_hia_subscriber_id` | TEXT / VARCHAR | current subscribers 側に保持されている HIA加入者ID |
| `current_identity_hash` | CHAR(64) | current subscribers 側 identity_hash |
| `current_compare_identity_norm_hash` | CHAR(64) | current subscribers 側 identity登録値 compare hash |
| `current_compare_other_hash` | CHAR(64) | current subscribers 側 subscriber属性 compare hash |
| `current_name_kana_full_match` | TEXT / VARCHAR | current subscribers 側 name_kana_full_match |
| `current_address_id` | BIGINT / INTEGER | current subscriber_addresses 側 address_id |
| `current_address_hash` | CHAR(64) | current address 側 address_hash |
| `current_phone_contact_point_id` | BIGINT / INTEGER | current phone contact point id |
| `current_email_contact_point_id` | BIGINT / INTEGER | current email contact point id |
| `current_lookup_status` | TEXT / VARCHAR | current lookup status |
| `current_lookup_checked_at` | DATETIME / TEXT | current lookup checked timestamp |

## current snapshot の保持方針

import値側:

```text
hia_subscriber_id
identity_hash
compare_identity_norm_hash
compare_other_hash
address_hash
phone
email
```

current snapshot側:

```text
current_subscriber_id
current_hia_subscriber_id
current_identity_hash
current_compare_identity_norm_hash
current_compare_other_hash
current_name_kana_full_match
current_address_id
current_address_hash
current_phone_contact_point_id
current_email_contact_point_id
current_lookup_status
current_lookup_checked_at
```

`current_hia_subscriber_id` は人間review時の重要な足がかりとして保持する。

例:

```text
hia_subscriber_id != current_hia_subscriber_id
```

の場合、HIA側ID変更・別人候補・上流ID差し替えなどを確認する材料になる。

compare hash も staging に保持することで、apply orchestration の prepare / compare 前に:

```text
compare_identity_norm_hash != current_compare_identity_norm_hash
compare_other_hash != current_compare_other_hash
address_hash != current_address_hash
```

を軽量に確認できる。

ただし address は履歴型であるため、`address_hash != current_address_hash` のみで最終判定しない。
詳細compareでは `subscriber_addresses` 全体に対して same address_hash の存在確認と `is_current` 判定を行う。

---

# 11. Prepare / Compare Columns

ADR-0021 以降、prepare / compare phase の結果を staging 側へ保持する。

| column | type | meaning |
|---|---|---|
| `apply_subscriber_id` | BIGINT / INTEGER | apply phase が更新対象とする subscribers.id |
| `apply_action` | TEXT / VARCHAR | apply phase の実行 action |
| `apply_diff_columns` | JSON / TEXT | 差分あり列の一覧 |
| `identity_match_status` | TEXT / VARCHAR | identity compare 結果 |
| `compare_identity_norm_hash` | CHAR(64) | identity登録値 compare hash |
| `compare_other_hash` | CHAR(64) | subscriber属性 compare hash |
| `address_hash` | CHAR(64) | address compare hash |
| `address_diff_status` | TEXT / VARCHAR | current address との差分状態 |
| `contact_point_diff_status` | TEXT / VARCHAR | current contact point との差分状態 |
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

apply orchestration 内の prepare / compare がこれらの列を更新する。
apply 本体は compare 判定を行わず、これらの列を参照して実行する。

---

# 12. Source Trace Columns

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

# 13. Run Management Columns

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

# 14. Row Lifecycle

`staging_subscribers_hub` の1行は次のライフサイクルを持つ。

## 1. Import

CSV から読み込まれ、正規化後に INSERT される。

状態:

```
import_run_id = <run_id>
processed_run_id = NULL
```

## 2. Waiting for Apply Orchestration

prepare / compare 待機状態。

意味:

```
compare未実行
```

## 3. Prepared / Compared

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

# 15. Constraints and Expectations

期待する性質:

- import 時点で列名揺れは吸収済み
- 正規化済みデータのみを保持する
- 1行は1加入者候補を表す
- apply は `import_run_id = :import_run_id` かつ `processed_run_id IS NULL` かつ `apply_action IN ('insert', 'update')` を前提に走る

この staging は **長期マスタではなく処理中間層** である。

---

# 16. Design Notes

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

- import orchestration / apply orchestration の分離
- subscriber resolve / join の入力
- compare hash による差分候補絞り込み
- current snapshot ID / hash / status の保持
- apply_action / diff / compare status の保持
- 履歴反映の入力
- run / source trace の保持
- 未処理キュー管理

identity_hash は:

```text
subscriber resolve / join 用
```

compare hash は:

```text
compare_identity_norm_hash
compare_other_hash
address_hash
```

を利用する。

compare hash は full compare を完全に無くすためではなく、
詳細compare候補を高速に絞るために利用する。

標準用途では:

```text
match値ではなく norm値
```

を hash 化する。

住所は `address_hash` と `is_current` を組み合わせて current 判定を行う。

連絡先 compare は現行 `subscriber_contacts` の hash 比較を行わず、
Hub apply では `subscriber_contact_points` を正本構造として扱う。

`subscriber_contacts` は legacy / backfill source / temporary reference として扱う。

を担うテーブルである。

current snapshot 値は current実データそのものを大量に複製するためではなく、
review と compare candidate filtering に必要な ID / hash / status に絞って保持する。

特に `current_hia_subscriber_id` は、人間が「名前変更・HIA側ID変更・別人候補」を確認するための足がかりとして保持する。