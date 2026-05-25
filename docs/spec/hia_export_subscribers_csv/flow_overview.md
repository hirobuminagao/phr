# Flow Overview
HIA 加入者 CSV を **PHR subscriber テーブル群へ反映するまでの全体フロー**。

旧実装は **2フェーズ構成** (`import → apply`) を前提としていたが、
ADR-0021 以降は **orchestration 分離構成** へ再整理する。

```text
import orchestration
  = CSV import + current snapshot update

apply orchestration
  = prepare / compare + apply + audit
```

```
HIA Export CSV
      │
      │ (import orchestration)
      ▼
staging_subscribers_hub
  - imported values
  - current snapshot
  - compare hashes
      │
      │ (apply orchestration)
      ▼
prepare / compare
      │
      ▼
apply / audit
      │
      ▼
subscribers
subscriber_addresses
subscriber_contact_points (Hub apply target)
subscriber_contacts (legacy source for backfill / temporary reference)
subscriber_audit
```

---

# 1. Import Orchestration
Script

```
import_subscribers_to_staging_hub.py
```

役割:

- HIA export CSV を読み込む
- 各列を normalize する
- compare hash を生成する（予定）
- staging_subscribers_hub に保存する
- current snapshot を staging 側へ反映する

処理内容

```
CSV
 ↓
column mapping
 ↓
normalize rules
 ↓
staging_subscribers_hub
 ↓
current snapshot update
```

使用モジュール

```
phr/lib/normalize/common.py
phr/lib/normalize/rules.py
phr/lib/normalize/subscriber.py
```

主な処理

- 保険証番号正規化
- 記号正規化
- カナ氏名正規化
- person_id_custom 生成
- identity_hash 生成
- compare_identity_norm_hash 生成（予定）
- compare_other_hash 生成（予定）
- address_hash 生成（予定）
- 日付正規化
- staging保存

この段階では

```
subscribers
subscriber_addresses
subscriber_contact_points
subscriber_contacts (legacy)
```

は更新しない。

---

# 2. Apply Orchestration

Script

```
apply_hia_subscriber_sync.py
```

役割

```
prepare / compare
apply
subscriber_audit
を orchestration する
```

責務

- subscriber identity resolve
- compare hash による候補絞り込み
- detailed compare
- apply_action 決定
- subscribers / address / contact point apply
- subscriber_audit 生成

比較結果は staging 側へ保持する。

想定カラム:

```text
current_subscriber_id
current_identity_hash
current_hia_subscriber_id
current_compare_identity_norm_hash
current_compare_other_hash
current_address_hash
current_lookup_status
current_phone_contact_point_id
current_email_contact_point_id
apply_action
apply_diff_columns
identity_match_status
compare_identity_norm_hash
compare_other_hash
address_hash
address_diff_status
contact_point_diff_status
apply_checked_at
```

想定 action:

```text
insert
update
noop
review
```

identity_hash は:

```text
subscriber resolve / join 用
```

として扱う。

登録値差分検知には:

```text
compare_identity_norm_hash
compare_other_hash
address_hash
```

を利用する。

compare hash は:

```text
full compare を完全に無くすためではなく、
詳細compare候補を高速に絞るために利用する。
```

---

# 3. Apply Phase
Script

```
apply_hia_subscriber_sync.py
```

役割

```
prepare / compare により生成された
apply_action をもとに本番テーブルへ反映する
```

apply orchestration 内では:

```text
prepare / compare
↓
apply
↓
audit
```

を順に実行する。

```text
apply_action = insert
  → insert

apply_action = update
  → update

apply_action = noop
  → skip

apply_action = review
  → skip
```

処理対象

```
staging_subscribers_hub
WHERE import_run_id = :import_run_id
  AND processed_run_id IS NULL
```

---

# 4. Subscriber Identity
同一人物判定キー

```text
identity_hash
```

identity_hash は:

```text
person_id_custom
+ name_kana_full_match
+ gender_code
```

を利用した resolve / join 用 hash として扱う。

旧実装ではこの3条件を主キーとして subscriber を探索していた。

ADR-0021 以降は:

```text
1. HIA subscriber ID
2. identity_hash
3. current snapshot
4. compare_identity_norm_hash
5. compare_other_hash
6. address_hash
```

を使い、resolve / join と登録値差分検知を分離する。

```text
identity_hash
  = subscriber resolve / join 用

compare_*_hash
  = detailed compare 候補絞り込み用
```

---

# 5. Subscriber Upsert

旧実装では apply 内で insert / update / noop 判定を同時に実施していた。

ADR-0021 以降は、prepare / compare により apply_action を事前生成する。

## Insert

```
subscribers に存在しない
```

場合

```
INSERT subscribers
```

---

## Update

既存 subscriber が存在する場合

```
差分比較
```

差分がある場合のみ

```
UPDATE subscribers
```

差分がない場合

```
noop
```

---

# 6. Address History

テーブル

```
subscriber_addresses
```

ポリシー

```
current row は1件
history row は複数保持
```

差分があれば

```text
same address_hash exists and is_current = 1
  -> noop

same address_hash exists and is_current = 0
  -> current切替

same address_hash not exists
  -> insert
```

---

# 7. Contact History

テーブル

```
subscriber_contacts
```

Hub apply では、新しい contact point 型テーブルを先に導入する。

新テーブル想定:

```text
subscriber_contact_points
```

想定構造:

```text
contact_point_id
subscriber_id
contact_type
contact_value
is_current
valid_from
valid_to
source
created_at
updated_at
```

`contact_type` の初期値:

```text
phone
email
```

現行 `subscriber_contacts` は:

```text
phone + email 同居型
```

であり、phoneのみ変更 / emailのみ変更 / null時の current解除 / 複数連絡先管理を安全に扱いにくい。

そのため、Hub 側の apply orchestration では `subscriber_contact_points` を正として実装する。

既存データの移行:

```text
subscriber_contacts
  ↓
phone が空でなければ subscriber_contact_points(contact_type='phone') へ backfill
email が空でなければ subscriber_contact_points(contact_type='email') へ backfill
```

旧1行は最大2行へ分解される。

```text
旧:
subscriber_id + phone + email + is_current

新:
subscriber_id + contact_type='phone' + contact_value + is_current
subscriber_id + contact_type='email' + contact_value + is_current
```

Hub apply 時の null 扱い:

```text
HIA CSV phone が null
  -> subscriber_id に紐づく phone current を全て current から外す

HIA CSV email が null
  -> subscriber_id に紐づく email current を全て current から外す
```

つまり null は:

```text
何もしない
```

ではなく:

```text
HIA正本上、現在値なし
```

として扱う。

現時点では contact compare hash は導入しない。
contact は `contact_type + contact_value + is_current` を基準に compare / apply する。

---

# 8. Subscriber Audit

テーブル

```
subscriber_audit
```

生成タイミング

```text
subscriber insert
subscriber update
compare_identity_norm_hash change
compare_other_hash change
address current change
contact point current change
```

実装

```
Python apply script
```

理由

- run_id を記録できる
- ETLコンテキスト保持
- trigger依存回避

詳細は

```
ADR-0010
```

---

# 9. Processed Mark

apply 成功後

```
staging_subscribers_hub
```

に刻印する

```
processed_run_id
processed_at
```

これにより

```
未処理キュー
```

として扱える。

---

# 10. Run Management

apply 実行は

```
etl_runs
etl_errors
```

で管理する。

保存内容

```
run_id
status
rows_inserted
rows_updated
errors
```

---

# 11. Contact Point Migration Order

contact point 化は、Hub 側を先に完走させる。

実装順:

```text
1. Hub側だけ subscriber_contact_points 前提に整える
   - DDL
   - migration
   - 既存 subscriber_contacts からの backfill
   - Hub current projection 差し替え
   - Hub compare / apply 実装

2. Hub apply orchestration を完成させる
   - prepare
   - compare
   - apply
   - audit
   - dry-run / 小件数検証

3. fund側は後で見直す
   - staging_subscribers_fund diff関連
   - projection / compare 共通化
   - 旧 subscriber_contacts 参照停止
```

理由:

```text
Hub加入者更新ラインを先に完走させるため。
fund側を同時に触ると、contact再設計とapply設計が並行して中途半端になるため、後工程へ分離する。
```

---

# Summary

処理フロー

```text
CSV
 ↓
normalize
 ↓
identity_hash / compare hash generation
 ↓
staging_subscribers_hub
  - import values
  - current snapshot values
 ↓
compare hash candidate filtering
 ↓
detailed compare
 ↓
apply_action decision
 ↓
subscribers apply
 ↓
address apply
 ↓
contact point apply
 ↓
subscriber audit
 ↓
processed mark
```