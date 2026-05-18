# Flow Overview
HIA 加入者 CSV を **PHR subscriber テーブル群へ反映するまでの全体フロー**。

旧実装は **2フェーズ構成** (`import → apply`) を前提としていたが、
ADR-0021 以降は **3フェーズ構成** (`import → prepare / compare → apply`) へ再整理する。

```
HIA Export CSV
      │
      │ (import)
      ▼
staging_subscribers_hub
      │
      │ (prepare / compare)
      ▼
apply_action / diff / compare status
      │
      │ (apply)
      ▼
subscribers
subscriber_addresses
subscriber_contacts
subscriber_audit
```

---

# 1. Import Phase
Script

```
import_subscribers_to_staging_hub.py
```

役割:

- HIA export CSV を読み込む
- 各列を normalize する
- staging_subscribers_hub に保存する

処理内容

```
CSV
 ↓
column mapping
 ↓
normalize rules
 ↓
staging_subscribers_hub
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
- 日付正規化
- staging保存

この段階では

```
subscribers
subscriber_addresses
subscriber_contacts
```

は更新しない。

---

# 2. Prepare / Compare Phase

Script (planned)

```
prepare_subscriber_apply_actions.py
```

役割

```text
staging と subscribers / address / contact を比較し、
apply_action と diff 情報を staging 側へ保持する
```

責務

- HIA subscriber ID による既存 subscriber 照合
- identity_hash 比較
- address 比較
- contact 比較
- diff columns 生成
- apply_action 決定
- compare audit 情報生成

比較結果は staging 側へ保持する。

想定カラム:

```text
matched_subscriber_id
apply_subscriber_id
apply_action
apply_diff_columns
identity_match_status
address_diff_status
contact_diff_status
apply_checked_at
```

想定 action:

```text
insert
update
noop
identity_changed
review
```

identity_hash が変更された場合でも、
HIA 側を最新正本として subscribers 側へ反映する。

ただし:

```text
name_kana_match changed
```

の場合は、既存 name parts をクリアし、
後続 normalize / split により再生成する。

---

# 3. Apply Phase
Script

```
apply_subscribers_from_staging_hub.py
```

役割

```
prepare / compare phase により生成された
apply_action をもとに本番テーブルへ反映する
```

Apply phase 自身は compare 判定を行わない。

```text
apply_action = insert
  → insert

apply_action = update
  → update

apply_action = noop
  → skip
```

処理対象

```
staging_subscribers_hub
WHERE processed_run_id IS NULL
```

---

# 4. Subscriber Identity
同一人物判定キー

```
(person_id_custom,
 name_kana_full,
 gender_code)
```

この3つが一致するレコードを

```
同一 subscriber
```

旧実装ではこの3条件を主キーとして subscriber を探索していた。

ADR-0021 以降は:

```text
1. HIA subscriber ID
2. identity_hash
3. parts / compare status
```

を組み合わせた compare phase を導入する。

---

# 5. Subscriber Upsert

旧実装では apply phase 内で insert / update / noop 判定を同時に実施していた。

ADR-0021 以降は、prepare / compare phase により apply_action を事前生成する。

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
現用1件のみ
```

```
is_current = 1
```

差分があれば

```
旧住所
  is_current = 0
  valid_to = now()

新住所
  INSERT
  is_current = 1
```

---

# 7. Contact History

テーブル

```
subscriber_contacts
```

ポリシー

```
現用1件のみ
```

差分があれば

```
旧連絡先終了
新連絡先追加
```

---

# 8. Subscriber Audit

テーブル

```
subscriber_audit
```

生成タイミング

```
subscriber insert / update
```

identity_hash 変更
address 変更
contact 変更

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

# Summary

処理フロー

```
CSV
 ↓
normalize
 ↓
staging_subscribers_hub
 ↓
prepare / compare
 ↓
apply_action 作成
 ↓
insert / update / noop
 ↓
address apply
 ↓
contact apply
 ↓
subscriber audit
 ↓
processed mark
```