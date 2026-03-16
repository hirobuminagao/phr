

# Flow Overview
HIA 加入者 CSV を **PHR subscriber テーブル群へ反映するまでの全体フロー**。

この処理は **2フェーズ構成**で実装されている。

```
HIA Export CSV
      │
      │ (import)
      ▼
staging_subscribers_hub
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

# 2. Apply Phase
Script

```
apply_subscribers_from_staging_hub.py
```

役割

```
staging → 本番テーブル反映
```

処理対象

```
staging_subscribers_hub
WHERE processed_run_id IS NULL
```

---

# 3. Subscriber Identity
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

として扱う。

---

# 4. Subscriber Upsert

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

# 5. Address History

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

# 6. Contact History

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

# 7. Subscriber Audit

テーブル

```
subscriber_audit
```

生成タイミング

```
subscriber insert / update
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

# 8. Processed Mark

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

# 9. Run Management

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
subscriber identity match
 ↓
insert / update
 ↓
address history
 ↓
contact history
 ↓
subscriber audit
 ↓
processed mark
```