# Subscriber Data Flow

## Purpose

本ドキュメントは、加入者更新業務においてデータがどのテーブル・システム間を流れるかを整理する。

業務フローは以下を参照する。

```text
docs/spec/subscriber/01_business_flow.md
```

---

## Overall Data Flow

```text
健保受領CSV
  ↓
staging_subscribers_fund
  ↓
HIA反映CSV
  ↓
HIA
  ↓
HIA export CSV
  ↓
staging_subscribers_hub
  ↓
Hub apply
  ↓
subscribers
  ├─ subscriber_addresses
  ├─ subscriber_contact_points
  └─ subscriber_audit

dashboard
```

---

## Fund Side

```text
健保受領CSV
  ↓
staging_subscribers_fund
```

役割:

```text
受領データ保持
正規化値保持
比較用データ保持
```

詳細:

```text
docs/spec/from_fund_subscribers
```

---

## HIA

```text
HIA反映CSV
  ↓
HIA
  ↓
HIA export CSV
```

加入者運用上の正本として扱う。

---

## Hub Side

```text
HIA export CSV
  ↓
staging_subscribers_hub
```

役割:

```text
current snapshot
prepare
compare
apply
Hub正本反映の入口
```

詳細:

```text
docs/spec/hia_export_subscribers_csv
```

---

## Subscriber Root

```text
staging_subscribers_hub
  ↓
Hub apply
  ↓
subscribers
```

加入者正本。

identity解決と基本属性を保持する。

---

## Address

```text
subscribers
  ↓
subscriber_addresses
```

住所履歴を保持する。

current と history を管理する。

---

## Contact Point

```text
subscribers
  ↓
subscriber_contact_points
```

連絡先履歴を保持する。

```text
phone
email
```

を別レコードとして管理する。

---

## Audit

```text
subscribers
  ↓
subscriber_audit
```

加入者更新監査情報を保持する。

---

## Dashboard

```text
subscribers
subscriber_addresses
subscriber_contact_points
  ↓
dashboard
```

加入者状態を表示する。

subscribers のみではなく、住所・連絡先などの加入者関連情報も参照対象となる。

年度管理は主に dashboard 側で扱う。

---

## Related Documents

```text
docs/spec/subscriber/README.md
docs/spec/subscriber/01_business_flow.md
docs/spec/subscriber/02_script_map.md
docs/spec/from_fund_subscribers
docs/spec/hia_export_subscribers_csv
```