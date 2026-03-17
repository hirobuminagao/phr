

# Dashboard Person-Year Join

このドキュメントは、`work_other.hia_dashboard_status` を起点として、`work_other.hia_person_years` に保持された HIA export ZIP / XML 由来の person-year 情報を join して確認するための仕様を定義する。

本仕様は dashboard CSV そのものの ingestion 仕様ではなく、dashboard 人物一覧に対して HIA XML ledger 側の情報を重ねて確認する **運用・確認ビュー仕様** である。

関連ADR:

- `docs/adr/0011-hia-dashboard-person-years-join-v1.0.1.md`

関連DDL:

- `sql/ddl/work_other/0057_work_other__hia_dashboard_status.sql`
- `sql/ddl/work_other/0052_work_other__hia_person_years.sql`

---

## 1. Purpose

目的は以下の通り。

- dashboard 側の人物一覧に対して、HIA export 側の person-year 情報を join して確認できるようにする
- ZIP 名 / XML ファイル名 / DL 月を一覧上で追えるようにする
- Navicat / SQL で確認可能な形で v1.0.1 の到達点を固定する

本仕様は、v1.0.1 時点の確認用・運用用 join を定義する。

---

## 2. Source Tables

### 2.1 dashboard side

テーブル:

```text
work_other.hia_dashboard_status
```

主に使用する列:

- `hia_dashboard_person_id`
- `status`
- `name`
- `name_match`
- `subscriber_person_id_custom`
- `subscriber_name_kana_full`
- `subscriber_gender_code`
- `subscriber_birth`

### 2.2 person-year side

テーブル:

```text
work_other.hia_person_years
```

主に使用する列:

- `person_year_id`
- `person_id_custom`
- `name_kana_norm`
- `gender_code`
- `exam_year`
- `dl_count`
- `first_seen_dl_date`
- `first_seen_zip_name`
- `first_seen_xml_filename`
- `last_seen_dl_date`
- `last_seen_zip_name`
- `last_seen_xml_filename`

---

## 3. Join Policy (v1.0.1)

v1.0.1 では、重い多条件 join を避け、以下の軽量方針を採用する。

### 3.1 primary join key

主キー的な join は次を使用する。

```text
hia_dashboard_status.subscriber_person_id_custom
=
hia_person_years.person_id_custom
```

`person_id_custom` は以下を元に生成される人物識別キーである。

- birthdate
- insurance_number
- insurer_number
- insurance_symbol

そのため、保険者番号・記号・番号・生年月日を個別に再比較する代わりに、v1.0.1 では `person_id_custom` を主軸とする。

### 3.2 secondary checks

v1.0.1 では dashboard 側に `name_kana_full_match` がまだ無いため、暫定的に以下を補助条件とする。

- `subscriber_name_kana_full = name_kana_norm`
- `subscriber_gender_code = gender_code`

ただし `hia_person_years.name_kana_norm` は将来的に正規化定義を見直す予定であり、厳密仕様は v1.0.2 で再設計する。

---

## 4. Adopted Lightweight Query

v1.0.1 時点では、以下の SQL を確認用 / Navicat 用クエリとして採用する。

```sql
SELECT
    d.hia_dashboard_person_id,
    d.status,
    d.name,
    d.name_match,

    d.subscriber_person_id_custom,
    d.subscriber_name_kana_full,
    d.subscriber_gender_code,
    d.subscriber_birth,

    p.person_year_id,
    p.exam_year,
    p.dl_count,
    p.first_seen_dl_date,
    DATE_FORMAT(p.first_seen_dl_date, '%Y-%m') AS first_seen_dl_month,
    p.first_seen_zip_name,
    p.first_seen_xml_filename,
    p.last_seen_dl_date,
    DATE_FORMAT(p.last_seen_dl_date, '%Y-%m') AS last_seen_dl_month,
    p.last_seen_zip_name,
    p.last_seen_xml_filename

FROM work_other.hia_dashboard_status AS d
LEFT JOIN work_other.hia_person_years AS p
  ON p.person_id_custom = d.subscriber_person_id_custom
 AND p.name_kana_norm = d.subscriber_name_kana_full
 AND p.gender_code = CAST(d.subscriber_gender_code AS CHAR)

WHERE d.subscriber_person_id_custom IS NOT NULL

ORDER BY
    d.hia_dashboard_person_id,
    p.exam_year;
```

---

## 5. Output Meaning

この一覧により、dashboard 側の人物に対して以下を確認できる。

- dashboard 側の人物ID / status / 氏名
- subscriber enrichment により補完された人物キー
- person-year ごとの exam_year
- DL回数
- first seen / last seen の DL 日付
- DL 月
- ZIP 名
- XML ファイル名

つまり、dashboard 人物一覧から HIA export XML 側の実体ファイルまで追跡できる。

---

## 6. Why This Query Is Lightweight

以下の理由で、v1.0.1 ではこの join を軽量版として採用する。

- `person_id_custom` を使うことで、保険者番号・記号・番号・生年月日を個別に再比較しない
- dashboard 側で subscriber enrichment 済みの値をそのまま利用できる
- 過剰な多条件 join を避けられる
- Navicat 上での確認用途に十分な応答性を確保しやすい

---

## 7. Known Limitations

v1.0.1 の時点では以下の制約がある。

- dashboard 側に `name_kana_full_match` がまだ存在しない
- `hia_person_years.name_kana_norm` は小書きカナ正規化などが未対応であり、norm の定義が不十分
- 漢字氏名については CJK互換漢字・旧字体・異体字起因の未一致が残る
- unmatched は処理バグではなく照合機能不足によるものがある

---

## 8. Deferred to v1.0.2

以下は v1.0.2 で対応する。

- dashboard 側への `name_kana_full_match` 追加
- `hia_person_years.name_kana_norm` の定義見直し
- 小書きカナ (`ャュョッァィゥェォ` など) の正規化追加
- 漢字 match 列への NFKC + 漢字正規化辞書の適用
- subscribers / dashboard / person_years の match / norm backfill

---

## 9. Position in v1.0.1

v1.0.1 の完了条件は、dashboard 一覧に対して `hia_person_years` の ZIP / XML 情報を join して確認可能な状態に到達することとする。

したがって本仕様は、v1.0.1 の最終到達点を固定する確認ビュー仕様として扱う。