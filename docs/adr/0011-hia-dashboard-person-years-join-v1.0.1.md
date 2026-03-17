# ADR-0011: HIA Dashboard ↔ Person-Year Join (v1.0.1 Completion)

## Status
Accepted

## Date
2026-03-18

## Context
PHR v1.0.1 では、HIA dashboard CSV の取込基盤と subscriber enrichment までを整備した。

現時点で `work_other.hia_dashboard_status` には、`dev_phr.subscribers` から補完した以下の列が入る。

- `subscriber_person_id_custom`
- `subscriber_name_kana_full`
- `subscriber_gender_code`
- `subscriber_birth`

また、HIA export XML 側では `work_other.hia_person_years` に person-year 単位の集約情報が保持されている。

v1.0.1 の最終的な着地点は、dashboard 側の人物一覧に対して、HIA export ZIP / XML 由来の person-year 情報を join した一覧を SQL / Navicat で出せる状態にすることとする。

この段階では、dashboard ↔ subscribers enrichment は動作しており、一部 unmatched は存在するが、その原因は処理バグではなく氏名正規化ルールの不足による機能不足である。

したがって v1.0.1 はここで一旦完了扱いとし、氏名 match / norm の強化は v1.0.2 へ送る。

## Decision
v1.0.1 では、`hia_dashboard_status` と `hia_person_years` の join について、以下の方針を採用する。

1. 主キー的な join は `subscriber_person_id_custom = person_id_custom` を使う
2. dashboard 側には `name_kana_full_match` がまだ存在しないため、暫定的に `subscriber_name_kana_full` と `hia_person_years.name_kana_norm` を比較に使う
3. 性別は `subscriber_gender_code` と `hia_person_years.gender_code` を比較に使う
4. v1.0.1 の確認用一覧では、重い多条件 join を避け、軽量 join を採用する

この join は v1.0.1 の確認・運用用途として十分とし、氏名正規化の厳密化は v1.0.2 の対象とする。

## Adopted Query (lightweight)
以下の SQL を v1.0.1 時点の確認用 / Navicat 用クエリとして採用する。

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

## Consequences
### Positive
- dashboard 側の人物一覧に対して、HIA export ZIP / XML 側の person-year 情報を join して確認できる
- `first_seen_*` / `last_seen_*` により DL月・ZIP名・XMLファイル名まで追跡できる
- v1.0.1 の到達点を明確に固定できる
- unmatched の存在を理由に v1.0.1 全体を未完扱いにせずに済む

### Negative / Limitations
- dashboard 側に `name_kana_full_match` がまだ無いため、カナ match の厳密 join にはなっていない
- `hia_person_years.name_kana_norm` は小書きカナの正規化が未対応であり、norm 定義が不十分
- 漢字氏名については CJK互換漢字・旧字体・異体字に起因する未一致が残る
- 氏名 match / norm の精度改善は v1.0.2 で改めて対応する必要がある

## Deferred to v1.0.2
以下は v1.0.2 の対象とする。

- dashboard 側への `name_kana_full_match` 追加
- `hia_person_years.name_kana_norm` の定義見直し
- 小書きカナ (`ャュョッァィゥェォ` など) の正規化追加
- 漢字 match 生成時の NFKC + 漢字正規化辞書適用
- subscribers / dashboard / person_years の match / norm 再生成 backfill

## Notes
v1.0.1 の unmatched は「処理失敗」ではなく「照合機能不足」である。

したがって v1.0.1 の完了条件は、dashboard 一覧に対して hia_person_years の ZIP / XML 情報を join して確認可能な状態に到達することとする。
