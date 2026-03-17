# ADR-0012: Identity Canonicalization and Join Hash Policy (v1.0.2)

## Status
Accepted

## Date
2026-03-18

## Context
PHR v1.0.1 では、以下の到達点を固定した。

- `staging_subscribers_hub -> subscribers` apply パイプラインを整備
- `subscriber_addresses` / `subscriber_contacts` / `subscriber_audit` を整備
- `hia_dashboard_status` に subscriber enrichment を追加
- `hia_dashboard_status` と `hia_person_years` を join して、ZIP / XML 情報を確認できる状態を確立

しかし、dashboard / subscribers / hia_person_years 間の人物照合では、氏名漢字・氏名カナの正規化ルールが十分に統一されておらず、以下の問題が確認された。

- CJK互換漢字（例: `羽`, `神`, `塚`, `礼`, `猪`, `﨑`）に起因する未一致
- dashboard 側の氏名漢字が表示用 / 正規化後に寄った値で出力されるケース
- `hia_person_years.name_kana_norm` が小書きカナ正規化などを十分に含んでいない
- join 条件が複雑化し、テーブル件数増加時の保守性・性能に不安がある

このため v1.0.2 では、人物照合で利用する値の作り方を再定義し、raw / match / hash の三段構えを正式方針として採用する。

## Decision
v1.0.2 では、照合に使う項目について以下の原則を採用する。

### 1. raw / match / hash の三段構えを標準とする

#### raw
元の値は省略せず保持する。

- 元CSV
- 元XML
- 元システム出力

由来の文字列・コード・日付は、原値のまま保管する。

raw は表示・監査・再計算の基礎データであり、正規化やフォーマット変換で上書きしない。

#### match
照合用に正規化した値を必ず生成し、保管する。

match 列は、同一人物判定・同一データ判定のための canonical value として扱う。

match 系の基本ルールは以下とする。

- 参照用（match系）は英数字を半角に寄せる
- 参照用（match系）は先頭0を削除する
- raw は保持し、match は照合しやすさを優先する

このルールは、保険証記号・保険証番号・氏名カナ・氏名漢字など、人物照合に使う canonical value に適用する。

特に保険証記号については、match を「記号そのものを比較できる canonical value」として扱い、数字部分だけを残すのではなく、非数字部分も保持したまま照合に使う。

例:

- raw: `川崎-01`
- match: `川崎1`

したがって、`川崎-01` と `横浜-01` を同一の `1` に潰すような正規化は採用しない。

#### hash
join 最適化に必要な値が揃う場合は、match 列を元にハッシュ列を生成して保管する。

hash 列は raw や match の代替ではなく、結合・参照最適化のための派生キーとする。

#### formatted / export value
出力用・連携用の値正規化は、match とは別に扱う。

厚生労働省フォーマットに寄せる値については、以下を原則とする。

- 値がすべて数字なら半角
- 一文字でも数字以外が含まれるなら全体を全角

したがって、参照用（match系）の半角寄せルールと、出力用の厚生労働省フォーマット寄せルールは明確に分離する。

保険証記号については、raw / match / formatted(export) を分離して扱う。

例:

- raw: `川崎-01`
- match: `川崎1`
- formatted/export: `川崎ー０１`

別例:

- raw: `０００００１２３`
- match: `123`
- formatted/export: `123`

### 2. 人物照合の canonical input を明示する
人物照合用の canonical input は、少なくとも以下を基礎とする。

- `person_id_custom`
- `name_kana_full_match`
- `gender_code`

ここで `person_id_custom` は次を元に生成される。

- `birthdate`
- `insurance_number`
- `insurer_number`
- `insurance_symbol`

したがって、保険者番号・記号・番号・生年月日に加えて、カナ match と gender を人物照合の canonical input とする。

ここで使う `person_id_custom`・`name_kana_full_match`・各種 match 値は、いずれも参照用（match系）の半角寄せ・先頭0削除ルールに従う。

### 3. 漢字氏名 match は NFKC のみで終わらせない

漢字 match 列は、少なくとも以下を経て生成する。

1. Unicode 正規化（NFKC）
2. trim
3. 空白除去
4. 漢字正規化辞書の適用

これにより、CJK互換漢字・代表的な旧字体・異体字について、照合用に寄せた canonical value を生成する。

### 4. 漢字正規化辞書は DB テーブルで管理する

漢字正規化辞書は `dev_phr` 配下に配置し、コード埋め込みではなくデータとして管理する。

v1.0.2 の初期スコープでは、まず 1文字単位の正規化辞書を対象とする。

例:

- `羽 -> 羽`
- `神 -> 神`
- `塚 -> 塚`
- `礼 -> 礼`
- `猪 -> 猪`
- `﨑 -> 崎`
- `瀨 -> 瀬`
- `髙 -> 高`

word 単位の正規化（例: `渡邉 -> 渡辺`）は、必要が確認された段階で v1.0.x 以降に拡張する。

### 5. join 用 hash を導入する

人物照合 join 最適化のため、以下を元にした hash 列を導入する。

```text
person_id_custom + name_kana_full_match + gender_code
```

ハッシュ方式は SHA-256 を基本とし、区切り文字を含む canonical string を入力とする。

例:

```text
{person_id_custom}|{name_kana_full_match}|{gender_code}
```

### 6. hash の位置づけ

join 用 hash は以下のために使う。

- 大量データ join の軽量化
- SQL の簡素化
- downstream テーブル間の参照統一

ただし、デバッグ・監査・再計算のために raw と match は必ず残す。

## Scope (v1.0.2)
本 ADR の対象は以下。

- `dev_phr.subscribers`
- `work_other.hia_dashboard_status`
- `work_other.hia_person_years`

### 改修対象
- match 列生成ロジックの統一
- 漢字正規化辞書テーブル追加
- `name_kana_full_match` / `name_full_match` / `name_kana_norm` 系定義見直し
- `insurance_symbol` の raw / match / formatted(export) 分離方針の明確化
- join 用 hash 列追加
- 既存データの backfill

## Consequences
### Positive
- raw / match / hash の役割が明確になる
- 氏名・保険証系の照合ロジックを一貫して再利用できる
- 互換漢字・旧字体・異体字起因の未一致を減らせる
- 将来的な dashboard / subscribers / person_years / XML ledger join を軽量化できる
- 正規化ルールの改善を match / hash 再生成で追従できる

### Negative / Trade-offs
- テーブル列が増える
- backfill の実装と再実行が必要になる
- 正規化辞書の保守運用が発生する
- hash だけを見ても元の照合根拠は分からないため、raw / match を常に併存させる必要がある

## Deferred / Non-goals
現時点では以下は対象外とする。

- word 単位・複数文字単位の正規化辞書本格対応
- regex ベースの複雑な文字列正規化エンジン
- すべての UI 表示フォント問題の解決

ただし、将来的には UI 側でも漢字網羅性の高いフォント採用を検討する。

## Notes
本 ADR の大前提は以下である。

- 元の値は省かない
- 必ず match を作って保管する
- ハッシュに必要なものが揃うなら hash を作る
- 参照用（match系）は英数字を半角に寄せる
- 参照用（match系）は先頭0を削除する
- 出力用の値は「全てが数字なら半角、一文字でも数字以外が入るなら全部全角」のルールに従う
- 保険証記号は raw / match / formatted(export) を分離して扱う
- 保険証記号の match は非数字部分も保持した canonical value とする