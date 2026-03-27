# Identity Canonicalization Specification (v1.0.3)

## 概要

本仕様書は、PHR v1.0.3における正規化および同一性判定ポリシーを定義します。

これは ADR-0012: *Identity Canonicalization and Join Hash Policy (v1.0.2)* および ADR-0015: *medi系への identity_hash 導入と基本情報表現の整流化（v1.0.3）* を具現化したものです。

目的は以下のテーブル間で一貫した同一性判定を保証することです：

- `dev_phr.subscribers`
- `work_other.hia_dashboard_status`
- `work_other.hia_person_years`
- `work_other.medi_xml_ledger`
- `work_other.medi_xml_receipts`

これらのテーブルは異なるパイプラインから生成されています：

- HUB subscriber CSV
- HIA dashboard CSV
- HIA export XML
- medical institution XML / ZIP

これらのソースは同一性データを異なる方法で正規化しているため、統一された正規化レイヤーが必要です。

---

# 1. 基本原則: raw / norm / match / export / hash

すべての同一性フィールドは**三層構造**に従います。

## 1.1 raw

元の値は必ず保持されなければなりません。

例：

- XMLの元値
- CSVの元値
- ダッシュボードのエクスポート値

これらは以下の目的で保持されます：

- トレーサビリティ
- デバッグ
- 監査
- 再計算

raw値は決して破棄または上書きされてはなりません。

---

## 1.2 norm

`norm`値は**内部処理用の正規化済み値**です。

目的：

- 内部集約
- レコード間比較の前段
- 後続のmatch生成の入力安定化

norm値は、raw値を直接破壊せずに内部処理で扱うための中間層です。

典型例：

- 氏名カナのNFKC正規化
- 空白除去後の内部値
- 記号・番号の内部比較用前処理値

norm値は内部処理のための値であり、表示用や外部出力用ではありません。

---

## 1.3 match

`match`値は**同一性比較**に用いられる正規化済みの値です。

これらの値は正規化ルールを適用して生成されます。

### Match Canonical Rules (固定)

すべてのmatch値は以下の厳格なルールに従わなければなりません：

- 英数字は半角に正規化する
- 先頭のゼロは（数値の塊ごとに）削除する
- raw値は別途保持する
- match値は表示形式より比較の安定性を優先する

これらのルールは以下に適用されます：

- insurance_symbol_match
- insurance_number_match
- name_kana_full_match
- name_full_match

特に `insurance_symbol_match` は、保険証記号そのものを比較できる canonical value として扱います。

したがって、数字部分だけを残すのではなく、非数字部分も保持したまま照合に使います。

例:

- raw: `川崎-01`
- match: `川崎1`

このため、`川崎-01` と `横浜-01` を同一の `1` に潰すような正規化は採用しません。

典型的な変換例：

- Unicode正規化（NFKC）
- トリム（前後の空白削除）
- 空白除去
- カナ正規化
- 辞書による漢字正規化

match値は以下の用途に使われます：

- 人物の同一性判定
- 重複排除
- システム間の結合

---

## 1.4 export

`export`値は**外部仕様に合わせた出力用の整形値**です。

目的：

- XML出力
- CSV出力
- MHLW仕様準拠のフォーマット生成

export値は match 値とは目的が異なります。

- match = 比較最適化
- export = 出力仕様最適化

例：

- raw: `川崎-01`
- match: `川崎1`
- export: `川崎ー０１`

export値は比較用には使いません。

---

## 1.5 hash

必要な同一性入力がすべて揃った場合、hash列が生成されます。

目的：

- 結合の高速化
- SQLの簡素化
- 安定した同一性参照

このハッシュは**rawやmatch値の代替ではありません**。

---

# 2. 正規化された同一性入力

システム全体で使用される正規化された同一性入力は以下です：

```
person_id_custom
+ name_kana_full_match
+ gender_code
```

`person_id_custom`自体は以下から派生します：

- insurer_number
- insurance_symbol
- insurance_number
- birthdate

`person_id_custom` は raw 値をそのまま比較用に使うのではなく、`custom_id_gen` 内部で ID生成専用の正規化を行った上で生成されます。

具体的には、保険者番号・記号・番号については先頭ゼロを正規化してから固定長へフィットさせます。

- 見かけ上の桁長ではなく実効桁で幅判定する
- 実効桁が幅を超える場合は生成対象外とする
- 桁切りは行わない

したがって、`match` 値と `person_id_custom` は似た入力を参照していても責務が異なります。

- `match` = 比較用 canonical value
- `person_id_custom` = ID生成専用 canonical value

---

# 3. ハッシュ同一性キー

結合最適化用のキーは以下から生成されます：

```
person_id_custom | name_kana_full_match | gender_code
```

ハッシュアルゴリズム：

```
SHA-256
```

例となる正規化文字列：

```
01234567-00123-987654|ナガオヒロフミ|1
```

結果：

```
identity_hash
```

このハッシュにより大規模テーブル間の非常に効率的な結合が可能になります。

---

# 4. 漢字正規化

漢字名は複数のUnicode形式が存在するため追加の正規化が必要です。

正規化手順：

1. Unicode NFKC正規化
2. トリム
3. 空白除去
4. 漢字正規化辞書の適用

変換例：

| Original | Canonical |
|--------|--------|
| 羽 | 羽 |
| 神 | 神 |
| 塚 | 塚 |
| 礼 | 礼 |
| 猪 | 猪 |
| 﨑 | 崎 |
| 瀨 | 瀬 |
| 髙 | 高 |

### 重要な分離ルール

漢字正規化はmatch値のみに適用されます。

- raw値は変更しません
- match値はNFKC＋辞書正規化を適用
- フォーマット／エクスポート値はMHLWのフォーマットルールに従います（後述）

---

### 4.1 正規化フロー（実装ルール）

すべてのmatch値は共有の正規化レイヤーを通じて生成されなければなりません。

システムは中央集権的なオーケストレーションアプローチを採用しています：

- アプリケーションスクリプトで直接正規化ロジックを実装してはなりません
- すべての正規化は`common.py`に委譲されます
- `common.py`は正規化のオーケストレーションレイヤーとして機能します

正規化フロー：

```
アプリケーションスクリプト
    ↓
common.py（オーケストレーション）
    ↓
基本正規化（NFKC、トリム、空白除去等）
    ↓
（任意）辞書正規化（外部モジュール／DB）
    ↓
最終的なmatch値
```

漢字辞書正規化は外部依存として実装されています。

`common.py`は以下を担当します：

- 正規化手順を順序通りに実行すること
- 必要に応じて辞書変換を呼び出すこと
- 最終的な正規化値を返すこと

これにより以下の間で整合性が確保されます：

- subscribers
- dashboard
- person_years

---

# 5. カナ正規化

カナ正規化は安定したマッチングを保証しなければなりません。

ルール例：

- 半角カナ → 全角カナに変換
- ひらがな → カタカナに変換
- 小書きカナの正規化

例：

```
ャ → ヤ
ュ → ユ
ョ → ヨ
ッ → ツ
```

空白は除去されます。

---

# 5.1 フォーマット／エクスポート値ルール

XMLや外部システム向けのフォーマット値はMHLWのフォーマットルールに従わなければなりません。

ルール：

- 値が数字のみの場合 → 半角のまま保持
- 1文字でも数字以外が含まれる場合 → 文字列全体を全角に変換

このルールはmatch正規化とは意図的に異なります。

したがって：

- match = 比較最適化（半角、先頭ゼロ削除）
- formatted = 仕様最適化（MHLW準拠）

### 保険証記号の raw / match / formatted(export) 分離

保険証記号は raw / match / formatted(export) を分離して扱います。

例:

- raw: `川崎-01`
- match: `川崎1`
- formatted/export: `川崎ー０１`

別例:

- raw: `０００００１２３`
- match: `123`
- formatted/export: `123`

---

# 6. 正規化辞書

漢字正規化は`dev_phr`の辞書テーブルで実装されます。

テーブル例：

```
identity_kanji_normalization
```

構造例：

| original_char | normalized_char |
|---------------|----------------|
| 羽 | 羽 |
| 神 | 神 |
| 塚 | 塚 |
| 礼 | 礼 |

システムはこのマッピングを文字ごとに繰り返し適用する可能性があります。

---

# 7. 対象テーブル

以下のテーブルが正規化ポリシーを採用します。

## subscribers

場所：

```
dev_phr.subscribers
```

標準化対象カラム：

- `name_kana_full_match`
- `name_full_match`
- `identity_hash`
- `insurance_symbol_match`
- `insurance_symbol_export`

---

## dashboard

場所：

```
work_other.hia_dashboard_status
```

変更点：

- `name_kana_full_match`の追加
- `insurance_symbol` の raw / match / formatted(export) 分離方針の適用
- `identity_hash`の追加

---

## person_years

場所：

```
work_other.hia_person_years
```

変更点：

- `name_kana_norm`の改善
- `identity_hash`の追加
- `insurance_symbol_match` / `insurance_number_match` の common 準拠

---

## medi

場所：

```
work_other.medi_xml_ledger
work_other.medi_xml_receipts
```

変更方針（v1.0.3）：

- 既存粒度は変更しない
- `identity_hash` を後付けで付与できる構造へ寄せる
- 基本情報の持ち方を raw / norm / match / export で揃える
- subscribers / dashboard / person_years / HIA出力との人物軸を一致させる

---

# 7.1 現在の実装状態（2026-03 時点, v1.0.3 基準）

本節は、v1.0.2 における主要実装の現状を整理する。

## subscribers

`dev_phr.subscribers` は、以下の方針で整備済みである。

- `name_kana_full_match` は `common.py` の `normalize_name_kana_match(...)` を使用する
- `name_full_match` は `common.py` の `normalize_name_kanji_match(...)` を使用する
- `insurance_symbol_match` は HIA export ZIP v1 の固定ルールに合わせる
- `insurance_symbol_export` は raw / match と分離した出力用値として保持する

## dashboard

`work_other.hia_dashboard_status` は、以下の方針で整備済みである。

- `name_match` は `common.py` の `normalize_name_kanji_match(...)` を使用する
- `subscriber_name_kana_full_match` を subscriber enrichment として保持する
- dashboard 自身の照合は subscriber 側 canonical value と整合することを前提とする

## person_years / HIA ZIP import

`hia_import_zip.py` は、正規化ロジックをローカル実装せず、`common.py` に寄せる方針へ移行済みである。

対象:

- `insurance_symbol_match`
- `insurance_number_match`
- `symbol_for_custom_id`
- `birth_yyyymmdd`
- `name_kana_norm`

したがって、HIA ZIP import も subscribers / dashboard と同一の正規化レイヤーを参照する。

## hia_import_zips

`work_other.hia_import_zips` には、archive 後の物理保存先を追跡するため、以下を保持する。

- `archived_zip_path`
- `archived_at`

これにより、DB 参照時に「読み取った ZIP が物理的にどこへ移動したか」を追跡できる。

## medi

v1.0.3 では、medi系について以下の方針を採用する。

- 既存の ledger / receipts の粒度は壊さない
- `identity_hash` は取り込み時の直接参照ではなく、後段で付与・更新する
- `person_id_custom` は raw 値から `custom_id_gen` により生成する
- `custom_id_gen` では保険者番号・記号・番号の先頭ゼロを正規化してから幅判定を行う
- 正規化ロジックの原本は `scripts/work_folder` 側に置く
- `scripts/kenshin_list_pydir` 側で必要な場合は同一ロジックをコピーして反映する

これにより、v1の運用フローを崩さずに人物軸を medi 系へ通す。

# 7.2 既存データの不整合とバックフィル前提

運用開始前後で正規化ロジックが段階的に改善されたため、既存データには旧ロジックで作られた値が残存している可能性がある。

特に注意する点:

- 旧 `name_kana_full_match` / `name_kana_norm` では小書きカナが残っている可能性がある
- 旧 `insurance_symbol_match` では固定ルール以前の値が残っている可能性がある
- dashboard の subscriber enrichment は旧 canonical value のまま保持されている可能性がある

したがって、v1.0.2 の完全適用には backfill を前提とする。

# 8. バックフィル戦略

既存データは再計算されなければなりません。

バックフィルスクリプトは：

1. `subscribers` の match / export 系を再計算
2. `hia_dashboard_status` の subscriber enrichment / name_match 系を再計算
3. `hia_person_years` の `name_kana_norm` / `insurance_symbol_match` / `insurance_number_match` を再計算
4. `medi` 系の `identity_hash` と基本情報表現を再計算
5. 最後に `identity_hash` を再生成

実施順の原則は以下とする。

1. `subscribers` の match / export 系を再計算
2. `hia_dashboard_status` の subscriber enrichment / name_match 系を再計算
3. `hia_person_years` の `name_kana_norm` / `insurance_symbol_match` / `insurance_number_match` を再計算
4. `medi` 系の `identity_hash` と基本情報表現を再計算
5. 最後に `identity_hash` を再生成

この順序により、親テーブルである subscribers の canonical value を先に安定化させたうえで、下流テーブルへ反映できる。

これにより既存行も同一の同一性モデルに準拠します。

---

# 9. 将来的な拡張

将来的な改善案には以下が含まれます：

- 複数文字の正規化（例：渡邉 → 渡辺）
- 高度な正規表現による正規化
- UI安全なフォント正規化

これらはv1.0.2の範囲外です。

---

# Summary

PHRにおける同一性処理は以下の4原則に従います：

```
1. raw値を決して破棄しない
2. norm値を内部処理用の中間層として保持する
3. 常にmatch値を生成する（英数字は半角、先頭ゼロなし）
4. 保険証記号は raw / norm / match / export を明確に分離する
5. match（比較用）とexport（出力用）を明確に分離する
6. 入力が揃えばhashを生成する
```

このアーキテクチャにより：

- トレーサビリティが保証され
- 一貫した同一性解決が実現され
- スケーラブルな結合が可能になります

これがPHR全サブシステムにおける同一性判定の基盤となります。