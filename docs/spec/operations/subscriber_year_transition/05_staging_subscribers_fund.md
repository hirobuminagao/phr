# 05_staging_subscribers_fund

## 目的

本ファイルは、`staging_subscribers_fund` に関する親 spec である。

詳細な設計は以下の分割ファイルへ委譲し、本ファイルでは全体の位置づけ、責務、確定方針のみを整理する。

---

## 分割ファイル

| ファイル | 内容 |
|---|---|
| `05a_staging_subscribers_fund_template_import.md` | テンプレートベース取り込み、CSV配置、archive、rule / required、記号100本人の投入方針 |
| `05b_staging_subscribers_fund_column_policy.md` | raw / norm / match、identity、本人判別、会社情報、staging固有カラム、diffカラム |
| `05c_staging_subscribers_fund_ddl_inventory.md` | 現行DDLカラムの棚卸し、維持 / rename / 追加 / 削除方針 |
| `05d_staging_subscribers_fund_2026_diff_policy.md` | 2026年度受領データと2025年度固定済み基準の差分判定方針 |

---

## 現在の位置づけ

`staging_subscribers_fund` は、健保から受領した加入者データを一時的に受け止める staging テーブルである。

ただし、単なる取り込み先ではなく、年度更新運用では以下の役割を持つ。

- 健保受領データのテンプレートベース取り込み
- HIA登録補助に利用するための基礎データ保持
- `subscribers` 補完の入力面
- 2025年度固定済み基準と2026年度受領データの比較基盤
- 2026年度差分判定用ワークベンチ

---

## 加入者マスタではない

`staging_subscribers_fund` は `subscribers` そのものではない。

そのため、以下は主責務としない。

- 業務上の最終確定判定結果の保持
- 年度末状態の保持
- `subscribers` の完成形カラムをここで確定すること

ただし、年度比較のための一時的な判定補助情報は保持する。

---

## 確定方針サマリ

- `staging_subscribers_fund` は受領データの比較基盤として扱う
- 健保受領CSVはテンプレート定義に基づいて取り込む
- raw データの保持を主目的としない
- 主値は `*_norm` とする
- 照合・比較用に `*_match` を持つ
- `person_id_custom` / `identity_hash` は staging 取り込み時点で生成する
- identity 生成に必要な項目に欠損がある場合は、明示的にチェックしたうえで `NULL` とする
- `matched_subscriber_id` は identity_hash 生成後の `subscribers` 照合結果として保持する
- `diff_status` / `diff_status_method` / `diff_status_reason` を2026年度差分判定用に保持する
- `diff_status` は一時判定結果であり、業務上の最終確定結果とは分離する
- 記号100本人データは受領済みだがフォーマットが異なるため、既存取り込みフォーマットへヘッダー・列順を合わせて投入する
- 会社・部署コードは、HIAマスタ本体と健保別マッピングを分けて扱う
- staging では、マッピング後の `mapped_employer_code` / `mapped_department_code` と、現行 `subscribers` 由来の `subscribers_employer_code` / `subscribers_department_code` を保持して比較する

---

## 2026年度受領データ投入時の方針

- 記号100本人以外は、既存の取り込みテンプレートに従ってそのまま staging へ投入する
- 記号100本人データは、Excel側で取り込みフォーマットのヘッダーおよび列順へ整形したうえで staging へ投入する
- この段階ではテンプレート拡張を行わず、受領データ側を既存テンプレートへ寄せる
- 2025年度補完用に投入済みの staging データは、2026年度受領データ投入前に扱いを明確化する
- 年度混在を避けるため、truncate または年度管理のどちらを採用するかを実装前に確定する

---

## 現時点の確認結果

### 確認済み

- `staging_subscribers_fund` は現時点ではテーブルのみ存在し、取り込み基盤としては未整備である
- sqlite 版の取り込み実装が存在し、テンプレートベース取り込みの思想自体は既存資産に存在する
- `templates` / `template_mappings` テーブルが存在し、`fund_id + version` 単位でテンプレートを管理している
- `template_mappings` では、1つのCSV列から複数の `target_column` を生成する実データが存在する
- 現行DDLとの乖離が大きく、実データも存在しないため、DROP + CREATE 前提で再作成する方針とする
- 現行DDLカラムの棚卸し方針（維持 / rename / 追加 / 削除）は `05c` に分離済み
- 2026年度受領データの差分判定方針は `05d` に分離済み

### 未実施

- 分割後の各specをもとに新DDLへ落とし込む
- 会社 Ubuntu 環境側の現行DDLと最終突合を行う

---

## 関連 spec

- `01_overview.md`
- `02_operation_steps.md`
- `03_comparison_policy.md`
- `04_dashboard_year_end_status.md`
- `06_subscriber_enrichment.md`
- `07_import_staging_subscribers_fund.md`
- `10_staging_subscribers_fund_apply.md`