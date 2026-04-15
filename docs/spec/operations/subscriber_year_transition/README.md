# subscriber_year_transition

## 目的

本ディレクトリは、加入者の年度更新運用に関する業務手順・比較方針・年度末状態管理方針を整理するための運用仕様を置く。

本仕様は、画面操作や個別スクリプトの使い方そのものを詳細に説明するものではなく、年度更新運用の中で

- 何を基準面として扱うか
- どの順番で何を実施するか
- どの時点でどの比較・判定を行うか
- 各ステップでどの既存システム仕様を利用するか

を明確にすることを目的とする。

## 背景

2026年度の加入者更新では、単純に新しい受領ファイルを既存 `subscribers` に突き合わせるだけでは不十分である。

主な理由は以下の通り。

- `subscribers` は単なる受領原本ではなく、2025年度運用後の最終状態を含む
- 2025年度運用中にキー関連情報が変更された対象者が存在する
- HIA保持値と受領原本では、氏名カナ等のフォーマット差異がある
- 2025年度末のダッシュボード状態を翌年度比較用に軽量に保持する必要がある
- 新規加入・資格喪失・転籍候補を単純比較だけで確定すると誤判定が起きうる

そのため、本ディレクトリでは「年度比較運用」という観点で、比較前補完、一次分類、二次判定、年度末記帳の流れを扱う。

## スコープ

本ディレクトリで扱う内容は以下とする。

- 2025年度最終状態を基準面とした 2026年度加入者更新運用
- 比較前補完の考え方
- 比較結果の一次分類方針
- 転籍候補等の二次判定方針
- `hia_dashboard_year_end_status` による年度末状態管理方針
- 各運用ステップと既存 system spec との対応関係

## 非スコープ

本ディレクトリでは、以下は主目的として扱わない。

- 個別スクリプトの詳細な入出力仕様
- 正規化関数そのものの詳細仕様
- `hia_export_subscribers_csv` / `hia_fund_dashboard_csv` / `identity_canonicalization` 各ディレクトリで既に定義済みの system spec の重複記載
- 記号100本人の年度更新運用の詳細手順（本運用では優先度を下げ、別途整理対象とする）

必要な system spec は各ステップから参照する。

## 前提

- `subscribers` は 2025年度運用後の最終状態であり、2026/03/31 時点スナップショット相当として扱う
- 初期対象は記号100本人以外とする
- 記号100本人は別管理要素があるため本運用では優先度を下げるが、事前準備およびデータ処理対象からは除外しない
- HIAダッシュボードの 2025年度「未予約」は翌年度比較では追跡優先度を下げる
- 未予約以外のステータスは、実績または進行状態として翌年度比較の判断材料に用いる

## 関連ADR

- `docs/adr/0019-subscriber-year-end-comparison-and-snapshot-policy.md`

## 関連 system spec

各運用ステップでは、以下の既存 system spec を参照して利用する。

### HIA加入者CSVの取り込み・反映

- `docs/spec/hia_export_subscribers_csv/README.md`
- `docs/spec/hia_export_subscribers_csv/flow_overview.md`
- `docs/spec/hia_export_subscribers_csv/import_phase.md`
- `docs/spec/hia_export_subscribers_csv/staging_schema.md`
- `docs/spec/hia_export_subscribers_csv/identity_policy.md`
- `docs/spec/hia_export_subscribers_csv/subscriber_apply.md`

### HIAダッシュボードCSVの取り込み・分析

- `docs/spec/hia_fund_dashboard_csv/README.md`
- `docs/spec/hia_fund_dashboard_csv/dashboard_person_year_join.md`
- `docs/spec/hia_fund_dashboard_csv/snapshot_policy.md`

### identity / 正規化

- `docs/spec/identity_canonicalization/README.md`
- `docs/spec/identity_canonicalization/identity_layer_structure.md`
- `docs/spec/identity_canonicalization/identity_layers_norm_and_purpose.md`
- `docs/spec/identity_canonicalization/v1.1.0_identity_layer_commonization.md`

## このディレクトリで今後整理する想定ファイル

- `02_operation_steps.md`
  - 年度更新運用のステップと system spec 対応表
- `03_comparison_policy.md`
  - `no_change` / `update` / `missing_from_new` / `new_in_file` の定義
- `04_dashboard_year_end_status.md`
  - `hia_dashboard_year_end_status` の目的、保持項目、記帳タイミング

必要に応じて、比較前補完や転籍候補判定を個別ファイルへ分割する。