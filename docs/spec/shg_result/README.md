# shg_result 入力基盤と結果XMLチェックの前提整理

本ファイルは、shg_result を用いた特定保健指導結果チェック処理の再開アンカー兼、最小構成の設計メモである。
本領域を再開する際は、本ファイルを前提として扱う。

---

## 1. 目的

特定保健指導結果XMLと、健診時データ（腹囲・体重）および利用券情報を突合し、
整合性チェックを行ったうえでCSV出力する基盤を構築する。

---

## 2. 今回の到達点（v1.1.0前段）

以下の情報を `work_other.shg_result` に保持できる状態を、入力基盤の完成とする。

- identity 生成に必要な raw 項目
- 年度
- 利用券番号
- 利用券有効期限
- 健診実施日
- 腹囲
- 体重

また、既存データに対して以下を再構築する。

- person_id_custom
- identity_hash

---

## 3. 処理全体像

本処理は以下の3つのフェーズで構成する。

### 3.1 入力（手動投入）

- Navicat 等を用いて shg_result に必要項目を直接投入する
- 本フェーズでは取り込みスクリプトは作成しない

### 3.2 identity 生成（スクリプト）

- shg_result の raw データから以下を生成
  - person_id_custom
  - identity_hash
- update / backfill スクリプトとして実装する

### 3.3 XMLチェック + CSV出力

- HIA から取得した ZIP ファイルを入力とする
- `DATA/` 配下の XML を対象に内容をチェックする
- XML から利用券情報・最終評価情報・最終腹囲/体重・継続支援集計等を取得する
- shg_result に保持した健診時情報・利用券情報と突合する
- アウトカム矛盾および利用券情報整合性を判定する
- 整合性チェック結果をCSVとして出力する

※本処理は既存XMLチェックスクリプトの改修で対応する

---

## 4. DB前提（work_other.shg_result）

- 既存テーブルを拡張して利用する
- 既存データは削除せず、update により整合性を担保する
- 主キーや粒度の変更は行わない（v1.1.0で再設計予定）

---

## 5. ZIP入力仕様

入力は解凍済みではなく、ZIPをそのまま扱う。

### 5.1 対象

- `DATA/` 配下の `.xml` のみ対象
- XSD・補助XML（ix08等）は対象外

### 5.2 エラーポリシー

#### XML単位

- 個別XMLでエラーが発生しても、他XMLの処理は継続する

#### ZIP単位

以下の場合はZIP単位でスキップする。

- ZIPファイルが破損している
- `DATA/` ディレクトリが存在しない
- `DATA/` 配下に対象XMLが存在しない

ZIP単位のエラーはログとして記録する。

---

## 6. データ配置

```text
phr/data/hia_export/shg_result/
  ├ input_zip/<insurer_number>/
  ├ output_csv/<insurer_number>/
  └ logs/
```

## 6.1 共通設定ファイル

SHG結果XMLチェック処理で使用する outcome 集計設定は、以下のファイルを共通設定として利用する。

```text
resources/shg/outcome_process_config.json
```

本設定は従来 `scripts/work_folder/mat/outcome_process_config.json` に存在していたが、
v1.1.0 以降は `resources/shg/` 配下へ配置し、SHG系処理から参照する。

現時点では DB 管理は行わず、ファイルベースの共通設定として扱う。

現時点で本設定ファイルが担う役割は以下の通り。

- `90070` セクションの継続支援集計 XPath 定義
- 支援方式コード表示用の補助辞書（`mode_map_24010`）
- 継続期間判定ポリシー（`duration_policy`）

本設定は SHG 結果XMLチェック処理の共通設定として扱う。
DB 化は将来の検討事項とし、本フェーズではファイル参照を正とする。

## 6.2 OID / コード参照の扱い

既存の元スクリプトでは、`scripts/work_folder/mat/oid_code_master.csv` を参照して `(codeSystem, code) -> 表示名` の辞書を生成し、一部のXMLコード値の表示名引きに使用している。

ただし、OID / コード参照の正本は CSV ではなく、以下のDBテーブルを基準として整理する方針とする。

- `dev_phr.exam_item_master`
  - 健診項目マスタ
- `dev_phr.norm_variants`
  - 揺れ・別名・コード対応を含む拡張テーブル

本フェーズでは元スクリプトの OID 利用箇所の修正は行わず、現行用途の把握に留める。
CSV 依存の整理および DB 参照への置換は後続タスクとする。

## 6.3 元スクリプト解析メモ（現時点）

既存の元スクリプト解析により、現時点で以下を確認した。

### 6.3.1 `outcome_process_config.json` の実使用範囲

元スクリプトでは `outcome_process_config.json` を実際に参照しており、主な役割は以下の通り。

- `90070` セクションの継続支援集計 XPath 定義
  - 回数 (`counts`)
  - 時間 (`durations_min`)
  - 点数 (`points`)
- 継続期間判定ポリシー (`duration_policy`)
- 支援方式コード表示用の補助辞書 (`mode_map_24010`)

この設定は単なる候補ではなく、既存処理で実使用されている。

### 6.3.2 `oid_code_master.csv` の現行用途

元スクリプトでは `scripts/work_folder/mat/oid_code_master.csv` を読み込み、 `(codeSystem, code) -> 表示名` の辞書を生成している。
現時点で確認できた主用途は、一部XMLコード値の表示名引きである。

少なくとも現時点の解析では、OID CSV は以下の用途には直接使っていない。

- 利用券番号 / 利用券有効期限の取得
- 最終腹囲 / 最終体重の取得
- `90070` 継続支援集計
- `person_id_custom` 生成

### 6.3.3 `custom_id` 系の扱い

元スクリプトでは `custom_id_config.json` と外部 `custom_id_gen.py` を使って `person_id_custom` を生成している。
ただし本フェーズでは、`person_id_custom` 生成は既存外部スクリプトを正とせず、v1.1.0 の identity lib へ置換する前提とする。

### 6.3.4 現時点の置換方針

- `outcome_process_config.json`
  - `resources/shg/outcome_process_config.json` を共通設定として利用する
- `oid_code_master.csv`
  - 現行用途の把握に留め、修正は後続タスクとする
- `custom_id` 系
  - 新しい identity lib に置換する

---

## 7. 本フェーズで扱わない範囲

以下は本フェーズでは対象外とする。

- event モデルの正式テーブル化
- 入力処理の完全自動化
- ZIPのアーカイブ／移動管理
- 全体最適な運用設計

まずは「動く最小構成」の確立を優先する。

---

## 8. 再開時の確認ポイント

再開時は以下を必ず確認する。

1. 本ファイル（本README）
2. v1.1.0_resume_anchor.md
3. identity spec 一式
4. 既存XMLチェック用スクリプト
5. `resources/shg/outcome_process_config.json`
6. 元スクリプトにおける `oid_code_master.csv` の利用箇所

---

## 9. 要点

- shg_result を基準テーブルとする
- identity を再構築し、突合可能な状態を作る
- 健診データ（腹囲・体重）と結果XMLを照合する
- 整合性チェック結果をCSVとして出力する

本フェーズは「突合できる状態を作ること」までをゴールとする。