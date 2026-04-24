

# 05a_staging_subscribers_fund_template_import

## 目的

本ファイルは、`staging_subscribers_fund` へのテンプレートベース取り込み方針を整理するための spec である。

親 spec:
- `05_staging_subscribers_fund.md`

本ファイルでは、以下を扱う。

- 健保ごとのテンプレート定義
- `templates` / `template_mappings` の位置づけ
- CSV配置・archive運用
- `rule` / `required` の責務
- 2026年度受領データ投入時の前提
- 記号100本人データの取り込み方針

---

## テンプレートベース取り込みの前提

`staging_subscribers_fund` への取り込みは、健保ごとのテンプレート定義に基づいて行う前提とする。

### 1. 基本方針

- 健保ごとに CSV マッピング用テンプレートを登録する
- テンプレート登録はマニュアルで行う
- 受領CSVは、テンプレート定義に従って `staging_subscribers_fund` の各カラムへマッピングして取り込む
- 取り込み対象CSVは、以下の配置構成に従う

```text
/data/from_fund/import_subscribers_staging/
  ├── input/<insurer_number>/*.csv
  └── archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/*.csv
```

- `input/<insurer_number>/` 配下に配置されたCSVを取り込み対象とする
- CSV自体がエラーの場合は `input/` に残す
- run が正常に走り、読み込み判定まで到達したファイルは `archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/` へ移動する
- `archive/` は実行単位でディレクトリを切り、人間が見て実行日時を判別できるよう日付を含める
- archive への移動処理は取り込みスクリプトへ組み込む
- アーカイブは再現性・監査用途のために保持する
- `archive/` の削除は自動化せず、手動運用とする

---

## 現在把握しているテンプレート関連テーブル

### `templates`

健保ごとのテンプレート定義のヘッダ情報を保持する。

現時点の把握内容:

- `fund_id` に紐づく
- `version` を持つ
- `template_type` を持つ
- `target_table` を持つ

### `template_mappings`

テンプレートごとのマッピング定義を保持する。

現時点の把握内容:

- `fund_id`
- `version`
- `col_order`
- `csv_header`
- `target_column`
- `rule`
- `required`（値必須フラグ）
- `notes`

---

## テンプレート版カラムの命名方針

- templates / template_mappings / staging_subscribers_fund におけるテンプレート版カラムは `version` に統一する
- `template_ver` という命名は最終DDLでは使用しない
- 既存DDL上 `template_ver` が存在する場合は、新DDLで `version` へ統一する

---

## 本 spec 上の位置づけ

今回の `staging_subscribers_fund` 設計では、テンプレートテーブルを前提として以下を整理対象に含める。

- `templates` / `template_mappings` を一次入力定義として扱うか
- `target_table=staging_subscribers_fund` のテンプレート運用をどう定義するか
- `rule` の責務を raw / norm / match 生成のどこまでに含めるか
- `required` はテンプレート単位の値必須フラグとして扱う

---

## rule の位置づけ

今回の設計では、`rule` は従来のような「汎用的な変換ロジック指定」ではなく、`target_column` の責務に従属する変換指定として再定義する。

### 基本定義

`rule` は、CSV入力値を `target_column` の責務に応じた値へ変換するための限定的な変換指定とする。

- `rule` は `target_column` の種別（norm / match / 補助列）に従属する
- `rule` は単一入力列に対する変換を対象とする
- 1つのCSV列から複数の `target_column` を生成することは許容する

### rule の責務範囲

#### 許容する範囲

- 単一列からの norm 生成
- 単一列からの match 生成
- 単一列からの派生値（分解・補助列）の生成

#### 許容しない範囲

- 複数列をまたぐ値の組み立て
- `person_id_custom` / `identity_hash` の生成
- `subscribers` との照合（`matched_*` 系）
- 業務判定（転籍・資格状態など）

これらはすべてスクリプト側の責務とする。

### rule の分類

#### norm 用 rule

`*_norm` カラムへ値を生成するためのルール。

例:

- `symbol_norm`
- `birth_norm`
- `gender_code_norm`
- `date_or_null`
- `digits_required`

記号に関しては、identity 生成に必要な数字成分を抽出できることを rule 側で担保する。たとえば `神-01` は許容し、数字成分を持たない `神` はエラーとする。

#### match 用 rule

`*_match` カラムへ値を生成するためのルール。

例:

- `insurance_symbol_match`
- `insurance_number_match`
- `name_kana_full_match`
- `name_kanji_full_match`
- `relationship_name_match`

※ match 系は比較用途であり、norm とは別目的で生成する。

#### 補助列用 rule

分解や人手確認補助のための列を生成するルール。

例:

- `symbol_digits`
- `split_family`
- `split_middle`
- `split_given`
- `split_family_kana`
- `split_given_kana`

---

## required の位置づけ

`required` は、CSV入力値に対する必須チェック（NULL / 空値不可）を表すフラグとする。

- `required` は値の存在を確認する
- 値の形式チェックや変換は `rule` の責務とする
- identity 生成に必要な形式要件は `rule` 側で担保する

たとえば記号については、`required=1` で値の存在を確認しつつ、`rule` により数字成分が抽出可能であることを要求する。

### required 適用方針（今回健保）

今回の健保テンプレートにおける `required` は、以下を適用方針として確定する。

#### required=1（値必須とする項目）

- `insurance_symbol_norm`
- `insurance_number_norm`
- `birth_norm`
- `gender_code_norm`
- `name_kana_full_norm`

これらは、加入者識別および比較の最低限成立に必要な項目として扱う。

#### 条件付きで required=1 を検討する項目

- `name_kanji_full_norm`
- `relationship_code_norm`

これらは以下の観点で運用に応じて必須化を検討する。

- `name_kanji_full_norm`: 目視確認および比較補助の重要性が高い場合
- `relationship_code_norm`: 本人 / 本人以外判別を安定させたい場合

#### required=0（任意項目）

上記以外の項目は、値が存在すれば利用するが、欠損を理由に取り込み全体をエラーとはしない。

#### 補足（システム側必須項目）

- `insurer_number_norm` は CSV 由来ではなく外部付与値であるため、テンプレートの `required` ではなくシステム側必須項目として扱う。

#### identity との関係

- `required` はテンプレート単位の値必須制御であり、identity 生成可否とは別概念とする
- identity 構成要素に該当する項目が欠損した場合は、`required` の設定に関わらず identity は生成せず `NULL` とする

---

## 2026年度受領データ投入時の追加前提

2026年度受領データは、以下の方針で `staging_subscribers_fund` へ投入する。

- 記号100本人以外は、既存の取り込みテンプレートに従ってそのまま staging へ投入する
- 記号100本人データは受領済みであるが、フォーマットが異なる
- 記号100本人データは、Excel側で取り込みフォーマットのヘッダーおよび列順へ整形したうえで staging へ投入する
- この段階ではテンプレート拡張を行わず、受領データ側を既存テンプレートへ寄せる

2025年度補完用に投入済みの staging データは、2026年度受領データ投入前に扱いを明確化する。

- 原則として staging は年度比較の作業基盤であり、年度混在を避ける
- 2026年度受領データ投入前に truncate または年度管理のどちらを採用するかを明確にする

---

## 現時点の確認結果

### 確認済み

- sqlite 版の取り込み実装が存在し、テンプレートベース取り込みの思想自体は既存資産に存在する
- `templates` / `template_mappings` テーブルが存在し、`fund_id + version` 単位でテンプレートを管理している
- `template_mappings` では、1つのCSV列から複数の `target_column` を生成する実データが存在する
- `required` はテンプレート単位の値必須フラグとして扱う方針とする
- `rule` は単一入力列に対する norm / match / 補助列生成までを担う方針とする
- 受領CSVの配置構成は `input/<insurer_number>/` と `archive/<run_id>_<yyyymmdd_hhmmss>/<insurer_number>/` を基本とする
- archive への移動処理は取り込みスクリプトへ組み込む方針とする
- `archive/` は自動削除せず、手動運用とする
- `template_ver` は最終DDLでは `version` に統一する方針で確定済みとする
- 記号100本人データは受領済みだがフォーマットが異なるため、既存取り込みフォーマットへヘッダー・列順を合わせて投入する方針とする

### 未実施

- 本 spec の内容をもとに新DDLへ落とし込む
- 会社 Ubuntu 環境側の現行DDLと最終突合を行う