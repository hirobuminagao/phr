

# 05b_staging_subscribers_fund_column_policy

## 目的

本ファイルは、`staging_subscribers_fund` におけるカラム設計方針を整理するための spec である。

親 spec:
- `05_staging_subscribers_fund.md`

本ファイルでは以下を扱う。

- raw / norm / match の設計
- identity の扱い
- 本人 / 本人以外判別
- 会社情報
- staging 固有カラム
- 差分判定用カラム（2026年度対応）

---

## raw / norm / match の考え方

### raw

本テーブルは raw データの保持を主目的としない。

- 受領原本は CSV / archive / 実行ログで担保する
- staging では norm 格納を基本とする
- raw は必要最小限のみ保持する

保持対象:

- `src_file`
- `src_row_no`
- `src_line_no`

---

### norm

`*_norm` は本テーブルの主値とする。

- 登録
- 更新
- 比較

すべての基準値として扱う。

例:

- `insurer_number_norm`
- `insurance_symbol_norm`
- `insurance_number_norm`
- `name_kana_full_norm`
- `name_kanji_full_norm`
- `birth_norm`
- `gender_code_norm`

---

### match

`*_match` は照合・比較のための補助値とする。

例:

- `insurance_symbol_match`
- `insurance_number_match`
- `name_kana_full_match`
- `name_kanji_full_match`

設計方針:

- match は比較判定専用
- norm とは別目的
- 共通ライブラリで生成する

---

## identity の扱い

以下を staging 取り込み時点で生成する。

- `person_id_custom`
- `identity_hash`

### identity 構成要素

- `insurer_number_norm`
- `insurance_symbol_norm`
- `insurance_number_norm`
- `birth_norm`
- `name_kana_full_match`
- `gender_code_norm`

### 方針

- 欠損がある場合は生成しない
- 生成不可の場合は NULL
- subscribers 照合は identity_hash で行う

---

## 本人 / 本人以外判別

identity とは別軸で扱う。

### 使用カラム

- `relationship_code_norm`
- `relationship_name_norm`

### 方針

- コード優先
- 名称補助
- identity には含めない

---

## 会社情報の扱い

受領CSV由来の会社情報は staging で保持する。

保持カラム:

- `received_company_code_norm`
- `received_company_name_norm`

方針:

- HIA側会社情報とは別概念
- マッピングして利用する前提
- HIA側の会社・部署マスタは別テーブルで保持する（HIAの事実をそのまま保持）
- 健保別の読み替え・対応付けは別のマッピングテーブルで管理する（HIAマスタ本体に混ぜない）

### HIA登録コード比較用カラム

2026年度受領データと現行 `subscribers` の事業所・部署登録状態を比較するため、staging 上に以下を保持する。

受領データをHIA側コードへマッピングした結果:

- `mapped_employer_code`
- `mapped_department_code`

現行 `subscribers` から取得した値（比較用キャッシュ）:

- `subscribers_employer_code`
- `subscribers_department_code`

命名方針:

- `subscribers_*` は `subscribers` テーブル由来の現行登録値であることを示す
- カラム名は `subscribers` 側の項目名に合わせる（`subscribers_employer_code` / `subscribers_department_code`）
- 比較は `mapped_*` と `subscribers_*` の差分として行う

設計意図:

- 受領CSVの事業所コード・部署コードをそのまま `subscribers` と比較しない
- 健保別マッピングによりHIA側コードへ変換したうえで、現行 `subscribers` の登録値と比較する
- 健保固有ルール（例: LEFT 3桁での対応付け）はHIAマスタ本体ではなく、マッピングテーブルまたは処理に閉じ込める
 
---

## staging 固有カラム

以下は staging 運用上の補助カラム。

- `src_file`
- `src_row_no`
- `src_line_no`
- `import_run_id`
- `loaded_at`
- `matched_subscriber_id`
- `connect_id_norm`

---

## 差分判定用カラム（2026年度対応）

2026年度の差分比較のため、以下のカラムを追加する。

### 追加カラム

- `diff_status`
- `diff_status_method`
- `diff_status_reason`

### diff_status

比較結果を保持する。

想定値:

- `new`
- `transfer`
- `existing`
- `unknown`

### diff_status_method

判定手段を保持する。

- `script`
- `manual`

### diff_status_reason

判定理由を保持する。

例:

- identity_hash not found
- qualification_acquired_date 判定
- manual override

---

## 設計のポイント

- staging は最終判定テーブルではない
- 一時判定を保持する
- 判定と判定手段を分離する
- 判定理由を残すことで追跡可能にする

---

## 一文まとめ

> staging は「正規化された入力」と「比較のための補助情報」を持つ作業基盤である