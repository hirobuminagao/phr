# 05d_staging_subscribers_fund_2026_diff_policy

## 目的

本ファイルは、2026年度受領データと2025年度固定済み基準との**差分判定方針**を整理するための spec である。

親 spec:
- `05_staging_subscribers_fund.md`

関連:
- `01_overview.md`（基準面の考え方）
- `02_operation_steps.md`（実行フロー）
- `05b_staging_subscribers_fund_column_policy.md`（diffカラム仕様）

---

## 用語

- **2025基準面**: `hia_dashboard_year_end_status` を起点に、当時点の `subscribers` 補完情報を含めた固定状態
- **2026受領データ**: 健保から受領した加入者CSV（記号100本人は整形後に含める）
- **staging**: `staging_subscribers_fund`

---

## 比較の前提

- 比較は「2025固定」 vs 「2026受領」の**時点差分**で行う
- `subscribers` は比較時点の参照として使用するが、基準面は**更新されない**
- identity は `identity_hash` を主キーとする

---

## 判定で使う主な材料

- `identity_hash`（存在確認の第一キー）
- `person_id_custom`（補助）
- `insurance_symbol_match` + `insurance_number_match`（補助）
- `name_kana_full_match` / `gender_code_norm`（補助）
- `qualification_acquired_date_norm`（新規候補判定）
- `mapped_employer_code` / `mapped_department_code`（受領データをHIA側コードへ変換した値）
- `subscribers_employer_code` / `subscribers_department_code`（現行 `subscribers` の登録値）

---

## 会社・部署コード比較の前提

2026年度受領データに含まれる事業所コード・部署コードは、HIA側の登録コード体系とそのまま一致するとは限らない。

そのため、比較は以下の順で行う。

1. HIA側の会社・部署マスタを、そのままマスタテーブルとして保持する
2. 健保別の読み替え・対応付けは別のマッピングテーブルで管理する
3. 受領データの事業所・部署情報を、健保別マッピングによりHIA側コードへ変換する
4. 変換後の `mapped_employer_code` / `mapped_department_code` と、現行 `subscribers` 由来の `subscribers_employer_code` / `subscribers_department_code` を比較する

方針:

- 受領CSVのコードを直接 `subscribers` と比較しない
- HIAマスタ本体には健保固有の読み替えルールを入れない
- 健保固有ルール（例: 06139463のLEFT 3桁ルール）はマッピングテーブルまたはマッピング処理に閉じ込める
- staging には、マッピング結果と `subscribers` 由来の現行値を並べて保持する
- mapping テーブル上では、受領側照合元は `source_target_columns` / `source_match_rule`、HIA会社マスタ側照合元は `company_lookup_columns` / `company_lookup_rule` として定義する

### HIAマスタ照合時の基本手順

HIA会社部署マスタ（`hia_company_master`）との照合は、以下の順で行う。

1. 対応する部署コードが判明している場合
   - HIA企業コード（`employer_code`）とHIA部署コード（`department_code`）の組み合わせで紐づける
   - staging には `mapped_employer_code` / `mapped_department_code` を保持する

2. 部署コードが `NULL` または不明な場合
   - HIA企業コード（`employer_code`）のみを紐づける
   - staging には `mapped_employer_code` を保持し、`mapped_department_code` は `NULL` とする

3. 企業コードの紐づけ方法
   - 健保別の受領データ仕様に依存するため、別途マッピングルールで定義する
   - HIAマスタ本体には企業コード読み替えルールを持たせない

この手順により、HIAマスタの保持粒度（企業単位 / 企業＋部署単位）を加工せず、そのまま照合に利用する。

---

## 複数カラムによる照合キー生成（match_key）

受領データおよびHIAマスタにおいて、単一カラムではなく複数カラムの組み合わせで照合キー（match_key）を生成する必要があるケースに対応する。

### 基本方針

- 照合キーは単一カラムに限定せず、複数カラムから生成可能とする
- 照合キーの生成は `target_column(s)` と `match_rule` により定義する
- 照合キー（match_key）と、マッピング結果（HIAコード）は分離して扱う
- マッピングは HIA会社マスタ参照型（`lookup_company_master`）と固定値返却型（`fixed`）の2種類をサポートする

### 対応パターン

以下の照合キー生成およびマッピングパターンをサポートする。

1. 単一カラム
   - 例: `LEFT(received_company_code_norm, 3)`

2. 複数カラム連結
   - 例: 企業名 + 部署名
   - 実装例: `concat_with_pipe(received_company_name_norm, received_department_name_norm)`
   - 例: `トランスコスモス株式会社|社長室`

3. 固定値照合
   - 例: `insurance_symbol_norm = 100`

4. HIAマスタ参照型マッピング（`lookup_company_master`）
   - staging 側で生成した照合キーと、HIA会社マスタ側で生成した照合キーを突合する
   - 一致した `hia_company_master` の `employer_code` / `department_code` を採用する

5. 固定マッピング（`fixed`）
   - staging 側の条件一致のみで、固定の `employer_code` / `department_code` を返却する

### ルール定義例

- `source_target_columns = received_company_name_norm,received_department_name_norm`
- `source_match_rule = concat_with_pipe`

または

- `source_target_columns = received_company_code_norm`
- `source_match_rule = left3`

HIA会社マスタ参照型の場合:

- `company_lookup_columns = department_name`
- `company_lookup_rule = left3_before_colon`

固定マッピングの場合:

- `mapping_type = fixed`
- `fixed_employer_code = 103`
- `fixed_department_code = NULL`

### 設計意図

- 健保ごとの受領フォーマット差異を吸収する
- 企業名＋部署名など、人間可読な情報をキーとして利用可能にする
- 文字列連結時の曖昧性を避けるため、区切り文字（例: `|`）を明示的に使用する

### 補足

- 照合キーは staging 内で生成し、マッピング処理に使用する
- 必要に応じて `*_match_value` として保持することも可能（デバッグ・手動補正用途）
- mapping テーブルでは `mapping_type` により、`lookup_company_master` / `fixed` を切り替える
- `lookup_company_master` の場合、mapped 値は HIA会社マスタから取得する
- `fixed` の場合のみ、mapping テーブルに固定の `fixed_employer_code` / `fixed_department_code` を直接保持する

---

## 一次分類（初期ラベリング）

staging 取り込み後、以下の4分類を付与する。

- `no_change`
- `update`
- `missing_from_new`
- `new_in_file`

### 判定ロジック（概念）

- 2025基準面に存在し、2026にも同一 `identity_hash` が存在
  - 主要項目が同一 → `no_change`
  - 主要項目に差分あり → `update`
- 2025基準面に存在し、2026に存在しない → `missing_from_new`
- 2025基準面に存在せず、2026に存在 → `new_in_file`

---

## 補助判定（差分の意味付け）

一次分類に加えて、以下の補助判定を行い、`diff_status` へ反映する。

### 新規候補（new）

以下を満たす場合に `new` 候補とする。

- `identity_hash` が基準面に存在しない
- かつ `qualification_acquired_date_norm` が、
  - 現在の `subscribers` における当該保険者の**最新資格取得日以上**

※ 最終確定ではなく候補判定

---

### 転籍候補（transfer）

以下のいずれかで候補とする。

- `person_id_custom` が一致するが `identity_hash` が変化
- HIA側へマッピング後の会社・部署コード（`mapped_employer_code` / `mapped_department_code`）と、現行 `subscribers` 由来の会社・部署コード（`subscribers_employer_code` / `subscribers_department_code`）に差分がある

---

### 既存（existing）

- `identity_hash` が一致し、主要項目差分が軽微
- もしくは差分があっても同一人物継続と判断できる

---

### 不明（unknown）

- 上記ルールで自動判定できない
- identity 欠損
- キー不整合

---

## major / minor 定義

差分判定においては、項目を以下の2系統に分ける。

### major（本人性・同一性に関わる項目）

以下は「同一人物判定」または「重要変更」に該当する。

- identity 系
  - `person_id_custom`
  - `identity_hash`
  - `insurance_symbol_match`
  - `insurance_number_match`
  - `name_kana_full_match`
  - `gender_code_norm`
  - `birth_norm`

- 氏名系（parts含む）
  - `name_kana_full_norm`
  - `name_kana_family_norm`
  - `name_kana_middle_norm`
  - `name_kana_given_norm`
  - `name_kana_full_match`
  - `name_kanji_full_norm`
  - `name_kanji_family_norm`
  - `name_kanji_middle_norm`
  - `name_kanji_given_norm`
  - `name_kanji_full_match`

※ 氏名（特に漢字）は表記揺れが発生するため、自動更新には使用せず「確認対象」として扱う

---

### minor（登録情報更新項目）

以下は「同一人物前提で更新してよい情報」とする。

- 資格・続柄
  - `relationship_name`
  - `qualification_acquired_date`
  - `qualification_lost_date`

- 住所・連絡先（root minor更新対象からは除外）
  - `postal_code`
  - `address_line`
  - `building`
  - `phone`
  - `email`

※ 住所・連絡先は `subscribers` 本体の minor 更新対象ではなく、履歴型テーブル側の compare / apply 対象とする。

- 会社・部署・外部ID
  - `employer_code`
  - `department_code`
  - `distribution_code`
  - `employee_code`
  - `connect_id`

### 住所・連絡先の扱い（HUB apply構造への追従）

HUB側では、住所・連絡先を `subscribers` 本体の更新項目として扱わず、以下の履歴型テーブルを正本として扱う。

```text
住所
  → subscriber_addresses

連絡先
  → subscriber_contact_points
```

したがって fund側の差分ファイルチェックでも、住所・連絡先は identity / root minor 更新とは分離する。

本specでは、住所・連絡先を `subscribers` root更新から分離する方針までを定義する。
具体的な compare / apply 手順は未確定であり、別途設計する。

現時点の対象範囲:

住所については、HUB側では以下の考え方を採用している。

```text
address_hash 一致 = 同一住所値の存在
```

ただし、fund側で同じ比較手順を採用するかは未確定とする。

連絡先についても、HUB側では `contact_type` ごとに current / history を管理する。

現時点では、住所・連絡先の変更を `subscribers_audit` の必須対象とはしない方針に寄せる。
ただし、fund側での詳細な履歴管理・比較・反映手順は別途設計する。

---

## 差分判定フロー（確定版）

差分判定は以下の順で行う。

1. identity 判定
   - `identity_hash` で `subscribers` を検索
   - 一致あり → 同一人物として扱う
   - 一致なし → major_candidate 探索へ

2. 同一人物の場合（identity一致）
   - major項目は基本的に一致している前提
   - root minor項目のみ比較する
     - 差分なし → `no_change`
     - 差分あり → `update`（subscribers root minor更新）
   - 住所・連絡先は root minorとは別扱いとし、具体的な差分判定・反映手順は別途設計する

※ identity一致後に major差分が出るケースは基本的に想定外（データ不整合として扱う）

3. identity不一致の場合
   - major_candidate 判定を実施
   - 該当する場合は `major_candidate` として扱う
   - 該当しない場合は `add`

---

## major_candidate 判定

identity_hash が一致しない場合でも、以下の条件で「同一人物候補」として扱う。

### 目的

- 転籍（保険証記号・番号変更）
- 名字変更（氏名変更）

を検出する。

---

### パターン定義

#### 1. 転籍候補（transfer）

条件:

- `name_kana_full_match`
- `birth_norm`
- `gender_code_norm`

が一致

かつ

- `insurance_symbol_match` または `insurance_number_match` が不一致

出力:

- `major_candidate_transfer`

---

#### 2. 名字変更候補（family name change）

共通前提:

- `insurance_symbol_match`
- `insurance_number_match`
- `birth_norm`
- `gender_code_norm`

が一致

##### (A) カナgiven一致

- `name_kana_given_norm` 一致

→ `major_candidate_family_name_change_kana_given`

##### (B) 漢字given一致

- `name_kanji_given_norm` 一致

→ `major_candidate_family_name_change_kanji_given`

##### (C) 保険情報のみ一致

- 氏名は一致しないが、保険情報 + 生年月日 + 性別一致

→ `major_candidate_family_name_change_insurance_only`

---

### 判定優先順位

1. transfer
2. family_name_change_kana_given
3. family_name_change_kanji_given
4. family_name_change_insurance_only

---

### 出力方針

- major_candidate は **HIA登録CSVには出力しない**
- 別途ログCSVとして出力する
- `major_candidate_pattern` を必ず付与する

---

### missing との関係

`subscribers に存在し、staging に存在しないレコード`について、

- major_candidate と一致する場合
  - missing として扱うが
  - 対応する `major_candidate_pattern` を付与する

これにより、

- 単純な消失
- 転籍／名字変更による見かけ上の消失

を区別可能とする。

---

## diff カラムへの反映

staging に以下を記録する。

- `diff_status`
  - `new` / `transfer` / `existing` / `unknown`
- `diff_status_method`
  - `script`（自動判定）
  - `manual`（手動補正）
- `diff_status_reason`
  - 判定根拠（文字列）

例:

- `identity_hash not found`
- `acquired_date >= current_max`
- `person_id_custom matched but identity changed`
- `manual override`

---

## 記号100本人の扱い

- データは受領済み（Excel等）
- staging 投入前に**既存テンプレート形式へ整形**
- 本比較では他レコードと同等に扱う

制約:

- 元フォーマット差による正規化揺れに注意

---

## 注意点

- 本判定は**最終確定ではない**（staging上の一時判定）
- `subscribers` への反映ロジックとは分離する
- 誤判定は `diff_status_method=manual` で上書き可能にする

---

## 一文まとめ

> 2026差分判定は、2025固定基準に対する identity 主体の比較により初期分類を行い、取得日・補助キーで意味付けを行う二段階判定とする。住所・連絡先は subscribers root 更新から分離し、具体的な compare / apply 手順は別途設計する。