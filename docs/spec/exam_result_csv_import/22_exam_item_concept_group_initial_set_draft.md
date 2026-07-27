# Exam Item Concept Group Initial Set Draft

## Status

Draft.

このドキュメントは、CSV健診結果テンプレート登録で `namecode` 候補を探しやすくするための「上位グループ」初期案を整理する。
少なくとも厚労省付属2由来の `identity_item_code` は網羅し、その上でCSV取込で迷いやすい項目を入力支援用のbundleとしてまとめる。
医学的な完全分類を手作業で作ることが目的ではない。

## Goal

- CSVテンプレート登録画面で、利用者が最初から全 `namecode` を検索しなくて済むようにする。
- 付属2由来の `identity_item_code` を候補探索単位として必ず網羅する。
- CSV登録でよく出る上位単位を、付属2identityを束ねる入力支援bundleとして用意する。
- 労安法チェック用 `LSIO_Legal_Item` とは共用しないが、初期seed候補の材料として参照する。
- 全検査項目を手作業で医学分類する運用にはしない。

## Basic Policy

- デフォルトは `SINGLE_NAMECODE` 直指定。
- 最小網羅単位として、`exam_item_master.identity_item_code` ごとに `ANNEX2_IDENTITY` グループを機械生成する。
- `ANNEX2_IDENTITY` 197件は `phr_master` に物理seedとして保存する。
- `exam_item_concept_groups` / `exam_item_concept_group_members` は正式テーブル化する。
- 入力支援bundleは、CSV上で表記が曖昧になりやすい項目を探しやすくするために使う。
- 入力支援bundleも初期セットとしてできるだけ決め切る。
- 上位グループ配下でも、実際の保存先は必ず `target_namecode` 単位で明示する。
- `identity_item_code` は候補探索キーであり、排他性の根拠にはしない。
- `selection_mode` はグループまたはruleの初期値として使う。

## Proposed Tables

実DDL確定前の概念案。

### exam_item_concept_groups

| column | meaning |
|---|---|
| `concept_group_code` | 上位グループコード |
| `concept_group_name` | 画面表示名 |
| `concept_group_kind` | `ANNEX2_IDENTITY` / `INPUT_BUNDLE` |
| `parent_concept_group_code` | 親bundle。親なしの場合はNULL |
| `concept_group_depth` | 0=root, 1=child |
| `concept_group_category` | `BASIC`, `URINE`, `BLOOD`, `BIOCHEMISTRY`, `IMAGING`, `PHYSIOLOGY`, `QUESTIONNAIRE`, `DOCTOR` など |
| `default_selection_mode` | `DIRECT` / `EXCLUSIVE_ONE` / `MULTI_ENTRY` |
| `coverage_required` | 付属2網羅対象なら1 |
| `source_group_code` | seed材料にした制度グループ。例: `LSIO_Legal_Item` |
| `note` | 備考 |
| `sort_no` | 表示順 |
| `is_active` | 有効フラグ |

### exam_item_concept_group_members

| column | meaning |
|---|---|
| `concept_group_code` | 上位グループコード |
| `identity_item_code` | 候補探索用の同一性項目コード |
| `namecode` | 候補namecode。NULLならidentity配下を候補生成する |
| `member_role` | `RESULT_VALUE` / `PRESENCE_KEY` / `AUX` |
| `selection_hint` | `EXCLUSIVE_ONE` / `MULTI_ENTRY` / `DIRECT` |
| `priority` | 候補表示順 |
| `note` | 備考 |

## Coverage Layers

初期セットは2層に分ける。

### Layer 1: Annex2 Identity Coverage

付属2由来の `identity_item_code` は、`exam_item_master` を正として全件機械生成する。
現時点の `sql/export_sql/exam_item_master.sql` では以下を確認している。

| source | count |
|---|---:|
| distinct `identity_item_code` | 197 |
| `namecode` rows with `identity_item_code` | 322 |

生成ルール:

- `concept_group_kind = ANNEX2_IDENTITY`
- `concept_group_code = ANNEX2_<identity_item_code>`
- `concept_group_name = exam_item_master.identity_item_name`
- `concept_group_category = exam_item_master.kubun_name` を元に分類する
- `coverage_required = 1`
- `default_selection_mode = MULTI_ENTRY`
- memberは同じ `identity_item_code` 配下の全 `namecode`

この層は、付属2項目の候補表示漏れを防ぐためのベースラインである。
CSV画面で「尿蛋白」「血糖」「眼底検査」などを選べるようにする最小単位であり、入力を楽にするbundleとは分ける。

### Layer 2: Input Support Bundles

入力支援bundleは、CSVテンプレート登録で迷いやすいものを優先する。
これは付属2網羅の代替ではなく、複数の `ANNEX2_IDENTITY` を束ねて候補を探しやすくするための追加レイヤーである。
採用案は、画面では大きいbundleを見せ、内部では小さい意味単位の子bundleに分ける階層型とする。
階層が増える分メンテナンス対象は増えるが、CSV登録時の人の目と細かい設定のしやすさを優先する。

| concept_group_code | parent | name | category | default_selection_mode | initial identity candidates | reason |
|---|---|---|---|---|---|---|
| `BODY_MEASURE` | `NULL` | 身体計測 | `BASIC` | `MULTI_ENTRY` | `9N001`, `9N006`, `9N016` | 身長・体重・腹囲はCSVでまとまって出やすいが、値は別entry |
| `BLOOD_PRESSURE` | `NULL` | 血圧 | `PHYSIOLOGY` | `MULTI_ENTRY` | `9A750`, `9A760` | 収縮期/拡張期、1回目/2回目など複数entryが自然 |
| `VISION` | `NULL` | 視力 | `PHYSIOLOGY` | `MULTI_ENTRY` | `9E160` | 右/左、裸眼/矯正など複数entryが自然 |
| `HEARING` | `NULL` | 聴力 | `PHYSIOLOGY` | `MULTI_ENTRY` | `9D100` | 右/左、1000Hz/4000Hz、方法・所見が混在する |
| `URINE_BASIC` | `NULL` | 尿一般 | `URINE` | `MULTI_ENTRY` | `1A010`, `1A020`, `1A030`, `1A100`, `1A105` | 尿蛋白・尿糖・尿潜血などが同じCSV区画に出やすい |
| `CBC` | `NULL` | 血算 | `BLOOD` | `MULTI_ENTRY` | `2A010`, `2A020`, `2A030`, `2A040`, `2A050`, `2A060`, `2A070`, `2A080` | 白血球・赤血球・Hb・Htなど同一ブロックで出やすい |
| `LIVER_FUNCTION` | `NULL` | 肝機能 | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3B035`, `3B045`, `3B090` | AST/ALT/γ-GTは同一ブロックで出やすい |
| `GLUCOSE_RELATED` | `NULL` | 血糖関連 | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3D010`, `3D046` | 画面上は近くに置くが、内部では血糖とHbA1cを分ける |
| `GLUCOSE` | `GLUCOSE_RELATED` | 血糖 | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3D010` | 空腹時/随時や方法差はrule条件で扱う |
| `HBA1C` | `GLUCOSE_RELATED` | HbA1c | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3D046` | 血糖とは別identityとして扱う |
| `LIPID_RELATED` | `NULL` | 脂質関連 | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3F015`, `3F050`, `3F069`, `3F070`, `3F077` | 画面上は脂質ブロックとして探せるようにする |
| `TRIGLYCERIDE` | `LIPID_RELATED` | 中性脂肪 | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3F015` | 空腹時/随時や方法差はrule条件で扱う |
| `HDL_CHOLESTEROL` | `LIPID_RELATED` | HDLコレステロール | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3F070` | 脂質配下の独立identity |
| `LDL_CHOLESTEROL` | `LIPID_RELATED` | LDLコレステロール | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3F077` | 直接法/計算法はrule条件で扱う |
| `NON_HDL_CHOLESTEROL` | `LIPID_RELATED` | non-HDLコレステロール | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3F069` | 脂質配下の独立identity |
| `TOTAL_CHOLESTEROL` | `LIPID_RELATED` | 総コレステロール | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3F050` | 帳票差分に備えて脂質配下に置く |
| `RENAL_RELATED` | `NULL` | 腎機能関連 | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3C015`, `8A065`, `3C020`, `3A015` | 検体別ではなく意味別に近くへ置く |
| `CREATININE` | `RENAL_RELATED` | 血清クレアチニン | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3C015` | 実測の血清クレアチニン |
| `EGFR` | `RENAL_RELATED` | eGFR | `BIOCHEMISTRY` | `MULTI_ENTRY` | `8A065` | identity名は血清クレアチニンだが、項目実体はeGFR |
| `URIC_ACID` | `RENAL_RELATED` | 血清尿酸 | `BIOCHEMISTRY` | `MULTI_ENTRY` | `3C020` | 腎機能関連に含める |
| `URINE_ALBUMIN` | `RENAL_RELATED` | 尿中アルブミン | `URINE` | `MULTI_ENTRY` | `3A015` | 腎機能関連に含める |
| `CHEST_XRAY` | `NULL` | 胸部X線 | `IMAGING` | `MULTI_ENTRY` | `9N206`, `9N221` | 所見有無、所見テキスト、撮影情報などが混在する |
| `ECG` | `NULL` | 心電図 | `PHYSIOLOGY` | `MULTI_ENTRY` | `9A110` | 所見有無、所見テキスト、対象者、実施理由が混在する |
| `HISTORY_SYMPTOM` | `NULL` | 既往歴・症状 | `QUESTIONNAIRE` | `MULTI_ENTRY` | `9N051`, `9N056`, `9N061`, `9N066`, `9N071` | コード値と自由記載が混在する |
| `DOCTOR_JUDGEMENT` | `NULL` | 医師判定・意見 | `DOCTOR` | `MULTI_ENTRY` | `9N511`, `9N512`, `9N516`, `9N521`, `9N526` | 判定、未実施理由、医師名、意見が別entry |

## Selection Mode Notes

`ANNEX2_IDENTITY` と入力支援bundleの多くは `MULTI_ENTRY` を既定にする。
これは「血糖グループから1つだけ選ぶ」という意味ではなく、CSV登録時に候補を探しやすくするためである。

`EXCLUSIVE_ONE` は、同じCSV値を検査方法条件によりどれか1つの `namecode` へ割り当てる場合だけ使う。

例:

- 空腹時血糖の方法違いnamecodeから1つを選ぶ場合は `EXCLUSIVE_ONE`。
- 血糖値とHbA1cを同じCSVから両方取り込む場合は `MULTI_ENTRY`。
- LDL直接法とLDL計算法のどちらかを、CSVの方式列で選ぶ場合は `EXCLUSIVE_ONE`。
- HDL、LDL、中性脂肪を同じ脂質グループから複数取り込む場合は `MULTI_ENTRY`。

## Seed Generation Approach

初期seedは以下の順で作る。

1. `exam_item_master` から `identity_item_code`, `identity_item_name`, `category_name`, `xml_value_type`, `display_unit`, `xml_method_code` を取得する。
2. distinct `identity_item_code` ごとに `ANNEX2_IDENTITY` グループを生成する。
3. 同じ `identity_item_code` 配下の全 `namecode` を `exam_item_concept_group_members` に展開する。
4. `exam_item_group_members` があるものは `role`, `priority`, `notes` を優先する。
5. 上記 `Input Support Bundles` の `identity_item_code` に合う候補を、親bundle/子bundleとして展開する。
6. 実CSVテンプレート登録で迷った項目だけ、入力支援bundleへ追加する。

## Open Points

- `EXCLUSIVE_ONE` をグループ既定に持たせるか、rule単位だけに持たせるか。

## Bundle Review Items

入力支援bundleは、画面で候補を探しやすくするための束ねであり、保存先や排他性を決める本体ではない。
ただし、bundle名が広すぎると候補が増えすぎ、狭すぎると実CSV登録時に探しにくくなる。
そのため、採用案は「画面では大きく、内部では小さく」の階層型とする。

### GLUCOSE_RELATED

採用内容:

- 親bundle: `GLUCOSE_RELATED`
- 子bundle: `GLUCOSE`, `HBA1C`

理由:

- 健診CSVでは血糖とHbA1cが同じブロックに並ぶことが多い。
- 一方で、血糖値とHbA1cは別identityであり、同じ値から排他選択する関係ではない。
- 画面上は近くに置き、内部では別bundleにする。

### LIPID_RELATED

採用内容:

- 親bundle: `LIPID_RELATED`
- 子bundle: `TRIGLYCERIDE`, `HDL_CHOLESTEROL`, `LDL_CHOLESTEROL`, `NON_HDL_CHOLESTEROL`, `TOTAL_CHOLESTEROL`

理由:

- 健診CSVでは脂質項目が同じブロックに並ぶことが多い。
- HDL/LDL/TG/non-HDL/総コレステロールを一画面で確認しやすい。
- LDL直接法/計算法、中性脂肪の空腹時/随時はbundleではなくrule条件で扱う。

### RENAL_RELATED

採用内容:

- 親bundle: `RENAL_RELATED`
- 子bundle: `CREATININE`, `EGFR`, `URIC_ACID`, `URINE_ALBUMIN`

理由:

- 厚労省付属2や健診CSVの入力支援では、検体別の「血液検査」「尿検査」より、意味別の「腎機能関連」として近くに置く方が探しやすい。
- `8A065` はidentity名が「血清クレアチニン」だが、`exam_item_master` 上の項目実体は `eGFR` であるため、`CREATININE` とは分けて `EGFR` として扱う。
- 尿酸・尿中アルブミンは検体や厳密な分類では揺れるが、CSV登録時の候補探索では腎機能関連として近くに置く。
