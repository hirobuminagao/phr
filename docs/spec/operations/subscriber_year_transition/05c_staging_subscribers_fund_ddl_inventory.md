

# 05c_staging_subscribers_fund_ddl_inventory

## 目的

本ファイルは、`staging_subscribers_fund` の現行DDLカラムを、今後の新DDL方針へ落とし込むための棚卸し表である。

親 spec:
- `05_staging_subscribers_fund.md`

関連 spec:
- `05a_staging_subscribers_fund_template_import.md`
- `05b_staging_subscribers_fund_column_policy.md`
- `05d_staging_subscribers_fund_2026_diff_policy.md`

---

## 棚卸し区分

区分は以下の意味で用いる。

- `維持`: 現行カラムをそのまま維持する
- `rename`: 命名規則（主に `_norm` / `_match`）へ寄せて引き継ぐ
- `追加`: 現行DDLに存在しないが新DDLで追加する
- `削除`: staging の責務から外す

---

## 1. 主キー・テンプレート・実行管理

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `id` | 維持 | 主キーとして維持 |
| `fund_id` | 維持 | 健保単位の取り込み基盤として維持 |
| `template_ver` | rename | `version` へ rename（templates / template_mappings と命名統一するため確定） |
| `import_run_id` | 維持 | ETL run / archive と連動するため維持 |
| `created_at` | 維持 | 作成時刻として維持 |
| `loaded_at` | 維持 | `created_at` とズレうる取り込み完了時刻として維持 |

---

## 2. identity・照合結果

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `person_id_custom` | 維持 | staging 取り込み時点で生成・保持 |
| `identity_hash` | 追加 | 現行DDLに存在しないため追加 |
| `matched_subscriber_id` | 維持 | identity_hash 生成後、レコード生成処理の最後に行う subscribers 照合結果として保持 |
| `matched_checked_at` | 削除 | 照合はレコード生成処理の最後に実行するため、`created_at` と実質的に重複しやすく削除対象 |
| `processed_at` | 削除 | 意味が曖昧な状態管理カラムのため削除対象 |

---

## 2.5 差分判定

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `diff_status` | 追加 | 2026年度差分判定用の一時判定結果として追加 |
| `diff_status_method` | 追加 | 判定手段（script / manual）を保持するため追加 |
| `diff_status_reason` | 追加 | 判定根拠を保持するため追加 |

---

## 3. 氏名（カナ）

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `name_kana_full` | rename | `name_kana_full_norm` |
| `name_kana_family` | rename | `name_kana_family_norm` |
| `name_kana_middle` | rename | `name_kana_middle_norm` |
| `name_kana_given` | rename | `name_kana_given_norm` |
| `name_kana_full_match` | 追加 | identity構成要素として追加 |

### 氏名カナの match 設計方針

- `*_norm` はフル・分割すべての構造を保持する
- `*_match` は `name_kana_full_match` のみを保持する
- match 値は必ず `name_kana_full_norm` から生成する
- 分割カナ（family / middle / given）は match を直接生成しない

---

## 4. 氏名（漢字）

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `name_kanji_full` | rename | `name_kanji_full_norm` |
| `name_kanji_family` | rename | `name_kanji_family_norm` |
| `name_kanji_middle` | rename | `name_kanji_middle_norm` |
| `name_kanji_given` | rename | `name_kanji_given_norm` |
| `name_kanji_full_match` | 追加 | 共通ライブラリで生成し、比較判定の根拠として使用 |
| `name_kanji_family_match` | 追加 | 分割後に共通ライブラリを適用して生成 |
| `name_kanji_middle_match` | 追加 | 分割後に共通ライブラリを適用して生成 |
| `name_kanji_given_match` | 追加 | 分割後に共通ライブラリを適用して生成 |

---

## 5. 基本属性

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `gender_code` | rename | `gender_code_norm` |
| `birth` | rename | `birth_norm` |
| `relationship_code` | rename | `relationship_code_norm` |
| `relationship_name` | rename | `relationship_name_norm` |
| `relationship_name_match` | 追加 | `relationship_name_norm` を基準に生成し、名称がない場合はコード→名称変換ルールがあるときのみ生成 |

---

## 6. 保険情報

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `insurer_number` | rename | `insurer_number_norm` |
| `insurance_symbol` | rename | `insurance_symbol_norm` |
| `insurance_symbol_digits` | 維持 | 人手確認・運用補助に加え、person_id_custom 生成前提の数字成分確認列として維持 |
| `insurance_symbol_match` | 追加 | `insurance_symbol_digits` とは別に、照合用の match 列として追加 |
| `insurance_number` | rename | `insurance_number_norm` |
| `insurance_branchnumber` | rename | `insurance_branchnumber_norm` |
| `insurance_number_match` | 追加 | 照合用として追加 |

---

## 7. 資格・住所・連絡先

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `qualification_acquired_date` | rename | `qualification_acquired_date_norm` |
| `qualification_lost_date` | rename | `qualification_lost_date_norm` |
| `postal_code` | rename | `postal_code_norm` |
| `address_line` | rename | `address_line_norm` |
| `building` | rename | `building_norm` |
| `phone` | rename | `phone_norm` |
| `email` | rename | `email_norm` |

### 住所・連絡先の保持方針

住所・連絡先情報は、健保ごとに受領形式が異なるが、staging において削除対象としない。

保持対象:

- `postal_code_norm`
- `address_line_norm`
- `building_norm`
- `phone_norm`
- `email_norm`

---

## 8. 会社・組織・外部ID

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `employer_code` | rename | `received_company_code_norm` へ rename（受領会社コードとして扱うため命名変更） |
| `department_code` | 維持 | 個別健保で未使用でも fund共通項目として維持 |
| `distribution_code` | 維持 | 個別健保で未使用でも fund共通項目として維持 |
| `employee_code` | 維持 | 個別健保で未使用でも fund共通項目として維持 |
| `connect_id` | rename | `connect_id_norm` として保持確定 |
| `received_company_name_norm` | 追加 | 受領CSV由来の会社名として追加確定 |
| `mapped_employer_code` | 追加 | 健保別マッピング後のHIA側事業所コードとして追加 |
| `mapped_department_code` | 追加 | 健保別マッピング後のHIA側部署コードとして追加 |
| `subscribers_employer_code` | 追加 | 現行 `subscribers.employer_code` の比較用キャッシュとして追加 |
| `subscribers_department_code` | 追加 | 現行 `subscribers.department_code` の比較用キャッシュとして追加 |

---

## 9. 出所追跡

| 現行カラム | 区分 | 新方針 / コメント |
|---|---|---|
| `src_file` | 維持 | 入力起点追跡用として維持 |
| `src_row_no` | 維持 | 行特定用として維持 |
| `src_line_no` | 維持 | 行特定用として維持 |

---

## 10. この棚卸し表の位置づけ

本表は、新DDLを起こすための基礎表として扱う。

ここで整理した rename / 追加 / 維持 / 削除の方針を、次段のDDL設計へ反映する。

次の更新では、本表をもとに以下を詰める。

- `template_mappings.target_column` の更新方針
- 新DDLへの落とし込み

---

## 現時点の確認結果

### 確認済み

- 現行DDLカラムの棚卸し方針（維持 / rename / 追加 / 削除）は確定済み
- `matched_subscriber_id` は staging に保持する方針で確定済みとする
- `insurance_symbol_digits` は `insurance_symbol_match` とは別用途の補助列として保持する方針で確定済みとする
- `template_ver` は最終DDLでは `version` に統一する方針で確定済みとする
- `staging_subscribers_fund` は現行DDLとの乖離が大きく、実データも存在しないため、DROP + CREATE 前提で再作成する方針とする
- 2026年度受領データの差分判定用に `diff_status` / `diff_status_method` / `diff_status_reason` を保持する方針とする

### 未実施

- 本 spec の内容をもとに新DDLへ落とし込む
- 会社 Ubuntu 環境側の現行DDLと最終突合を行う