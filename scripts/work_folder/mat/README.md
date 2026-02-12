# mat (v1.0) — person_id_custom 生成仕様（一次情報）

このフォルダは `person_id_custom`（加入者突合用の固定キー）生成に必要な
**設定JSON + 対応表JSON** を保持する。

- 暗号鍵ではない（秘匿性や耐攻撃性の保証はしない）
- ただし **仕様そのもの** なので、変更はID仕様変更に等しい

---

## ファイル構成（必須）

- `custom_id_config.json`
  - add / mul / lengths / compose_order / mapping_file など
- `custom_id_mapping.json`（= mapping_file）
  - 0–9 → 任意文字 の写像表（フィールド別）

> v1.0 現状の既定探索先は `scripts/work_folder/mat/`  
> （`lib/custom_id_gen.py` の `default_mat_dir()` が参照）

---

## 生成ロジック（v1.0 as-is）

入力4要素（digits-only運用）
- insurer_number（保険者番号）
- symbol（保険証記号 ※v1.0は digits-only に寄せる）
- insurance_number（保険証番号）
- birth_yyyymmdd（生年月日）

処理手順（フィールドごと）
1. 数字のみへ正規化（必要に応じて0埋め）
2. `fit_width_max`：幅超過はエラー（切り捨てしない）
3. `(v + add) * mul` を適用（幅超過はエラー）
4. 1桁ずつ `mapping`（0–9→1文字）へ写像
5. `compose_order` の順で連結し `person_id_custom` を得る

---

## 対応表（mapping）の必須条件

- `custom_id_config.json` の `compose_order` で使うキーは、
  `custom_id_mapping.json` 側にも存在していること
- v1.0 現状設定: `strict_mapping=false`
  - mapping に不足があっても即例外にはしない（ただし仕様上は 0–9 を全て定義することが前提）
- v1.0 現状設定: `mapping_one_to_one=true`
  - 1桁→1文字（長さ1）を前提とする

---

## v1.0 現状 config サマリ（custom_id_config.json 抜粋要約）

- add: insurer=2850000000 / symbol=9710000000 / insurance_number=8650000000 / birth=2620000000
- mul: insurer=4 / symbol=2 / insurance_number=2 / birth=3
- lengths: insurer=11 / symbol=11 / insurance_number=11 / birth=10
- compose_order:
  1. birth_yyyymmdd
  2. insurance_number
  3. insurer_number
  4. symbol
- strict_mapping=false
- debug=true

※ 上記値の変更は v1.0 では禁止（変更＝ID仕様変更）。

---

## 重要：変更ポリシー（v1.0 Freeze）

このフォルダ内の変更は、過去に生成した `person_id_custom` と整合しなくなる可能性があるため、
v1.0 中は原則変更禁止。

- `add / mul / lengths / compose_order / mapping` の変更
  → **ID仕様変更（v2扱い）**
- v2以降で変更する場合は以下を推奨
  - `mat/v1/` `mat/v2/` のように version フォルダを分ける
  - 生成側に `--mat` を渡して明示的に切替える

---

## 使い方（切替）

通常は既定の `scripts/work_folder/mat/` を使う。

環境別に切替える場合は `--mat` で明示指定する：
```bash
python scripts/work_folder/lib/custom_id_gen.py --insurer ... --symbol ... --insurance ... --birth ... --mat /path/to/mat