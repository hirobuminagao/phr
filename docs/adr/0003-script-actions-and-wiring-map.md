# ADR 0003: Script Actions and Wiring Map (Hub side, v1.0)

## Status
Accepted (v1.0 freeze extension)

## Context

ADR 0001 および ADR 0002 にて、PHR v1.0 の baseline / work_folder の意味固定は完了している。

しかし、以下が未固定であった：

- 各スクリプトが実際にどのテーブルへどのアクションを行っているか（一次情報）
- スクリプト間のデータ配線（wiring）の現状構造
- DDL とスクリプトアクションの突合前提

本ADRは「推測なし・予測なし」で、
**スクリプトが実際に発行しているDBアクションを一次情報として固定すること**を目的とする。

対象は work_folder の Hub 側のみ（fund は未完成のため対象外）。

### Note (Temporary / Reconcile Later)

本ADRは **スクリプト側（docstring含む）を一次情報**として、まず "as-is" を固定する。
この段階では DDL との完全一致を保証しない（ズレがあり得る）。

次フェーズで `sql/ddl/dev_phr/*` と突合し、以下の差分を検出した場合は **本ADRと該当スクリプトdocstringを修正**して整合させる：

- INSERT/UPDATE で使用しているカラムが DDL に存在しない
- DDL 上 NOT NULL だがスクリプトが NULL/空を許容している
- 型（date/int/varchar 等）や長さが実運用入力と不整合
- テーブル名/スキーマ名の座標ズレ

（重要）推測で先に DDL に合わせない。**差分が出たタイミングで事実に基づき修正**する。

---

## Scope

対象スクリプト：

- `import_subscribers_to_staging_hub.py`
- `apply_subscribers_from_staging_hub.py`

スキーマ前提：

- `dev_phr`

---

# 1. Script Actions（一次情報固定）

## 1.1 import_subscribers_to_staging_hub.py

### DB Actions (Fact)

- start_run
    - INSERT INTO `etl_runs`
    - 直後に `conn.commit()` 実行（dry-run/失敗時も run_id の証跡を残す）

- per-row error handling
    - 正規化エラー: `log_normalize_error` → INSERT INTO `etl_errors`（行スキップで継続）
    - 例外発生時: `log_error` → INSERT INTO `etl_errors`（行スキップで継続）

- staging insert
    - INSERT INTO `staging_subscribers_hub`（明示カラム指定）
    - dry-run 時は実行しない
    - columns:
        - person_id_custom
        - name_kana_full
        - name_kanji_full
        - name_kanji_family
        - name_kanji_middle
        - name_kanji_given
        - name_kana_family
        - name_kana_middle
        - name_kana_given
        - gender_code
        - birth
        - insured_attribute_name
        - relationship_name
        - insurer_number
        - insurance_symbol
        - insurance_symbol_digits
        - insurance_number
        - insurance_branchnumber
        - qualification_acquired_date
        - qualification_lost_date
        - postal_code
        - address_line
        - building
        - phone
        - email
        - employer_code
        - department_code
        - distribution_code
        - employee_code
        - connect_id
        - created_at
        - loaded_at
        - processed_at
        - src_file
        - src_row_no
        - src_line_no
        - import_run_id

- finish_run
    - 正常系: `finish_run` 実行後
        - dry-run → `conn.rollback()`
        - 本番 → `conn.commit()`
    - 異常系: `conn.rollback()` → `finish_run(status=failed)` → `conn.commit()`

### DB Reads

- 参照テーブルなし（正規化はローカル処理）

### File I/O

- READ: `PHR_ROOT/input/subscribers_hub/active/<8桁保険者番号>/*.csv`
- READ: `scripts/work_folder/mat/custom_id_config.json`
- READ: `scripts/work_folder/mat/custom_id_mapping.json`

### Key Generation

- `person_id_custom` は mat 配下 JSON を一次情報として生成
- Python 内に乱数対応表のハードコードは存在しない

---

## 1.2 apply_subscribers_from_staging_hub.py

※ v1.0 現状は "apply" という名称だが、実態は import 相当（staging まで）。
`subscribers` テーブル更新は対象外。

### DB Actions (Fact)

- start_run
    - INSERT INTO `etl_runs`
    - 直後に `conn.commit()` 実行

- per-row error handling
    - 正規化エラー → INSERT INTO `etl_errors`
    - 例外 → INSERT INTO `etl_errors`

- staging insert
    - INSERT INTO `staging_subscribers_hub`（明示カラム指定）
    - dry-run 時は実行しない
    - columns:（1.1 と同一）

- finish_run
    - 正常系: dry-run → rollback / 本番 → commit
    - 異常系: rollback → finish_run(status=failed) → commit

### DB Reads

- 参照テーブルなし

### File I/O

- Hub CSV 読み込み
- mat JSON 読み込み

---

# 2. Wiring Map（Hub側）

```mermaid
flowchart LR

  subgraph Files
    CSV[HIA CSV]
    MAT[mat/*.json]
  end

  subgraph Scripts
    IMP[import_subscribers_to_staging_hub]
    APPLY[apply_subscribers_from_staging_hub\n(v1.0: import相当)]
  end

  subgraph DB[dev_phr]
    SHUB[staging_subscribers_hub]
    RUNS[etl_runs]
    ERR[etl_errors]
  end

  CSV --> IMP
  MAT --> IMP

  CSV --> APPLY
  MAT --> APPLY

  IMP --> SHUB
  IMP --> RUNS
  IMP --> ERR

  APPLY --> SHUB
  APPLY --> RUNS
  APPLY --> ERR
```

本図は将来設計ではなく、v1.0 現状の実装事実を固定したものである。

---

# 3. DDL突合ポリシー（削除しない固定）

DDLカラムは以下の分類で管理する：

- USED_WRITE
- USED_READ
- USED_BOTH
- UNUSED_RESERVED
- UNUSED_UNKNOWN

v1.0ではカラム削除は行わない。
未使用カラムは明文化のみ行う。

---

# 3.5 DDL突合チェックリスト（後で実施）

- [ ] `staging_subscribers_hub` の DDL と、Hub import の INSERT columns が一致する
- [ ] `etl_runs` / `etl_errors` の DDL と、log/start/finish の利用カラムが一致する
- [ ] NOT NULL / DEFAULT の前提と、スクリプトの値投入（NULL/空/固定値）が矛盾しない
- [ ] `staging_subscribers_hub` の "証跡" カラム（src_file/src_row_no/src_line_no/import_run_id 等）が DDL で担保されている
- [ ] 将来用途で残しているカラムは `UNUSED_RESERVED` として明文化されている

---

# 4. Decision

- Hub側スクリプトのDBアクションを一次情報として固定した
- 配線図をMermaidで明示した
- 今後のDDL突合は本ADRを基準とする

これにより、PHR v1.0 は
「意味固定」から「構造固定」へ拡張された。

---