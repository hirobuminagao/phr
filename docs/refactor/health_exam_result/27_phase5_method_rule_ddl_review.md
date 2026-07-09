# Phase5 method rule DDL review

## 1. Review target

- `docs/refactor/health_exam_result/03_decisions.md`
- `docs/refactor/health_exam_result/05_design_history.md`
- `docs/refactor/health_exam_result/22_for05design_history.md`
- `sql/ddl/dev_phr/0060_dev_phr__exam_item_group_method_members.sql`
- `sql/migrations/dev_phr/20260709_001_dev_phr_add_v2_rule_columns_to_exam_item_group_method_members.sql`

## 2. Summary

- 判定: GO
- Phase5のDDL / migrationは、`03_decisions.md` の正式決定事項と整合している。
- `exam_item_group_method_members` はmethod単位の制度ルール管理テーブルとして、v2で必要なカラムだけを追加している。
- `condition_code`、method側 `condition_expr`、`rule_params` JSON は追加されていない。
- 既存 `LSIO_Legal_Item` は維持され、v2 group追加方式と矛盾しない。

## 3. Findings

### High

- なし。

### Medium

- なし。

### Low

- migrationはpre-check / post-check / rollbackコメントを持つが、自動冪等化はしていない。既存migrationの運用と同様に、適用前のpre-check確認を前提とする。
- `is_active` は `NOT NULL DEFAULT 1` のため既存行にも値が入る。ただしrule系カラムはNULL許可であり、既存LSIO行へv2ルール意味を付与するものではない。

## 4. DDL / migration consistency

DDLとmigrationの追加カラム、型、NULL、default、順序は一致している。

| カラム | DDL | migration | 確認結果 |
|---|---|---|---|
| `presence_value_mode` | `varchar(32) DEFAULT NULL` | `varchar(32) DEFAULT NULL` | 一致 |
| `required_flag` | `tinyint(1) DEFAULT NULL` | `tinyint(1) DEFAULT NULL` | 一致 |
| `rule_code` | `varchar(64) DEFAULT NULL` | `varchar(64) DEFAULT NULL` | 一致 |
| `rule_source_identity_codes` | `varchar(255) DEFAULT NULL` | `varchar(255) DEFAULT NULL` | 一致 |
| `rule_source_method_codes` | `varchar(255) DEFAULT NULL` | `varchar(255) DEFAULT NULL` | 一致 |
| `rule_source_namecodes` | `text NULL` | `text NULL` | 一致 |
| `is_active` | `tinyint(1) NOT NULL DEFAULT 1` | `tinyint(1) NOT NULL DEFAULT 1` | 一致 |
| `updated_at` | `datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6)` | `datetime(6) DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(6)` | 一致 |

追加位置も、`presence_value_mode` から `is_active` までを `priority` 後に連続追加し、`updated_at` を `created_at` 後に追加する構成で一致している。

## 5. Decision consistency

`03_decisions.md` では、Phase5の責務を `dev_phr` 制度マスタ整備に固定し、制度チェック実装、`exam_check_results` DDL、Rule / Lookup / Calculate lib実装はPhase5対象外としている。

今回のDDL / migrationは、`exam_item_group_method_members` をv2制度チェック用のmethod単位ルールマスタとして拡張する決定と一致している。追加カラムも `03_decisions.md` の一覧どおりである。

`05_design_history.md` は意思決定履歴であり、DH-20260709-01の `condition_code` 採用案は、DH-20260709-02で不採用へ更新されている。実装は `03_decisions.md` の正式決定に従い、`condition_code` を追加していない。

`22_for05design_history.md` は05追記用テンプレートであり、決定事項と保留事項を分け、03へ同期済みの決定事項を再協議しない前提を明記する構成で妥当である。今回のDDL / migrationレビューとは矛盾しない。

## 6. Existing LSIO impact

既存 `LSIO_Legal_Item` グループは維持される。DDL / migrationは既存行の削除、更新、group置換、新規制度ルールテーブル作成を行っていない。

rule系カラムはNULL許可で追加されるため、既存LSIO行へ `presence_value_mode`、`required_flag`、`rule_code`、`rule_source_*` の意味を付与しない。`is_active` はdefault 1で追加されるが、これは行の有効状態を表す補助カラムであり、v2ルール内容を付与するものではない。

v2は既存LSIOを置き換えず、v2用groupを追加する方式で進める方針と整合している。seed作成時は、v2用groupに対して72項目specを正として投入する。

## 7. Explicit non-adopted items

- `condition_code`: 追加していない。DH-20260709-02および `03_decisions.md` の不採用決定と一致。
- method側 `condition_expr`: 追加していない。条件付き必須などの将来要件はPhase5対象外。
- `rule_params` JSON: 追加していない。参照元項目は `rule_source_*` 専用カラムで保持する決定と一致。

## 8. Remaining items before seed

- v2用groupを追加するseedを作成する。
- seedは `docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md` の72項目を正として作成する。
- 03で決定済みの `required_flag`、`presence_value_mode`、`rule_code`、`rule_source_*` の責務は未決事項として扱わない。
- 条件付き必須、年齢条件、性別条件、`exam_check_results` DDL、制度チェック実装、Rule / Lookup / Calculate lib実装はPhase5 seed前の不足入力ではなく、後続Phaseの対象である。

不足入力:

- なし。

## 9. Check results

- `condition_code` / `condition_expr` / `rule_params` 検索: 対象DDL / migration内に該当なし。
- `git diff --check`: エラーなし。
- 実行時に `pyenv: cannot rehash: /Users/hiro/.pyenv/shims isn't writable` の警告は出たが、レビュー対象のdiff check結果には影響なし。

