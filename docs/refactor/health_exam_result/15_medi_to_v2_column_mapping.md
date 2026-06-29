

# 15 medi → v2 カラム棚卸し

## 目的

旧 `medi_*` 系テーブルのカラムを棚卸しし、health_exam_result v2 での扱いを決定する。

本ドキュメントはDDLではなく、旧設計からv2への移行判断を記録するための設計メモとする。

関連資料

- 03_decisions.md
- 05_design_history.md
- 12_v2_ddl_design_notes.md
- 08_table_ddl_summary_codex.md

---

# 判定区分

| 区分 | 内容 |
|------|------|
| 移行 | 同じ責務でv2へ移す |
| 名称変更 | 名前を変更してv2へ移す |
| 再配置 | 別テーブルへ責務を移して保持する |
| 廃止 | v2では保持しない |
| 参照化 | v2で保持せず参照テーブルから取得する |
| 追加 | v2で新規追加したカラム |

---

# 棚卸し順序

1. medi_shared_files / medi_zip_receipts → file_receipts
2. medi_xml_receipts / medi_xml_ledger → xml_ledger
3. medi_xml_item_values / medi_exam_result_item_values → exam_item_values
4. medi_lsio_* / judge_* → exam_check_results
5. medi_import_runs 等 → etl_runs / etl_errors

---

# 1. file_receipts

対象旧テーブル

- medi_shared_files
- medi_zip_receipts

| 旧テーブル | 旧カラム | v2テーブル | v2カラム | 判定 | 理由・備考 |
|------------|-----------|------------|-----------|------|------------|
| medi_shared_files | | file_receipts | | | |
| medi_zip_receipts | | file_receipts | | | |

> この表を埋めながら file_receipts のカラム構成を確定する。