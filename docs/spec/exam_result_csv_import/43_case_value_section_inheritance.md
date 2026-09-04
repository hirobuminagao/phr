# case値のsection継承設計

## 目的

同じnamecodeが複数のCDA sectionに存在する受領XMLを、case統合後も元のsection構造を失わずに再出力する。
例として、同一namecodeが `01010` と `01060` にある場合は、両方をそれぞれのsectionへ出力する。

## 現状の問題

`exam_item_values` は `section_code`、`section_code_system`、`section_name` を保持しているが、
`exam_export_case_values` にはsection列がなかった。XML出力時は `exam_item_master.cda_section_code_default`
からsectionを再決定していたため、受領時のsectionが失われていた。

法定チェックは `01030` を優先し、`01030` がなければ対象namecodeの候補数で判定する。
このチェック上の選択規則と、XMLへ全採用値を出力する規則は分離する。

## 設計

1. `exam_export_case_values` にsection 3列を追加する。
2. case値作成時に、採用した `exam_item_values` のsectionをそのまま複写する。
3. XML出力はcase値の `section_code` を最優先する。
4. 既存caseは `source_exam_item_value_id` からsectionを埋め戻す。
5. 埋め戻せない旧データだけ、採用元、項目マスタ、`01990` の順でフォールバックする。
6. 法定チェックはcase値に保持したsectionを参照するが、既存の `01030` 優先規則は変更しない。

## 同一namecodeの扱い

XML取込の `occurrence_no` はsection単位ではなく、同じnamecode全体で連番になる。
そのため既存のcase値一意キー `case + namecode + occurrence_no` は維持する。
`01010` の occurrence 1 と `01060` の occurrence 2 は別のcase値として保持され、出力時にsection別に配置される。

同一section内に同じnamecodeが複数ある場合も、occurrence順を維持して出力する。
法定チェックで複数候補をエラーとするかはチェック仕様で判断し、出力時のsection欠落とは混同しない。

## 互換性と再処理

- migration適用直後に既存case値を採用元から埋め戻す。
- 新規または再生成したcase値は作成処理でsectionが設定される。
- migration後にcase値を再生成しなくても、採用元が残っている既存caseは正しいsectionで出力できる。
- 採用元がない手補正値などは、項目マスタの既定sectionへフォールバックする。

## 確認観点

- case値作成でsection 3列が保存されること。
- 法定チェックがcase値のsectionを参照すること。
- XML出力ローダーがcase値、採用元、マスタ、`01990` の順でsectionを解決すること。
- 同一namecodeの `01010` と `01060` が、生成XML内の両sectionに残ること。
