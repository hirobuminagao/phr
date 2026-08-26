# 健診結果CSVマッピング AI連携レギュレーション

## 目的

このZIPは、健診結果CSVの列解析結果をCodexに渡し、CSV取込マッピング候補をAIレビューして返してもらうためのものです。

Codexは、同梱された `analysis_prompt.json` を読み、列ごとのマッピング候補をJSONだけで返してください。

## 入力ファイル

ZIPには次のファイルが入ります。

- `REGULATION.md`: このレギュレーション。
- `analysis_prompt.json`: 解析済みCSV列情報と回答スキーマ。

`analysis_prompt.json` には、実CSVそのものではなく、列番、ヘッダー、サンプル値、型、空欄数、周辺列、既存の機械候補、AIレビュー状態、人の判断状態などが入ります。

登録済みルールがヒットしている列には `rule_hits` が入ります。これは過去判断から作った機械候補の根拠ですが、最終判断ではありません。

既存の `machine_candidate` は、管理画面が機械的に推定した現在の候補です。Codexはこの候補を点検し、正しければ維持、違っていれば置き換え、判断不能ならスキップしてください。

## 個人情報・機微情報の扱い

`analysis_prompt.json` のサンプル値は、解析DBへ保存される前に機械的にサンプル化されています。

氏名、氏名カナ、健康保険証記号/番号/枝番、社員番号、加入者ID、受診券、生年月日、住所、電話、郵便、メールなどは、実値を保存しない方針です。

Codexは、サンプル値を実在人物・実在情報として扱わないでください。

## Codexの作業範囲

Codexは、各列について次を判断します。

- 基本情報として取り込む列か。
- 健診結果値として取り込む列か。
- 使わない列か。
- 今は取り込まないが、将来値が入ったら確認したい監視列か。
- 人間確認が必要な列か。
- 関連列と組み合わせて扱うべき列か。

Codexは、このZIPへの回答では最終seedを作成しません。候補とAIレビュー状態を返すだけです。

人の最終判断である `current_decision.status` は変更しません。AIレビューは、人が画面で最終判断する前の補助情報です。

## 判断方針

- ヘッダー名だけで決めず、サンプル値、型、周辺列も確認してください。
- `rule_hits` は優先的な判断材料として扱ってください。ただしヘッダーや値の実態と矛盾する場合は採用しないでください。
- `machine_candidate` は現在の機械候補です。正しければ `KEEP_MACHINE`、修正するなら `REPLACE_MACHINE`、未使用に寄せるなら `MARK_IGNORE`、監視に寄せるなら `MARK_WATCH`、人確認に寄せるなら `MARK_REVIEW` を返してください。
- 意味が曖昧な列は `REVIEW` または `NEEDS_CONFIRMATION` にしてください。
- ヘッダーとして意味があるが今回の取込対象にはしない列で、将来値が入ったら気づきたいものは `WATCH` / `WATCH_IF_PRESENT` にしてください。
- サンプル値が空、または `non_blank_count` が0という理由だけで `IGNORE` にしないでください。
- 健診結果値は、可能な限り既存の `namecode` に寄せてください。
- `analysis_prompt.json` と参照可能な既存定義から確認できない `namecode` は作らないでください。
- 基本情報は、`candidate_ledger_field` に寄せてください。
- 判定、コメント、所見、実施理由などは、検査値本文との違いに注意してください。
- 空腹時/随時、検査方法違い、左右別、複数所見などは、関連列を `related_column_nos` に入れてください。
- 判断できない列は、無理に候補を作らず `REVIEW` または `NEEDS_CONFIRMATION` にしてください。

## 返却形式

返却はJSONだけにしてください。Markdownの説明文やコードフェンスは不要です。

```json
{
  "analysis_file_id": 3,
  "reviewed_by": "codex",
  "updates": [
    {
      "column_no": 151,
      "ai_review_status": "REVIEWED",
      "candidate_target_kind": "EXAM_ITEM_VALUE",
      "candidate_namecode": "9N001000000000001",
      "candidate_ledger_field": null,
      "confidence": 0.95,
      "mapping_strategy": "DIRECT",
      "related_column_nos": [],
      "ai_review_note": "ヘッダーが身長で、値も数値のため身長として扱えます。",
      "reason": "ヘッダーが身長で、値も数値のため。",
      "needs_human_review": false,
      "review_points": [],
      "candidate_action": "KEEP_MACHINE"
    }
  ]
}
```

古い `suggestions` 形式は使わないでください。必ず `updates` で返してください。

## `ai_review_status`

- `REVIEWED`: AIとして確認済み。候補を提示する、または `IGNORE` / `REVIEW` として明示できる状態。
- `SKIPPED`: 情報不足などでAIでは判断せず、人間に残す状態。
- `FAILED`: 入力不備や処理上の問題で、その列をAIレビューできなかった状態。

## `target_kind`

- `LEDGER_FIELD`: 氏名カナ、保険証記号、受診日など、健診結果ledgerの基本情報。
- `EXAM_ITEM_VALUE`: 身長、血圧、検査値、問診、所見など、健診結果値。
- `IGNORE`: 取り込み不要。
- `REVIEW`: AIでは判断しきれない。
- `WATCH`: 今は取り込まないが、将来この列に値が入ったら確認したい。

## `candidate_action`

- `KEEP_MACHINE`: 既存の `machine_candidate` が妥当。
- `REPLACE_MACHINE`: 既存の `machine_candidate` とは別の候補に置き換える。
- `MARK_IGNORE`: 取り込み不要として候補化する。
- `MARK_WATCH`: 監視対象として候補化する。
- `MARK_REVIEW`: 人間確認対象として候補化する。
- `NO_CANDIDATE`: 候補を作らず、AIレビュー状態だけを返す。

## `mapping_strategy`

- `DIRECT`: 1列をそのまま1項目に対応。
- `MULTI_COLUMN_JOIN`: 複数列を連結または組み合わせて1項目に対応。
- `DERIVED_CODE`: 値からコード変換が必要。
- `METHOD_SELECTION`: 検査方法や条件によって対象namecodeが変わる。
- `IGNORE`: 取り込み不要。
- `NEEDS_CONFIRMATION`: 健診機関や仕様確認が必要。
- `WATCH_IF_PRESENT`: 今は取り込まないが、将来非空値が出たら確認する。

## 注意

AIの回答は候補です。最終判断は作業者が管理画面で確認して確定します。

AIの回答をDBへ反映する場合も、更新対象は `analysis_columns` のAIレビュー系カラムと機械候補系カラムまでです。人の確定状態である `decision_status` は、管理画面で人が操作したときだけ変更します。
