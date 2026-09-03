# Case統合と保険者番号解決

## 1. 目的

受領XML/CSVの誤った保険者番号や加入者突合後の補正によって、同じ受診が別の `exam_export_cases` へ分裂することを防ぐ。
既に分裂したcaseは履歴を削除せず、明示的な統合操作によって1つの有効caseへ集約できるようにする。

この設計は、単一保険者番号のeventだけでなく、同一健保が複数の保険者番号を持つ場合を対象に含む。

## 2. 基本原則

- `exam_ledgers.insurer_number` は受領原本であり、case補正や統合で書き換えない。
- `insurer_number_export_value` は出力用の補正値であり、原本と分離して保持する。
- eventの `insurer_number` は無条件に上書きする固定値ではない。eventの所属健保と許可保険者番号集合を解決する入口として使う。
- caseの業務上の同一受診キーは `event_id + subscriber_id + exam_date + exam_facility_id` とする。
- 保険者番号は同一受診の解決済み属性とし、相違だけを理由に別caseを作らない。
- 同一受診内に複数の正当な保険者番号候補が残る場合は推測せず停止する。
- source ledgerのcase間移管は通常のupsertでは行わず、明示的なcase統合処理だけに許可する。
- case、source、レビュー、補正、出力履歴は物理削除しない。

## 3. Event許可保険者番号集合

現行 `dev_phr.event` は単一の `insurer_number` を持ち、直接 `fund_id` を持たない。
初期実装では次の手順で許可集合を得る。

1. eventの保険者番号を8桁へ正規化する。
2. `dev_phr.fund_insurer_numbers` からevent番号に対応する `fund_id` を特定する。
3. 同じ `fund_id` に属し、対象受診日に有効な保険者番号を許可集合とする。
4. masterから健保を一意に特定できない場合は、event番号だけを許可集合とする。
5. master上で複数の `fund_id` に曖昧一致する場合は処理を停止する。

ローカル検証DBなど `fund_insurer_numbers` 自体が存在しない環境では、event番号だけの単一許可集合へ安全に退避する。この場合、複数保険者番号対応は有効にならず、event番号以外を自動許可しない。

有効期間は `valid_from <= exam_date` かつ `valid_to IS NULL OR valid_to >= exam_date` とする。
`is_current` は現在日時の表示用属性であり、過去受診日の判定では有効期間を優先する。

将来eventに許可保険者番号を直接設定するテーブルを追加した場合は、その明示設定を最優先とする。

## 4. Case保険者番号の解決順

候補値は数字化・8桁化して比較し、空値、8桁超過、全桁0を無効とする。

1. 加入者突合で確定したsubscriberの保険者番号
2. 手動補正済みのledger/case `insurer_number_export_value`
3. 受領値のうちevent許可集合に含まれる値
4. 許可集合が1件だけの場合のevent補完

上位候補が許可集合に含まれない場合は自動採用しない。
複数候補が許可集合内に残り1件へ決定できない場合は `INSURER_NUMBER_UNRESOLVED` としてcase作成・更新を停止する。

解決結果はcaseの `insurer_number` と `insurer_number_export_value` で矛盾させない。
ZIP名、IX08、個人XML `receiver`、出力リストの保険者単位は同じ共通解決結果を使う。

## 5. Caseライフサイクル

`merge_status` はsource内容の結合判定に既に使用しているため、case自体の統合状態には流用しない。
`exam_export_cases` に次の専用列を追加する。

| column | meaning |
| --- | --- |
| `case_lifecycle_status` | `ACTIVE` / `MERGED` |
| `merged_into_case_id` | 統合先の有効case ID |
| `merged_at` | 統合日時 |
| `merged_by_app_user_id` | 操作者。保守CLIではNULL可 |
| `merge_operation_reason` | 統合理由 |
| `active_case_guard` | `ACTIVE`だけ1、`MERGED`はNULLとなる生成列 |

`MERGED` caseは詳細・監査・過去出力履歴の参照対象には残すが、次から除外する。

- case重複判定と通常upsert
- case値作成
- caseチェックとreadiness更新
- 出力リスト候補
- XML出力
- 通常のcase件数、施設集計、作業対象一覧

統合先は必ず `ACTIVE` とし、自己参照、循環参照、統合済みcaseへの統合を禁止する。
既存の保険者番号込みnatural uniqueは廃止し、`event_id + subscriber_id + exam_date + exam_facility_id + active_case_guard` を一意にする。これにより同じ受診の `ACTIVE` caseは1件だけに制限しつつ、過去の `MERGED` caseは複数保持できる。

## 6. Source所有権

`exam_export_case_sources.source_exam_ledger_id` は一意であり、1つのsource ledgerは同時に1つの有効caseだけへ所属する。

通常の `upsert_sources()` は既存sourceの `exam_export_case_id` を更新しない。
異なる有効caseに所属済みの場合は `CASE_SOURCE_OWNERSHIP_CONFLICT` で停止する。

case間移管は明示的なcase統合サービス/保守CLIだけがtransaction内で行う。
統合処理はdry-run診断を既定とし、実更新には明示オプションを要求する。

## 7. 明示的なCase統合

入力は `target_case_id`、`source_case_id`、理由、任意の操作者IDとする。

事前条件:

- 両caseが同じevent、subscriber、受診日、健診機関である。
- targetが `ACTIVE` である。
- sourceが `ACTIVE` で、別caseへ統合済みではない。
- event許可集合からtargetの保険者番号を1件に解決できる。
- 本番出力履歴、出力リスト、レビュー、手入力draft参照の状態を診断済みである。

実行内容:

1. target/sourceと関連行をロックする。
2. source ledgerをtargetへ移管する。
3. 手入力draft等の現在参照をtargetへ付け替える。
4. source caseを `MERGED` とし、target ID、日時、理由を保存する。
5. targetの保険者番号を共通解決結果へ揃える。
6. transactionをcommitする。

過去のXML出力履歴、監査ログ、レビュー履歴は移動・削除しない。
履歴は当時のcase IDを保持し、source case詳細から統合先へ辿れるようにする。

## 8. 再生成と人手判断

case再生成ではaggregateの `manual_export_*` を保存値として盲目的に戻さない。
正は `exam_case_check_review_items` とそのaudit、および `exam_case_basic_info_corrections` とする。

- 同じ不足が残る場合は、理由、操作者、日時が揃った `APPROVED_WITH_REASON` を再集約する。
- source追加で不足が解消した場合は `RESOLVED_BY_SOURCE_VALUE` へ自動遷移する。
- 解消後に再び不足した場合、過去承認を自動復活させず再確認対象にする。
- 値や業務チェック対象が変わった場合も再確認対象にする。
- `EXCLUDED` は出力を妨げない正式状態として扱うが、除外理由の記録要件は別途既存仕様に従う。
- 基本情報補正はcase更新後に再適用する。

## 9. 実装範囲

今回実装するもの:

- caseライフサイクル列のmigrationと基準DDL同期
- event許可保険者番号集合とcase保険者番号解決の共通処理
- 通常case作成とcase限定再生成への適用
- `upsert_sources()` の所有権競合停止
- 明示的case統合サービスとdry-run対応の保守CLI
- case値、チェック、readiness、出力候補、XML出力の `MERGED` 除外
- 理由ありOK、解消済み、基本情報補正の再生成テスト
- 単一番号、複数番号、全桁0、許可外番号、複数候補、既存重複のテスト

今回実装しないもの:

- 実行環境の具体的な重複case修正
- `EXPORTED` の解除
- 本番XMLの再出力
- 過去の出力・監査履歴のcase ID付け替え
- 管理画面からの汎用case統合操作
- event許可保険者番号を直接編集する新画面

## 10. Migration適用順

1. `20260903_001_health_exam_result_add_case_lifecycle.sql` を適用し、case統合状態を保持できるようにする。
2. 既存の同一受診ACTIVE caseを調査し、`merge_exam_export_cases.py` で明示的に統合する。
3. `20260903_002_health_exam_result_enforce_active_case_identity.sql` を適用し、ACTIVE caseの再重複を禁止する。

既存重複をmigration内で自動統合しない。統合先、レビュー、補正値、出力履歴を確認してから保守操作として統合する。

## 11. 初回補修対象の扱い

case 51072を統合先、case 123073を統合元とする案は、実行環境でdry-run診断した後に確定する。
本設計の実装では実データを変更しない。

補修時は旧MANUAL 6759の `REVERTED_TO_DRAFT` を復活させず、実行環境で確認された対象XML ledgerと、現在有効なMANUAL ledgerだけを統合対象にする。
case値作成、caseチェック、readiness、XML候補判定は統合先caseに限定して順番に再実行する。
