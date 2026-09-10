# 人×イベント 健診進捗照会 設計

## 1. 目的

加入者情報、予約、健診結果受領、case確認、HIA向け結果出力、HIAダッシュボード、健保納品を、`event × subscriber` の同じ画面で時系列に確認できる参照専用ユーティリティを作る。

この画面は各業務テーブルの正本を置き換えない。工程を横断して現在位置と不整合を見つけ、既存の詳細画面へ移動するための索引とする。

画面名は **健診進捗照会** とし、HOMEのユーティリティに配置する。既存の加入者情報確認からも導線を設ける。

## 2. 集約単位

### 2.1 親単位

親は既存の `dev_phr.person_event` とする。

```text
event_id + subscriber_id = 1 person_event
```

これはイベント内の人物サマリーであり、1回の受診を意味しない。年度内複数受診、再受領、XMLとCSVの複数sourceは潰さず、配下の予約、ledger、caseとして複数表示する。

eventには将来、受診可能回数または必要受診回数が設定される可能性がある。初期版では「eventにつき1回」を固定ルールにせず、実受診回数と予約回数を数えて表示する。必要回数との充足判定はevent側の回数ルールが定義された段階で追加する。

### 2.2 受診単位

受診の正本は次の優先順で扱う。

1. `exam_export_cases`: subscriber、受診日、健診機関が確定した出力単位
2. `exam_ledgers`: case前またはcaseに束ねられた受領source
3. `reservation_site_records`: 結果受領前の予約単位

予約とcaseを無理に1行へ結合しない。予約日・施設・加入者が一致する場合は同じ受診グループ候補として表示し、曖昧な場合は別行のまま「要確認」とする。

同じevent・同じ加入者に複数予約が存在することを正常系とする。特に次を想定する。

```text
施設Aを予約 -> キャンセル
施設Bを再予約 -> 受診
施設Bの結果をXML/CSVで受領
```

キャンセル予約は削除せず予約履歴として表示する。最新予約だけを取得して過去のキャンセルを隠したり、キャンセルと再予約を1件へ上書き統合したりしない。

## 3. 画面構成

### 3.1 健診進捗一覧

URL案: `/utilities/person-event-progress`

検索条件:

- event（必須、初期値は現在運用中のevent）
- HIA加入者ID
- subscriber ID
- 保険証記号、番号
- 氏名カナ
- 生年月日
- 健診機関
- 予約状況
- 受領状況
- case/readiness状態
- HIA出力状況
- ダッシュボード状況
- 健保納品状況
- 要確認のみ

一覧は1人1行とし、次を表示する。

| 列 | 内容 |
| --- | --- |
| 加入者 | subscriber ID、HIA加入者ID、権限に応じた氏名・保険情報 |
| 予約 | 有効予約、キャンセル、全予約の各件数、次回予約日 |
| 結果受領 | ledger件数、XML/CSV/手入力、最新受領日 |
| case | case件数、実受診回数、最新case ID、check/readiness |
| HIA出力 | 出力リスト、XML出力、アップロード状態 |
| ダッシュボード | 最新状態、予約日、受診日、更新日時 |
| 健保納品 | 未納品/候補/納品済み/再納品、最新納品日 |
| 要確認 | 未突合、予約競合、複数case、工程矛盾の件数 |

件数カードはevent全体を集計し、ページ内件数から作らない。初期一覧は100件/ページのサーバ側ページングとする。

### 3.2 人×イベント詳細

URL案: `/utilities/person-event-progress/{person_event_id}`

上部に加入者とeventの追跡用IDを表示し、その下を次の順に並べる。

```text
1. 加入者登録
2. 予約
3. 結果受領・加入者突合
4. case結合・チェック
5. HIA向けXML出力・アップロード
6. HIAダッシュボード
7. HIAからの結果取得・健保納品
```

各工程には状態、件数、最終更新日時、正本ID、理由を表示する。色だけに依存せず、状態名を文字で出す。

詳細内のタブ:

1. **進捗**: 工程タイムラインと現在の要確認事項
2. **受診・source**: 全予約、キャンセル、ledger、caseの対応関係
3. **出力・納品**: 出力リスト、XML member/ZIP、HIA、健保納品履歴
4. **履歴**: status itemの更新元runと閲覧監査

既存の加入者、受領ファイル、ledger、case、出力リスト、HIAアップロード、健保納品へリンクする。初期版に更新操作は置かない。

### 3.3 未突合・孤立データ

`subscriber_id` が確定していない予約またはledgerは `person_event` に混ぜない。一覧上部に別カードを置き、既存の加入者突合画面または将来の予約突合画面へ誘導する。

- 予約未突合
- 予約候補複数
- ledger未突合
- event不一致
- 健診機関未対応

未突合データを「未予約」「未受領」として人数集計すると誤集計になるため、母集団の進捗集計とは分離する。

## 4. 工程状態

| 工程 | 主な正本 | 完了の基本条件 | 注意状態 |
| --- | --- | --- | --- |
| 加入者 | `dev_phr.subscribers`, `person_event` | event母集団に存在 | 資格情報不足、重複候補 |
| 予約 | `work_other.reservation_site_records` | 有効な予約が加入者へ紐付く | 仮予約、キャンセル、未突合、同時に複数の有効予約 |
| 結果受領 | `exam_ledgers` | 突合済みledgerが1件以上 | 未突合、import NG、施設不一致 |
| case確認 | `exam_export_cases` | active caseのreadinessが出力可能 | BLOCKED、WAITING、複数active case |
| HIA出力 | `ops_xml_export_list_cases`, `xml_export_members/zips` | XML member作成済み | EXPORT_ERROR、再出力待ち |
| ダッシュボード | `hia_dashboard_status` | event対象の最新状態を取得 | caseとの受診日・施設不一致 |
| 健保納品 | `fund_delivery_*` | person/yearの納品履歴あり | 候補未作成、除外、再納品待ち |

`完了` は画面で独自に上書き保存せず、正本状態から同期する。同じ工程に複数行がある場合は最悪状態だけで潰さず、件数と代表状態を併記する。複数予約・複数受診そのものはエラーにしない。互いに矛盾する有効予約、eventの回数ルール超過、同一受診と思われるcase重複だけを要確認とする。

## 5. 予約と加入者の紐付け

### 5.1 必要性

`reservation_site_records` は現時点で `subscriber_id` を持たない。HIA加入者IDには重複実績があるため、毎回の曖昧な文字列JOINを正式な紐付けとして使用しない。

### 5.2 推奨テーブル

後続実装で `work_other.reservation_site_subscriber_links` を追加する。

主な列:

- `reservation_site_record_id`（UNIQUE）
- `event_id`
- `subscriber_id`
- `match_status`: `AUTO_MATCHED / CONFIRMED / UNMATCHED / MULTIPLE / CONFLICT`
- `match_method`: `HIA_ID / INSURANCE / NAME_BIRTH / MANUAL`
- `match_reason`
- `matched_at`
- `matched_by_app_user_id`
- `source_run_id`
- `created_at`, `updated_at`

自動候補の優先順:

1. event一致 + 正規化保険者番号一致 + HIA加入者IDが一意
2. event一致 + 保険者番号 + 記号 + 番号 + 生年月日
3. event一致 + 保険者番号 + 氏名カナ + 生年月日

候補が複数なら自動確定しない。予約CSVの原値は変更せず、確定した対応だけをlinkテーブルへ保存する。

### 5.3 予約と実受診の対応

予約と加入者の対応が確定しても、どの予約がどの実受診になったかは別問題である。加入者linkへcase IDを直接持たせず、必要になった段階で `work_other.reservation_site_exam_links` を追加する。

主な列:

- `reservation_site_record_id`
- `exam_export_case_id`
- `link_status`: `AUTO_MATCHED / CONFIRMED / UNMATCHED / MULTIPLE / CONFLICT`
- `match_method`: `DATE_FACILITY / DATE_ONLY / MANUAL`
- `match_reason`
- `matched_at`, `matched_by_app_user_id`

自動対応は、加入者確定済みかつevent・受診日・健診機関が一致する場合だけを基本とする。キャンセル予約は自動対応対象外とする。予約サイト施設IDが健診機関マスタへ未対応の場合、日付だけで自動確定せず候補表示に留める。

同じ人が同じeventで複数回受診した場合は、それぞれ異なるcaseへ対応できる。1予約を複数caseへ結び付ける必要が生じた場合は、重複caseか分割受領かを確認し、通常の自動処理では確定しない。

## 6. 読み取りモデル

一覧と詳細で取得方法を分ける。

### 6.1 一覧

一覧は `person_event` と `person_event_status_items` を中心に読む。各正本テーブルへ行ごとの相関サブクエリを発行しない。

追加するstatus item案:

- `RESERVATION_STATUS`, `RESERVATION_COUNT`, `ACTIVE_RESERVATION_COUNT`, `CANCELLED_RESERVATION_COUNT`
- `NEXT_RESERVATION_ID`, `NEXT_RESERVATION_DATE`, `LATEST_RESERVATION_UPDATED_AT`
- `RESULT_RECEIVED_COUNT`, `MATCHED_LEDGER_COUNT`, `LATEST_EXAM_LEDGER_ID`
- `ACTIVE_CASE_COUNT`, `EXAM_OCCURRENCE_COUNT`, `LATEST_CASE_ID`, `CASE_READINESS_STATUS`
- `XML_EXPORT_STATUS`, `LATEST_XML_EXPORT_MEMBER_ID`, `HIA_UPLOAD_STATUS`
- `HIA_DASHBOARD_STATUS`, `HIA_DASHBOARD_UPDATED_AT`
- `FUND_DELIVERY_STATUS`, `LATEST_FUND_DELIVERY_RUN_ID`, `FUND_DELIVERED_AT`
- `PROGRESS_ATTENTION_COUNT`, `PROGRESS_ATTENTION_CODES`

同期処理は領域別に分け、他領域のitemを削除しない。

### 6.2 詳細

詳細は1人分に限定して各正本を直接取得する。status itemと正本が不一致なら正本を優先表示し、「集約更新待ち」を警告する。

この二層構成により、一覧性能と追跡可能性を両立する。

## 7. 同期処理

既存の同期を拡張し、次の再実行可能な処理に分ける。

1. `sync_person_event_population`: eventの加入者母集団
2. `sync_reservation_site_subscriber_links`: 予約の候補抽出・一意な自動突合
3. `sync_reservation_site_exam_links`: 予約と実受診の候補抽出・一意な自動対応
4. `sync_person_event_reservation_status`: 有効予約・キャンセル・全予約の状態
5. `sync_person_event_status_items`: ledger・case・受診回数（既存処理を責務別に整理）
6. `sync_person_event_hia_dashboard_status`: ダッシュボード状態（既存）
7. `sync_person_event_export_status`: XML出力・HIAアップロード状態
8. `sync_person_event_fund_delivery_status`: 健保納品状態

各処理は `--event-id` 必須、`--subscriber-id` 任意、`--dry-run` 対応とする。画面は同期を自動実行せず、最終同期日時を表示する。

## 8. 権限と個人情報

初期版は既存の `subscriber_reference.view` を表示権限として利用する。氏名、生年月日、記号番号、住所等は既存の `FULL / MASKED / HIDDEN` をそのまま適用する。

- 通常一覧・詳細はマスク表示を既定とする。
- 完全表示は既存の専用POST導線と監査記録を利用する。
- 予約の電話番号・住所も同じPIIレベルで制御する。
- 閲覧監査にはIDと表示modeだけを保存し、検索値や個人情報を記録しない。

## 9. 不整合検知

初期検知対象:

- 予約はあるが加入者未確定
- キャンセル後の再予約はあるが、古い予約を現在予約として表示している
- 同時に有効な予約が複数あり、現在予約を一意に決められない
- 予約とcaseで受診日または健診機関が不一致
- 予約とcaseの対応候補が複数
- eventに受診回数ルールがある場合の不足または超過
- 受領済みだがactive caseなし
- active caseが複数
- 出力済みだがダッシュボードに反映なし
- ダッシュボード受診済みだが結果未受領
- HIA結果取得済みだが健保納品未完了
- status itemの参照IDが正本に存在しない

不整合は自動修正せず、attention codeと参照先を表示する。

## 10. migration方針

設計段階ではmigrationを作成しない。実装時に必要となる新規DB構造は、予約と加入者の確定関係を持つ `work_other.reservation_site_subscriber_links` を必須とする。予約と実受診の明示的な対応が必要な段階で `work_other.reservation_site_exam_links` を追加する。

eventの受診回数ルールは現時点で確定していないため、既存 `dev_phr.event` へ先行して固定列を追加しない。複数健診種別や期間別回数などの要件を確認してから、単純な必要回数列または別ルールテーブルを選ぶ。

`person_event` 自体へ予約・case・納品ごとの固定列は追加しない。工程追加で横持ち列が増殖するのを避け、一覧用集約は既存 `person_event_status_items` を使う。

## 11. 実装順

1. 予約加入者linkテーブルと突合dry-run
2. 予約突合結果の確認画面
3. 有効予約・キャンセル・複数予約の集約
4. 予約と実受診の対応候補表示
5. 領域別status item同期
6. 健診進捗一覧
7. 人×event詳細
8. 不整合カードと既存画面リンク
9. 実行環境データで件数・性能・誤突合を検証

## 12. 第一段階の実装状況

2026-09-10時点では、健診結果処理への変更を後段へ分離し、次の参照用初期版を実装した。

- `person_event` を母集団とするイベント必須の対象者一覧
- subscriber ID、HIA加入者ID、氏名、記号番号による部分検索
- 既存の予約候補照合による予約件数、直近予約、有効・キャンセル件数の表示
- `person_event_status_items` に同期済みのHIAダッシュボード状態の表示
- 予約候補複数・有効予約複数の注意表示
- 加入者詳細への導線と、加入者情報確認と同じPII権限・閲覧監査

この段階では予約候補を確定関係として保存しない。実行環境で候補なし・一意候補・複数候補の割合を確認した後、`reservation_site_subscriber_links` と予約状態同期を実装する。

ledger、case、XML出力、HIAアップロード、健保納品は初期版一覧へ直接JOINしない。これらは健診結果処理側の同期を整備した後、`person_event_status_items` を介して追加する。
