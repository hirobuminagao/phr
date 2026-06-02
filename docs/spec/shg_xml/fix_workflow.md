# SHG XML Fix Workflow

本ドキュメントは、現行 `check_shg_result_xml.py` の fix（XML補正）責務を対象とする。

現行 v1 では、XML / ZIP入力、identity_hash pairing、利用券fix、outcome判定、CSV出力までを実装済みとする。

## 目的

本ドキュメントは、SHG結果XMLチェック処理における fix（XML自動修正）運用の方針を定義する。

本fixは「XML内容の妥当性確認」と「機械的に確定可能な最小範囲のXML補正」を目的とする。

人手判断が必要な項目については、スクリプトによる推定修正を行わない。

## fix対象

現時点で自動修正対象とする項目は以下のみとする。

- 利用券整理番号
- 利用券有効期限

上記2項目は、DB側を正としてXML修正対象にできる。

現行実装では、利用券差異判定後に必要時のみXML更新を行う。

## fix対象外

以下は自動修正対象にしない。

- outcome系結果
- 一般カテゴリ判定
- 腹囲体重判定
- 喫煙判定
- 面談方式
- 支援実績
- 日付系判定
- level_code / level_text

これらはチェック結果のみ出力し、人手確認・手動修正対象とする。

これらは outcome判定・業務判定・保健指導ロジックに関わるため、現フェーズでは自動修正対象に含めない。

## 利用券と受診券の扱い

利用券と受診券は必ず別物として扱う。

- 利用券 ≠ 受診券
- 利用券整理番号 ≠ 受診券整理番号
- 利用券有効期限 ≠ 受診券有効期限

利用券判定は必ず `functionCode/@code = "2"` を用いる。

利用券ノード特定時に、participant順・node位置・id出現順のみを根拠にしてはならない。

出現順・位置・participant順による推定は禁止する。

## XML解析時の禁止事項

以下のような実装は禁止する。

- 先頭 participant を利用券とみなす
- 2件目 participant を受診券とみなす
- 最初の id を利用券整理番号とみなす
- section順で意味を推定する
- 出現順のみで initial / final を推定する

意味判定は必ず code / codeSystem / functionCode を用いる。

特に、XML構造の「たまたまの並び順」に依存したfix実装は禁止する。

## report_code の扱い

現フェーズでは以下のみを対象とする。

| report_code | 用途 | 扱い |
|---|---|---|
| 21 | initial XML | チェック対象 |
| 22 | final XML | チェック対象 |
| 23 | 中間・その他 | 現時点では対象外 |

`report_code = 23` は pair対象・fix対象に含めない。

現行 v1 では、21 / 22 の initial / final を中心に outcome集計を行う。

## pair 判定

現フェーズでは ZIP内 `identity_hash` ベースで pair 判定を行う。

- 同一 identity_hash の 21 を initial XML とする
- 同一 identity_hash の 22 を final XML とする
- initial / final が両方存在する場合は pair とする
- 片側のみ存在する場合も単独XMLとして扱う

現行実装では ZIP内 pair を基本とするが、内部束ねキーは `identity_hash` を使用する。

## finalのみ動機づけ支援の扱い

以下条件を満たす場合、腹囲体重以外の outcome 矛盾として扱わない。

- report_code = 22
- initial XML が存在しない
- 保健指導区分が動機づけ支援
- 対象カテゴリが腹囲体重以外

理由:

動機づけ支援では、計画情報が initial XML（21）側にしか存在しないケースがある。

そのため、final XML単体では目標と結果の矛盾を機械的に確定できない。

腹囲体重は、実測値とDB健診時値から再判定可能なため除外対象には含めない。

この除外ポリシーは `outcome_policy.py` へ外出し済みとする。

## fix実行タイミング

fix処理は XML単位抽出後、people集約前に実施する。

概略順序:

```text
XML読込
↓
basic抽出
↓
identity_bundle生成
↓
DB値取得
↓
利用券差異判定
↓
必要ならXML修正
↓
XML単位CSV記帳
↓
people集約
↓
outcome判定
```

実装上、利用券fixは以下の2段階に分かれる。

1. `ticket_fix.py`
   - XML値とDB値を比較する
   - fix要否と対象フィールドを `TicketFixResult` として返す
   - XML更新は行わない

2. `xml_ticket_writer.py`
   - `TicketFixResult` を受け取り、利用券XML更新へ橋渡しする
   - `basic.py` の `get_ticket_info()` から利用券整理番号・有効期限の location を取得する
   - `scripts.lib.shg.xml.update.update_xml_value()` を使って既存XML属性値を更新する
   - 更新が発生した場合のみ `save_xml()` で同じXMLパスへ保存する

## fix単位

XML修正自体は XML単位で行う。

ただし、展開フォルダの保持 / 削除判定は ZIP単位で行う。

ZIP由来XMLの作業ディレクトリ管理は `xml_io.py` が担当する。

## ZIP展開フォルダの扱い

現行実装では、ZIPファイルは `_work_zip_extract/` 配下へ展開して処理する。

### ZIP内に1件でもfixがある場合

ZIP内に1件でもfix対象XMLが存在する場合:

- ZIP展開フォルダを保持する
- 修正対象XMLだけでなく、ZIP内の全XMLを保持する
- 修正されなかったXMLも同じ展開フォルダへ残す

理由:

後続で人手修正・確認を行う可能性があるため。

fix済みXMLのみを別ディレクトリへ隔離する運用は行わない。

## ZIP内に1件もfixがない場合

ZIP内に1件もfix対象が存在しない場合:

- ZIP展開フォルダを削除する

## CSVへの記帳

現行 `export_shg_report.csv` では、XML単位で fix関連情報を保持する。

`ticket_fix_status` は主に `ticket_fix.py` 側の fix要否判定結果を表す。

XML更新そのものの結果は、`xml_ticket_writer.py` 側の `XmlTicketWriteResult` として管理する。

- XML側利用券整理番号
- XML側利用券有効期限
- DB側利用券整理番号
- DB側利用券有効期限
- ticket_fix_status

### ticket_fix_status

| 値 | 意味 |
|---|---|
| NO_DIFF | XML値とDB値が一致 |
| FIX_REQUIRED | XML値とDB値に差異があり、XML更新対象がある |
| SKIPPED_NO_DB | DB値不足でfix要否を確定できない |
| SKIPPED_NO_XML | XML側の利用券値が不足しており、比較またはfix判定を完了できない |

必要に応じて、追加の warning / review 系列を将来的に拡張する。

### XmlTicketWriteResult

`xml_ticket_writer.py` は、XML更新結果を `XmlTicketWriteResult` として返す。

主な項目:

| 項目 | 意味 |
|---|---|
| updated | XML属性値を1項目以上更新したか |
| updated_fields | 更新できた項目名の一覧 |
| reason | 更新結果または失敗理由 |
| update_results | `update_xml_value()` の項目別結果 |
| save_result | `save_xml()` の保存結果。保存が発生しない場合は `None` |

`ticket_fix_status = FIX_REQUIRED` は「更新が必要」という判定であり、保存完了を意味しない。

実際に保存されたかどうかは `XmlTicketWriteResult.updated` と `save_result` を確認する。

## 禁止事項

以下は禁止する。

- 元ZIPを上書きする
- XML単位で展開フォルダを部分削除する
- fix対象XMLだけを別ディレクトリへ移動する
- 推定修正を行う
- 受診券情報を利用券へ転記する
- 新規XMLブロックを作成して補完する

特に「推定で直す」実装は禁止し、fix可能条件を明示的に満たす場合のみXML補正を許可する。

## 将来方針

現フェーズでは ZIP内 pair を使用する。

将来的には、DBへ格納したイベント単位管理へ移行する。

```text
現行 v1:
  ZIP内 identity_hash pair
  XML / ZIP単位 orchestration

将来フェーズ:
  DB event / person 単位管理
  年度横断pair
  DBベース outcome追跡
```

## 現在位置（2026-06時点）

現時点では、以下を現行実装済みとして扱う。

- XML / ZIP入力対応
- `identity_hash` pairing
- 利用券番号 / 有効期限の差異判定と必要時XML更新
- XML更新
- `xml_ticket_writer.py` は比較・fix要否判定を行わず、既存XML属性値の更新と保存のみを担当
- finalのみ動機づけ支援のoutcome矛盾除外
- ZIP単位作業フォルダ管理
- `ticket_fix.py` / `xml_ticket_writer.py` / `outcome_policy.py` 外出し

今後は、初回面談方式、目標取得精度、outcome最終ルール、CSV表示整理を優先的に調整する。