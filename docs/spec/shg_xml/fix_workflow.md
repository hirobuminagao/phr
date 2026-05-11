

# SHG XML Fix Workflow

## 目的

本ドキュメントは、SHG結果XMLチェック処理における fix（XML自動修正）運用の方針を定義する。

本fixは「XML内容の妥当性確認」と「最低限の機械修正」を目的とする。

人手判断が必要な項目については、スクリプトによる推定修正を行わない。

## fix対象

現時点で自動修正対象とする項目は以下のみとする。

- 利用券整理番号
- 利用券有効期限

上記2項目は、DB側を正としてXML修正対象にできる。

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

## 利用券と受診券の扱い

利用券と受診券は必ず別物として扱う。

- 利用券 ≠ 受診券
- 利用券整理番号 ≠ 受診券整理番号
- 利用券有効期限 ≠ 受診券有効期限

利用券判定は必ず `functionCode/@code = "2"` を用いる。

出現順・位置・participant順による推定は禁止する。

## XML解析時の禁止事項

以下のような実装は禁止する。

- 先頭 participant を利用券とみなす
- 2件目 participant を受診券とみなす
- 最初の id を利用券整理番号とみなす
- section順で意味を推定する
- 出現順のみで initial / final を推定する

意味判定は必ず code / codeSystem / functionCode を用いる。

## report_code の扱い

現フェーズでは以下のみを対象とする。

| report_code | 用途 | 扱い |
|---|---|---|
| 21 | initial XML | チェック対象 |
| 22 | final XML | チェック対象 |
| 23 | 中間・その他 | 現時点では対象外 |

`report_code = 23` は pair対象・fix対象に含めない。

## pair 判定

現フェーズでは ZIP内 `identity_hash` ベースで pair 判定を行う。

- 同一 identity_hash の 21 を initial XML とする
- 同一 identity_hash の 22 を final XML とする
- initial / final が両方存在する場合は pair とする
- 片側のみ存在する場合も単独XMLとして扱う

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

## fix実行タイミング

fix処理は XML単位抽出後、people集約前に実施する。

概略順序:

```text
XML読込
↓
basic抽出
↓
DB値取得
↓
利用券差異判定
↓
必要ならXML修正
↓
CSV記帳
↓
people集約
```

## fix単位

XML修正自体は XML単位で行う。

ただし、展開フォルダの保持 / 削除判定は ZIP単位で行う。

## ZIP展開フォルダの扱い

### ZIP内に1件でもfixがある場合

ZIP内に1件でもfix対象XMLが存在する場合:

- ZIP展開フォルダを保持する
- 修正対象XMLだけでなく、ZIP内の全XMLを保持する
- 修正されなかったXMLも同じ展開フォルダへ残す

理由:

後続で人手修正・確認を行う可能性があるため。

## ZIP内に1件もfixがない場合

ZIP内に1件もfix対象が存在しない場合:

- ZIP展開フォルダを削除する

## CSVへの記帳

CSVには最低限以下を出力する。

- XML側利用券整理番号
- XML側利用券有効期限
- DB側利用券整理番号
- DB側利用券有効期限
- ticket_fix_status

### ticket_fix_status

| 値 | 意味 |
|---|---|
| NO_DIFF | XML値とDB値が一致 |
| FIXED | DB値でXML修正した |
| SKIPPED_NO_DB | DB値不足で修正不可 |
| SKIPPED_NO_TICKET_NODE | 利用券ノードを特定できない |

## 禁止事項

以下は禁止する。

- 元ZIPを上書きする
- XML単位で展開フォルダを部分削除する
- fix対象XMLだけを別ディレクトリへ移動する
- 推定修正を行う
- 受診券情報を利用券へ転記する

## 将来方針

現フェーズでは ZIP内 pair を使用する。

将来的には、DBへ格納したイベント単位管理へ移行する。

```text
現フェーズ:
  ZIP内 identity_hash pair

将来フェーズ:
  DB event / person 単位管理
```