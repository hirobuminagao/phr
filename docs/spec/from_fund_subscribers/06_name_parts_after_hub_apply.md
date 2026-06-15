# Name Parts After Hub Apply

## Overview

目的:

- Hub apply 後に作成された subscribers を対象とする
- import 時点では補完できなかった name parts を後追い補完する
- staging_subscribers_fund を name parts の補完元とする

対象:

- staging_subscribers_fund
- subscribers

---

## Background

import 時点では subscribers に存在しない加入者が存在する。

そのため、通常の name parts 補完では対象外となる場合がある。

Hub apply 後に subscribers が作成された後、後追いで name parts を補完するための仕組みを定義する。

---

## Preconditions

前提条件:

```text
fund import
↓
staging_subscribers_fund

↓

HIA登録
↓
HIA export
↓
Hub apply
↓
subscribers 作成
```

Hub apply 完了後に実施する。

---

## Processing Flow

```text
staging_subscribers_fund
↓
補完候補抽出

↓

subscribers 再探索

↓

parts_apply_subscriber_id 解決
parts_apply_status 更新
parts_apply_reason 更新

↓

name parts 補完
```

---

## Operation Procedure

本処理は、Hub apply 後に subscribers が作成された後で実施する。

処理は以下の3段階に分ける。

```text
1. parts_apply_subscriber_id 解決
2. name parts 補完 apply
3. parts_apply_status 完了更新
```

### 1. parts_apply_subscriber_id 解決

対象:

```text
staging_subscribers_fund.parts_apply_subscriber_id IS NULL
```

解決条件:

```text
identity_hash が同一の subscribers.id を一意に特定できること
```

identity_hash には、insurer_number / birth / gender_code 等の identity 要素を含むため、
本処理では identity_hash 一致を補完先解決の基準とする。

一致しない場合、または複数候補が存在する場合は、再実行可能な未解決状態として扱う。

その場合、parts_apply_subscriber_id / parts_apply_status / parts_apply_reason は NULL のままとする。

判定結果は staging_subscribers_fund に保存する。

```text
parts_apply_subscriber_id
parts_apply_status
parts_apply_reason
```

本処理は identity_hash 起点の parts_apply refresh として扱う。

通常 import では matched_subscriber_id を起点に parts_apply_subscriber_id を解決する。

After Hub Apply Backfill では identity_hash を起点に parts_apply_subscriber_id を解決する。

status:

```text
IDENTITY_MATCHED
```

identity_hash により補完先 subscribers.id を解決できた行を、name parts 補完 apply の対象候補とする。

ID が解決できない行は status を付与しない。

### 2. name parts 補完 apply

以下を満たす行のみ subscribers へ補完する。

```text
parts_apply_subscriber_id IS NOT NULL
parts_apply_status = IDENTITY_MATCHED
staging 側の漢字 parts またはカナ parts が有効な parts として成立している
subscriber 側の該当 parts グループの全項目が NULL または空文字
```

漢字グループとカナグループは独立して判定する。

片方のグループのみ補完条件を満たす場合は、そのグループのみ補完対象とする。

parts は漢字グループ・カナグループをそれぞれ独立して扱う。

```text
漢字 parts:
family_name_kanji
middle_name_kanji
given_name_kanji

カナ parts:
family_name_kana
middle_name_kana
given_name_kana
```

staging 側 parts は、1グループ内で2項目以上に値がある場合のみ補完元として有効とする。

1項目のみ値がある場合は、分割できていないものとして扱い、補完対象外とする。

identity_hash により同一人物であることを確認済みのため、parts 単位の個別照合は行わない。

漢字 parts とカナ parts は、それぞれのグループ単位で補完する。

グループ内の一部項目のみを個別補完するのではなく、有効な parts グループを一式で補完する。

full name と parts の整合管理は、HIA export → staging_subscribers_hub → Hub apply 側の責務とする。

補完対象:

```text
family_name_kanji
middle_name_kanji
given_name_kanji
family_name_kana
middle_name_kana
given_name_kana
```

該当グループに既存値がある場合は上書きしない。

補完結果は audit に記録する。

### 3. parts_apply_status 完了更新

name parts 補完 apply 後、staging 行の parts_apply_status を更新する。

status:

```text
PARTS_APPLIED
PARTS_FAILED
```

### PARTS_APPLIED

漢字 parts またはカナ parts のいずれか1グループ以上を補完した場合に設定する。

補完内容は subscribers_audit に記録する。

### PARTS_FAILED

parts_apply_subscriber_id は解決済みだが、漢字グループ・カナグループのいずれも補完対象とならなかった場合に設定する。

失敗・スキップ理由は parts_apply_reason に記録する。

例:

```text
PARTS_GROUP_ALREADY_FILLED
NO_STAGING_PARTS
INVALID_STAGING_PARTS
NOTHING_TO_UPDATE
```

補完対象が無い場合、既存値があり上書きしなかった場合、staging 側 parts が1項目のみで有効な parts として成立しない場合などは PARTS_FAILED とする。

---

## Matching Policy

補完対象探索時は subscribers の現在状態を利用する。

matched_subscriber_id は import 時点の判定結果として扱う。

後追い補完では、matched_subscriber_id を直接の更新対象にしない。

補完実行時点の解決結果は parts_apply_subscriber_id に保持する。

parts_apply_subscriber_id の解決は identity_hash 一致を基準とする。

本 spec は After Hub Apply Backfill を対象とするため、identity_hash 起点の parts_apply refresh を定義する。

通常 import 側では matched_subscriber_id 起点の parts_apply refresh を利用する。

identity_hash で一意に一致した場合のみ IDENTITY_MATCHED とする。

一致しない場合、または複数候補がある場合は、parts_apply_subscriber_id / parts_apply_status / parts_apply_reason は NULL のままとする。

これは後続の Hub apply 後に再実行できるようにするためである。

---

## Column Responsibilities

### matched_subscriber_id

```text
import 時点の判定結果
```

### parts_apply_subscriber_id

```text
補完実行時点の判定結果
```

matched_subscriber_id と同一であることを保証しない。

---

## Update Targets

staging_subscribers_fund:

- parts_apply_subscriber_id
- parts_apply_status
- parts_apply_reason

subscribers:

- family_name_kanji
- middle_name_kanji
- given_name_kanji
- family_name_kana
- middle_name_kana
- given_name_kana

---

## parts_apply_reason Policy

parts_apply_reason は、主に PARTS_FAILED の詳細理由を保持する。

PARTS_APPLIED の更新詳細は subscribers_audit を正とする。

想定 reason:

```text
PARTS_GROUP_ALREADY_FILLED
NO_STAGING_PARTS
INVALID_STAGING_PARTS
NOTHING_TO_UPDATE
```

NO_STAGING_PARTS:
漢字 parts / カナ parts のいずれにも補完元となる値が存在しない。

INVALID_STAGING_PARTS:
補完元となる値は存在するが、1グループ内の値が1項目のみであり、有効な parts として成立しない。

PARTS_GROUP_ALREADY_FILLED:
subscriber 側の該当 parts グループに既存値があるため、グループ単位で補完対象外とする。

複数理由がある場合は、固定順で `|` 区切りにして記録する。

該当する理由は1つに丸めず、複数理由を保持する。

固定順:

```text
NO_STAGING_PARTS
INVALID_STAGING_PARTS
PARTS_GROUP_ALREADY_FILLED
NOTHING_TO_UPDATE
```

reason の役割:

```text
status = 大分類
reason = 失敗・スキップ理由
subscribers_audit = 実際に更新した内容
```

---

## Audit

補完処理は subscribers_audit に監査ログを記録する。

監査ログは更新項目ごとに1レコードずつ記録する。

例:

```text
family_name_kanji
given_name_kanji
family_name_kana
given_name_kana
```

PARTS_APPLIED の更新詳細は subscribers_audit を正とする。

---

## Future Work

- 判定条件詳細化
- dry-run 時の件数サマリ仕様明文化
- 運用手順整備