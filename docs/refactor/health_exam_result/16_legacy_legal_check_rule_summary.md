# 旧法定健診チェック仕様調査メモ

## 1. 概要

旧法定健診チェックは、取り込んだ健診XMLが労基署提出向けの法定健診項目を満たしているかを、XML単位で確認するための一時的な充足判定である。

判定対象は `dev_phr.exam_item_group_identity_members` の `group_code = 'LSIO_Legal_Item'` に登録された同一性項目コードである。`LSIO_Legal_Item` は、`exam_item_groups` 上では「Labor Standards Inspection Office: Regular Health Checkups: Legal Items」と定義され、安衛則44条等の定期健康診断における法定項目グループとして扱われている。

旧処理では、XMLから抽出済みの `work_other.medi_xml_item_values` を入力に、`work_other.medi_lsio_identity_presence` へ「XMLごと・同一性項目ごとの存在事実」を作り、その集約結果を `work_other.medi_xml_ledger` の `lsio_legal_*` カラムへ反映する。

## 2. 関連テーブル

| テーブル | 役割 |
| --- | --- |
| `dev_phr.exam_item_groups` | 健診項目グループの親マスタ。`LSIO_Legal_Item` が法定健診チェック用グループを表す。 |
| `dev_phr.exam_item_group_identity_members` | グループに所属する同一性項目コード単位のルール。必須扱い、presence判定用namecode、判定モードを持つ。 |
| `dev_phr.exam_item_group_members` | グループに所属する `namecode` 単位の項目一覧。`PRESENCE_KEY`、`RESULT_VALUE`、`AUX` の役割を持つ。 |
| `dev_phr.exam_item_group_method_members` | グループに所属する XML method code 単位の項目一覧。旧STEP 1/2では直接参照していない。 |
| `dev_phr.exam_item_master` | `namecode` の項目マスタ。`identity_item_code`、XML method code、値型、XPath、付属2由来の法定/任意情報を持つ。 |
| `work_other.medi_xml_item_values` | XMLから抽出した項目値の縦持ちテーブル。`xml_sha256`、`namecode`、値、出現順を持つ。 |
| `work_other.medi_lsio_identity_presence` | 旧法定健診チェックの中間テーブル。`xml_sha256 × group_code × identity_item_code` ごとに存在した項目だけを記録する。 |
| `work_other.medi_xml_ledger` | XML台帳。旧処理では `lsio_legal_required_count`、`lsio_legal_present_count`、`lsio_legal_is_complete`、`lsio_legal_judged_at` を更新する。 |

## 3. DDL上の責務

`exam_item_groups` はグループそのものを表す。今回の対象では `LSIO_Legal_Item` が、労基署向け定期健康診断の法定項目セットである。

`exam_item_group_identity_members` は、同一性項目コード単位の制度チェックルールを表す。`required_flag = 1` の行がチェック上の必須項目として集計対象になる。`condition_expr` は必須条件式用の列だが、対象データではすべて `NULL` であり、旧STEP 1/2では評価していない。

`required_presence_namecodes` は、通常の `identity_item_code -> namecode` 対応ではなく、presence判定に使う `namecode` を明示するためのCSVである。対象データでは胸部X線 `9N206` のみ、直接撮影と間接撮影の「所見の有無」namecodeをOR集合として持つ。

`presence_value_mode` は、`required_presence_namecodes` をどう評価するかを表す。対象データでは `ANY_NONEMPTY` のみ確認できる。旧STEP 1/2の実装上は、CSV内のいずれかの `namecode` が `medi_xml_item_values` に存在すれば present とする扱いであり、値内容の妥当性までは見ていない。

`exam_item_group_members` は、グループに含める `namecode` の一覧である。`role` により、存在判定のキー、結果値、補助情報を区別できる。ただし旧STEP 1/2ではこのテーブルを直接JOINせず、ルールなし項目は `exam_item_master.identity_item_code` から `namecode` を引いている。

`exam_item_group_method_members` は、グループに含める XML method code の一覧である。旧 `medi_xml_ledger` には `lsio_legal_missing_methods` があるため method 単位の欠損管理を想定していた可能性があるが、今回対象のSTEP 1/2では method ではなく同一性項目コード単位で集約している。

`exam_item_master` は、`namecode` と `identity_item_code` の対応、およびXML抽出に必要な情報を持つ。旧STEP 1では、`required_presence_namecodes` がない必須同一性項目について、`exam_item_master.identity_item_code = exam_item_group_identity_members.identity_item_code` で対応する `namecode` を探す。

## 4. 実データ上の使われ方

`LSIO_Legal_Item` の同一性項目メンバーは29件である。そのうち `required_flag = 1` は25件、`required_flag = 0` は4件である。

`required_flag = 1` の項目は以下である。

| 区分 | 同一性項目コード |
| --- | --- |
| 身体計測・診察 | `9N001` 身長、`9N006` 体重、`9N016` 腹囲、`9N051` 業務歴、`9N056` 既往歴、`9N061` 自覚症状、`9N066` 他覚症状 |
| 血圧・生理検査 | `9A750` 収縮期血圧、`9A760` 拡張期血圧、`9A110` 心電図、`9D100` 聴力、`9E160` 視力、`9N206` 胸部X線 |
| 尿・血液・生化学 | `1A010` 尿蛋白、`1A020` 尿糖、`2A020` 赤血球数、`2A030` 血色素量、`3B035` AST、`3B045` ALT、`3B090` γ-GT、`3D010` 血糖、`3F015` 中性脂肪、`3F050` 総コレステロール、`3F070` HDL、`3F077` LDL |

`required_flag = 0` の項目は、`2A040` ヘマトクリット値、`6A010` 喀痰検査（一般細菌）、`6A205` 喀痰検査（抗酸菌）、`7A010` 喀痰細胞診である。旧STEP 1/2ではこれらは required_count に含まれず、presence作成対象にもならない。

`required_presence_namecodes` がある項目は `9N206` 胸部X線のみである。この行は、同一性項目コードとしては `9N206` だが、presence判定には `9N206160700000011` と `9N221160700000011` を使う。つまり、一般直接撮影と一般間接撮影の「所見の有無」のどちらかがXMLに存在すれば、胸部X線ありとみなす。

`required_presence_namecodes` がない項目は、`exam_item_master.identity_item_code` に紐づくいずれかの `namecode` が `medi_xml_item_values` に存在すれば present とみなす。たとえば血糖 `3D010` は、空腹時血糖・随時血糖など複数の `namecode` が同じ `identity_item_code` に紐づくため、そのいずれかが存在すれば `3D010` が present になる。

注意点として、`LSIO_Legal_Item` の `exam_item_group_members` には `3F050` 総コレステロールの `namecode` が確認できない一方、`exam_item_group_identity_members` には `3F050` が required として登録されている。旧STEP 1は `exam_item_group_members` ではなく `exam_item_master` を参照するため、`exam_item_master` に `3F050` の `namecode` があれば presence 判定自体は動く。この差分が意図したものかは要確認である。

## 5. STEP 1 の処理仕様

STEP 1 は `work_other.medi_lsio_identity_presence` に、XMLごとの必須同一性項目の存在事実を記録する。

処理単位は `xml_sha256 × group_code × identity_item_code` である。`present_flag` は存在した場合のみ `1` が入る。存在しない項目の行は作らない。

`required_presence_namecodes` がある場合は、`dev_phr.exam_item_group_identity_members` と `work_other.medi_xml_item_values` を、`FIND_IN_SET(v.namecode, g.required_presence_namecodes) > 0` でJOINする。対象は `group_code = 'LSIO_Legal_Item'`、`required_flag = 1`、`presence_value_mode = 'ANY_NONEMPTY'` の行である。CSV内のどれかの `namecode` がXMLに存在すれば、その `identity_item_code` を present とする。

`required_presence_namecodes` がない場合は、`dev_phr.exam_item_group_identity_members` から `dev_phr.exam_item_master` へ `identity_item_code` でJOINし、さらに `work_other.medi_xml_item_values` へ `namecode` でJOINする。対象は `group_code = 'LSIO_Legal_Item'`、`required_flag = 1`、`required_presence_namecodes IS NULL` の行である。対応する `namecode` のどれかがXMLに存在すれば、その `identity_item_code` を present とする。

元の単体STEP 1ファイルでは、2つのSELECTを `UNION ALL` し、`ON DUPLICATE KEY UPDATE` で `present_flag` を更新する。統合版のSTEP 2ファイル内コメントでは、LSIO分のpresenceを一度削除してから再生成する方針になっている。

旧STEP 1は、値の中身、コード値、単位、nullFlavor、数値妥当性までは評価しない。`medi_xml_item_values` に対象 `namecode` の行が存在するかどうかを presence として扱う。

## 6. STEP 2 の処理仕様

STEP 2 は、`work_other.medi_lsio_identity_presence` をXML単位に集約し、`work_other.medi_xml_ledger` の `lsio_legal_*` カラムへ反映する。

まず、`dev_phr.exam_item_group_identity_members` から `group_code = 'LSIO_Legal_Item'` かつ `required_flag = 1` の件数を数え、`required_count` とする。対象データでは25件である。

次に、`medi_lsio_identity_presence` を `xml_sha256` 単位に集約し、`COUNT(DISTINCT identity_item_code)` を `present_count` とする。presenceテーブルには present の行しかないため、欠損項目はこの集約には現れない。

`medi_xml_ledger` は、集約結果をLEFT JOINして更新される。JOIN条件は統合版SQL上では `s.xml_sha256 = l.zip_inner_path_sha256` であり、コメントに「`zip_inner_path_sha256` に `xml_sha256` が入っている前提」とある。ただし `medi_xml_ledger` DDLには別途 `xml_sha256` カラムも存在するため、このJOIN前提は要確認である。

更新内容は以下である。

| ledgerカラム | 更新内容 |
| --- | --- |
| `lsio_legal_required_count` | 必須同一性項目数。対象データでは25。 |
| `lsio_legal_present_count` | presenceから集約した present 同一性項目数。presenceがないXMLは0。 |
| `lsio_legal_is_complete` | `required_count = present_count` のとき1、それ以外は0。 |
| `lsio_legal_judged_run_id` | 旧SQLでは `NULL` を設定。 |
| `lsio_legal_judged_at` | 更新時刻 `NOW(6)` を設定。 |

`missing_items` や `missing_methods` は、統合版STEP 2では作らない方針とコメントされている。`medi_xml_ledger.lsio_legal_missing_methods` はDDL上存在するが、今回対象SQLでは更新しない。

## 7. v2設計への示唆

v2では、旧 `work_other.medi_lsio_identity_presence` のような中間presenceテーブルを作らず、`health_exam_result.exam_item_values` と制度チェック用ルールマスタから、直接 `health_exam_result.exam_check_results` を生成する想定である。

既存の `dev_phr.exam_item_group_*` 系マスタは、制度チェックのルールマスタとして流用できる可能性がある。特に `exam_item_group_identity_members` は、同一性項目コード単位で required、presence判定用namecode、判定モードを持っており、旧法定健診チェックの中心ルールになっている。

一方で、現行データは旧LSIOチェックに必要な最小限のpresence判定に寄っている。`docs/spec/health_examinations/02_exam_check_item_spec_v2_0_0.md` が要求する、算出、代替、条件付き必須、特定健診との共通化、項目別 `status` / `reason` まで表現できるかは別途確認が必要である。

特定健診ルールを同じ仕組みに追加する場合も、同一性項目コード単位のグループを作る方針は合いそうである。ただし、特定健診ではBMI、non-HDL、メタボ判定、保健指導レベル、服薬・質問票など、算出・代替・条件付き・情報入手時のみ報告の扱いが増えるため、`required_flag` と `ANY_NONEMPTY` だけでは不足する可能性が高い。

## 8. 未確認・要確認事項

- 算出項目: BMI、non-HDL、メタボ判定、保健指導レベルなどを、ルールマスタ上でどう表すかは要確認。
- 代替項目: 血糖とHbA1c、腹囲と内臓脂肪面積、LDLとnon-HDLなどの代替関係を、旧 `required_presence_namecodes` のOR集合だけで足りるかは要確認。
- 条件付き必須: 年齢、省略基準、医師判断、実施理由要否などを、`condition_expr` や別テーブルで表現する必要があるか要確認。
- 特定健診への拡張: `LSIO_Legal_Item` と同じ `exam_item_group_*` 構造で特定健診グループを作れるか要確認。
- status/reason: v2の `status_9N001` / `reason_9N001` のような項目別結果へ落とすには、旧presenceだけでは「値あり」以外の理由情報が不足する。
- 値妥当性: 旧STEP 1は `namecode` の存在しか見ていない。空文字、nullFlavor、コード体系、単位、範囲外、不正値をどう扱うか要確認。
- `3F050` 差分: `exam_item_group_identity_members` では必須だが、`exam_item_group_members` には該当 `namecode` が見当たらない。旧STEP 1は `exam_item_master` を使うため動作するが、マスタ整合性として要確認。
- ledger JOINキー: STEP 2では `zip_inner_path_sha256` に `xml_sha256` が入っている前提でJOINしている。DDL上の `xml_sha256` カラムとの使い分けは要確認。
- `lsio_legal_missing_methods`: DDL上は存在するが、今回対象SQLでは更新しない。method単位の欠損を今後使うか要確認。

## v2でこの仕組みを使う場合の確認ポイント

- `02_exam_check_item_spec_v2_0_0.md` の制度チェック対象項目と、`LSIO_Legal_Item` の25必須項目・4任意項目の差分を棚卸しする。
- `exam_item_group_identity_members` に、算出、代替、条件付き必須、情報入手時のみ報告、実施理由要否を表現する列または関連テーブルが必要か確認する。
- `presence_value_mode = ANY_NONEMPTY` の正式な意味を、単なる行存在か、非空値か、nullFlavor除外まで含むか決める。
- `exam_item_group_members`、`exam_item_group_method_members`、`exam_item_master` のどれをチェック時の正とするか決める。
- `exam_check_results` の `status_<identity_item_code>` / `reason_<identity_item_code>` に落とすためのステータスコードと理由コードを定義する。
- 法定健診と特定健診を同じ集計器で処理する場合、グループコード、必須判定、代替判定、総合判定を制度別に切り替えられるルール構造にする。
- 旧 `medi_lsio_identity_presence` のような中間テーブルを作らない場合でも、デバッグ用にXMLごとの項目別判定根拠を `exam_check_results` またはログで追えるようにする。
