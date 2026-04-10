# 02 Header and Non-Section Spec

## 1. 目的

本ドキュメントは、特定保健指導（SHG）XMLにおける「CDAセクション以外」の要素を整理するためのspecである。

対象は、section（90010 / 90020 / 90030 ...）そのものではなく、
それらの外側にある文書全体構造・利用者情報・利用券情報・保健指導実施情報・提出元情報などとする。

本specでは、実装観点で以下を整理する。

- ClinicalDocument 全体構造
- report_code / 文書種別の整理
- 利用者情報
- 利用券情報
- 保健指導実施情報
- 作成者 / 管理者 / 提出元情報
- participant / documentationOf / serviceEvent の役割

---

## 2. 対象範囲

- 特定保健指導 XML の section 外要素
- ClinicalDocument 直下の header 系要素
- participant
- documentationOf / serviceEvent
- author / custodian / recordTarget

---

## 3. 一次情報

- 厚生労働省 `5-1A.pdf` 特定保健指導情報ファイル仕様説明書 Version 4
  - 2.1 本文書の位置付け
  - 2.2 記載内容の優先度
  - 2.3.1 1報告1ファイル
  - 2.3.3 HL7 CDA 規格との関係
  - 3.2 ヘッダ部
  - 3.2.2 CDA 管理情報
  - 3.2.3 保健指導管理情報
- 特定保健指導 XML スキーマ（`hg08_V08.xsd` ほか）

本ファイルでは、PDF本文に明示されている事実を優先し、XMLスキーマは補助的に参照する。
PDF本文とスキーマの記述に差がある場合は、仕様書本文の記述を優先する。

---

## 4. 整理対象（section以外）

```yaml
non_section_targets:
  CDA_header:
    - ClinicalDocument
    - typeId
    - id
    - code
    - title
    - effectiveTime
    - confidentialityCode
    - languageCode
    - setId
    - versionNumber

  management_information:
    - recordTarget
    - author
    - custodian
    - participant
    - documentationOf
```

---

## 5. ClinicalDocument 全体構造（概要）

```yaml
ClinicalDocument:
  xmlns:
  xsi:schemaLocation:
  typeId:
  id:
  code:
  title:
  effectiveTime:
  confidentialityCode:
  languageCode:
  setId:
  versionNumber:
  recordTarget:
  author:
  custodian:
  participant:
  documentationOf:
  component:
    structuredBody:
      component:
        section:
```

- 5-1A は HL7 CDA Release 2 に完全準拠する前提で記述されている
- 本ファイルでは header 部および section 外要素のみを扱い、section の詳細は後続の section spec 側で整理する

---

## 6. report_code / 文書種別

```yaml
report_document:
  xpath: /ClinicalDocument/code/@code
  codeSystem: 1.2.392.200119.6.1001
```

- 報告区分コードは `ClinicalDocument/code` で表現する
- ヘッダ部サンプルでは `code="21"` が「特定保健指導の初回報告」として示されている
- 初回XML / 最終XML / その他の区別に関わる header 要素である
- 各 code 値の完全一覧は、後続で別ファイルへ整理する

---

## 7. 利用者情報

```yaml
subject_information:
  xpath_root: /ClinicalDocument/recordTarget
  xml_spec_no: 6
  multiplicity: 1..1
  includes:
    - insurer number
    - insurance symbol
    - insurance number
    - insurance branch number
    - postal code
    - name_kana
    - gender
    - birth date
```

```yaml
recordTarget_xpaths:
  insurer_number:
    xpath: /ClinicalDocument/recordTarget/patientRole/id[@root="1.2.392.200119.6.101"]/@extension
    xml_spec_no: 6.8.1

  insurance_symbol:
    xpath: /ClinicalDocument/recordTarget/patientRole/id[@root="1.2.392.200119.6.204"]/@extension
    xml_spec_no: 6.9.1

  insurance_number:
    xpath: /ClinicalDocument/recordTarget/patientRole/id[@root="1.2.392.200119.6.205"]/@extension
    xml_spec_no: 6.10.1

  insurance_branch_number:
    xpath: /ClinicalDocument/recordTarget/patientRole/id[@root="1.2.392.200119.6.211"]/@extension
    xml_spec_no: 6.11.1

  postal_code:
    xpath: /ClinicalDocument/recordTarget/patientRole/addr/postalCode/text()
    xml_spec_no: 6.13.1

  name_kana:
    xpath: /ClinicalDocument/recordTarget/patientRole/patient/name/text()
    xml_spec_no: 6.15.1

  gender_code:
    xpath: /ClinicalDocument/recordTarget/patientRole/patient/administrativeGenderCode/@code
    xml_spec_no: 6.16.1
    allowed:
      - "1"
      - "2"

  birth_date:
    xpath: /ClinicalDocument/recordTarget/patientRole/patient/birthTime/@value
    xml_spec_no: 6.17.1
    format: YYYYMMDD
```

```yaml
recordTarget_structure:
  recordTarget:
    patientRole:
      id:
        - @extension
        - @root
      addr:
        postalCode:
          text()
      patient:
        name:
          text()
        administrativeGenderCode:
          @code
          @codeSystem: 1.2.392.200119.6.1104
        birthTime:
          @value
```

- 利用者情報は `recordTarget` に記述される
- 5-1A では、被保険者証等番号に関する情報は `participant` ではなく `recordTarget` に記述されると明記されている
- 保険者番号・記号・番号・枝番は、いずれも `patientRole/id` で表現され、`@root` の違いで識別する
- 郵便番号は `patientRole/addr/postalCode/text()` で表現される
- 利用者のカナ氏名は `patientRole/patient/name/text()` で表現される
- 性別は `administrativeGenderCode/@code` で表現され、男=`1`、女=`2`
- 生年月日は `birthTime/@value` で表現され、書式は `YYYYMMDD`
- 保健指導情報では利用者の住所本文はなく、郵便番号のみが出現する点に注意する

---

## 8. 利用券情報

```yaml
ticket_information:
  xpath_root: /ClinicalDocument/participant
  typeCode: HLD
  multiplicity: 0..2
  functionCode:
    1: 受診券
    2: 利用券
```

```yaml
participant_ticket_xpaths:
  ticket_type_code:
    receive_ticket:
      xpath: /ClinicalDocument/participant[functionCode/@code="1"]/functionCode/@code
      expected: "1"
    guidance_ticket:
      xpath: /ClinicalDocument/participant[functionCode/@code="2"]/functionCode/@code
      expected: "2"

  guidance_ticket_expiration:
    xpath: /ClinicalDocument/participant[functionCode/@code="2"]/time/high/@value
    xml_spec_no: 9.4.1

  guidance_ticket_number:
    xpath: /ClinicalDocument/participant[functionCode/@code="2"]/associatedEntity/id/@extension
    xml_spec_no: 9.6.1

  guidance_ticket_insurer_number:
    xpath: /ClinicalDocument/participant[functionCode/@code="2"]/associatedEntity/scopingOrganization/id/@extension
    xml_spec_no: 9.8.1

  receive_ticket_number:
    xpath: /ClinicalDocument/participant[functionCode/@code="1"]/associatedEntity/id/@extension
    xml_spec_no: 9.6.1

  receive_ticket_insurer_number:
    xpath: /ClinicalDocument/participant[functionCode/@code="1"]/associatedEntity/scopingOrganization/id/@extension
    xml_spec_no: 9.8.1
```

```yaml
participant_ticket_structure:
  participant:
    @typeCode: HLD
    functionCode:
      @code:
      @codeSystem: 1.2.392.200119.6.208
    time:
      high:
        @value:
    associatedEntity:
      @classCode: IDENT
      id:
        @extension:
        @root:
      scopingOrganization:
        id:
          @extension:
          @root:
```

- `participant` は受診券情報・利用券情報および所属保険者情報を表現する
- 利用券は `functionCode code="2"` で識別する
- 利用券の有効期限は `time/high/@value` で表現する
- 利用券整理番号は `associatedEntity/id/@extension` で表現する
- 利用券を発行した保険者番号は `associatedEntity/scopingOrganization/id/@extension` で表現する
- 受診券整理番号と受診券発行保険者番号も同様に `functionCode code="1"` 側で取得する
- `participant` の多重度は `0..2` で、受診券のみ / 利用券のみ / 両方あり のいずれにも対応する
- 健診および保健指導がそれぞれ個別契約で実施され、受診券も利用券も存在しない場合には `participant` は出現しない
- 保険者番号は `recordTarget` 側で記述される保険者番号と同一でなければならず、値が異なる場合は `recordTarget` 側を正とする
- 特定健診当日に初回面接を実施した場合、利用券整理番号および有効期限には受診券の整理番号（種別番号「5」）および有効期限を設定するため注意が必要

---

## 9. 保健指導実施情報

```yaml
guidance_execution_information:
  xpath_root: /ClinicalDocument/documentationOf
  xml_spec_no: 10
  multiplicity: 1..1
  includes:
    - guidance level
    - execution date / period
```

```yaml
documentationOf_xpaths:
  guidance_level_code:
    xpath: /ClinicalDocument/documentationOf/serviceEvent/code/@code
    xml_spec_no: 10.1.1
    codeSystem: 1.2.392.200119.6.1006

  execution_date:
    xpath: /ClinicalDocument/documentationOf/serviceEvent/effectiveTime/@value
    xml_spec_no: 10.3.1

  execution_date_low:
    xpath: /ClinicalDocument/documentationOf/serviceEvent/effectiveTime/low/@value
    xml_spec_no: 未確認

  execution_date_high:
    xpath: /ClinicalDocument/documentationOf/serviceEvent/effectiveTime/high/@value
    xml_spec_no: 未確認
```

```yaml
documentationOf_structure:
  documentationOf:
    serviceEvent:
      code:
        @code:
        @codeSystem: 1.2.392.200119.6.1006
      effectiveTime:
        @value:
        low:
          @value:
        high:
          @value:
```

- `documentationOf` は保健指導実施情報を表現する
- 5-1A では `documentationOf` の多重度は `1..1 M` とされ、保健指導実施情報の本体として扱われている
- 保健指導区分コードは `serviceEvent/code/@code` で表現される
- 実施日付は `serviceEvent/effectiveTime/@value` で表現される
- 2017年版の修正履歴では、初回面接を分割して行っている場合、初回面接実施日付は「初回面接②の実施日付」であることが追記されている
- 実装では、単一日付 `@value` と期間表現 `low/high` の両方があり得る前提で扱う
- initial_date / final_date の実装では、どの XPath を優先するかを別途明示する必要がある

---

## 10. 提出元 / 作成者 / 管理者情報

```yaml
organization_information:
  - author
  - custodian
```

- `author` は本ファイルの作成者情報を表現する
- `custodian` は本ファイル作成管理責任機関情報を表現する
- 5-1A では `custodian` は「本仕様では使用しないが HL7 CDA 規格上必須」であるため、所定の形式で記述するとされている

---

## 11. participant の役割

```yaml
participant:
  role:
    - 受診券情報
    - 利用券情報
    - 利用券発行保険者情報
```

- `participant` は単なる付随情報ではなく、受診券・利用券・保険者情報を持つ正式な header 管理情報要素である
- 実装では `functionCode/@code="1"` と `functionCode/@code="2"` を明示的に分岐して解釈する必要がある
- 特定保健指導XMLでは、利用券情報の取得と修正に直結する重要要素である

---

## 12. documentationOf / serviceEvent の役割

```yaml
documentationOf:
  serviceEvent:
    role:
      - 保健指導実施情報
      - 実施日 / 期間情報
      - 保健指導区分情報
```

- `documentationOf` / `serviceEvent` は保健指導の実施に関する基本情報を保持する
- `serviceEvent/code` は保健指導区分を表現する
- `serviceEvent/effectiveTime` は実施日または実施期間を表現する
- 特定保健指導の期間判定、initial_date / final_date、報告時期の判定に関わる要素である
- 実装では section 情報と混同せず、header 管理情報として別レイヤーで扱うべき要素である

---

## 13. 実装チェック観点

```yaml
Checks:
  - participant count is within 0..2
  - participant/@typeCode == HLD
  - functionCode 1/2 is interpreted correctly
  - guidance ticket expiration xpath resolves correctly
  - guidance ticket number xpath resolves correctly
  - guidance ticket insurer number xpath resolves correctly
  - receive ticket number xpath resolves correctly when present
  - participant insurer number matches recordTarget insurer number
  - documentationOf exists and multiplicity is 1..1
  - serviceEvent/code xpath resolves correctly
  - serviceEvent/effectiveTime xpath resolves correctly
  - effectiveTime value vs low/high usage is handled explicitly
```

---

## 14. 未整理 / TODO

```yaml
TODO:
  - 5-1A.pdf の該当章と本ファイル各節の対応表を作る
  - ClinicalDocument/code の報告区分コード一覧を別ファイルに切り出す
  - recordTarget の XPath を項目単位で確定する
  - participant の XPath を項目単位で確定する
  - documentationOf / serviceEvent の XPath は概ね確定済み。effectiveTime の value / low / high 優先順位を整理する
```
