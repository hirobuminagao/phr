

# 01 File Archive Spec (L0)

本ドキュメントは、厚生労働省「送付用ファイルアーカイブ仕様説明書（Ver.4）」に基づき、
特定健診・特定保健指導データの送付用ZIP構造および命名規則を、
実装可能な形式で定義する。

---

## 1. 概要

### 1.1 目的

本仕様は、特定健診・特定保健指導データの電子的交換において、提出に必要な一連のファイルのフォルダ構成、およびファイル名の仕様を定める。

本specでは、特に以下を実装観点で整理対象とする。

- 送付用ファイルアーカイブの格納体系
- 送付用ファイルアーカイブの命名規則
- ZIP圧縮ファイルとしてのアーカイブ規則
- 健診 / 特定保健指導それぞれの送付単位の分離ルール

### 1.2 対象
- 特定健診データ
- 特定保健指導データ
- 送付用アーカイブ（ZIP）
- 送付用アーカイブ内のフォルダ構成
- 送付用アーカイブ内のXML / XSDファイル命名規則

### 1.3 基本原則
- 健診と保健指導は別アーカイブとする
- アーカイブはZIP形式とする
- フォルダ構成・命名規則は固定とする
- 同一提出先に健診データと特定保健指導データの両方を提出する場合でも、それぞれ別アーカイブを作成する
- 本specでいう「アーカイブ」は、規則に従ってファイルを配置し、命名し、最後に1つのZIPへ圧縮した成果物全体を指す

### 1.4 このspecで扱うこと / 扱わないこと

#### 扱うこと
- 送付用アーカイブのルートフォルダ構造
- DATA / CLAIMS / XSD の配置ルール
- ルートフォルダ名規則
- XMLファイル名規則
- ZIPファイル名規則
- 交換パターン別の必須 / 任意構成

#### 扱わないこと
- XML本文（ClinicalDocument や section 構造）の中身
- 健診XML個票仕様の詳細
- 特定保健指導XML個票仕様の詳細
- DB格納設計や内部ID設計

### 1.5 一次情報
- 厚生労働省 `8-1A.pdf` 送付用ファイルアーカイブ仕様説明書 Version 4
- 主な参照箇所
  - 1.1 目的
  - 2. 送付用ファイルアーカイブ仕様
  - 2.1 フォルダ構成
  - 2.2 ファイル命名規則
  - 2.3 アーカイブ規則
  - 2.4 各交換パターンにおけるファイル構成

---

## 2. アーカイブ構造

### 2.1 全体構造

```yaml
root:
  - ix08_V08.xml
  - su08_V08.xml
  - DATA/
  - CLAIMS/
  - XSD/
```

### 2.2 必須 / 任意ファイル

```yaml
required:
  - ix08_V08.xml
  - DATA/
  - XSD/

optional:
  - su08_V08.xml
  - CLAIMS/
```

- ix08_V08.xml は必須（インデックス）
- DATA フォルダは必須
- XSD フォルダは必須
- su08_V08.xml は決済がある場合のみ必須
- CLAIMS フォルダは決済がある場合のみ必須

---

### 2.3 フォルダ仕様

#### DATAフォルダ

- 健診XML / 特定保健指導XMLを格納
- ファイル単位は「1受診者 = 1XML」
- XMLファイルは命名規則に従う必要がある

```yaml
constraints:
  - only_xml_files: true
  - one_record_per_file: true
```

---

#### CLAIMSフォルダ

- 決済情報XMLを格納
- DATA内のXMLと1対1対応する

```yaml
constraints:
  - one_to_one_with_DATA: true
  - filename_match_except_prefix: true
```

例:

```yaml
DATA:
  h12345678902025010101100001.xml

CLAIMS:
  c12345678902025010101100001.xml
```

---

#### XSDフォルダ

```yaml
XSD:
  - ix08_V08.xsd
  - su08_V08.xsd
  - cc08_V08.xsd
  - gc08_V08.xsd
  - co08_V08.xsd
  - hc08_V08.xsd
  - hg08_V08.xsd
  - coreschemas/
```

- XML検証に必要なスキーマ一式を格納
- coreschemas配下も含め完全なセットが必要

```yaml
constraints:
  - full_schema_set_required: true
```

---

### 2.4 インデックスファイル（ix08）

- アーカイブ内のXML一覧を管理するインデックスファイル
- DATA内のXMLと整合している必要がある

```yaml
constraints:
  - must_reference_all_DATA_files: true
```

---

### 2.5 決済ファイル（su08）

- 決済情報の集約ファイル
- CLAIMSフォルダと整合が必要

```yaml
constraints:
  - required_if_claims_exist: true
```

---

### 2.6 実装チェック観点

```yaml
Checks:
  - ix08存在チェック
  - DATAフォルダ存在チェック
  - XMLファイル存在チェック
  - CLAIMSとDATAの対応関係チェック
  - XSD完全性チェック
```
---

## 3. ルートフォルダ命名規則

```yaml
format:
  {sender_id}_{receiver_id}_{yyyymmdd}{sequence}_{type_code}
```

---

## 4. ファイル命名規則

### 4.1 基本形式

特定健診データファイル、特定保健指導データファイル、およびそれぞれの決済情報ファイルの命名規則は以下とする。

```yaml
Rule: xml_filename

format:
  {prefix}{facility_id}{yyyymmdd}{sequence}{type_code}{serial}.xml
```

### 4.2 プレフィックス

```yaml
prefix:
  h: 特定健診データファイル
  c: 特定健診決済情報ファイル
  g: 特定保健指導データファイル
  p: 特定保健指導決済情報ファイル
```

### 4.3 各要素の意味

```yaml
prefix:
  description: ファイル種別識別子
  length: 1

facility_id:
  description: 健診機関番号
  length: 10
  format: numeric
  note: 事業者等から保険者へ提出する場合は 5521111111 または 6631111116 を使用する

file_date:
  description: ファイル生成日付またはアーカイブ生成日付
  length: 8
  format: YYYYMMDD

sequence:
  description: 同日分割送信回数
  length: 1
  format: numeric(0-9)

type_code:
  description: 実施区分コード
  length: 1
  format: numeric(1-9)

serial:
  description: 同一フォルダ内で同一ファイル名とならないように振る6桁連番
  length: 6
  format: numeric
```

### 4.4 基本制約

```yaml
constraints:
  - prefix must be one of h/c/g/p
  - facility_id must be 10-digit numeric
  - file_date must be valid YYYYMMDD
  - sequence must be 0-9
  - type_code must be defined in root folder rule
  - serial must be 6-digit numeric
```

- XML拡張子は常に `.xml` とする
- 同一フォルダ内で同一ファイル名が重複してはならない
- DATAとCLAIMSが対応する場合、prefix以外のファイル名部分は一致する

### 4.5 対応関係

送付用アーカイブ内に、ある受診者の結果データと対応する決済情報が含まれる場合、両者のファイルは1対1に対応し、先頭1文字のprefixのみが異なる。

例:

```yaml
DATA:
  h12345678992024061201000005.xml

CLAIMS:
  c12345678992024061201000005.xml
```

```yaml
DATA:
  g12345678992024061202000005.xml

CLAIMS:
  p12345678992024061202000005.xml
```

### 4.6 個別契約時の扱い

個別契約の場合には、健診結果データのみ、または保健指導結果データのみを委託元へ提出する場合がある。

この場合は以下を含めればよい。

```yaml
required_if_individual_contract:
  - result_data_files
  - ix08_V08.xml
  - XSD/

not_required_if_individual_contract:
  - claims_files
  - su08_V08.xml
```

### 4.7 異動者データ提出時の扱い

保険者間における異動者の健診 / 保健指導結果データ提出では、健診データファイル名および保健指導データファイル名は自由とし、本節の命名規則に従う必要はない。

```yaml
special_case:
  insurer_to_insurer_transfer:
    filename_rule: free
```

### 4.8 実装チェック観点

```yaml
Checks:
  - filename matches base pattern
  - prefix is valid for file role
  - facility_id is valid 10-digit numeric
  - file_date is valid YYYYMMDD
  - sequence is within 0-9
  - type_code is valid
  - serial is 6-digit numeric
  - paired DATA/CLAIMS filenames match except prefix
```

---

## 5. アーカイブ規則

### 5.1 基本形式

送付用ファイルアーカイブは、ルートフォルダをそのままZIP圧縮した形式とする。

```yaml
Rule: archive

format:
  {root_folder_name}.zip
```

- ZIPファイル名はルートフォルダ名と一致させる
- 拡張子は `.zip` とする

---

### 5.2 圧縮対象

ZIP圧縮の対象は、以下の構造を持つルートフォルダ一式とする。

```yaml
root:
  - ix08_V08.xml
  - su08_V08.xml (optional)
  - DATA/
  - CLAIMS/ (optional)
  - XSD/
```

```yaml
constraints:
  - must_include_root_directory: true
  - no_extra_parent_directory: true
```

- ZIP内に余分な親ディレクトリを含めてはならない
- ルート直下に必要ファイルおよびフォルダが存在すること

---

### 5.3 圧縮形式

```yaml
compression:
  type: ZIP
  encoding: standard_zip
```

- 圧縮形式はZIPとする
- 特殊な圧縮形式（rar, 7z 等）は使用不可

---

### 5.4 交換パターン別構成

```yaml
Patterns:

normal_submission:
  DATA: required
  CLAIMS: required
  su08: required

no_claims_submission:
  DATA: required
  CLAIMS: not_required
  su08: not_required

individual_contract:
  DATA: required
  CLAIMS: not_required
  su08: not_required
```

---

### 5.5 ファイル整合性

```yaml
constraints:
  - ix08 must reference all DATA XML files
  - su08 must match CLAIMS XML files
  - DATA and CLAIMS must be one-to-one if both exist
```

---

### 5.6 実装チェック観点

```yaml
Checks:
  - zip filename matches root folder name
  - zip contains correct root structure
  - required files exist
  - optional files are consistent with content
  - no extra files exist outside defined structure
  - ix08 consistency with DATA
  - su08 consistency with CLAIMS
```