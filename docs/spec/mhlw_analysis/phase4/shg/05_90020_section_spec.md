

# 90020 初回面接実施情報 セクション仕様

---

## 1. 目的

本ファイルは、厚生労働省「特定保健指導XML仕様（5-1A）」を親資料とし、その中の 90020 セクション（初回面接実施情報）について整理するものである。

付属3に記載された 90020 セクションの構造と、付属3を正規化した `fuzoku3.csv` の項目情報を対応づけ、section 単位で参照しやすい形に固定することを目的とする。

※ 本ファイルは実装方針の定義ではなく、仕様上の事実整理を目的とする。

---

## 2. 親資料と参照資料

- 親資料：厚生労働省「特定保健指導XML仕様（5-1A）」
- 構造詳細：付属3（XML 用特定保健指導項目情報）
- 項目整理：`fuzoku3.csv`
- 共通ポリシー：`../common/06_xpath_policy.md`

本ファイルでは、90020 セクションの構造事実は付属3を根拠とし、項目単位の条件・型・出現タイミングは `fuzoku3.csv` を参照して整理する。

### ■ 主な参照箇所（5-1A）

- 3.3.3.1 セクション部仕様（表20）
- 3.3.3.2 説明ブロック仕様（表21・表22）
- 3.3.3.3 エントリ部仕様（表23）

※ 本ファイルにおける section / text / entry の構造定義は、上記の章および表を一次根拠とする。

---

## 3. 前提（項目FIXベース）

90020 セクションで扱う項目は、`fuzoku3.csv` において項目単位で FIX 済みとする。
本ファイルでは、その FIX 結果を section 単位で整理し、90020 に属する項目群を明示する。

---

## 4. セクション概要

| 項目 | 内容 |
|------|------|
| セクションコード | 90020 |
| セクション名 | 初回面接実施情報 |

※ classCode / moodCode は entry 配下の act に対する属性として扱う（sectionそのものの属性ではない）

---

## 5. セクション部仕様

5-1A 表20（3.3.3.1）および付属3のセクション定義に従い、90020 は section コード `90020` を持つ初回面接実施情報セクションとして扱う。

```xml
<section>
  <code code="90020" codeSystem="1.2.392.200119.6.1010"/>
  <text/>
```

---

## 6. エントリ部仕様

5-1A 3.3.3.3（表23）に基づき、90020 セクションの entry 配下について、確認できた事実を以下に整理する。

```xml
<entry>
  <act classCode="ACT" moodCode="EVN">
    <code code="XXXX" codeSystem="..." />
    <effectiveTime value="YYYYMMDD"/>
    ...
  </act>
</entry>
```

---

## 7. 項目対応（fuzoku3ベース）

| No | 項目ID | 名称 | データ型 | 出現条件（report_code） | 配置メモ |
|----|--------|------|----------|-------------------------|----------|
| 1301 | 1022000011 | 初回面接の実施日付 | 年月日 | first_claim / legal_report | effectiveTime |
| 1302 | 1022000012 | 支援形態 | コード | first_claim / legal_report | code |
| 1303 | 1022000016 | 健診後早期の初回面接 | コード | first_claim / second_claim / legal_report | code |
| 1304 | 1022000013 | 実施時間 | 数字 | first_claim / legal_report | OBS（時間） |
| 1305 | 1022000015 | 実施者 | コード | first_claim / legal_report | performer |
| 1306 | 1022000090 | 初回面接情報 | 文字列 | first_claim | ST（備考） |

※ 項目単位の条件・型は fuzoku3.csv 側でFIX済み。

---

## 8. 出現条件（項目別）

- 1301 / 1302 / 1304 / 1305：first_claim / legal_report
- 1303：first_claim / second_claim / legal_report
- 1306：first_claim のみ

※ セクション単位で一律に second_claim とはならないため、項目単位で管理する。

---

## 9. このファイルで固定すること

- セクションコードは 90020 で固定
- `entry` は 1..1（内部に複数 observation / act / performer を含む）
- 1301〜1306 を 90020 の対象項目とする
- entry 配下に act（EVN）として配置する
- effectiveTime は日付（YYYYMMDD）として扱う（1301）
- 出現条件は項目単位で管理する（section単位では固定しない）

---

## 10. 留保事項

- performer の詳細構造（ID/role）は別途定義
- コード体系の厳密定義は codeSystem 側で管理

---

## 11. 関連ファイル

- `04_90010_section_spec.md`
- `06_90030_section_spec.md`
- `fuzoku3.csv`
- `report_timing_and_section_matrix.md`