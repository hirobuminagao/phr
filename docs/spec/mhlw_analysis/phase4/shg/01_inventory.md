# 01 SHG Inventory

## 1. 目的

本ドキュメントは、特定保健指導（SHG）XML仕様を整理するための目録である。

目的は以下の2点。

- SHG XML を「CDAセクション以外の要素」と「CDAセクション」に分けて整理する
- 後続の spec を、迷わず分割・参照できるようにする

---

## 2. 対象範囲

- 特定保健指導情報ファイル仕様書（5-1A.pdf）
- SHG XML に関する header / participant / documentationOf / section 構造

---

## 3. 一次情報

- 厚生労働省 `5-1A.pdf`
- 特定保健指導XMLスキーマ（hg08_V08.xsd ほか）

---

## 4. 整理方針

SHG XML は、以下の2系統に分けて整理する。

### 4.1 CDAセクション以外

- ClinicalDocument 全体構造
- report_code / 文書種別
- 利用者情報
- 利用券情報
- 保健指導実施情報
- 作成者 / 管理者 / 提出元情報
- documentationOf / serviceEvent
- participant

### 4.2 CDAセクション

- 90010
- 90020
- 90030
- 90040
- 90050
- 90060
- 90090

---

## 5. SHG spec 分割案

```yaml
shg:
  01_inventory.md:
    role: SHG全体目録

  02_header_and_non_section_spec.md:
    role: CDAセクション以外の仕様

  03_section_inventory.md:
    role: CDAセクション一覧と役割の目録

  04_90010_section_spec.md:
    role: 90010 指導共通情報

  05_90020_section_spec.md:
    role: 90020 初回面接実施情報

  06_90030_section_spec.md:
    role: 90030 保健指導計画情報

  07_90040_section_spec.md:
    role: 90040 継続支援情報

  08_90050_section_spec.md:
    role: 90050 中間評価情報

  09_90060_section_spec.md:
    role: 90060 実績評価情報

  10_90090_section_spec.md:
    role: 90090 指導機関情報

  fuzoku3.csv:
    role: 付属3を正規化した項目一覧

  report_timing_and_section_matrix.md:
    role: report_code / timing / section 対応整理
```

---

## 6. まず先に整理するもの

優先順は以下とする。

1. CDAセクション以外の仕様
2. CDAセクション一覧
3. 04_90010_section_spec.md
4. 05_90020_section_spec.md
5. 06_90030_section_spec.md
6. 07_90040_section_spec.md / 08_90050_section_spec.md / 09_90060_section_spec.md / 10_90090_section_spec.md

理由:
- まず section 以外の土台を固めないと、90030 など個別セクションの責務がぶれやすい
- 90010 / 90020 / 90030 は初期実装に直結しやすいため優先する

---

## 7. 注意

- 現時点では、まだ特定保健指導寄りの知識と実装都合が混ざっている可能性がある
- spec化では、必ず「一次情報にある事実」と「実装のための整理」を分ける
- sectionコードの意味やentry構造は、この inventory では確定せず、後続ファイルで分離して整理する

---

## 8. 未整理 / TODO

```yaml
TODO:
  - 5-1A.pdf の章立てと spec 分割案の対応表を作る
  - CDAセクション以外の項目を確定する
  - 04_90010 / 05_90020 / 06_90030 の section spec の役割差分を整理する
```
