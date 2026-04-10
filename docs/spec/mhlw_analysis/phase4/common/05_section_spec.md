

# 05 section Spec

## 1. 目的

section は CDA 文書内における情報の意味単位を表す構造であり、
健診結果・保健指導などのデータを「意味ごと」に区切るための単位である。

本specでは、実装観点で以下を整理する。

- section の役割
- DATA 内での位置付け
- コード体系（90030等）
- entry構造（概要）

本ファイルは section という構造そのものの共通概念を整理することを目的とし、健診 / 特定保健指導の個別コード体系そのものは後続で分離して整理する。

---

## 2. 対象範囲

- DATA XML 内の section 要素
- 各種セクションコード（90030 / 24100 / 24090 等）

---

## 3. 一次情報

- 厚生労働省 `8-1A.pdf`
- CDA構造仕様

---

## 4. section の役割

- XML内データを意味単位で分類する
- code によりセクションの種類を識別する
- entry により実データを格納する

---

## 5. DATA 内での位置

```yaml
ClinicalDocument:
  component:
    structuredBody:
      component:
        section
```

- CDAのボディ内に配置される

---

## 6. 基本構造

```yaml
section:
  code:
    description: セクション識別コード

  entry:
    description: セクションに属する実データ
```

---

## 7. sectionの意味

```yaml
concept:
  section = データの意味単位
```

- DATA（XML）は構造
- section は意味

---

## 8. コード体系（概要）

```yaml
examples:
  health_guidance_examples:
    - 90030: 実施内容
    - 24100: 生活習慣改善
    - 24090: 体重・腹囲改善

  health_check_examples:
    - 未整理
```

- 上記 examples は section の共通概念を説明するための仮置き例であり、現時点では特定保健指導寄りである
- 健診側の code 例は未精査のため、後続で分離整理する
- したがって、本節の code 例は「確定した共通コード体系」ではなく、「現時点の説明用サンプル」として扱う

---

## 9. entryの役割（概要）

```yaml
entry:
  - 実測値
  - 判定
  - 評価結果
```

- sectionごとの具体データを保持

---

## 10. 実装観点

```yaml
Checks:
  - section が存在する
  - code が存在する
  - entry が存在する
```

---

## 11. 未整理 / TODO

```yaml
TODO:
  - 各codeごとの詳細構造
  - entry内のXML構造分解
  - code体系の完全一覧化
  - 健診 / 特定保健指導での section code 例を分離して整理
```