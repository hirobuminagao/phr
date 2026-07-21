# 労安法 第44条 一般健康診断 項目詳細チェック仕様

## 1. 目的

本ドキュメントは `労安法_一般健康診断_項目対応表.xlsx` の「値の有無の判定の整理」シートを、正式なMarkdown仕様書へ変換したものである。

目的は実装ではなく、Excelで整理された法令項目詳細Noごとの判定仕様をMarkdownとして固定することである。

## 2. 変換方針

- Excelの1行を、本仕様書の1項目として扱う。
- Excelの内容を推測で変更しない。
- `method` / `namecode` はExcel記載値をそのまま記載する。
- 日本語判定フローはExcelの内容をそのまま整理する。
- `OK` / `WARNING` / `MISSING` 条件を記載する。
- 判定パターンを記載する。
- 制度解釈は追加しない。
- Excel上で不明または空欄の内容は `TODO（Excel記載なし）` として残す。

## 3. 判定構造

31_phase7_legal_check_redesign.md の決定に従い、判定主体は法令項目詳細Noとする。

```text
則44
↓
法令項目
↓
法令項目詳細
↓
identity
↓
method
↓
namecode
```

各項目詳細Noの判定結果は `OK` / `WARNING` / `MISSING` を中心に記録する。Excelに該当ステータスの明示がない場合はTODOとして残す。

## 4. 項目詳細Noごとの仕様

### 4401001001 既往歴キオウレキ

- No.: 1
- 検査項目: 既往歴及び業務歴の調査
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: FINDING

#### require_methods

```text
9N05616040
9N05600000
```

#### require_namecodes

```text
9N056160400000049
9N056000000000011
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:method:9N05600000
CD=1:namecode:9N056160400000049(ST)がNULL⇒NG
CD=2⇒OK
```

#### 日本語判定条件

**OK**

```text
[既往歴所見の有無]
所見なし（CD=2）
所見あり（CD=1）かつ既往歴所見が空欄じゃない場合OK
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
それ以外キオウレキショケnショケnショケnキオウレキショケnクウバアイ
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4401001001（既往歴）

【入力】
- EvaluationContext

【使用データ】
- method: 9N05600000
  - namecode: 9N056000000000011（既往歴所見有無）
- method: 9N05616040
  - namecode: 9N056160400000049（既往歴所見）

【判定】

① 既往歴所見有無(CD)を取得する。

② CDが取得できない場合
→ MISSING

③ CD = 2
→ OK

④ CD = 1
　既往歴所見を取得する。

　・値あり
　　→ OK

　・NULL / 空文字
　　→ MISSING

⑤ 上記以外
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4401001002 業務歴ギョウムレキ

- No.: 1
- 検査項目: 既往歴及び業務歴の調査
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 任意ニンイ
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 業務歴は結果での登録が少ない為必須チェックからは除くSTだから書くことがなければnullになるから判定困難ギョウムレキケッカトウロクスクタメヒッスノゾカハンテイコンナン
- 詳細No: 2
- 単純/複雑: -
- 単純/種類: -
- 判定パターン: -

#### require_methods

```text
-
```

#### require_namecodes

```text
-
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
任意なのでなしニn
```

#### 日本語判定条件

**OK**

```text
TODO（Excel記載なし）
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
TODO（Excel記載なし）
```

#### 日本語判定フロー

```text
-
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4402001001 自覚症状ジカクショウジョウ

- No.: 2
- 検査項目: 自覚症状及び他覚症状の有無の検査
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: FINDING

#### require_methods

```text
9N06116080
9N06100000
```

#### require_namecodes

```text
9N061160800000049
9N061000000000011
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:method:9N06100000
CD=1:namecode:9N061160800000049(ST)がNULL⇒NG
CD=2⇒OK
```

#### 日本語判定条件

**OK**

```text
[自覚症状所見の有無]
所見なし（CD=2）
所見あり（CD=1）かつ自覚症状所見が空欄じゃない場合OK
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
それ以外
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4402001001（自覚症状）

【入力】
- EvaluationContext

【使用データ】
- method: 9N06100000
  - namecode: 9N061000000000011（自覚症状所見有無）
- method: 9N06116080
  - namecode: 9N061160800000049（自覚症状所見）

【判定】

① 自覚症状所見有無（CD）を取得する。

② CDが取得できない場合
→ MISSING

③ CD = 2
→ OK

④ CD = 1
　自覚症状所見を取得する。

　・値あり
　　→ OK

　・NULL / 空文字
　　→ MISSING

⑤ 上記以外
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4402001002 他覚症状タカクショウジョウ

- No.: 2
- 検査項目: 自覚症状及び他覚症状の有無の検査
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 2
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: FINDING

#### require_methods

```text
9N06616080
9N06600000
```

#### require_namecodes

```text
9N066160800000049
9N066000000000011
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:method:9N06600000
CD=1:namecode:9N066160800000049(ST)がNULL⇒NG
CD=2⇒OK
```

#### 日本語判定条件

**OK**

```text
[他覚症状所見の有無]
所見なし（CD=2）
所見あり（CD=1）かつ他覚症状所見が空欄じゃない場合OK
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
それ以外
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4402001002（他覚症状）

【入力】
- EvaluationContext

【使用データ】
- method: 9N06600000
  - namecode: 9N066000000000011（他覚症状所見有無）
- method: 9N06616080
  - namecode: 9N066160800000049（他覚症状所見）

【判定】

① 他覚症状所見有無（CD）を取得する。

② CDが取得できない場合
→ MISSING

③ CD = 2
→ OK

④ CD = 1
　他覚症状所見を取得する。

　・値あり
　　→ OK

　・NULL / 空文字
　　→ MISSING

⑤ 上記以外
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4403001001 身長シンチョウ

- No.: 3-1
- 検査項目: 身長
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：20歳以上は省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる、体測は基本測定する為キホンテキジギョウショモトタイソクキホンソクテイタメ
- 詳細No: 1
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
9N00100000
```

#### require_namecodes

```text
9N001000000000001
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity:9N001
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：身長
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がないコウモクアタイドウイテゥシンチョウアタイ
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4403001001（身長）

【入力】
- EvaluationContext

【使用データ】
- identity: 9N001

【判定】

① identity「9N001」の検査値を取得する。

② 値あり
→ OK

③ 値なし
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4403002001 体重タイジュウ

- No.: 3-2
- 検査項目: 体重
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 1
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
9N00600000
```

#### require_namecodes

```text
9N006000000000001
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity:9N006
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：体重
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4403002001（体重）

【入力】
- EvaluationContext

【使用データ】
- identity: 9N006

【判定】

① identity「9N006」の検査値を取得する。

② 値あり
→ OK

③ 値なし
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4403003001 腹囲フクイ

- No.: 3-3
- 検査項目: 腹囲
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：①40歳未満(35歳を除く) ②妊娠中の女性等で腹囲が内臓脂肪蓄積を反映しないと診断 ③BMI<20 ④BMI<22かつ自己申告　のいずれかで省略可
- システム設定での必須: 条件つき任意ジョウケンニンイ
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: BMI>=20|
- 値で判断できない項目: 妊娠中ニンシンチュウ
- システム設定での内容: 省略の②以外はアラートショウリャクイガイ
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: PRIORITY

#### require_methods

```text
9N01616010
9N01616020
9N01616030
9N01100000
```

#### require_namecodes

```text
9N016160100000001
9N016160200000001
9N016160300000001
9N011000000000001
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
実測を優先し、なければ自己測定を判定
実測:Any:method:9N01616010
自己測定:Any:method:9N01616020
自己申告:Any:method:9N01616030
BMI:Any:method:9N01100000
実測がNULLなら自己測定を確認⇒どちらもNULL⇒
自己申告がNULLじゃなくBMI<22ならOK→条件を満たせないならNGジコシンコジコシンコジョウケnミタ
```

#### 日本語判定条件

**OK**

```text
腹囲（実測）に値がある
腹囲（実測）に値がない場合、腹囲（自己測定）に値がある
腹囲（実測）・腹囲（自己測定）に値がなく、腹囲（自己申告）に値があり、かつBMIが22未満

※実測を最優先とし、次に自己測定、最後にBMI条件付きで自己申告を確認する
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
腹囲（実測）・腹囲（自己測定）・腹囲（自己申告）のいずれにも値がない
腹囲（自己申告）に値があるが、BMIが取得できない、数値として判定できない、または22以上フクジッソクフクイジコソクアタイフクイジコシンコアタイミマnフクジコシンクフクジコシンクイジョウ
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4403003001（腹囲）

【入力】
- EvaluationContext

【使用データ】
- method: 9N01616010
  - namecode: 9N016160100000001（腹囲 実測）
- method: 9N01616020
  - namecode: 9N016160200000001（腹囲 自己測定）
- method: 9N01616030
  - namecode: 9N016160300000001（腹囲 自己申告）
- method: 9N01100000
  - namecode: 9N011000000000001（BMI）

【判定】

① 腹囲（実測）を取得する。

② 実測に値がある場合
→ OK

③ 実測に値がない場合、腹囲（自己測定）を取得する。

④ 自己測定に値がある場合
→ OK

⑤ 実測・自己測定のどちらにも値がない場合、腹囲（自己申告）を取得する。

⑥ 自己申告にも値がない場合
→ MISSING

⑦ 自己申告に値がある場合、BMIを取得する。

⑧ BMIが取得でき、かつ22未満の場合
→ OK

⑨ BMIが取得できない、数値として判定できない、または22以上の場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4403004001 視力シリョク

- No.: 3-4
- 検査項目: 視力
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: ALL(ANY)

#### require_methods

```text
9E16016210
9E16016220
9E16016250
9E16016260
```

#### require_namecodes

```text
9E160162100000001
9E160162200000001
9E160162500000001
9E160162600000001
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
以下の視力右 and 視力左 どちらも入っていればOK
視力右:Any:method:9E16016210 or 9E16016250
視力左:Any:method:9E16016220 or 9E16016260
```

#### 日本語判定条件

**OK**

```text
視力右に値がある
かつ、視力左に値がある
※裸眼・矯正のいずれかに値があればOK
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
視力右・視力左のいずれかに値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4403004001（視力）

【入力】
- EvaluationContext

【使用データ】

右視力
- method: 9E16016210
  - namecode: 9E160162100000001（裸眼）
- method: 9E16016250
  - namecode: 9E160162500000001（矯正）

左視力
- method: 9E16016220
  - namecode: 9E160162200000001（裸眼）
- method: 9E16016260
  - namecode: 9E160162600000001（矯正）

【判定】

① 右視力を確認する。
　裸眼・矯正のいずれかに値がある場合、右視力ありとする。

② 左視力を確認する。
　裸眼・矯正のいずれかに値がある場合、左視力ありとする。

③ 右視力・左視力の両方に値がある場合
→ OK

④ 右視力・左視力のいずれかに値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4403005001 聴力チョウリョク

- No.: 3-5
- 検査項目: 聴力(1000Hz・4000Hz)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：45歳未満(35歳・40歳を除く)は医師が適当と認める聴力検査に代替可（雇入時の代替可否は労働局運用資料を確認）
- システム設定での必須: 必須ヒッス
- n歳以上必須: 45
- 指定年齢必須: 35|40
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 医師が適当と認めるものが不明の為必須で対応イシテキトウミトフメイタメヒッスタイオウ
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: ALL + FALLBACK

#### require_methods

```text
9D10016310
9D10016090
9D10016320
9D10016350
9D10016360
```

#### require_namecodes

```text
9D100163100000011
9D100160900000049
9D100163200000011
9D100163500000011
9D100163600000011
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
All:method:9D10016310,9D10016320,9D10016350,9D10016360
ない場合:method:9D10016090もなかった場合missing、あった場合warningバアイバアイバアイ
```

#### 日本語判定条件

**OK**

```text
聴力検査の4項目すべてに値がある
```

**WARNING**

```text
4項目のいずれかに値がなく、会話法による聴力検査に値がある
```

**MISSING**

```text
4項目のいずれかに値がなく、会話法による聴力検査にも値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4403005001（聴力）

【入力】
- EvaluationContext

【使用データ】

聴力検査
- method: 9D10016310
  - namecode: 9D100163100000011（右1000Hz）
- method: 9D10016320
  - namecode: 9D100163200000011（左1000Hz）
- method: 9D10016350
  - namecode: 9D100163500000011（右4000Hz）
- method: 9D10016360
  - namecode: 9D100163600000011（左4000Hz）

代替検査
- method: 9D10016090
  - namecode: 9D100160900000049（会話法）

【判定】

① 聴力検査4項目を確認する。

② 4項目すべてに値がある場合
→ OK

③ 4項目のいずれかに値がない場合、会話法による聴力検査を確認する。

④ 会話法に値がある場合
→ WARNING

⑤ 会話法にも値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4404001001 胸部X線キョウブセン

- No.: 4-1
- 検査項目: 胸部エックス線検査
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期：40歳未満(20・25・30・35歳を除く)で、感染症法の結核定期健診対象施設等・じん肺法対象のいずれにも該当しない者は省略可。／特定業務：1年以内ごとに1回で足りる
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 20|25|30|35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: 妊娠中ニンシンチュウ
- システム設定での内容: 年齢で任意ネンレイニンイ
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: ANY + FINDING(OR)

#### require_methods

```text
9N20100000
9N20616070
9N20616080
9N22116070
9N22116080
9N21600000
```

#### require_namecodes

```text
9N201000000000011
9N206160700000011
9N206160800000049
9N221160700000011
9N221160800000049
9N216000000000011
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
any:method:9N20100000,9N21600000
どちらもなかった場合
method:9N20616070=2ならOK,1でかつmethod9N20616080がnullじゃなければOK,3ならwarning
もしくはmethod:9N22116070=2ならOK,1でかつmethod:9N22116080がnullじゃなければOK,3ならwarning
それ以外はNGバアイイガイ
```

#### 日本語判定条件

**OK**

```text
胸部X線検査結果のいずれかに値がある
または、[胸部X線所見の有無]で所見なし（CD=2）
または、所見あり（CD=1）かつ対応する胸部X線所見が空欄じゃない
```

**WARNING**

```text
[胸部X線所見の有無]が判定不能（CD=3）
```

**MISSING**

```text
それ以外
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4404001001（胸部X線）

【入力】
- EvaluationContext

【使用データ】

胸部X線検査結果
- method: 9N20100000
  - namecode: 9N201000000000011
- method: 9N21600000
  - namecode: 9N216000000000011

胸部X線所見（パターン1）
- method: 9N20616070
  - namecode: 9N206160700000011（所見有無）
- method: 9N20616080
  - namecode: 9N206160800000049（所見）

胸部X線所見（パターン2）
- method: 9N22116070
  - namecode: 9N221160700000011（所見有無）
- method: 9N22116080
  - namecode: 9N221160800000049（所見）

【判定】

① 胸部X線検査結果を確認する。

② 胸部X線検査結果のいずれかに値がある場合
→ OK

③ 検査結果に値がない場合、胸部X線所見（パターン1）を確認する。

　・所見なし（CD=2）
　　→ OK

　・所見あり（CD=1）かつ所見あり
　　→ OK

　・判定不能（CD=3）
　　→ WARNING

④ パターン1で判定できなかった場合、胸部X線所見（パターン2）を確認する。

　・所見なし（CD=2）
　　→ OK

　・所見あり（CD=1）かつ所見あり
　　→ OK

　・判定不能（CD=3）
　　→ WARNING

⑤ 上記のいずれにも該当しない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4404002001 喀痰カクタン

- No.: 4-2
- 検査項目: 喀痰検査
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 雇入時は項目に含まれない。／定期・特定業務：胸部X線で病変所見なし・結核発病のおそれなしと診断された者、及び胸部X線を省略した者は省略可。特定業務は1年以内ごとに1回で足りる
- システム設定での必須: 任意ニンイ
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 求められる条件が特定の場合のみなのでシステムでのチェックは任意モトジョウケントクテイバアイニンイ
- 詳細No: 1
- 単純/複雑: -
- 単純/種類: -
- 判定パターン: TODO（Excel記載なし）

#### require_methods

```text
-
```

#### require_namecodes

```text
-
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
任意なのでなしニn
```

#### 日本語判定条件

**OK**

```text
TODO（Excel記載なし）
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
TODO（Excel記載なし）
```

#### 日本語判定フロー

```text
TODO（Excel記載なし）
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4405001001 収縮期血圧シュウシュクキケツアツ

- No.: 5
- 検査項目: 血圧の測定
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 1
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(method)

#### require_methods

```text
9A75500000
9A75200000
9A75100000
```

#### require_namecodes

```text
9A755000000000001
9A752000000000001
9A751000000000001
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity9A750
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：収縮期血圧
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4405001001（収縮期血圧）

【入力】
- EvaluationContext

【使用データ】
- method: 9A75500000
  - namecode: 9A755000000000001（代表値・その他）
- method: 9A75200000
  - namecode: 9A752000000000001（2回目）
- method: 9A75100000
  - namecode: 9A751000000000001（1回目）

【判定】

① 収縮期血圧の候補値を確認する。

② 代表値・その他、2回目、1回目のいずれかに値がある場合
→ OK

③ すべてに値がない場合
→ MISSING
```

#### メモ

```text
* 1回だけ測定して送る
* 2回測定して両方送る
* 代表値だけ送る
* 1回目・2回目・代表値全部送る
システムによって扱いが違うため仕様不明アツカイチガウシヨウ フメイ
```

### 4405001002 拡張期血圧カクチョウキケツアツ

- No.: 5
- 検査項目: 血圧の測定
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 2
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(method)

#### require_methods

```text
9A76500000
9A76200000
9A76100000
```

#### require_namecodes

```text
9A765000000000001
9A762000000000001
9A761000000000001
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity9A760
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：拡張期血圧
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4405001002（拡張期血圧）

【入力】
- EvaluationContext

【使用データ】
- method: 9A76500000
  - namecode: 9A765000000000001（代表値・その他）
- method: 9A76200000
  - namecode: 9A762000000000001（2回目）
- method: 9A76100000
  - namecode: 9A761000000000001（1回目）

【判定】

① 拡張期血圧の候補値を確認する。

② 代表値・その他、2回目、1回目のいずれかに値がある場合
→ OK

③ すべてに値がない場合
→ MISSING
```

#### メモ

```text
* 1回だけ測定して送る
* 2回測定して両方送る
* 代表値だけ送る
* 1回目・2回目・代表値全部送る
システムによって扱いが違うため仕様不明アツカイチガウシヨウ フメイ
```

### 4406001001 血色素量（ヘモグロビン）ケッショクソリョウ

- No.: 6
- 検査項目: 貧血検査(血色素量・赤血球数)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 1
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
2A03000000
```

#### require_namecodes

```text
2A030000001930101
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity2A030
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：血色素量（ヘモグロビン）
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4406001001（血色素量（ヘモグロビン））

【入力】
- EvaluationContext

【使用データ】
- identity: 2A030

【判定】

① identity「2A030」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4406001002 赤血球数セッケッキュウスウ

- No.: 6
- 検査項目: 貧血検査(血色素量・赤血球数)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 2
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
2A02000000
```

#### require_namecodes

```text
2A020000001930101
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity2A020
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：赤血球数
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4406001002（赤血球数）

【入力】
- EvaluationContext

【使用データ】
- identity: 2A020

【判定】

① identity「2A020」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4407001001 AST

- No.: 7
- 検査項目: 肝機能検査(AST・ALT・γ-GT)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 1
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
3B03500000
```

#### require_namecodes

```text
3B035000002327201
3B035000002399901
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity3B035
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：AST
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4407001001（AST）

【入力】
- EvaluationContext

【使用データ】
- identity: 3B035

【判定】

① identity「3B035」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4407001002 ALT

- No.: 7
- 検査項目: 肝機能検査(AST・ALT・γ-GT)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 2
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
3B04500000
```

#### require_namecodes

```text
3B045000002327201
3B045000002399901
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity3B045
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：ALT
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4407001002（ALT）

【入力】
- EvaluationContext

【使用データ】
- identity: 3B045

【判定】

① identity「3B045」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4407001003 γ-GT

- No.: 7
- 検査項目: 肝機能検査(AST・ALT・γ-GT)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 3
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
3B09000000
```

#### require_namecodes

```text
3B090000002327101
3B090000002399901
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity3B090
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：γ-GT
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4407001003（γ-GT）

【入力】
- EvaluationContext

【使用データ】
- identity: 3B090

【判定】

① identity「3B090」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4408001001 LDL

- No.: 8
- 検査項目: 血中脂質検査(LDL・HDL・TG)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: TODO（Excel記載なし）

#### require_methods

```text
3F07710000
3F07720000
3F07730009
```

#### require_namecodes

```text
3F077000002327101
3F077000002327201
3F077000002399901
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
3F077000002391901
```

#### 有無のチェック

```text
Any:identity3F077 対象外:method3F07740009（method 3F07710000,3F07720000,3F07730009のいずれかに値がある）タイショウ
```

#### 日本語判定条件

**OK**

```text
LDLに値がある
※対象外の測定方法を除き、対象となる測定方法のいずれかに値があればOK
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
対象となる測定方法のいずれにも値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4408001001（LDLコレステロール）

【入力】
- EvaluationContext

【使用データ】
- identity: 3F077

【対象外】
- method: 3F07740009（計算法）

【判定】

① identity「3F077」の検査値を取得する。

② method「3F07740009（計算法）」のデータは判定対象から除外する。

③ 対象となる測定方法（直接法・その他）のいずれかに値がある場合
→ OK

④ 対象となる測定方法のいずれにも値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4408001002 HDL

- No.: 8
- 検査項目: 血中脂質検査(LDL・HDL・TG)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 2
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
3F07000000
```

#### require_namecodes

```text
3F070000002327101
3F070000002327201
3F070000002399901
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity3F070
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：HDL
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4408001002（HDLコレステロール）

【入力】
- EvaluationContext

【使用データ】
- identity: 3F070

【判定】

① identity「3F070」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4408001003 TG

- No.: 8
- 検査項目: 血中脂質検査(LDL・HDL・TG)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 3
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
3F01500000
3F01512990
```

#### require_namecodes

```text
3F015000002327101
3F015000002327201
3F015000002399901
3F015129902327101
3F015129902327201
3F015129902399901
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity3F015
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：TG
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4408001003（中性脂肪（TG））

【入力】
- EvaluationContext

【使用データ】
- identity: 3F015

【判定】

① identity「3F015」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
空腹時と随時では基準値、評価が変わるだけなので有無のチェックはどれかでいいクウズイジキジュnヒョウカカワルウムノ 
```

### 4409001001 血糖ケットウ

- No.: 9
- 検査項目: 血糖検査(空腹時血糖・随時血糖・HbA1cのいずれか)
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可。〔検査方法は下部の注記を参照〕
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: 基本的に事業所より求められる為必須キホンテキジギョウショモトタメヒッス
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: CONDITIONAL

#### require_methods

```text
3D01000000
3D04600000
3D01012990
9N14100000
```

#### require_namecodes

```text
3D010000001926101
3D010000002227101
3D010000001927201
3D010000001999901
3D046000001906202
3D046000001920402
3D046000001927102
3D046000001999902
3D010129901926101
3D010129902227101
3D010129901927201
3D010129901999901
9N141000000000011
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:namecode 3D010000001926101,3D010000002227101,3D010000001927201,3D010000001999901,3D046000001906202,3D046000001920402,3D046000001927102,3D046000001999902

上記がMISSINGの場合、
Any:namecode 3D010129901926101,3D010129902227101,3D010129901927201,3D010129901999901
があり、かつ9N141が2または3であればOKジョウ
```

#### 日本語判定条件

**OK**

```text
空腹時血糖に値がある（採血時間は不要）
または、随時血糖に値があり、採血時間（9N141）がCD=2またはCD=3
または、HbA1cに値がある（採血時間は不要）
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
それ以外
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4409001001（血糖）

【入力】
- EvaluationContext

【使用データ】

空腹時血糖
- identity: 3D010（空腹時）

HbA1c
- identity: 3D046

随時血糖
- identity: 3D010（随時）

採血時間
- method: 9N14100000
  - namecode: 9N141000000000011

【判定】

① 空腹時血糖を確認する。

② 空腹時血糖に値がある場合
→ OK

③ HbA1cを確認する。

④ HbA1cに値がある場合
→ OK

⑤ 空腹時血糖・HbA1cのどちらにも値がない場合、随時血糖を確認する。

⑥ 随時血糖に値がない場合
→ MISSING

⑦ 随時血糖に値がある場合、採血時間を確認する。

⑧ 採血時間がCD=2またはCD=3の場合
→ OK

⑨ 採血時間が取得できない、またはCD=1・その他の場合
→ MISSING
```

#### メモ

```text
処理順は制度上の優先順位ではなく、条件判定を単純化するために
「空腹時血糖・HbA1c」→「条件付き随時血糖」
の順で評価する。クウズイジジュnヨイグユウセnショリツゴウクウフクケットウズイジジュn
```

### 4410001001 尿糖ニョウトウ

- No.: 10
- 検査項目: 尿検査(尿中の糖及び蛋白の有無)
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 1
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
1A02000000
```

#### require_namecodes

```text
1A020000000191111
1A020000000190111
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity1A020
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：尿糖
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4410001001（尿糖）

【入力】
- EvaluationContext

【使用データ】
- identity: 1A020

【判定】

① identity「1A020」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4410001002 尿蛋白ニョウタンパク

- No.: 10
- 検査項目: 尿検査(尿中の糖及び蛋白の有無)
- 定期健診（則44条）: ●
- 省略・代替の主な条件: 省略規定なし
- システム設定での必須: 必須ヒッス
- n歳以上必須: TODO（Excel記載なし）
- 指定年齢必須: TODO（Excel記載なし）
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 2
- 単純/複雑: 単純タンジュn
- 単純/種類: identity
- 判定パターン: SIMPLE(identity)

#### require_methods

```text
1A01000000
```

#### require_namecodes

```text
1A010000000191111
1A010000000190111
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
Any:identity1A010
```

#### 日本語判定条件

**OK**

```text
いずれかの項目に値がある
同一性項目：尿蛋白
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
値がない
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4410001002（尿蛋白）

【入力】
- EvaluationContext

【使用データ】
- identity: 1A010

【判定】

① identity「1A010」の検査値を取得する。

② 値がある場合
→ OK

③ 値がない場合
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```

### 4411001001 心電図シンデンズ

- No.: 11
- 検査項目: 心電図検査
- 定期健診（則44条）: ○
- 省略・代替の主な条件: 定期・特定業務：40歳未満(35歳を除く)は医師の判断で省略可。特定業務は前回健診で受けた者も医師の判断で省略可
- システム設定での必須: 必須ヒッス
- n歳以上必須: 40
- 指定年齢必須: 35
- 数値による必須: TODO（Excel記載なし）
- 値で判断できない項目: TODO（Excel記載なし）
- システム設定での内容: TODO（Excel記載なし）
- 詳細No: 1
- 単純/複雑: TODO（Excel記載なし）
- 単純/種類: TODO（Excel記載なし）
- 判定パターン: FINDING

#### require_methods

```text
9A11016070
9A11016080
```

#### require_namecodes

```text
9A110160700000011
9A110160800000049
```

#### excluded_methods

```text
TODO（Excel記載なし）
```

#### excluded_namecodes

```text
TODO（Excel記載なし）
```

#### 有無のチェック

```text
OK:method9A11016070=2,methodmethod9A11016070=1 and 9A11016080 not null
```

#### 日本語判定条件

**OK**

```text
[心電図所見の有無]
所見なし（CD=2）
所見あり（CD=1）かつ心電図所見が空欄じゃない場合OK
```

**WARNING**

```text
TODO（Excel記載なし）
```

**MISSING**

```text
それ以外
```

#### 日本語判定フロー

```text
【対象】
法令項目詳細No: 4411001001（心電図）

【入力】
- EvaluationContext

【使用データ】
- method: 9A11016070
  - namecode: 9A110160700000011（心電図所見有無）
- method: 9A11016080
  - namecode: 9A110160800000049（心電図所見）

【判定】

① 心電図所見有無（CD）を取得する。

② CDが取得できない場合
→ MISSING

③ CD = 2
→ OK

④ CD = 1
　心電図所見を取得する。

　・値あり
　　→ OK

　・NULL / 空文字
　　→ MISSING

⑤ 上記以外
→ MISSING
```

#### メモ

```text
TODO（Excel記載なし）
```
