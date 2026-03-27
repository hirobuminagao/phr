### 2. 既存粒度は壊さない
medi系 ledger / receipt の粒度が人単位か XML単位かは、現時点では保留とする。

したがって v1.0.3 では:

- 既存テーブルの粒度変更は行わない
- 既存の主キー / unique key の思想は維持する
- 追加するのは `identity_hash` と基本情報表現の整流化に限定する

- medi_xml_ledger は XML単位の台帳として扱い、個人単位の集約台帳へ変更しない

---

### 3. identity_hash は medi系へ後付けで通す
v1.0.3 では、取り込み時に subscribers を同時参照して直接記帳する方式までは採用しない。

代わりに以下の方針を採用する。

- medi系テーブルへ `person_id_custom` と `identity_hash` を追加する
- `person_id_custom` は raw 値から既存 `custom_id_gen` により生成する
  - 生成時は `custom_id_gen` 内部で保険者番号・記号・番号の先頭ゼロを正規化した上で処理する
  - 見かけ上の桁長ではなく実効桁で幅判定を行う
  - 実効桁が幅を超える場合は従来どおり生成対象外とする（桁切りは行わない）
- `identity_hash` は `person_id_custom + name_kana_match + gender_code` を用いて後段で付与・更新する
- これにより、既存フローを壊さずに人物横断を可能にする