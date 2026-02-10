# -*- coding: utf-8 -*-
"""
kenshin_lib/phr/db_phr.py

PHR（dev_phr）スキーマ参照専用のDBアクセス関数群。

目的:
- 健診XML生成・値正規化処理に必要な
  exam_item_master の参照を一元化する
- work_other 側へマスタを複製せず、
  dev_phr を一次情報（正）として扱う

設計方針:
- 本モジュールは「参照専用」
  INSERT / UPDATE / DELETE は行わない
- 取得対象は XML抽出・正規化に必要な最小限の列のみ
- namecode を主キーとして扱う前提

利用箇所:
- item_extract
- normalize_item_values
- exam_value_normalizer
- XML生成系スクリプト

注意:
- dev_phr.exam_item_master の構造変更時は
  本モジュールの SELECT 項目も必ず見直すこと
"""

from __future__ import annotations

from typing import Optional


def db_select_exam_items(cur, *, only_with_xpath: bool = True) -> list[dict]:
    """
    exam_item_master から、値抽出に必要な列を取得する。

    only_with_xpath=True:
      xpath_template が入っているものだけ対象
    """
    where = "WHERE xpath_template IS NOT NULL AND xpath_template<>''" if only_with_xpath else ""

    cur.execute(
        f"""
        SELECT
          namecode,
          item_name,
          xml_value_type,
          item_code_oid,
          result_code_oid,
          display_unit,
          ucum_unit,
          method_name,
          category_name,
          data_type_label,
          xml_method_code,
          xpath_template,
          value_method,
          nullflavor_allowed
        FROM exam_item_master
        {where}
        ORDER BY namecode ASC
        """
    )
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def db_select_exam_item_by_namecode(cur, *, namecode: str) -> Optional[dict]:
    cur.execute(
        """
        SELECT
          namecode,
          item_name,
          xml_value_type,
          item_code_oid,
          result_code_oid,
          display_unit,
          ucum_unit,
          method_name,
          category_name,
          data_type_label,
          xml_method_code,
          xpath_template,
          value_method,
          nullflavor_allowed
        FROM exam_item_master
        WHERE namecode=%s
        """,
        (namecode,),
    )
    r = cur.fetchone()
    return dict(r) if r else None
