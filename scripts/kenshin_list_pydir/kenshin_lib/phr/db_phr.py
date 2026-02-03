# -*- coding: utf-8 -*-
"""
kenshin_lib/phr/db_phr.py

dev_phr 側の参照専用DBアクセス。
現時点では exam_item_master を読むだけ。

方針:
- work_other 側に法定マスタ等をコピーしない
- item_master は dev_phr を一次情報として読む
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
