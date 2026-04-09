from __future__ import annotations

from typing import Any, Optional

# 旧CSV / 新CSV どちらでも扱えるように、短縮カテゴリ名へ正規化する。
GENERAL_CONFLICT_CATEGORY_ORDER = [
    "食",
    "運動",
    "喫煙",
    "休養",
    "その他",
]

PLAN_KEY_ALIASES = {
    "腹囲体重": ["腹囲体重", "腹囲・体重の改善"],
    "食": ["食", "生活習慣の改善(食習慣)"],
    "運動": ["運動", "生活習慣の改善(運動習慣)"],
    "喫煙": ["喫煙", "生活習慣の改善(喫煙習慣)"],
    "休養": ["休養", "生活習慣の改善(休養習慣)"],
    "その他": ["その他", "生活習慣の改善(その他)", "生活習慣の改善(その他の生活習慣)"],
}

OUTCOME_KEY_ALIASES = {
    "腹囲体重": ["腹囲体重", "腹囲・体重の改善"],
    "食": ["食", "生活習慣の改善(食習慣)"],
    "運動": ["運動", "生活習慣の改善(運動習慣)"],
    "喫煙": ["喫煙", "生活習慣の改善(喫煙習慣)"],
    "休養": ["休養", "生活習慣の改善(休養習慣)"],
    "その他": ["その他", "生活習慣の改善(その他)", "生活習慣の改善(その他の生活習慣)"],
}


# ----------------------------------------
# 共通ヘルパ
# ----------------------------------------

def _pick_bool(source: dict[str, Any], aliases: list[str]) -> bool:
    """候補キー群から最初に見つかった truthy / falsy 値を bool 化して返す。"""
    for key in aliases:
        if key in source:
            return bool(source.get(key, False))
    return False


def _pick_int(source: dict[str, Any], aliases: list[str]) -> Optional[int]:
    """候補キー群から最初に見つかった値を int 化して返す。"""
    for key in aliases:
        if key not in source:
            continue
        raw = source.get(key)
        if raw is None or raw == "":
            return None
        try:
            return int(str(raw).strip())
        except Exception:
            return None
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


# ----------------------------------------
# 一般カテゴリ（食 / 運動 / 喫煙 / 休養 / その他）
# ----------------------------------------

def normalize_plan_goal_map(plan_goal_map: dict[str, Any]) -> dict[str, bool]:
    """計画側マップを短縮カテゴリ名へ正規化する。"""
    return {
        short_name: _pick_bool(plan_goal_map, aliases)
        for short_name, aliases in PLAN_KEY_ALIASES.items()
    }



def normalize_outcome_map(outcome_map: dict[str, Any]) -> dict[str, bool]:
    """結果側マップを短縮カテゴリ名へ正規化する。"""
    return {
        short_name: _pick_bool(outcome_map, aliases)
        for short_name, aliases in OUTCOME_KEY_ALIASES.items()
    }



def build_conflict_result(
    plan_goal_map: dict[str, Any],
    outcome_map: dict[str, Any],
    has_final: bool,
) -> dict[str, str]:
    """一般カテゴリの矛盾判定結果を返す。

    対象カテゴリ:
    - 食
    - 運動
    - 喫煙
    - 休養
    - その他

    判定:
    - conflict = (not goal) and achieve
    - final XML が無い場合は空
    - final XML がある場合
      - True  -> NG
      - False -> OK
    - 総合列は NG 項目があれば `Yes: 項目一覧`、無ければ `No`

    ※ 腹囲体重は専用ロジックで扱うため、この関数では判定しない。
    """
    if not has_final:
        return {
            "summary": "",
            "食": "",
            "運動": "",
            "喫煙": "",
            "休養": "",
            "その他": "",
        }

    plan_map = normalize_plan_goal_map(plan_goal_map)
    result_map = normalize_outcome_map(outcome_map)

    result: dict[str, str] = {"summary": "No"}
    ng_items: list[str] = []

    for short_name in GENERAL_CONFLICT_CATEGORY_ORDER:
        goal = plan_map.get(short_name, False)
        achieve = result_map.get(short_name, False)
        conflict = (not goal) and achieve
        result[short_name] = "NG" if conflict else "OK"
        if conflict:
            ng_items.append(short_name)

    if ng_items:
        result["summary"] = f"Yes: {', '.join(ng_items)}"

    return result


# ----------------------------------------
# 腹囲体重専用チェック
# ----------------------------------------

def compute_waist_weight_measured_level(
    exam_waist_cm: Any,
    final_waist_cm: Any,
    exam_weight_kg: Any,
    final_weight_kg: Any,
) -> Optional[int]:
    """健診時と最終報告時の実測差分から腹囲体重の達成レベルを返す。

    返り値:
    - 2 : 腹囲も体重も 2 以上改善
    - 1 : 腹囲も体重も 1 以上改善（ただし 2 には届かない）
    - 0 : 上記以外
    - None : 計算不能（値欠損など）
    """
    exam_waist = _to_float(exam_waist_cm)
    final_waist = _to_float(final_waist_cm)
    exam_weight = _to_float(exam_weight_kg)
    final_weight = _to_float(final_weight_kg)

    if None in {exam_waist, final_waist, exam_weight, final_weight}:
        return None

    waist_diff = float(exam_waist) - float(final_waist)
    weight_diff = float(exam_weight) - float(final_weight)

    if waist_diff >= 2.0 and weight_diff >= 2.0:
        return 2
    if waist_diff >= 1.0 and weight_diff >= 1.0:
        return 1
    return 0



def build_waist_weight_check_result(
    plan_level: Any,
    report_level: Any,
    exam_waist_cm: Any,
    final_waist_cm: Any,
    exam_weight_kg: Any,
    final_weight_kg: Any,
    has_final: bool,
) -> dict[str, Any]:
    """腹囲体重の整合チェック結果を返す。

    優先順位:
    1. 実測差分で計算可能なら、報告判定結果と実測差分の結果が一致するかを見る
    2. 1 が計算不能な場合のみ、報告判定結果と計画値が一致するかを見る

    返り値:
    - summary: `OK` / `NG` / ``
    - source: `measured` / `plan_fallback` / `uncheckable` / ``
    - plan_level: int | None
    - report_level: int | None
    - measured_level: int | None
    """
    if not has_final:
        return {
            "summary": "",
            "source": "",
            "plan_level": None,
            "report_level": None,
            "measured_level": None,
        }

    normalized_plan_level: Optional[int]
    normalized_report_level: Optional[int]

    try:
        normalized_plan_level = None if plan_level in {None, ""} else int(str(plan_level).strip())
    except Exception:
        normalized_plan_level = None

    try:
        normalized_report_level = None if report_level in {None, ""} else int(str(report_level).strip())
    except Exception:
        normalized_report_level = None

    measured_level = compute_waist_weight_measured_level(
        exam_waist_cm=exam_waist_cm,
        final_waist_cm=final_waist_cm,
        exam_weight_kg=exam_weight_kg,
        final_weight_kg=final_weight_kg,
    )

    if measured_level is not None and normalized_report_level is not None:
        return {
            "summary": "OK" if normalized_report_level == measured_level else "NG",
            "source": "measured",
            "plan_level": normalized_plan_level,
            "report_level": normalized_report_level,
            "measured_level": measured_level,
        }

    if normalized_plan_level is not None and normalized_report_level is not None:
        return {
            "summary": "OK" if normalized_report_level == normalized_plan_level else "NG",
            "source": "plan_fallback",
            "plan_level": normalized_plan_level,
            "report_level": normalized_report_level,
            "measured_level": measured_level,
        }

    return {
        "summary": "",
        "source": "uncheckable",
        "plan_level": normalized_plan_level,
        "report_level": normalized_report_level,
        "measured_level": measured_level,
    }