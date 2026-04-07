# -*- coding: utf-8 -*-
"""
identity generator

責務:
- raw を受ける
- field を通して canonical を生成
- builder に渡す
- person_id_custom / identity_hash を返す

注意:
- DBやI/Oは持たない
- builderの前段オーケストレーションのみ
"""

from scripts.lib.identity.field.birthdate import normalize_birthdate
from scripts.lib.identity.field.insurer_number import normalize_insurer_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.name_kana import normalize_name_kana_full

from scripts.lib.identity.builder.person_id_custom import build_person_id_custom
from scripts.lib.identity.builder.identity_hash import build_identity_hash


# ------------------------------------------------------------
# person_id_custom
# ------------------------------------------------------------
def generate_person_id_custom(
    *,
    birthdate,
    insurer_number_raw,
    insurance_symbol_raw,
    insurance_number_raw,
) -> dict:
    """person_id_custom を生成する"""

    field_results = {}

    birth_res = normalize_birthdate(birthdate)
    insurer_res = normalize_insurer_number(insurer_number_raw)
    symbol_res = normalize_insurance_symbol(insurance_symbol_raw)
    number_res = normalize_insurance_number(insurance_number_raw)

    field_results["birthdate"] = birth_res
    field_results["insurer_number"] = insurer_res
    field_results["insurance_symbol"] = symbol_res
    field_results["insurance_number"] = number_res

    for field_key, res in field_results.items():
        if not res["ok"]:
            return {
                "ok": False,
                "value": None,
                "builder_result": None,
                "field_results": field_results,
                "reason": f"{field_key} NG: {res['reason']}",
            }

    builder_result = build_person_id_custom(
        birthdate_match=birth_res["match"],
        insurer_number_match=insurer_res["match"],
        insurance_symbol_person_id_custom=symbol_res["person_id_custom"],
        insurance_number_match=number_res["match"],
    )

    if not builder_result["ok"]:
        return {
            "ok": False,
            "value": None,
            "builder_result": builder_result,
            "field_results": field_results,
            "reason": f"person_id_custom NG: {builder_result['reason']}",
        }

    return {
        "ok": True,
        "value": builder_result["value"],
        "builder_result": builder_result,
        "field_results": field_results,
        "reason": None,
    }


# ------------------------------------------------------------
# identity_hash
# ------------------------------------------------------------
def generate_identity_hash(
    *,
    name_kana_full_raw,
    gender_code,
    person_id_custom=None,
    birthdate=None,
    insurer_number_raw=None,
    insurance_symbol_raw=None,
    insurance_number_raw=None,
) -> dict:
    """identity_hash を生成する（2モード対応）"""

    field_results = {}
    person_id_custom_result = None

    # person_id_custom が無い場合は内部生成
    if person_id_custom is None:
        pid_res = generate_person_id_custom(
            birthdate=birthdate,
            insurer_number_raw=insurer_number_raw,
            insurance_symbol_raw=insurance_symbol_raw,
            insurance_number_raw=insurance_number_raw,
        )

        if not pid_res["ok"]:
            return {
                "ok": False,
                "value": None,
                "person_id_custom": None,
                "person_id_custom_result": pid_res,
                "builder_result": None,
                "field_results": pid_res["field_results"],
                "reason": pid_res["reason"],
            }

        person_id_custom = pid_res["value"]
        person_id_custom_result = pid_res
        field_results.update(pid_res["field_results"])

    name_res = normalize_name_kana_full(name_kana_full_raw)
    field_results["name_kana_full"] = name_res

    if not name_res["ok"]:
        return {
            "ok": False,
            "value": None,
            "person_id_custom": person_id_custom,
            "person_id_custom_result": person_id_custom_result,
            "builder_result": None,
            "field_results": field_results,
            "reason": f"name_kana_full NG: {name_res['reason']}",
        }

    builder_result = build_identity_hash(
        person_id_custom=person_id_custom,
        name_kana_full_match=name_res["match"],
        gender_code_match=gender_code,
    )

    if not builder_result["ok"]:
        return {
            "ok": False,
            "value": None,
            "person_id_custom": person_id_custom,
            "person_id_custom_result": person_id_custom_result,
            "builder_result": builder_result,
            "field_results": field_results,
            "reason": f"identity_hash NG: {builder_result['reason']}",
        }

    return {
        "ok": True,
        "value": builder_result["value"],
        "person_id_custom": person_id_custom,
        "person_id_custom_result": person_id_custom_result,
        "builder_result": builder_result,
        "field_results": field_results,
        "reason": None,
    }


# ------------------------------------------------------------
# bundle
# ------------------------------------------------------------
def generate_identity_bundle(**kwargs) -> dict:
    """person_id_custom + identity_hash をまとめて生成"""

    pid_res = generate_person_id_custom(
        birthdate=kwargs.get("birthdate"),
        insurer_number_raw=kwargs.get("insurer_number_raw"),
        insurance_symbol_raw=kwargs.get("insurance_symbol_raw"),
        insurance_number_raw=kwargs.get("insurance_number_raw"),
    )

    if not pid_res["ok"]:
        return {
            "ok": False,
            "person_id_custom": None,
            "identity_hash": None,
            "person_id_custom_result": pid_res,
            "identity_hash_result": None,
            "field_results": pid_res["field_results"],
            "reason": pid_res["reason"],
        }

    hash_res = generate_identity_hash(
        person_id_custom=pid_res["value"],
        name_kana_full_raw=kwargs.get("name_kana_full_raw"),
        gender_code=kwargs.get("gender_code"),
    )

    if not hash_res["ok"]:
        return {
            "ok": False,
            "person_id_custom": pid_res["value"],
            "identity_hash": None,
            "person_id_custom_result": pid_res,
            "identity_hash_result": hash_res,
            "field_results": {
                **pid_res["field_results"],
                **hash_res["field_results"],
            },
            "reason": hash_res["reason"],
        }

    return {
        "ok": True,
        "person_id_custom": pid_res["value"],
        "identity_hash": hash_res["value"],
        "person_id_custom_result": pid_res,
        "identity_hash_result": hash_res,
        "field_results": {
            **pid_res["field_results"],
            **hash_res["field_results"],
        },
        "reason": None,
    }