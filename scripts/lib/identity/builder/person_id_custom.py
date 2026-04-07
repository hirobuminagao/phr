from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast


def _default_custom_id_resource_dir() -> Path:
    """custom_id 用静的リソース配置先を返す。"""
    return Path(__file__).resolve().parents[4] / "resources" / "identity" / "custom_id"


def _load_custom_id_config(resource_dir: Path) -> dict[str, Any]:
    """custom_id 設定 JSON を読み込む。"""
    path = resource_dir / "custom_id_config.json"
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_custom_id_mapping(resource_dir: Path, mapping_file: str) -> dict[str, dict[str, str]]:
    """custom_id 置換表 JSON を読み込む。"""
    path = resource_dir / mapping_file
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _validate_required_sections(config: dict[str, Any]) -> tuple[bool, str | None]:
    """設定ファイルの必須セクション存在を確認する。"""
    required_sections = ["add", "mul", "lengths", "mapping_file", "compose_order"]
    for section in required_sections:
        if section not in config:
            return False, f"missing_config_section:{section}"
    return True, None


def _validate_required_keys(config: dict[str, Any]) -> tuple[bool, str | None]:
    """canonical key が揃っているか確認する。"""
    required_keys = {"birth_yyyymmdd", "insurance_number", "insurer_number", "symbol"}

    for section_name in ("add", "mul", "lengths"):
        section = config.get(section_name, {})
        missing = required_keys - set(section.keys())
        if missing:
            missing_names = ",".join(sorted(missing))
            return False, f"missing_config_keys:{section_name}:{missing_names}"

    compose_order = config.get("compose_order", [])
    missing_in_order = required_keys - set(compose_order)
    if missing_in_order:
        missing_names = ",".join(sorted(missing_in_order))
        return False, f"missing_compose_order_keys:{missing_names}"

    return True, None


def _validate_mapping_keys(
    mapping: dict[str, dict[str, str]],
    compose_order: list[str],
) -> tuple[bool, str | None]:
    """mapping 側に必要な key と digit mapping があるか確認する。"""
    for field in compose_order:
        if field not in mapping:
            return False, f"missing_mapping_field:{field}"
        field_map = mapping[field]
        for digit in "0123456789":
            if digit not in field_map:
                return False, f"missing_mapping_digit:{field}:{digit}"
    return True, None


def _validate_canonical_inputs(canonical_inputs: dict[str, str]) -> tuple[bool, str | None]:
    """builder に渡された canonical input を確認する。"""
    for field, value in canonical_inputs.items():
        if value == "":
            return False, f"empty_canonical_input:{field}"
        if not value.isdigit():
            return False, f"invalid_non_digit_input:{field}"
    return True, None


def _apply_add_mul_mod(value: str, add: int, mul: int, length: int) -> str:
    """(value + add) * mul を length 桁へ収め、0埋め文字列で返す。"""
    n = int(value)
    mod_base = 10**length
    computed = ((n + add) * mul) % mod_base
    return str(computed).zfill(length)


def _apply_digit_mapping(value: str, field_mapping: dict[str, str]) -> str:
    """数字文字列に置換表を適用する。"""
    return "".join(field_mapping[ch] for ch in value)


def _build_encoded_parts(
    *,
    canonical_inputs: dict[str, str],
    config: dict[str, Any],
    mapping: dict[str, dict[str, str]],
) -> dict[str, str]:
    """各 field の encoded part を生成する。"""
    add_cfg: dict[str, int] = config["add"]
    mul_cfg: dict[str, int] = config["mul"]
    lengths_cfg: dict[str, int] = config["lengths"]

    encoded_parts: dict[str, str] = {}
    for field, raw_value in canonical_inputs.items():
        padded_digits = _apply_add_mul_mod(
            value=raw_value,
            add=add_cfg[field],
            mul=mul_cfg[field],
            length=lengths_cfg[field],
        )
        encoded_parts[field] = _apply_digit_mapping(padded_digits, mapping[field])

    return encoded_parts


def _compose_person_id_custom(*, encoded_parts: dict[str, str], compose_order: list[str]) -> str:
    """compose_order に従って encoded part を連結する。"""
    return "".join(encoded_parts[field] for field in compose_order)


def build_person_id_custom(
    *,
    birthdate_match: str | None,
    insurance_number_match: str | None,
    insurer_number_match: str | None,
    insurance_symbol_person_id_custom: str | None,
    resource_dir: str | Path | None = None,
) -> dict:
    """person_id_custom を生成する。

    v1.1.0 方針:

    - builder は再正規化しない
    - field 側で生成された canonical 値だけを受け取る
    - 必須材料が1つでも欠ければ生成しない
    - 生成ロジックは本ファイル内の private helper に閉じ込める

    使用する canonical input は以下で固定する。

    1. `birthdate_match`               -> `birth_yyyymmdd`
    2. `insurance_number_match`       -> `insurance_number`
    3. `insurer_number_match`         -> `insurer_number`
    4. `insurance_symbol_person_id_custom` -> `symbol`
    """
    inputs = {
        "birthdate_match": birthdate_match,
        "insurance_number_match": insurance_number_match,
        "insurer_number_match": insurer_number_match,
        "insurance_symbol_person_id_custom": insurance_symbol_person_id_custom,
    }

    missing_fields = [name for name, value in inputs.items() if value in (None, "")]
    if missing_fields:
        return {
            "name": "person_id_custom",
            "value": None,
            "ok": False,
            "missing_fields": missing_fields,
            "upstream_missing_fields": missing_fields[:],
            "reason": "missing_required_fields",
        }

    canonical_inputs = {
        "birth_yyyymmdd": cast(str, birthdate_match),
        "insurance_number": cast(str, insurance_number_match),
        "insurer_number": cast(str, insurer_number_match),
        "symbol": cast(str, insurance_symbol_person_id_custom),
    }

    resource_root = Path(resource_dir) if resource_dir is not None else _default_custom_id_resource_dir()

    config = _load_custom_id_config(resource_root)
    ok, reason = _validate_required_sections(config)
    if not ok:
        return {
            "name": "person_id_custom",
            "value": None,
            "ok": False,
            "missing_fields": [],
            "upstream_missing_fields": [],
            "reason": reason,
        }

    ok, reason = _validate_required_keys(config)
    if not ok:
        return {
            "name": "person_id_custom",
            "value": None,
            "ok": False,
            "missing_fields": [],
            "upstream_missing_fields": [],
            "reason": reason,
        }

    ok, reason = _validate_canonical_inputs(canonical_inputs)
    if not ok:
        return {
            "name": "person_id_custom",
            "value": None,
            "ok": False,
            "missing_fields": [],
            "upstream_missing_fields": [],
            "reason": reason,
        }

    mapping = _load_custom_id_mapping(resource_root, config["mapping_file"])
    compose_order: list[str] = config["compose_order"]

    ok, reason = _validate_mapping_keys(mapping, compose_order)
    if not ok:
        return {
            "name": "person_id_custom",
            "value": None,
            "ok": False,
            "missing_fields": [],
            "upstream_missing_fields": [],
            "reason": reason,
        }

    encoded_parts = _build_encoded_parts(
        canonical_inputs=canonical_inputs,
        config=config,
        mapping=mapping,
    )
    value = _compose_person_id_custom(
        encoded_parts=encoded_parts,
        compose_order=compose_order,
    )

    if value == "":
        return {
            "name": "person_id_custom",
            "value": None,
            "ok": False,
            "missing_fields": [],
            "upstream_missing_fields": [],
            "reason": "generator_returned_empty",
        }

    return {
        "name": "person_id_custom",
        "value": value,
        "ok": True,
        "missing_fields": [],
        "upstream_missing_fields": [],
        "reason": None,
    }