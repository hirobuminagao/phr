from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, cast

import yaml


def load_yaml_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}
    if not isinstance(data, Mapping):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return dict(cast(Mapping[str, Any], data))


def config_value(config: Mapping[str, Any], key: str, default: Any) -> Any:
    value = config.get(key, default)
    return default if value is None else value


def config_bool(config: Mapping[str, Any], key: str, default: bool = False) -> bool:
    value = config_value(config, key, default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"{key} must be boolean-like: {value!r}")
