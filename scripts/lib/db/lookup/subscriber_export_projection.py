from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from scripts.lib.examination.lookup import qname
from scripts.lib.identity.field.insurance_number import normalize_insurance_number
from scripts.lib.identity.field.insurance_symbol import normalize_insurance_symbol
from scripts.lib.identity.field.name_kana import normalize_name_kana_full


SUBSCRIBER_EXPORT_SOURCE = "SUBSCRIBER"
SOURCE_EXPORT_SOURCE = "SOURCE"
NONE_EXPORT_SOURCE = "NONE"


@dataclass(frozen=True)
class ExportValue:
    value: str | None
    source: str
    reason: str | None


@dataclass(frozen=True)
class SubscriberBasicExportProjection:
    subscriber_id: int
    hia_subscriber_id: str | None
    insurance_symbol: str | None
    insurance_symbol_export: str | None
    insurance_number: str | None
    name_kana_full: str | None


@dataclass(frozen=True)
class BasicIdentityExportValues:
    insurance_symbol_export_value: str | None
    insurance_symbol_export_source: str
    insurance_symbol_export_reason: str | None
    insurance_number_export_value: str | None
    insurance_number_export_source: str
    insurance_number_export_reason: str | None
    name_kana_export_value: str | None
    name_kana_export_source: str
    name_kana_export_reason: str | None

    def as_db_params(self) -> dict[str, Any]:
        return {
            "insurance_symbol_export_value": self.insurance_symbol_export_value,
            "insurance_symbol_export_source": self.insurance_symbol_export_source,
            "insurance_symbol_export_reason": self.insurance_symbol_export_reason,
            "insurance_number_export_value": self.insurance_number_export_value,
            "insurance_number_export_source": self.insurance_number_export_source,
            "insurance_number_export_reason": self.insurance_number_export_reason,
            "name_kana_export_value": self.name_kana_export_value,
            "name_kana_export_source": self.name_kana_export_source,
            "name_kana_export_reason": self.name_kana_export_reason,
        }


def _subscribers_table(dev_db: str) -> str:
    return f"{qname(dev_db)}.`subscribers`"


def load_subscriber_basic_export_projection_by_id(
    cur: Any,
    *,
    subscriber_id: int | None,
    dev_db: str,
) -> SubscriberBasicExportProjection | None:
    if subscriber_id is None:
        return None
    cur.execute(
        f"""
        SELECT
          id AS subscriber_id,
          hia_subscriber_id,
          insurance_symbol,
          insurance_symbol_export,
          insurance_number,
          name_kana_full
        FROM {_subscribers_table(dev_db)}
        WHERE id = %s
        """,
        (subscriber_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return SubscriberBasicExportProjection(
        subscriber_id=int(row["subscriber_id"]),
        hia_subscriber_id=row.get("hia_subscriber_id"),
        insurance_symbol=row.get("insurance_symbol"),
        insurance_symbol_export=row.get("insurance_symbol_export"),
        insurance_number=row.get("insurance_number"),
        name_kana_full=row.get("name_kana_full"),
    )


def _symbol_export(raw: Any, source: str) -> ExportValue:
    result = normalize_insurance_symbol(None if raw is None else str(raw))
    if result.get("ok") and result.get("export"):
        return ExportValue(str(result["export"]), source, None)
    return ExportValue(None, NONE_EXPORT_SOURCE, str(result.get("reason") or "INSURANCE_SYMBOL_EXPORT_UNAVAILABLE"))


def _number_export(raw: Any, source: str) -> ExportValue:
    result = normalize_insurance_number(None if raw is None else str(raw))
    if result.get("ok") and result.get("field_norm"):
        return ExportValue(str(result["field_norm"]), source, None)
    return ExportValue(None, NONE_EXPORT_SOURCE, str(result.get("reason") or "INSURANCE_NUMBER_EXPORT_UNAVAILABLE"))


def _kana_export(raw: Any, source: str) -> ExportValue:
    result = normalize_name_kana_full(None if raw is None else str(raw))
    if result.get("ok") and result.get("field_norm"):
        return ExportValue(str(result["field_norm"]), source, None)
    return ExportValue(None, NONE_EXPORT_SOURCE, str(result.get("reason") or "NAME_KANA_EXPORT_UNAVAILABLE"))


def resolve_basic_identity_export_values(
    source_row: Mapping[str, Any],
    *,
    subscriber: SubscriberBasicExportProjection | None,
) -> BasicIdentityExportValues:
    """Resolve XML/HIA export values without using subscribers match-only columns."""
    if subscriber is not None:
        symbol_raw = subscriber.insurance_symbol_export or subscriber.insurance_symbol
        symbol = _symbol_export(symbol_raw, SUBSCRIBER_EXPORT_SOURCE)
        number = _number_export(subscriber.insurance_number, SUBSCRIBER_EXPORT_SOURCE)
        kana = _kana_export(subscriber.name_kana_full, SUBSCRIBER_EXPORT_SOURCE)
    else:
        symbol = _symbol_export(source_row.get("insurance_symbol_raw"), SOURCE_EXPORT_SOURCE)
        number = _number_export(source_row.get("insurance_number_raw"), SOURCE_EXPORT_SOURCE)
        kana = _kana_export(source_row.get("name_kana_raw"), SOURCE_EXPORT_SOURCE)

    if subscriber is not None and symbol.value is None:
        symbol = _symbol_export(source_row.get("insurance_symbol_raw"), SOURCE_EXPORT_SOURCE)
    if subscriber is not None and number.value is None:
        number = _number_export(source_row.get("insurance_number_raw"), SOURCE_EXPORT_SOURCE)
    if subscriber is not None and kana.value is None:
        kana = _kana_export(source_row.get("name_kana_raw"), SOURCE_EXPORT_SOURCE)

    return BasicIdentityExportValues(
        insurance_symbol_export_value=symbol.value,
        insurance_symbol_export_source=symbol.source,
        insurance_symbol_export_reason=symbol.reason,
        insurance_number_export_value=number.value,
        insurance_number_export_source=number.source,
        insurance_number_export_reason=number.reason,
        name_kana_export_value=kana.value,
        name_kana_export_source=kana.source,
        name_kana_export_reason=kana.reason,
    )
