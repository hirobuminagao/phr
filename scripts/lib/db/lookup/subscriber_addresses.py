from typing import Any, Dict, Iterable, Mapping, cast

from scripts.lib.db.mysql import dict_cursor
from scripts.lib.db.schemas import DEV_PHR


class SubscriberAddressLookupError(RuntimeError):
    pass


def get_current_addresses_by_subscriber_ids(
    conn,
    subscriber_ids: Iterable[int],
) -> Mapping[int, Dict[str, Any]]:
    """
    Return current addresses keyed by subscriber_id.

    Example:
    {
        1001: {
            "postal_code": "1000001",
            "address_line": "東京都...",
            "building": "サンプルビル101",
        }
    }

    Missing addresses are allowed.
    Missing subscriber ids raise SubscriberAddressLookupError.
    """

    requested_ids = list(dict.fromkeys(subscriber_ids))
    if not requested_ids:
        return {}

    result: Dict[int, Dict[str, Any]] = {}

    placeholders = ",".join(["%s"] * len(requested_ids))

    sql = f"""
        SELECT
            s.id AS subscriber_id,
            a.postal_code,
            a.address_line,
            a.building
        FROM {DEV_PHR}.subscribers s
        LEFT JOIN {DEV_PHR}.subscriber_addresses a
            ON a.subscriber_id = s.id
           AND a.is_current = 1
        WHERE s.id IN ({placeholders})
    """

    cursor = dict_cursor(conn)
    try:
        cursor.execute(sql, tuple(requested_ids))
        rows = cast(list[Mapping[str, Any]], cursor.fetchall() or [])
    finally:
        cursor.close()

    existing_subscriber_ids: set[int] = set()

    for row in rows:
        subscriber_id = int(row["subscriber_id"])
        existing_subscriber_ids.add(subscriber_id)

        result[subscriber_id] = {
            "postal_code": row.get("postal_code"),
            "address_line": row.get("address_line"),
            "building": row.get("building"),
        }

    missing_ids = set(requested_ids) - existing_subscriber_ids
    if missing_ids:
        raise SubscriberAddressLookupError(
            f"subscriber ids not found: {sorted(missing_ids)}"
        )

    for subscriber_id in requested_ids:
        result.setdefault(subscriber_id, {})

    return result