from typing import Any, Dict, Iterable, Mapping, cast

from scripts.lib.db.mysql import dict_cursor
from scripts.lib.db.schemas import DEV_PHR



class SubscriberContactPointLookupError(RuntimeError):
    pass


# Simple PK lookup helper for subscriber_contact_points
def get_contact_point_by_id(
    conn,
    contact_point_id: int | None,
) -> Dict[str, Any] | None:
    """Return one subscriber_contact_points row by contact_point_id.

    This is a simple PK lookup helper.
    It does not validate expected contact_type because the caller already knows
    whether the id came from current_phone_contact_point_id or
    current_email_contact_point_id.
    """
    if contact_point_id is None:
        return None

    sql = f"""
        SELECT
            contact_point_id,
            subscriber_id,
            contact_type,
            contact_value,
            is_current,
            valid_from,
            valid_to,
            source,
            created_at,
            updated_at
        FROM {DEV_PHR}.subscriber_contact_points
        WHERE contact_point_id = %s
        LIMIT 1
    """

    cur = dict_cursor(conn)
    try:
        cur.execute(sql, (contact_point_id,))
        row = cast(Mapping[str, Any] | None, cur.fetchone())
    finally:
        cur.close()

    return dict(row) if row else None


def get_current_contact_points_by_subscriber_ids(
    conn,
    subscriber_ids: Iterable[int],
) -> Mapping[int, Dict[str, Any]]:
    """
    Return current contact points keyed by subscriber_id.

    Example:
    {
        1001: {
            "phone": "090xxxx",
            "email": "aaa@example.com",
        }
    }

    Missing contact points are allowed.
    Missing subscriber ids raise SubscriberContactPointLookupError.
    """
    requested_ids = list(dict.fromkeys(subscriber_ids))
    if not requested_ids:
        return {}

    # Prepare result dict
    result: Dict[int, Dict[str, Any]] = {}

    placeholders = ",".join(["%s"] * len(requested_ids))
    sql = f"""
        SELECT
            s.id AS subscriber_id,
            cp.contact_type,
            cp.contact_value
        FROM {DEV_PHR}.subscribers s
        LEFT JOIN {DEV_PHR}.subscriber_contact_points cp
            ON cp.subscriber_id = s.id
           AND cp.is_current = 1
        WHERE s.id IN ({placeholders})
    """

    cur = dict_cursor(conn)
    try:
        cur.execute(sql, requested_ids)
        rows = cast(list[Mapping[str, Any]], cur.fetchall() or [])
    finally:
        cur.close()

    # Build result and track existing subscriber ids
    existing_subscriber_ids = set()
    for row in rows:
        sid = row["subscriber_id"]
        existing_subscriber_ids.add(sid)
        if sid not in result:
            result[sid] = {}
        contact_type = row.get("contact_type")
        contact_value = row.get("contact_value")
        if contact_type is not None:
            result[sid][contact_type] = contact_value

    missing_ids = set(requested_ids) - existing_subscriber_ids
    if missing_ids:
        raise SubscriberContactPointLookupError(
            f"subscriber ids not found: {sorted(missing_ids)}"
        )

    for sid in requested_ids:
        result.setdefault(sid, {})

    return result