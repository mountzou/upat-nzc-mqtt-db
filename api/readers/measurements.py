"""Latest measurement snapshots, without URL dispatch or JSON serialization."""
import database
from schemas import normalize_metrics


def round_numeric(value):
    if isinstance(value, (int, float)) and value is not None:
        return round(value, 1)
    return value


def format_response_object(device_id, rows, metrics=None):
    """Shared snapshot envelope; preserve native timestamps until HTTP output."""
    snapshots = []
    snapshots_by_time = {}

    for row in rows:
        event_time = row["event_time"]
        event_time_key = event_time.isoformat() if event_time else "null"

        if metrics and row["metric"] not in metrics:
            continue

        if event_time_key not in snapshots_by_time:
            snapshot = {
                "device_id": row["device_id"],
                "event_time": event_time,
                "measurements": {},
            }
            snapshots_by_time[event_time_key] = snapshot
            snapshots.append(snapshot)

        snapshots_by_time[event_time_key]["measurements"][row["metric"]] = {
            "value": round_numeric(row["value"]),
            "unit": row["unit"],
        }

    return {
        "device_id": device_id,
        "count": len(snapshots),
        "items": snapshots,
    }


def fetch_device_latest(table_name, device_id, metrics, limit, *, connection_factory=None):
    """Read the existing minute averages from an internally selected table.

    Routes may supply their connection factory; services use the shared database
    module directly. The SQL and snapshot limit have the original semantics.
    """
    normalized_metrics = normalize_metrics(metrics)

    query_params = []
    query_parts = [f"""
        WITH aggregated AS (
            SELECT
                device_id,
                metric,
                AVG(value) AS value,
                unit,
                date_bin(
                    INTERVAL '1 minute',
                    event_time,
                    TIMESTAMPTZ '2001-01-01 00:00:00+00'
                ) AS bucket_time
            FROM {table_name}
            WHERE device_id = %s
    """]
    query_params.append(device_id)

    if normalized_metrics:
        query_parts.append(" AND metric = ANY(%s)")
        query_params.append(normalized_metrics)

    query_parts.append("""
            GROUP BY device_id, metric, unit, bucket_time
        ),
        selected_times AS (
            SELECT DISTINCT bucket_time
            FROM aggregated
            ORDER BY bucket_time DESC
            LIMIT %s
        )
        SELECT
            device_id,
            metric,
            value,
            unit,
            bucket_time AS event_time
        FROM aggregated
        WHERE bucket_time IN (SELECT bucket_time FROM selected_times)
        ORDER BY bucket_time DESC, metric ASC;
    """)
    query_params.append(limit)

    connect = connection_factory if connection_factory is not None else database.get_connection
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("".join(query_parts), query_params)
            rows = cur.fetchall()

    return format_response_object(device_id, rows, normalized_metrics)
