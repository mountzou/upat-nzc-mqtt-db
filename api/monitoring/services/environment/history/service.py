"""One SQL aggregation from disjoint rollups and raw samples, with exact edges.

A partially used rollup cannot be divided without the original samples. Check
that its processed samples still exist before reading the clipped edge. This
fails explicitly after raw retention instead of inventing a historical value.
"""
from fastapi import HTTPException
from pydantic import BaseModel, AwareDatetime

from monitoring.read_limits import _reads
from monitoring.schemas import DeviceHistoryBucketItem, OverviewReading
from monitoring.utils.timezone import to_app_timezone
from .contract import HistoryPlan

class SampleMean(OverviewReading):
    sample_count: int

class HistoryBucket(DeviceHistoryBucketItem):
    end: AwareDatetime
    measurements: dict[str, SampleMean]

class EnvironmentalHistoryResponse(BaseModel):
    device_id: str
    start: AwareDatetime
    end: AwareDatetime
    interval: str
    alignment: str
    limit: int
    count: int
    items: list[HistoryBucket]


def query_history_rows(device_id: str, plan: HistoryPlan):
    import main
    hourly = plan.alignment == 'clock'
    table = 'upat_measurements_hourly' if hourly else 'upat_measurements_5min'
    step = '1 hour' if hourly else '5 minutes'
    # Only trusted table/interval literals are interpolated; inputs are bound.
    sql = f"""
    WITH state AS MATERIALIZED (
        SELECT last_measurement_id FROM upat_rollup_state WHERE pipeline_name='upat'
    ), requested AS (
        SELECT lo, hi FROM unnest(%s::timestamptz[], %s::timestamptz[]) AS b(lo,hi)
    ), bins AS MATERIALIZED (
        SELECT lo, hi,
          date_bin(INTERVAL '{step}', lo, TIMESTAMPTZ '2001-01-01 00:00Z')
            + CASE WHEN lo = date_bin(INTERVAL '{step}', lo, TIMESTAMPTZ '2001-01-01 00:00Z')
                   THEN INTERVAL '0' ELSE INTERVAL '{step}' END AS full_lo,
          date_bin(INTERVAL '{step}', hi, TIMESTAMPTZ '2001-01-01 00:00Z') AS full_hi
        FROM requested
    ), split_rollups AS MATERIALIZED (
        SELECT DISTINCT r.bucket_start, r.metric, r.sample_count
        FROM bins b JOIN {table} r ON r.device_id=%s AND r.metric=ANY(%s)
          AND r.bucket_start >= date_bin(INTERVAL '{step}', %s::timestamptz, TIMESTAMPTZ '2001-01-01 00:00Z')
          AND r.bucket_start < %s
          AND r.bucket_start < b.hi AND r.bucket_start > b.lo - INTERVAL '{step}'
          AND (r.bucket_start < b.lo OR r.bucket_start + INTERVAL '{step}' > b.hi)
    ), unavailable_edges AS (
        SELECT COUNT(*) AS n FROM split_rollups r CROSS JOIN state s
        WHERE r.sample_count <> (
          SELECT COUNT(*) FROM upat_measurements m
          WHERE m.device_id=%s AND m.metric=r.metric AND m.value IS NOT NULL
            AND m.id <= s.last_measurement_id
            AND m.event_time >= r.bucket_start
            AND m.event_time < r.bucket_start + INTERVAL '{step}'
        )
    ), sources AS (
        SELECT b.lo, b.hi, r.metric, r.unit,
               r.value_avg * r.sample_count AS value_sum, r.sample_count::bigint AS n
        FROM bins b JOIN {table} r ON r.device_id=%s AND r.metric=ANY(%s)
          AND r.bucket_start >= %s AND r.bucket_start < %s
          AND r.bucket_start >= b.full_lo AND r.bucket_start < b.full_hi
        WHERE r.sample_count > 0 AND r.value_avg IS NOT NULL
        UNION ALL
        SELECT b.lo, b.hi, m.metric, MAX(m.unit), SUM(m.value), COUNT(*)
        FROM bins b CROSS JOIN state s JOIN upat_measurements m
          ON m.device_id=%s AND m.metric=ANY(%s) AND m.value IS NOT NULL
          AND m.event_time >= %s AND m.event_time < %s
          AND m.event_time >= b.lo AND m.event_time < b.hi
          AND (m.id > s.last_measurement_id OR NOT (m.event_time >= b.full_lo AND m.event_time < b.full_hi))
        GROUP BY b.lo,b.hi,m.metric
    ), means AS (
        SELECT lo,hi,metric,MAX(unit) AS unit,
               SUM(value_sum)/NULLIF(SUM(n),0) AS value, SUM(n)::bigint AS sample_count
        FROM sources GROUP BY lo,hi,metric
    )
    SELECT m.*, e.n AS unavailable_edges, (SELECT COUNT(*) FROM state) AS state_count
    FROM unavailable_edges e LEFT JOIN means m ON TRUE
    ORDER BY lo DESC, metric
    """
    metrics = list(plan.metrics)
    params = ([b[0] for b in plan.buckets], [b[1] for b in plan.buckets],
              device_id, metrics, plan.start, plan.end, device_id,
              device_id, metrics, plan.start, plan.end,
              device_id, metrics, plan.start, plan.end)
    if not _reads.acquire(timeout=5):
        raise HTTPException(503, 'Monitoring data is busy. Try again shortly.')
    try:
        with main.get_connection() as conn, conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '10s'")
            # Small indexed reads do not benefit from compiling this CTE tree.
            cur.execute("SET LOCAL jit = off")
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        _reads.release()
    if not rows or rows[0]['state_count'] != 1:
        raise HTTPException(503, 'Environmental rollup state is unavailable')
    if rows[0]['unavailable_edges']:
        raise HTTPException(422, 'Exact window boundaries require raw samples that are no longer retained; use complete 5-minute window buckets or clock analytics')
    return rows


def get_environmental_history(device_id: str, plan: HistoryPlan) -> EnvironmentalHistoryResponse:
    rows = query_history_rows(device_id, plan)
    grouped = {}
    for row in rows:
        if row['lo'] is None:
            continue
        key = row['lo']
        if key not in grouped:
            grouped[key] = HistoryBucket(device_id=device_id, event_time=to_app_timezone(key),
                                         end=to_app_timezone(row['hi']), measurements={})
        grouped[key].measurements[row['metric']] = SampleMean(
            value=row['value'], unit=row['unit'], sample_count=row['sample_count'])
    return EnvironmentalHistoryResponse(
        device_id=device_id, start=to_app_timezone(plan.start), end=to_app_timezone(plan.end),
        interval=plan.interval.value, alignment=plan.alignment, limit=plan.limit,
        count=len(grouped), items=list(grouped.values()))
