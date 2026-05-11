/*
  mart_anomalies
  --------------
  Anomaly firings sorted for the operations dashboard's default view:
  most recent first, then by severity (critical above warning above
  info), then by store_id for stable tiebreaking.

  Severity ordering uses an inline case expression — the canonical
  ordering is defined in economic-data-etl/src/schemas.SEVERITY_LEVELS
  and mirrored here. Keeping this mapping inline (instead of joining a
  seed) avoids a dbt seed dependency for three rows.
*/

select
    flag_date,
    store_id,
    rule_id,
    actual_value,
    expected_low,
    expected_high,
    distance_from_band,
    severity_score,
    severity_level
from {{ ref('stg_anomaly_flags') }}
order by
    flag_date desc,
    case severity_level
        when 'critical' then 0
        when 'warning'  then 1
        when 'info'     then 2
        else 3
    end,
    store_id
