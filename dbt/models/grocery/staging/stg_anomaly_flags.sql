/*
  stg_anomaly_flags
  -----------------
  Cleaned view of raw.fact_anomaly_flags. Casts the date column to
  SQL DATE and renames it to flag_date to avoid the reserved keyword.
  All other columns pass through with their canonical names from the
  detection schema (rule_id, severity_level, etc.).

  Materialised as a view. The mart layer joins this view with the
  store-grain metrics for dashboard rollups.
*/

select
    cast(date as date)         as flag_date,
    store_id,
    rule_id,
    actual_value,
    expected_low,
    expected_high,
    distance_from_band,
    severity_score,
    severity_level
from {{ source('grocery', 'fact_anomaly_flags') }}
