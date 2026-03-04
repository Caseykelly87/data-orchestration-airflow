/*
  mart_economic_summary
  ---------------------
  One row per series showing the most recent available observation.

  Provides a 14-row snapshot of the current state of the U.S. economy
  across all tracked indicators — suitable for dashboards, alerts, or
  period-over-period comparison reports.

  Approach:
    1. Find the max (most recent) observation_date for each series.
    2. Join back to stg_observations to retrieve the corresponding value.

  Materialized as a table (configured in dbt_project.yml).
  Sorted by source (FRED first, BLS second) then series_name.
*/

with latest_dates as (
    select
        series_id,
        series_name,
        source,
        max(observation_date) as latest_date
    from {{ ref('stg_observations') }}
    group by
        series_id,
        series_name,
        source
)

select
    ld.series_id,
    ld.series_name,
    ld.source,
    ld.latest_date,
    obs.value as latest_value
from latest_dates as ld
join {{ ref('stg_observations') }} as obs
    on  obs.series_id        = ld.series_id
    and obs.observation_date = ld.latest_date
order by
    ld.source,
    ld.series_name
