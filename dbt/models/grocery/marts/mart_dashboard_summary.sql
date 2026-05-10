/*
  mart_dashboard_summary
  ----------------------
  KPI rollup with one row per date. Aggregates store-day metrics
  across all stores for the trend chart at the top of the operations
  dashboard, then attaches the count of anomaly firings on that date
  so the portal can highlight days with operational issues.

  Columns:
    metric_date          — calendar date of the rollup
    total_net_sales      — sum of total_sales across all stores that day
    total_transactions   — sum of transaction_count across all stores
    avg_labor_cost_pct   — store-weighted mean of labor_cost_pct
    anomaly_count        — number of anomaly firings on the date (zero
                           when none fired)

  Materialised as a table — one row per date over the rolling window
  is small enough that materialisation is cheap and the portal benefits
  from a fast, indexed scan.
*/

with daily_store as (
    select
        metric_date,
        sum(total_sales)                            as total_net_sales,
        sum(transaction_count)                      as total_transactions,
        avg(labor_cost_pct)                         as avg_labor_cost_pct
    from {{ ref('stg_store_metrics') }}
    group by metric_date
),
daily_anomalies as (
    select
        flag_date                                   as metric_date,
        count(*)                                    as anomaly_count
    from {{ ref('stg_anomaly_flags') }}
    group by flag_date
)

select
    s.metric_date,
    s.total_net_sales,
    s.total_transactions,
    s.avg_labor_cost_pct,
    coalesce(a.anomaly_count, 0)                    as anomaly_count
from daily_store as s
left join daily_anomalies as a
    on s.metric_date = a.metric_date
order by
    s.metric_date
