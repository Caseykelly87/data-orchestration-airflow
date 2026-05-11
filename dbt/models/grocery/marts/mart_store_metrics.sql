/*
  mart_store_metrics
  ------------------
  Store-day operational metrics for the dashboard. Materialised as a
  table for fast scans across the date dimension when the portal
  renders a multi-week trend.

  All columns are passed through from stg_store_metrics; the mart
  exists to provide a stable, materialised contract that the API and
  portal can rely on independently of staging-layer changes.

  Sorted by metric_date ascending then store_id ascending so the
  written table has spatial locality on the most common scan axis
  (date filter + store filter).
*/

select
    metric_date,
    store_id,
    total_sales,
    transaction_count,
    avg_basket_size,
    labor_cost_pct
from {{ ref('stg_store_metrics') }}
order by
    metric_date,
    store_id
