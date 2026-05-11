/*
  stg_store_metrics
  -----------------
  Cleaned view of raw.fact_store_metrics. Casts the date column from
  whatever the parquet reader produced to a SQL DATE for proper date
  arithmetic in downstream marts. Renames `date` to `metric_date` to
  avoid colliding with the reserved DATE keyword in some query contexts.

  Materialised as a view (configured for the grocery directory in
  dbt_project.yml). Always reflects the current state of the raw table
  without storing duplicate data.
*/

select
    cast(date as date)         as metric_date,
    store_id,
    total_sales,
    transaction_count,
    avg_basket_size,
    labor_cost_pct
from {{ source('grocery', 'fact_store_metrics') }}
