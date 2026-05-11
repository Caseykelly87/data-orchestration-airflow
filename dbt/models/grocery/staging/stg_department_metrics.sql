/*
  stg_department_metrics
  ----------------------
  Cleaned view of raw.fact_department_metrics. Casts the date column
  to SQL DATE and renames it to metric_date for consistency with the
  store-grain staging view.

  Materialised as a view. Always reflects the current state of the
  raw table.
*/

select
    cast(date as date)         as metric_date,
    store_id,
    department_id,
    net_sales,
    transactions,
    units_sold,
    gross_margin_pct
from {{ source('grocery', 'fact_department_metrics') }}
