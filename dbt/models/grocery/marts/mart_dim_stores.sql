/*
  mart_dim_stores
  ---------------
  Materialised passthrough of the raw dim_stores table. Provides a
  stable mart-layer contract for the API and portal so consumers do
  not need to know about the raw schema.

  Columns are listed explicitly (rather than `select *`) so a future
  addition to the raw schema does not silently appear in the mart —
  the contract has to be widened deliberately.
*/

select
    store_id,
    store_name,
    address,
    city,
    zip,
    county_fips,
    trade_area_profile,
    sqft,
    open_date,
    base_daily_revenue
from {{ source('grocery', 'dim_stores') }}
order by
    store_id
