/*
  mart_dim_departments
  --------------------
  Materialised passthrough of the raw dim_departments table.

  Same contract pattern as mart_dim_stores: explicit column list so
  the mart's schema does not drift silently when the sim engine adds
  columns.
*/

select
    department_id,
    department_name,
    is_perishable,
    seasonal_profile,
    base_margin_pct
from {{ source('grocery', 'dim_departments') }}
order by
    department_id
