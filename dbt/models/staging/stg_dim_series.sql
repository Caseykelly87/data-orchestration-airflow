/*
  stg_dim_series
  --------------
  Clean passthrough of the dim_series dimension table.

  Dimension rows are stable (never updated after initial load), so no
  transformation is needed beyond explicit column selection and typing.
  Downstream mart models join to this view for series metadata.
*/

select
    series_id,
    series_name,
    source
from {{ source('economic_data', 'dim_series') }}
