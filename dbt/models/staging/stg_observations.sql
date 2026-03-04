/*
  stg_observations
  ----------------
  Cleans the raw fact_economic_observations table:
    - Casts the date column from TEXT (YYYY-MM-DD) to DATE for proper
      date arithmetic and range queries in downstream mart models.
    - Renames `date` → `observation_date` to avoid collision with the
      reserved SQL keyword DATE in certain query contexts.
    - Filters NULL values — raw data contains NULLs where FRED uses "."
      and BLS uses "-" to indicate missing observations. Downstream mart
      models operate on complete observations only.

  This is a view (configured in dbt_project.yml). It always reflects the
  current state of the raw table without storing duplicate data.
*/

select
    series_id,
    series_name,
    cast(date as date)  as observation_date,
    value,
    source
from {{ source('economic_data', 'fact_economic_observations') }}
where value is not null
