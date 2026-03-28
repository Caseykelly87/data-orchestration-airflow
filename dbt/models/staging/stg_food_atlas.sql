/*
  stg_food_atlas
  --------------
  Cleans the raw fact_food_atlas table produced by the food_atlas_pipeline DAG.

  Transformations applied:
    - Casts downloaded_at from TEXT (YYYY-MM-DD) to DATE for proper range queries.
    - Filters NULL values — indicators with no recorded value for a FIPS
      code are excluded from downstream marts.

  The source data is long-format: one row per (fips, indicator) pair.
  Each indicator is a USDA Food Environment Atlas variable (e.g. GROCPTH11,
  SNAPSPTH12) drawn from one of nine Atlas sheets (STORES, RESTAURANTS,
  ACCESS, ASSISTANCE, INSECURITY, TAXES, LOCAL, HEALTH, SOCIOECONOMIC).

  FIPS scope: 29189 (St. Louis County, MO), 29510 (St. Louis City, MO).

  This is a view (configured in dbt_project.yml). It always reflects the
  current state of the raw table without storing duplicate data.
*/

select
    fips,
    indicator,
    value,
    cast(downloaded_at as date) as downloaded_at
from {{ source('economic_data', 'fact_food_atlas') }}
where value is not null
