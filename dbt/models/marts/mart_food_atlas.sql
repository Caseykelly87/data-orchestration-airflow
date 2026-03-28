/*
  mart_food_atlas
  ---------------
  Food environment indicators for St. Louis from the USDA Food Environment Atlas.

  Source: USDA Economic Research Service (ERS) — Food Environment Atlas
  Coverage: St. Louis County (FIPS 29189) and St. Louis City (FIPS 29510)
  Sheets: STORES, RESTAURANTS, ACCESS, ASSISTANCE, INSECURITY,
          TAXES, LOCAL, HEALTH, SOCIOECONOMIC

  Each row is one indicator value for one geography. The `geography` column
  translates the FIPS code into a human-readable label for reporting.

  `downloaded_at` reflects the date the Atlas was last fetched by the pipeline
  (updated every 60 days). Because the USDA publishes Atlas updates annually,
  this date may remain stable across several pipeline runs.

  Materialized as a table (configured in dbt_project.yml).
  Sorted by geography, then indicator for consistent output ordering.
*/

select
    atl.fips,
    case atl.fips
        when '29189' then 'St. Louis County, MO'
        when '29510' then 'St. Louis City, MO'
        else atl.fips
    end                  as geography,
    atl.indicator,
    atl.value,
    atl.downloaded_at
from {{ ref('stg_food_atlas') }} as atl
order by
    atl.fips,
    atl.indicator
