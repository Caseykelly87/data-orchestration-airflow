/*
  mart_gdp
  --------
  GDP and consumption indicators — all sourced from FRED.

  Series included:
    GDPC1    Real Gross Domestic Product (quarterly, inflation-adjusted)
    PCEC     Nominal Personal Consumption Expenditures
    PCECC96  Real Personal Consumption Expenditures (inflation-adjusted)
    RSXFS    Retail Sales Excluding Food Services (monthly)
    PSAVERT  Personal Saving Rate (%)

  Materialized as a table (configured in dbt_project.yml).
  Sorted by series_id, then observation_date for time-series queries.
*/

select
    obs.observation_date,
    obs.series_id,
    obs.series_name,
    obs.value,
    obs.source
from {{ ref('stg_observations') }} as obs
where obs.series_id in (
    'GDPC1',    -- Real GDP
    'PCEC',     -- Nominal PCE
    'PCECC96',  -- Real PCE
    'RSXFS',    -- Retail Sales ex-Food
    'PSAVERT'   -- Personal Saving Rate
)
order by
    obs.series_id,
    obs.observation_date
