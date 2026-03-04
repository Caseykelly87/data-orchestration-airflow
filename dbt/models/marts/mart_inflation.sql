/*
  mart_inflation
  --------------
  Price levels and monetary policy indicators (FRED + BLS).

  Series included:
    CPIAUCSL          CPI for All Urban Consumers: All Items (FRED, monthly)
    FEDFUNDS          Federal Funds Effective Rate (FRED, monthly)
    CUUR0000SA0       CPI Urban: All Items (BLS, monthly) — headline
    CUUR0000SA0L1E    CPI Urban: All Items Less Food & Energy (BLS) — core
    APU000074714      Avg Price: Gasoline, All Types (BLS, monthly)

  CPIAUCSL (FRED) and CUUR0000SA0 (BLS) measure similar things from
  different sources — including both enables cross-source validation.

  Materialized as a table (configured in dbt_project.yml).
*/

select
    obs.observation_date,
    obs.series_id,
    obs.series_name,
    obs.value,
    obs.source
from {{ ref('stg_observations') }} as obs
where obs.series_id in (
    'CPIAUCSL',         -- FRED headline CPI
    'FEDFUNDS',         -- Fed Funds Rate
    'CUUR0000SA0',      -- BLS headline CPI (urban)
    'CUUR0000SA0L1E',   -- BLS core CPI (ex food & energy)
    'APU000074714'      -- BLS avg gasoline price
)
order by
    obs.series_id,
    obs.observation_date
