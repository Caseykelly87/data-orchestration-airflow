/*
  mart_labor_market
  -----------------
  Labor market health and consumer sentiment indicators.

  Series included:
    UNRATE              Unemployment Rate, % (FRED, monthly)
    CES0500000003       Avg Hourly Earnings, All Employees, Total Private
                        (BLS, monthly — nominal wages in $/hr)
    CIU2020000000000I   Employment Cost Index: Wages & Salaries, Private
                        (BLS, quarterly — measures labor cost growth)
    UMCSENT             University of Michigan: Consumer Sentiment (FRED, monthly)

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
    'UNRATE',                   -- Unemployment rate
    'CES0500000003',            -- Avg hourly earnings
    'CIU2020000000000I',        -- Employment cost index
    'UMCSENT'                   -- Consumer sentiment
)
order by
    obs.series_id,
    obs.observation_date
