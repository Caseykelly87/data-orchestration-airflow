/*
  mart_grocery
  ------------
  Food price outlook indicators sourced from USDA Economic Research Service (ERS).

  Series included:
    ERS_ALL_FOOD    All food (aggregate price index)
    ERS_FOOD_HOME   Food at home (grocery/retail)
    ERS_FOOD_AWAY   Food away from home (restaurant)
    ERS_CEREALS     Cereals and bakery products
    ERS_MEATS       Meats, poultry, and fish
    ERS_DAIRY       Dairy products
    ERS_FRUITS_VEG  Fruits and vegetables
    ERS_BEVERAGES   Nonalcoholic beverages and beverage materials

  Values represent annual percent change forecasts from the USDA ERS
  Food Price Outlook report. One row per category per year.

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
    'ERS_ALL_FOOD',    -- All food aggregate
    'ERS_FOOD_HOME',   -- Food at home (grocery)
    'ERS_FOOD_AWAY',   -- Food away from home (restaurant)
    'ERS_CEREALS',     -- Cereals and bakery products
    'ERS_MEATS',       -- Meats, poultry, and fish
    'ERS_DAIRY',       -- Dairy products
    'ERS_FRUITS_VEG',  -- Fruits and vegetables
    'ERS_BEVERAGES'    -- Nonalcoholic beverages
)
order by
    obs.series_id,
    obs.observation_date
