"""
USDA Food Environment Atlas Pipeline DAG
=========================================

Project:   data-orchestration-airflow (Project 2)
Module:    dags/food_atlas_dag.py
Author:    data-platform
Schedule:  Every 60 days

Purpose
-------
Downloads the USDA Food Environment Atlas Excel workbook, filters it to
St. Louis County (FIPS 29189) and St. Louis City (FIPS 29510), and loads
all food environment indicators into the `fact_food_atlas` table.

Unlike the daily economic pipeline, this DAG runs every 60 days because
the USDA publishes Atlas updates infrequently (~annually). Each run checks
for updates via SHA-256 hashing in the extract script — if the source file
has not changed, the load step detects no diff and exits quickly.

Architecture
------------
This DAG follows the same orchestration-only pattern as economic_pipeline_dag.py:
  - Extract : calls get_atlas_url() + download_and_filter() from the ETL
              scripts layer. The script writes the filtered JSON to disk at
              /opt/airflow/etl/data/stl_food_environment.json and the load
              task reads from that path directly (avoids XCom size limits
              for wide tabular data).
  - Load    : normalises the wide-format JSON to a long-format fact table
              (fips, indicator, value, downloaded_at) and upserts into
              fact_food_atlas. Table creation is idempotent.
  - dbt     : rebuilds stg_food_atlas (view) and mart_food_atlas (table)
              from the freshly loaded data.

Note: fact_food_atlas table creation and upsert logic is defined inline here
because it does not yet exist in economic-data-etl/src/load.py. When that
project is updated, move the DB logic there and import it the same way as
upsert_observations / upsert_dim_series.

Source Data
-----------
USDA Economic Research Service — Food Environment Atlas
  https://www.ers.usda.gov/data-products/food-environment-atlas/
Sheets: STORES, RESTAURANTS, ACCESS, ASSISTANCE, INSECURITY,
        TAXES, LOCAL, HEALTH, SOCIOECONOMIC
FIPS scope: 29189 (St. Louis County, MO), 29510 (St. Louis City, MO)
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)

# Path where the extract script writes its output inside the container.
# The ETL project is bind-mounted from ../economic-data-etl to /opt/airflow/etl.
_ATLAS_JSON_PATH = "/opt/airflow/etl/data/stl_food_environment.json"

# FIPS → human-readable geography label for the mart layer.
_FIPS_LABELS = {
    "29189": "St. Louis County, MO",
    "29510": "St. Louis City, MO",
}

# ---------------------------------------------------------------------------
# Default Arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    # USDA pages can be slow; be generous with retries.
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=3),
    "email": [os.getenv("AIRFLOW_ADMIN_EMAIL", "admin@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": datetime(2025, 1, 1),
}


# ---------------------------------------------------------------------------
# Task Callables
# ---------------------------------------------------------------------------

def run_extract_atlas(**context: dict) -> dict:
    """
    Calls the ETL extract script to download and filter the USDA Food
    Environment Atlas Excel workbook.

    The script (scripts/download_food_atlas.py) handles:
      - Dynamic URL discovery by scraping the ERS page
      - SHA-256 change detection (skips write if file unchanged)
      - Filtering to St. Louis FIPS codes 29189 and 29510
      - Merging all 9 Atlas sheets on FIPS key
      - Writing the result to data/stl_food_environment.json

    The task pushes only lightweight metadata to XCom (URL, row count).
    The load task reads the JSON file from disk directly to avoid XCom
    size limits on wide tabular data.
    """
    import json
    import sys
    sys.path.insert(0, "/opt/airflow/etl/scripts")
    from download_food_atlas import get_atlas_url, download_and_filter

    url = get_atlas_url()
    logger.info("Downloading Food Environment Atlas from: %s", url)
    download_and_filter(url)

    with open(_ATLAS_JSON_PATH) as f:
        records = json.load(f)

    # Normalise to list-of-dicts regardless of whether the script wrote
    # a JSON array or a FIPS-keyed dict.
    if isinstance(records, dict):
        record_list = [{"FIPS": k, **v} for k, v in records.items()]
    else:
        record_list = records

    logger.info("Atlas extract complete: %d FIPS records", len(record_list))
    return {"source_url": url, "record_count": len(record_list)}


def run_load_atlas(**context: dict) -> dict:
    """
    Reads the Atlas JSON written by the extract task, normalises the
    wide-format data to long format, and upserts into fact_food_atlas.

    Wide format (one row per FIPS, one column per indicator):
        [{"FIPS": "29189", "GROCPTH11": 0.34, "SNAPSPTH12": 1.2, ...}, ...]

    Long format (one row per FIPS + indicator):
        fips | indicator    | value | downloaded_at
        -----+--------------+-------+--------------
        29189 | GROCPTH11   |  0.34 | 2025-06-01
        29189 | SNAPSPTH12  |  1.20 | 2025-06-01
        29510 | GROCPTH11   |  0.51 | 2025-06-01

    Upsert strategy: insert new (fips, indicator) pairs; update value +
    downloaded_at when the value has changed; skip unchanged rows.
    """
    import json
    import sys
    from datetime import date

    import pandas as pd
    sys.path.insert(0, "/opt/airflow/etl")
    from sqlalchemy import create_engine, text

    # Columns that are geographic metadata, not indicators — skip them.
    _GEO_COLS = {"FIPS", "fips", "State", "STATE", "County", "COUNTY"}

    downloaded_at = date.today().isoformat()

    with open(_ATLAS_JSON_PATH) as f:
        raw = json.load(f)

    if isinstance(raw, dict):
        records = [{"FIPS": k, **v} for k, v in raw.items()]
    else:
        records = raw

    # Normalise to long format.
    rows = []
    for record in records:
        # FIPS column may appear as "FIPS", "fips", or a numeric value.
        fips = str(
            record.get("FIPS") or record.get("fips", "")
        ).strip().zfill(5)
        if not fips or fips == "00000":
            logger.warning("Skipping record with unparseable FIPS: %s", record)
            continue
        for col, val in record.items():
            if col in _GEO_COLS:
                continue
            numeric_val = None
            if val is not None:
                try:
                    numeric_val = float(val)
                except (TypeError, ValueError):
                    continue  # skip non-numeric indicator values
            rows.append({
                "fips": fips,
                "indicator": col,
                "value": numeric_val,
                "downloaded_at": downloaded_at,
            })

    logger.info("Normalised %d indicator rows from %d FIPS records", len(rows), len(records))

    engine = create_engine(os.environ["ETL_DATABASE_URL"], future=True)

    # Idempotent table creation.
    # TODO: move to ensure_tables_exist() in economic-data-etl/src/load.py
    #       once that project is updated to include Atlas support.
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_food_atlas (
                fips          TEXT NOT NULL,
                indicator     TEXT NOT NULL,
                value         REAL,
                downloaded_at TEXT NOT NULL,
                PRIMARY KEY (fips, indicator)
            )
        """))
        conn.commit()

    # Load existing rows for diff comparison.
    with engine.connect() as conn:
        existing = pd.read_sql(
            "SELECT fips, indicator, value FROM fact_food_atlas", conn
        )
    existing_map = {
        (r["fips"], r["indicator"]): r["value"]
        for _, r in existing.iterrows()
    }

    to_insert = []
    to_update = []
    unchanged = 0

    for row in rows:
        key = (row["fips"], row["indicator"])
        if key not in existing_map:
            to_insert.append(row)
        else:
            existing_val = existing_map[key]
            new_val = row["value"]
            # Treat both-None as unchanged; otherwise compare numerically.
            both_none = existing_val is None and new_val is None
            if both_none or (
                existing_val is not None
                and new_val is not None
                and abs(float(existing_val) - float(new_val)) < 1e-9
            ):
                unchanged += 1
            else:
                to_update.append(row)

    if to_insert:
        pd.DataFrame(to_insert).to_sql(
            "fact_food_atlas", engine, if_exists="append", index=False
        )

    if to_update:
        with engine.connect() as conn:
            for row in to_update:
                conn.execute(
                    text("""
                        UPDATE fact_food_atlas
                           SET value         = :value,
                               downloaded_at = :downloaded_at
                         WHERE fips      = :fips
                           AND indicator = :indicator
                    """),
                    row,
                )
            conn.commit()

    stats = {
        "inserted": len(to_insert),
        "updated": len(to_update),
        "unchanged": unchanged,
    }
    logger.info("Atlas load complete — %s", stats)
    return stats


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="food_atlas_pipeline",
    description=(
        "Every-60-day ingestion of the USDA Food Environment Atlas for "
        "St. Louis County (FIPS 29189) and St. Louis City (FIPS 29510). "
        "Normalises wide-format Atlas data to a long-format fact table and "
        "rebuilds the mart_food_atlas dbt model. Change detection skips the "
        "load when the USDA source file has not been updated."
    ),
    default_args=default_args,
    schedule_interval=timedelta(days=60),
    catchup=False,
    max_active_runs=1,
    tags=["food-atlas", "usda", "grocery", "postgres"],
    doc_md=__doc__,
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Extract
    # -----------------------------------------------------------------------
    extract_task = PythonOperator(
        task_id="extract_food_atlas",
        python_callable=run_extract_atlas,
        doc_md="""
## Extract Food Atlas

Calls `get_atlas_url()` and `download_and_filter()` from
`economic-data-etl/scripts/download_food_atlas.py`.

Discovers the current XLSX URL by scraping the ERS Food Environment Atlas
page (falls back to a pinned URL if scraping fails). Downloads, filters to
St. Louis FIPS codes, merges all 9 sheets, and writes the result to
`/opt/airflow/etl/data/stl_food_environment.json`.

SHA-256 hashing in the script prevents redundant writes when the source
file has not changed between 60-day runs.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 2: Load
    # -----------------------------------------------------------------------
    load_task = PythonOperator(
        task_id="load_food_atlas",
        python_callable=run_load_atlas,
        doc_md="""
## Load Food Atlas

Reads the JSON produced by `extract_food_atlas`, normalises it from
wide format (one row per FIPS, one column per indicator) to long format
(one row per FIPS + indicator), and upserts into `fact_food_atlas`.

Upsert behaviour:
- New (fips, indicator) pairs → INSERT
- Changed values → UPDATE value + downloaded_at
- Unchanged → skip

Table is created idempotently if it does not yet exist.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 3: dbt Transform (Atlas models only)
    # -----------------------------------------------------------------------
    # Runs only the Atlas-specific dbt models. The economic pipeline's models
    # (mart_gdp, mart_inflation, etc.) are rebuilt by the daily DAG and do
    # not depend on fact_food_atlas, so there is no need to rebuild them here.
    # -----------------------------------------------------------------------
    dbt_task = BashOperator(
        task_id="dbt_transform",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt "
            "--select stg_food_atlas mart_food_atlas"
        ),
        doc_md="""
## dbt Transform (Atlas)

Runs the two Atlas-specific dbt models:
- `stg_food_atlas` — staging view: casts downloaded_at to DATE, filters NULLs
- `mart_food_atlas` — materialized table: adds FIPS geography labels,
  ready for dashboard consumption
        """,
    )

    extract_task >> load_task >> dbt_task
