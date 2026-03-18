"""
Economic Data Pipeline DAG
===========================

Project:   data-orchestration-airflow (Project 2)
Module:    dags/economic_pipeline_dag.py
Author:    data-platform
Schedule:  Daily at midnight UTC

Architecture Philosophy
-----------------------
This module is the ORCHESTRATION LAYER ONLY. It contains zero business
logic. All data-fetching, normalization, and persistence logic lives
exclusively in Project 1: economic-data-etl (src/extract.py,
src/transform.py, src/load.py).

This DAG's responsibilities:
  1. Define the daily execution schedule
  2. Define retry and failure behavior for transient API errors
  3. Wire tasks into the correct dependency order
  4. Pass execution context between phases via Airflow's XCom
  5. Log progress at every phase boundary for observability

This DAG does NOT:
  - Fetch data from APIs  → that is src/extract.py
  - Parse or normalize data → that is src/transform.py
  - Write to the database  → that is src/load.py

Separation of Concerns
-----------------------
The strict boundary between orchestration and logic serves several
production purposes:

  - **Testability**: ETL logic can be tested independently of Airflow
    (see economic-data-etl/tests/). DAG structure can be tested
    without ETL dependencies (see tests/test_dag_structure.py).

  - **Replaceability**: The orchestration layer can be swapped for
    a different scheduler (Prefect, Dagster, cron) without touching
    any ETL code.

  - **Debuggability**: When a run fails, the traceback points to the
    ETL function, not Airflow boilerplate. Logs are clean and meaningful.

  - **Future compatibility**: dbt integration and cloud deployment can
    be added by modifying this file, not the ETL layer.

ETL Integration
---------------
economic-data-etl is mounted at /opt/airflow/etl/. Each task callable
imports from src/ at runtime via sys.path. Inter-task data passes via
XCom: extract returns raw API dicts, transform returns DataFrames
serialised as JSON, load upserts into postgres-etl using
ETL_DATABASE_URL.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Module Logger
# Airflow captures records from the root logger and from named loggers.
# Using __name__ scopes these records to this module in the Airflow UI task
# log viewer, making it easy to distinguish DAG-level logs from ETL logs.
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default Arguments
# ---------------------------------------------------------------------------
# These arguments are applied to every task in the DAG unless a task
# explicitly overrides them. This is Airflow's mechanism for applying
# a consistent operational policy (retries, alerting) across all tasks
# without repeating configuration at every operator instantiation.
#
# Retry policy rationale:
#   - FRED API has documented rate limits and occasional 503 errors
#   - BLS API batch endpoint can return 429 on high-traffic days
#   - 3 retries with a 5-minute delay is appropriate for a daily pipeline:
#     total worst-case added time = 15 minutes, well within a daily window
# ---------------------------------------------------------------------------
default_args = {
    # Pipeline ownership — used for routing alerts in Airflow Connections
    # and as a label in the Airflow UI's DAG list.
    "owner": "data-platform",

    # Do not require the previous run's task to succeed before running.
    # This pipeline is idempotent: each run fetches its own window of data.
    "depends_on_past": False,

    # Retry policy: 3 attempts with 5-minute spacing between each.
    "retries": 3,
    "retry_delay": timedelta(minutes=5),

    # SLA: alert if any task has not completed within 2 hours of its
    # scheduled start. Worst-case pipeline runtime (3 retries × 5 min
    # delay × 3 tasks) is ~45 minutes — 2 hours gives comfortable headroom
    # while still catching genuine hangs before the next daily window.
    "sla": timedelta(hours=2),

    # Alert recipients. Reads AIRFLOW_ADMIN_EMAIL from the container
    # environment (set in docker-compose.yml). Failures and SLA misses
    # are emailed to this address via the configured Gmail SMTP relay.
    "email": [os.getenv("AIRFLOW_ADMIN_EMAIL", "admin@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,

    # Anchors the DAG schedule. Setting this in the past with catchup=False
    # means the DAG begins running from today's scheduled time, not from
    # this date. See the DAG-level `catchup=False` setting below.
    "start_date": datetime(2025, 1, 1),
}


# ---------------------------------------------------------------------------
# Task Callables
# ---------------------------------------------------------------------------
# Each callable imports from /opt/airflow/etl at runtime. Inter-task data
# flows via XCom — PythonOperator pushes the return value automatically.
# ---------------------------------------------------------------------------

def run_extract(**context: dict) -> dict:
    """
    Fetches all FRED and BLS series via the ETL extract module.
    Raw API responses are written to data/raw/ (idempotent — SHA-256
    skips unchanged series). Return value is pushed to XCom for transform.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/etl")
    sys.path.insert(0, "/opt/airflow/etl/src")
    from src.extract import fetch_fred_data, fetch_bls_data
    from src.config import FRED_SERIES, BLS_SERIES

    end_year = int(context["ds"][:4])

    logger.info("Extracting %d FRED series", len(FRED_SERIES))
    fred_data = {}
    for name, series_id in FRED_SERIES.items():
        fred_data[name] = fetch_fred_data(series_id)

    logger.info("Extracting BLS batch (2021-%d)", end_year)
    bls_data = fetch_bls_data(BLS_SERIES, 2021, end_year)

    logger.info("Extract complete: %d FRED series, BLS batch fetched", len(fred_data))
    return {"fred_data": fred_data, "bls_data": bls_data}


def run_transform(**context: dict) -> dict:
    """
    Pulls raw API dicts from XCom, normalises them into a long-format
    fact DataFrame and a dimension DataFrame using the ETL transform
    module. Serialises both to JSON and pushes to XCom for the load task.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/etl")
    sys.path.insert(0, "/opt/airflow/etl/src")
    from src.transform import (
        parse_fred_observations,
        parse_bls_batch,
        build_dim_series,
        combine_fact_tables,
    )
    from src.config import FRED_SERIES, BLS_SERIES

    extract_result = context["ti"].xcom_pull(task_ids="extract_economic_data")
    fred_data = extract_result["fred_data"]
    bls_data  = extract_result["bls_data"]

    fred_frames = [
        parse_fred_observations(fred_data[name], series_id, name)
        for name, series_id in FRED_SERIES.items()
        if fred_data.get(name) is not None
    ]
    bls_frame = parse_bls_batch(bls_data, BLS_SERIES)
    fact_df   = combine_fact_tables(fred_frames, bls_frame)
    dim_df    = build_dim_series(FRED_SERIES, BLS_SERIES)

    logger.info("Transform complete: %d fact rows, %d dim rows", len(fact_df), len(dim_df))
    return {"fact_df": fact_df.to_json(date_format="iso"), "dim_df": dim_df.to_json()}


def run_load(**context: dict) -> dict:
    """
    Pulls serialised DataFrames from XCom, creates a SQLAlchemy engine
    from ETL_DATABASE_URL, and upserts fact and dimension rows into the
    ETL PostgreSQL database via the ETL load module.
    """
    import io
    import sys
    sys.path.insert(0, "/opt/airflow/etl")
    sys.path.insert(0, "/opt/airflow/etl/src")
    import pandas as pd
    from sqlalchemy import create_engine
    from src.load import ensure_tables_exist, upsert_observations, upsert_dim_series

    transform_result = context["ti"].xcom_pull(task_ids="transform_economic_data")
    fact_df = pd.read_json(io.StringIO(transform_result["fact_df"]))
    dim_df  = pd.read_json(io.StringIO(transform_result["dim_df"]))

    engine = create_engine(os.environ["ETL_DATABASE_URL"], future=True)
    ensure_tables_exist(engine)
    obs_stats = upsert_observations(fact_df, engine)
    dim_stats = upsert_dim_series(dim_df, engine)

    logger.info("Load complete — observations: %s | dim_series: %s", obs_stats, dim_stats)
    return {"observations": obs_stats, "dim_series": dim_stats}


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
# The `with DAG(...)` context manager registers all operators defined
# inside the block with this DAG instance. Airflow's scheduler discovers
# this `dag` object when it scans the dags/ directory.
# ---------------------------------------------------------------------------
with DAG(
    dag_id="economic_data_pipeline",
    description=(
        "Daily orchestration of the U.S. macroeconomic indicator ETL pipeline. "
        "Ingests 14 series from the FRED and BLS public APIs, normalizes data "
        "to a tidy star schema, and upserts fact and dimension tables into "
        "PostgreSQL. Orchestration only — all business logic lives in "
        "the economic-data-etl module."
    ),
    default_args=default_args,
    schedule_interval="@daily",
    # Disable historical backfill on first DAG activation.
    # The pipeline is designed to run forward from today, not to replay history.
    catchup=False,
    # Enforce single active run. The ETL is stateful (database upserts) and
    # must not run concurrently with itself to prevent data races.
    max_active_runs=1,
    # Tags for Airflow UI filtering. Use these to find this DAG quickly.
    tags=["economic-data", "etl", "production", "postgres"],
    # Enable the DAG-level doc panel in the Airflow UI.
    doc_md=__doc__,
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Extract
    # -----------------------------------------------------------------------
    # Fetches raw data from the FRED and BLS public APIs via the ETL extract
    # module. The extract is idempotent — repeated runs with unchanged API
    # data produce no new file writes (SHA-256 hash comparison).
    # -----------------------------------------------------------------------
    extract_task = PythonOperator(
        task_id="extract_economic_data",
        python_callable=run_extract,
        doc_md="""
## Extract

Calls `fetch_fred_data()` and `fetch_bls_data()` from `economic-data-etl/src/extract.py`.

**Sources:**
- **FRED API** — 9 macroeconomic series (GDP, CPI, Fed Funds Rate, etc.)
- **BLS API** — 5 labor and price series (wages, CPI urban, gas prices, etc.)

**Idempotency:** SHA-256 hashing of API responses prevents redundant
file writes. Re-running a failed extract is always safe.

**Retry behavior:** Up to 3 retries with 5-minute delay, as set in
`default_args`. Handles FRED 429/503 and BLS transient errors.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 2: Transform
    # -----------------------------------------------------------------------
    # Normalizes raw API response dicts into typed pandas DataFrames.
    # Produces a long-format fact table (date, series_id, value) and a
    # dimension table (series metadata). Pure functional transformation —
    # no database writes.
    # -----------------------------------------------------------------------
    transform_task = PythonOperator(
        task_id="transform_economic_data",
        python_callable=run_transform,
        doc_md="""
## Transform

Calls the transform functions from `economic-data-etl/src/transform.py`.

**Operations:**
- `parse_fred_observations()` — FRED dict → typed DataFrame
- `parse_bls_batch()` — BLS batch response → typed DataFrame
- `combine_fact_tables()` — merge into long-format fact table
- `build_dim_series()` — construct series dimension table

**Missing value handling:** FRED encodes missing as `"."`, BLS as `"-"`.
Both are normalized to `NaN` by the ETL transform layer.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 3: Load
    # -----------------------------------------------------------------------
    # Upserts normalized fact and dimension DataFrames into the ETL
    # PostgreSQL database. Uses an upsert strategy: insert new rows,
    # update changed rows, skip unchanged rows. Reports final statistics.
    # -----------------------------------------------------------------------
    load_task = PythonOperator(
        task_id="load_economic_data",
        python_callable=run_load,
        doc_md="""
## Load

Calls the load functions from `economic-data-etl/src/load.py`.

**Operations:**
- `ensure_tables_exist()` — idempotent schema creation
- `upsert_observations()` — fact table upsert (insert/update/skip)
- `upsert_dim_series()` — dimension table upsert

**Database:** ETL PostgreSQL container (`postgres-etl:5432`).
Connection string sourced from `ETL_DATABASE_URL` environment variable.

**Output stats:** `{"inserted": N, "updated": N, "unchanged": N}`
        """,
    )

    # -----------------------------------------------------------------------
    # Task 4: dbt Transform
    # -----------------------------------------------------------------------
    # Runs dbt models after the load task has written raw data to postgres-etl.
    # dbt creates analytics-ready mart tables (materialized) and staging views
    # on top of the raw fact and dimension tables.
    #
    # The dbt project lives at /opt/airflow/dbt/ (bind-mounted from ./dbt/).
    # profiles.yml is co-located and uses env_var() for all credentials —
    # no secrets are hardcoded in the dbt project files.
    #
    # Models produced:
    #   views:  stg_observations, stg_dim_series
    #   tables: mart_gdp, mart_inflation, mart_labor_market, mart_economic_summary
    # -----------------------------------------------------------------------
    dbt_task = BashOperator(
        task_id="dbt_transform",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt"
        ),
        doc_md="""
## dbt Transform

Runs all dbt models against the ETL PostgreSQL database after `load_economic_data`
completes. Creates analytics-ready views and tables from the raw loaded data.

**Staging (views):**
- `stg_observations` — raw fact table with date cast to DATE, null values filtered
- `stg_dim_series` — clean passthrough of series dimension

**Marts (materialized tables):**
- `mart_gdp` — GDP, PCE, retail sales, personal savings rate
- `mart_inflation` — CPI, Fed Funds Rate, BLS price series
- `mart_labor_market` — unemployment, wages, employment cost index, sentiment
- `mart_economic_summary` — latest value for each of the 14 economic series
        """,
    )

    # -----------------------------------------------------------------------
    # Task Dependency Chain
    # -----------------------------------------------------------------------
    # Defines the execution order using Airflow's bitshift operator.
    # This creates directed edges in the DAG's task graph:
    #
    #   extract_economic_data
    #          │
    #          ▼
    #   transform_economic_data
    #          │
    #          ▼
    #   load_economic_data
    #          │
    #          ▼
    #   dbt_transform
    #
    # The linear chain enforces sequential execution. A failure in any task
    # halts the downstream chain and triggers the retry policy in default_args.
    # -----------------------------------------------------------------------
    extract_task >> transform_task >> load_task >> dbt_task
