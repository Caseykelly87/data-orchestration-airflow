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
# Grocery Branch Constants
# ---------------------------------------------------------------------------
# The grocery branch shares one per-execution-date directory across all four
# downstream tasks (sim engine, ingest, detect, load). Centralising the path
# pattern here avoids drift between callables.
#
# Directory shape (created on the bind-mounted economic-data-etl volume):
#
#   /opt/airflow/etl/data/sim_output/{{ ds }}/
#   ├── daily/{MM}/{DD}/{YYYY}/
#   │   ├── store_summary.csv          (sim engine)
#   │   └── department_sales.csv       (sim engine)
#   ├── dimensions/
#   │   ├── dim_calendar.csv           (sim engine)
#   │   ├── dim_departments.csv        (sim engine)
#   │   └── dim_stores.csv             (sim engine)
#   ├── store_daily_metrics.parquet    (grocery_etl_ingest)
#   ├── department_daily_metrics.parquet (grocery_etl_ingest)
#   ├── dim_stores.parquet             (grocery_etl_ingest)
#   └── anomaly_flags.parquet          (grocery_etl_detect)
#
# This tree is intentionally not deleted between tasks within a DAG run —
# detection reads dim_stores from the same tree the sim engine wrote.
# ---------------------------------------------------------------------------
SIM_ENGINE_REPO_PATH = "/opt/airflow/sim-engine"
SIM_OUTPUT_BASE = "/opt/airflow/etl/data/sim_output"
DETECTION_RULES_PATH = "/opt/airflow/etl/config/detection_rules.yaml"


def _sim_output_dir(ds: str) -> str:
    """Return the per-execution-date sim output directory for the given ds."""
    return f"{SIM_OUTPUT_BASE}/{ds}"


# ---------------------------------------------------------------------------
# Grocery Branch Task Callables
# ---------------------------------------------------------------------------
# These follow the same pattern as the macro callables: thin wrappers, lazy
# imports inside the function, structured logging, and a return value that
# Airflow pushes to XCom for downstream tasks. Each callable's docstring
# describes the contract it satisfies.
# ---------------------------------------------------------------------------

def run_sim_engine(**context: dict) -> dict:
    """
    Invokes the sim engine via subprocess to produce a fresh per-execution-date
    output tree. Runs `python -m knot_shore init` (idempotent dimension/promo
    schedule generation) followed by `python -m knot_shore run --date {{ ds }}`,
    which generates 8 dates of store-day data per call (anchor + 6 prior + T-365).

    The 8-date window is intrinsic to the sim engine's design — the orchestrator
    accepts the window and uses {{ ds }} as the anchor; downstream tasks operate
    on the entire tree.
    """
    import subprocess

    ds = context["ds"]
    output_dir = _sim_output_dir(ds)

    logger.info("sim_engine: ensuring init artifacts at %s", output_dir)
    init_result = subprocess.run(
        ["python", "-m", "knot_shore", "init", "--output", output_dir],
        cwd=SIM_ENGINE_REPO_PATH,
        capture_output=True,
        text=True,
        check=False,
    )
    if init_result.returncode != 0:
        logger.error("sim_engine init failed: %s", init_result.stderr)
        raise RuntimeError(
            f"sim_engine init exited with code {init_result.returncode}. "
            f"stderr: {init_result.stderr}"
        )
    logger.info("sim_engine: init complete")

    logger.info("sim_engine: generating data for anchor date %s", ds)
    run_result = subprocess.run(
        ["python", "-m", "knot_shore", "run", "--date", ds, "--output", output_dir],
        cwd=SIM_ENGINE_REPO_PATH,
        capture_output=True,
        text=True,
        check=False,
    )
    if run_result.returncode != 0:
        logger.error("sim_engine run failed: %s", run_result.stderr)
        raise RuntimeError(
            f"sim_engine run exited with code {run_result.returncode}. "
            f"stderr: {run_result.stderr}"
        )
    logger.info("sim_engine: run complete for anchor %s", ds)
    return {"sim_output_root": output_dir, "anchor_date": ds}


def run_grocery_ingest(**context: dict) -> dict:
    """
    Reads the sim engine's CSV output tree and writes the three canonical
    parquet artifacts (store-day metrics, department-day metrics, dim_stores)
    via economic-data-etl/src/sim_cli.

    Calls the public functions directly (run, run_department_grain,
    load_dim_stores, write_dim_stores_parquet) — NOT main() — to avoid
    main()'s side effects (configure_logging() and an os.environ mutation).
    """
    import sys
    sys.path.insert(0, "/opt/airflow/etl")
    sys.path.insert(0, "/opt/airflow/etl/src")
    from pathlib import Path
    from src.sim_cli import (
        run as sim_cli_run,
        run_department_grain,
        write_dim_stores_parquet,
    )
    from src.sim_ingest import load_dim_stores

    ds = context["ds"]
    sim_output_root = Path(_sim_output_dir(ds))

    logger.info("grocery_ingest: building store-day metrics from %s", sim_output_root)
    store_metrics_path = sim_cli_run(sim_output_root, sim_output_root)

    logger.info("grocery_ingest: building department-day metrics")
    dept_metrics_path = run_department_grain(sim_output_root, sim_output_root)

    logger.info("grocery_ingest: writing dim_stores.parquet")
    dim_stores_df = load_dim_stores(sim_output_root)
    dim_stores_path = write_dim_stores_parquet(dim_stores_df, sim_output_root)

    paths = {
        "store_metrics": str(store_metrics_path),
        "department_metrics": str(dept_metrics_path),
        "dim_stores_parquet": str(dim_stores_path),
        "sim_output_root": str(sim_output_root),
    }
    logger.info("grocery_ingest complete: %s", paths)
    return paths


def run_grocery_detect(**context: dict) -> dict:
    """
    Evaluates the five anomaly-detection rules from
    `economic-data-etl/config/detection_rules.yaml` against the store-day
    metrics parquet, writing `anomaly_flags.parquet` to the same per-execution
    -date directory.

    Calls `detect_cli.run` directly — not `main()` — to avoid logging
    reconfiguration and os.environ mutation inside a managed worker process.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/etl")
    sys.path.insert(0, "/opt/airflow/etl/src")
    from pathlib import Path
    from src.detect_cli import run as detect_cli_run

    ingest_result = context["ti"].xcom_pull(task_ids="grocery_etl_ingest")
    sim_output_root = Path(ingest_result["sim_output_root"])
    metrics_path = Path(ingest_result["store_metrics"])

    logger.info("grocery_detect: running rules from %s", DETECTION_RULES_PATH)
    output_path = detect_cli_run(
        metrics_path=metrics_path,
        sim_output_root=sim_output_root,
        rules_path=Path(DETECTION_RULES_PATH),
        output_dir=sim_output_root,
    )
    logger.info("grocery_detect complete: %s", output_path)
    return {"anomaly_flags_parquet": str(output_path)}


# Expected column sets for the five raw tables. Asserted before each
# to_sql write so a schema drift in either the sim engine or the ETL fails
# loudly with a precise diagnostic instead of a downstream dbt error.
GROCERY_RAW_TABLE_COLUMNS = {
    "fact_store_metrics": {
        "date", "store_id", "total_sales", "transaction_count",
        "avg_basket_size", "labor_cost_pct",
    },
    "fact_department_metrics": {
        "date", "store_id", "department_id", "net_sales", "transactions",
        "units_sold", "gross_margin_pct",
    },
    "fact_anomaly_flags": {
        "date", "store_id", "rule_id", "actual_value", "expected_low",
        "expected_high", "distance_from_band", "severity_score", "severity_level",
    },
    "dim_stores": {
        "store_id", "store_name", "address", "city", "zip", "county_fips",
        "trade_area_profile", "sqft", "open_date", "base_daily_revenue",
    },
    "dim_calendar": {
        "date_key", "day_of_week", "day_of_week_num", "is_weekend",
        "is_holiday", "holiday_name", "is_snap_window", "fiscal_week",
        "fiscal_period", "month", "quarter", "year",
    },
    "dim_departments": {
        "department_id", "department_name", "is_perishable",
        "seasonal_profile", "base_margin_pct",
    },
}


def _ensure_raw_schema_exists(engine) -> None:
    """Create the raw schema if it does not exist. Idempotent."""
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()


def _assert_columns(table_name: str, df) -> None:
    """Raise ValueError if df's columns do not match the expected set exactly."""
    expected = GROCERY_RAW_TABLE_COLUMNS[table_name]
    actual = set(df.columns)
    missing = expected - actual
    extra = actual - expected
    if missing or extra:
        raise ValueError(
            f"{table_name} schema mismatch.\n"
            f"  Missing columns: {sorted(missing)}\n"
            f"  Unexpected columns: {sorted(extra)}\n"
            f"  Expected: {sorted(expected)}\n"
            f"  Actual:   {sorted(actual)}"
        )


def run_grocery_load(**context: dict) -> dict:
    """
    Loads the five canonical grocery artifacts (3 ETL parquets + 2 sim engine
    CSV dimensions) into the `raw` schema of the ETL postgres database.

    Validation runs before each `to_sql` write:
      - Row-count bounds appropriate for an 8-store / 10-department / ~8-date
        DAG run.
      - Schema-shape assertion against the column set declared in
        GROCERY_RAW_TABLE_COLUMNS.
      - Primary-key uniqueness for the three dimensions.

    A failed assertion fails the task, which fails the DAG run, which routes
    through the configured email alert in default_args. This is the platform's
    data-quality checkpoint at the load boundary — a separate ETL transform
    pass would just duplicate the sim engine's existing schema work.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/etl")
    sys.path.insert(0, "/opt/airflow/etl/src")
    from pathlib import Path
    import pandas as pd
    from sqlalchemy import create_engine

    ingest_result = context["ti"].xcom_pull(task_ids="grocery_etl_ingest")
    detect_result = context["ti"].xcom_pull(task_ids="grocery_etl_detect")
    sim_output_root = Path(ingest_result["sim_output_root"])

    store_metrics_df = pd.read_parquet(ingest_result["store_metrics"])
    dept_metrics_df = pd.read_parquet(ingest_result["department_metrics"])
    anomaly_flags_df = pd.read_parquet(detect_result["anomaly_flags_parquet"])
    dim_stores_df = pd.read_parquet(ingest_result["dim_stores_parquet"])
    dim_calendar_df = pd.read_csv(sim_output_root / "dimensions" / "dim_calendar.csv")
    dim_departments_df = pd.read_csv(sim_output_root / "dimensions" / "dim_departments.csv")

    # Row-count bounds. Lower bounds catch silent empty inputs; upper bounds
    # catch a sim engine that ran for an unexpected window (e.g., a multi-week
    # backfill into a per-DAG-run directory).
    if not (1 <= len(store_metrics_df) <= 80):
        raise ValueError(
            f"fact_store_metrics row count is {len(store_metrics_df)}, "
            f"expected 1..80 (8 stores x ~8 dates plus buffer)."
        )
    if not (1 <= len(dept_metrics_df) <= 800):
        raise ValueError(
            f"fact_department_metrics row count is {len(dept_metrics_df)}, "
            f"expected 1..800 (8 stores x 10 departments x ~8 dates plus buffer)."
        )
    if not (0 <= len(anomaly_flags_df) <= 200):
        raise ValueError(
            f"fact_anomaly_flags row count is {len(anomaly_flags_df)}, "
            f"expected 0..200 (zero is valid: clean run with no firings)."
        )
    if len(dim_stores_df) != 8:
        raise ValueError(
            f"dim_stores row count is {len(dim_stores_df)}, expected exactly 8. "
            "The sim engine should produce 8 stores. "
            "Check seed_data/store_locations.json in the sim engine repo."
        )
    if not (1460 <= len(dim_calendar_df) <= 1462):
        raise ValueError(
            f"dim_calendar row count is {len(dim_calendar_df)}, expected 1460..1462 "
            "(four years with leap-year tolerance)."
        )
    if len(dim_departments_df) != 10:
        raise ValueError(
            f"dim_departments row count is {len(dim_departments_df)}, expected exactly 10. "
            "Check the sim engine's dimensions module."
        )

    # Schema-shape assertions
    _assert_columns("fact_store_metrics", store_metrics_df)
    _assert_columns("fact_department_metrics", dept_metrics_df)
    _assert_columns("fact_anomaly_flags", anomaly_flags_df)
    _assert_columns("dim_stores", dim_stores_df)
    _assert_columns("dim_calendar", dim_calendar_df)
    _assert_columns("dim_departments", dim_departments_df)

    # Primary-key uniqueness for dimensions. A duplicate would indicate
    # corruption upstream and would silently produce fan-out joins in dbt.
    if not dim_stores_df["store_id"].is_unique:
        raise ValueError("dim_stores.store_id must be unique; found duplicates.")
    if not dim_departments_df["department_id"].is_unique:
        raise ValueError(
            "dim_departments.department_id must be unique; found duplicates."
        )
    if not dim_calendar_df["date_key"].is_unique:
        raise ValueError("dim_calendar.date_key must be unique; found duplicates.")

    engine = create_engine(os.environ["ETL_DATABASE_URL"], future=True)
    _ensure_raw_schema_exists(engine)

    table_to_df = {
        "fact_store_metrics": store_metrics_df,
        "fact_department_metrics": dept_metrics_df,
        "fact_anomaly_flags": anomaly_flags_df,
        "dim_stores": dim_stores_df,
        "dim_calendar": dim_calendar_df,
        "dim_departments": dim_departments_df,
    }
    row_counts: dict[str, int] = {}
    for table_name, df in table_to_df.items():
        df.to_sql(
            name=table_name,
            con=engine,
            schema="raw",
            if_exists="replace",
            index=False,
        )
        row_counts[table_name] = len(df)
        logger.info("grocery_load: wrote raw.%s (%d rows)", table_name, len(df))

    logger.info("grocery_load complete: %s", row_counts)
    return row_counts


def run_portal_refresh(**context: dict) -> dict:
    """
    Convergence task. Emits a structured log entry naming the macro and
    grocery marts whose downstream caches should be invalidated. This is a
    placeholder for actual cache-invalidation HTTP calls to economic-data-api
    in a future iteration; today it only signals that both branches succeeded.
    """
    logger.info(
        "portal_refresh signal",
        extra={
            "event": "portal_cache_invalidate",
            "macro_marts": [
                "mart_gdp",
                "mart_inflation",
                "mart_labor_market",
                "mart_economic_summary",
            ],
            "grocery_marts": [
                "mart_store_metrics",
                "mart_anomalies",
                "mart_dashboard_summary",
                "mart_dim_stores",
                "mart_dim_departments",
            ],
            "execution_date": context["ds"],
        },
    )
    return {"refreshed_at": context["ds"]}


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
    # Task 4: Macro dbt Transform
    # -----------------------------------------------------------------------
    # Runs the macro-side dbt models after the load task has written raw data
    # to postgres-etl. dbt creates analytics-ready mart tables (materialized)
    # and staging views on top of the raw fact and dimension tables.
    #
    # The dbt project lives at /opt/airflow/dbt/ (bind-mounted from ./dbt/).
    # profiles.yml is co-located and uses env_var() for all credentials —
    # no secrets are hardcoded in the dbt project files.
    #
    # Models produced (macro side):
    #   views:  stg_observations, stg_dim_series
    #   tables: mart_gdp, mart_inflation, mart_labor_market, mart_economic_summary
    #
    # The grocery-side models are produced by `grocery_dbt_transform` on the
    # parallel branch — selecting macro models explicitly here keeps each
    # branch independent.
    # -----------------------------------------------------------------------
    macro_dbt_task = BashOperator(
        task_id="macro_dbt_transform",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt "
            "--select staging.stg_observations staging.stg_dim_series marts.mart_gdp "
            "marts.mart_inflation marts.mart_labor_market marts.mart_economic_summary"
        ),
        doc_md="""
## Macro dbt Transform

Runs the macro-side dbt models against the ETL PostgreSQL database after
`load_economic_data` completes. Creates analytics-ready views and tables from
the raw loaded data. The grocery-side dbt models are run by a separate task
(`grocery_dbt_transform`) on the parallel branch.

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
    #   macro_dbt_transform
    #
    # The linear chain enforces sequential execution. A failure in any task
    # halts the downstream chain and triggers the retry policy in default_args.
    # -----------------------------------------------------------------------
    extract_task >> transform_task >> load_task >> macro_dbt_task
