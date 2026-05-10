# data-orchestration-airflow

[![CI](https://github.com/Caseykelly87/data-orchestration-airflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Caseykelly87/data-orchestration-airflow/actions/workflows/ci.yml)

The orchestration layer of the **Knot Shore platform**.

A production-grade ELT pipeline orchestrated by Apache Airflow. The DAG runs two parallel branches converging on a portal-refresh signal: a macro branch that ingests FRED/BLS economic indicators and a grocery branch that drives the Knot Shore Grocery simulation engine, ETL, anomaly detection, and analytics. Fully containerized with Docker Compose, environment-driven configuration, and validated by a structural test suite.

---

## Knot Shore Platform

The platform is five repositories. This one is the orchestration layer; the four siblings are the components it schedules and serves.

| Repo | Role |
|---|---|
| `knot-shore-grocery-simulation-engine` | Python batch generator — synthetic store-day grocery data |
| `economic-data-etl` | Two ETL pipelines: macro (FRED + BLS) and grocery (sim engine ingest + anomaly detection) |
| **`data-orchestration-airflow`** | **Airflow + dbt orchestration of all platform pipelines** |
| `economic-data-api` | FastAPI service serving canonical parquet artifacts as JSON |
| `knot-shore-portal` | Next.js frontend rendering dashboards over the API |

This repo does not duplicate any ETL or simulation logic. DAG tasks call sim engine and ETL functions directly; Airflow is responsible only for scheduling, retries, observability, and routing data between layers.

---

## Architecture

```text
                          ┌──────────────────────────────────────────────────┐
                          │              Docker Compose Stack                 │
                          │                                                   │
  Host :8080 ────────────►│   airflow-webserver  (UI + REST API)             │
                          │                                                   │
                          │   airflow-scheduler  (DAG execution)              │
                          │           │                                       │
                          │           ▼                                       │
                          │   ┌─────────────────────────────────────────────┐ │
                          │   │           economic_data_pipeline             │ │
                          │   │                                              │ │
                          │   │   Macro branch                               │ │
                          │   │     extract → transform → load               │ │
                          │   │                       → macro_dbt_transform ─┼─┼──┐
                          │   │                                              │ │  │
                          │   │   Grocery branch                             │ │  ▼
                          │   │     sim_engine_run → grocery_etl_ingest      │ │  portal_refresh
                          │   │       → grocery_etl_detect                   │ │  ▲
                          │   │       → grocery_load_to_postgres             │ │  │
                          │   │       → grocery_dbt_transform ───────────────┼─┼──┘
                          │   └─────────────────────────────────────────────┘ │
                          │           │                  │                    │
                          │           ▼                  ▼                    │
                          │   postgres-airflow         AWS RDS                 │
                          │   (Airflow metadata)      (ETL/grocery data        │
                          │    Docker container)       and dbt marts)          │
                          └──────────────────────────────────────────────────┘
                            ../economic-data-etl/                        (mounted at /opt/airflow/etl/)
                            ../knot-shore-grocery-simulation-engine/     (mounted at /opt/airflow/sim-engine/)
```

**Executor:** LocalExecutor — tasks run as subprocesses on the scheduler container. Suitable for single-node deployments. Upgrade to CeleryExecutor for horizontal scaling.

**Database separation:** Airflow metadata uses a local Docker Postgres container (orchestration layer). ETL and grocery application data are stored in AWS RDS (data layer). Each can be migrated or scaled independently.

**ELT pattern:** Data lands in raw tables first (the load tasks), then dbt shapes it into typed staging views and materialized mart tables. The macro and grocery branches each have their own staging/marts directory under `dbt/models/`.

---

## DAG: economic_data_pipeline

### Macro branch

| Task | Type | What it does |
|---|---|---|
| `extract_economic_data` | PythonOperator | Pulls FRED + BLS series via API |
| `transform_economic_data` | PythonOperator | Normalizes and validates records |
| `load_economic_data` | PythonOperator | Upserts into `fact_economic_observations` + `dim_series` on RDS |
| `macro_dbt_transform` | BashOperator | Runs the macro dbt models — 2 staging views + 4 mart tables |

### Grocery branch

| Task | Type | What it does |
|---|---|---|
| `sim_engine_run` | PythonOperator | Invokes `python -m knot_shore init` then `run --date {{ ds }}` to produce the per-execution-date sim output tree |
| `grocery_etl_ingest` | PythonOperator | Calls `sim_cli.run` / `run_department_grain` / `write_dim_stores_parquet` to produce three canonical parquets |
| `grocery_etl_detect` | PythonOperator | Calls `detect_cli.run` to produce `anomaly_flags.parquet` |
| `grocery_load_to_postgres` | PythonOperator | Validates and loads six raw tables (3 facts + 3 dims) to `raw` schema |
| `grocery_dbt_transform` | BashOperator | Runs the grocery dbt models — 3 staging views + 5 mart tables |

### Convergence

| Task | Type | What it does |
|---|---|---|
| `portal_refresh` | PythonOperator | Emits a structured cache-invalidation signal naming the affected marts; runs only when both branches succeed |

Schedule: `@daily` · Retries: 3 · SLA: 2 hours · Email on failure: yes

---

## dbt ELT Layer

dbt models run inside the Airflow scheduler container after each branch's load. Both branches' models land in the same RDS database as the raw tables.

### Macro models (`dbt/models/staging/`, `dbt/models/marts/`)

| Model | Type | Contents |
|---|---|---|
| `stg_observations` | view | `fact_economic_observations` — date cast to DATE, nulls filtered |
| `stg_dim_series` | view | `dim_series` — clean passthrough |
| `mart_gdp` | table | GDP, PCE, retail sales, savings rate (FRED) |
| `mart_inflation` | table | CPI, Fed Funds Rate, BLS price series |
| `mart_labor_market` | table | Unemployment, wages, consumer sentiment |
| `mart_economic_summary` | table | Latest value for each of the 14 series (14 rows) |

### Grocery models (`dbt/models/grocery/staging/`, `dbt/models/grocery/marts/`)

| Model | Type | Contents |
|---|---|---|
| `stg_store_metrics` | view | `raw.fact_store_metrics` — date typed and renamed |
| `stg_department_metrics` | view | `raw.fact_department_metrics` — typed |
| `stg_anomaly_flags` | view | `raw.fact_anomaly_flags` — typed |
| `mart_store_metrics` | table | Store-day metrics ordered by date+store |
| `mart_anomalies` | table | Anomaly firings ordered by date desc, severity, store |
| `mart_dashboard_summary` | table | Daily KPI rollup with anomaly counts |
| `mart_dim_stores` | table | Materialised passthrough of `dim_stores` |
| `mart_dim_departments` | table | Materialised passthrough of `dim_departments` |

Run dbt manually:
```bash
docker exec airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"
docker exec airflow_scheduler bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt"
```

---

## v1.0.0 — Complete ✅

- [x] Docker Compose stack boots cleanly — all services healthy
- [x] Airflow UI accessible at `http://localhost:8080`
- [x] Live FRED + BLS extract, transform, load confirmed end-to-end
- [x] Gmail SMTP alerting — `email_on_failure=True`, alerts to `AIRFLOW_ADMIN_EMAIL`
- [x] SLA of 2 hours on all tasks
- [x] GitHub Actions CI — ruff lint + 44 structural tests on every push/PR
- [x] Production Docker Compose overlay (`docker-compose.prod.yml`)
- [x] dbt ELT layer — 4 mart tables, 2 staging views, 32 data tests passing
- [x] AWS RDS (PostgreSQL 17) — ETL data layer migrated from Docker to managed cloud DB
- [x] Environment-driven host config — switch between Docker and RDS via `POSTGRES_ETL_HOST` in `.env`

---

## Platform Integration

The grocery branch and the platform-level convergence task were added on top of the v1.0.0 foundation. The macro side is preserved untouched; the grocery branch follows the same architectural patterns (lazy imports inside callables, structured logging, XCom for task-to-task data flow, BashOperator for dbt CLI invocation).

- [x] Sim engine repo bind-mounted at `/opt/airflow/sim-engine` via `SIM_ENGINE_PATH` env var so non-sibling local checkouts work without committing user-specific paths
- [x] `sim_engine_run` task — `python -m knot_shore init` plus `run --date {{ ds }}` per execution
- [x] `grocery_etl_ingest` — calls `sim_cli` public functions (avoiding `main()`'s side effects) to produce three canonical parquets
- [x] `grocery_etl_detect` — calls `detect_cli.run` to produce `anomaly_flags.parquet`
- [x] `grocery_load_to_postgres` — six raw tables loaded with inline row-count, schema-shape, and primary-key uniqueness validation
- [x] `grocery_dbt_transform` — runs grocery models via `--select grocery`
- [x] `portal_refresh` — convergence task with both branch terminals as upstream; emits a structured cache-invalidation log entry (placeholder for future HTTP cache-bust calls to the API)
- [x] Three new test classes (`TestGroceryTaskExistence`, `TestPortalRefreshTask`, `TestBranchConvergence`) plus minor updates to existing classes — total structural test count moves from 44 to 56
- [x] `dbt/models/grocery/` — three staging views and five marts with not-null, unique, and accepted-values data tests

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose v2)
- Python 3.12+ (for running tests locally — optional)
- FRED API key: [register here](https://fred.stlouisfed.org/docs/api/api_key.html)
- BLS API key: [register here](https://data.bls.gov/registrationEngine/)
- AWS account with an RDS PostgreSQL instance (or leave `POSTGRES_ETL_HOST` blank to use the local Docker container)
- Local checkouts of `economic-data-etl` and `knot-shore-grocery-simulation-engine` — bind-mounted into the scheduler container. Path to the sim engine is set via `SIM_ENGINE_PATH` in `.env` (defaults to a sibling-repo layout)

---

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd data-orchestration-airflow
```

### 2. Create the `.env` file

```bash
cp .env.example .env
```

Open `.env` and fill in all required values.

**Generate the Fernet key** (required — Airflow encrypts secrets with this):

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Generate the webserver secret key:**

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

**Minimum required `.env` values:**

```ini
AIRFLOW_FERNET_KEY=<generated above>
AIRFLOW_SECRET_KEY=<generated above>
AIRFLOW_ADMIN_PASSWORD=<choose a password>
POSTGRES_AIRFLOW_PASSWORD=<choose a password>
POSTGRES_ETL_PASSWORD=<choose a password>
FRED_API_KEY=<your FRED key>
BLS_API_KEY=<your BLS key>
AIRFLOW_SMTP_USER=<your-gmail@gmail.com>
AIRFLOW_SMTP_PASSWORD=<16-char Gmail App Password>

# Optional — leave blank to use the local Docker postgres-etl container:
POSTGRES_ETL_HOST=<your-rds-endpoint>.rds.amazonaws.com

# Optional — only needed when the sim engine repo is not at the default
# sibling-repo location (`../knot-shore-grocery-simulation-engine`):
SIM_ENGINE_PATH=/absolute/path/to/knot-shore-grocery-simulation-engine
```

### 3. Create required directories

```bash
# These already exist if you cloned the repo. If not:
mkdir -p logs plugins
```

---

## Running the Stack

### Start all services

```bash
docker compose up -d
```

First-time startup takes 3–5 minutes — the `_PIP_ADDITIONAL_REQUIREMENTS` mechanism installs dbt inside the container. Subsequent restarts are normal speed.

### Check service status

```bash
docker compose ps
```

All services should show `running` (or `exited 0` for `airflow-init`).

### View logs

```bash
docker compose logs -f
docker compose logs -f airflow-scheduler
```

### Stop the stack

```bash
# Stop containers (data persists in Docker volumes)
docker compose down

# Stop and delete all data (full reset)
docker compose down -v
```

### Production configuration overlay

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

| Setting | Local dev | Production overlay |
|---|---|---|
| `postgres-etl` host port | `5433` exposed | Removed |
| `AIRFLOW_ENV` | not set | `production` |
| ETL database | Docker `postgres-etl` (if `POSTGRES_ETL_HOST` unset) | AWS RDS (set `POSTGRES_ETL_HOST` in `.env`) |

---

## Accessing the Airflow UI

1. Open **http://localhost:8080**
2. Log in with the credentials from `.env` (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`)

The `economic_data_pipeline` DAG starts **paused** — unpause it in the UI before triggering.

---

## Triggering the DAG

### Via the Airflow UI

1. Navigate to **DAGs**
2. Find `economic_data_pipeline` and unpause it
3. Click **▶ Trigger DAG**

### Via the Airflow CLI

```bash
docker exec airflow_scheduler airflow dags trigger economic_data_pipeline
docker exec airflow_scheduler airflow dags list-runs --dag-id economic_data_pipeline
```

### Via the Airflow REST API

```bash
curl -X POST http://localhost:8080/api/v1/dags/economic_data_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:<password> \
  -d '{"conf": {}}'
```

---

## Running Tests

### Locally (outside Docker)

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux

pip install apache-airflow==2.10.2 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

pip install -r requirements.txt
pytest
```

### Inside the Docker container

```bash
docker exec airflow_scheduler bash -c "cd /opt/airflow && python -m pytest tests/ -v"
```

### Expected output

```
56 passed in Xs
```

---

## Project Structure

```text
data-orchestration-airflow/
│
├── .github/workflows/
│   └── ci.yml                         # Ruff lint + pytest on push/PR
│
├── dags/
│   ├── __init__.py
│   ├── economic_pipeline_dag.py       # 10-task DAG: macro + grocery branches
│   └── tasks/
│       └── __init__.py
│
├── dbt/
│   ├── dbt_project.yml                # staging=view, marts=table (per branch)
│   ├── profiles.yml                   # Postgres connection via env_var() — safe to commit
│   └── models/
│       ├── staging/                   # Macro staging
│       │   ├── stg_observations.sql
│       │   ├── stg_dim_series.sql
│       │   └── schema.yml
│       ├── marts/                     # Macro marts
│       │   ├── mart_gdp.sql
│       │   ├── mart_inflation.sql
│       │   ├── mart_labor_market.sql
│       │   ├── mart_economic_summary.sql
│       │   └── schema.yml
│       └── grocery/
│           ├── staging/               # Grocery staging
│           │   ├── stg_store_metrics.sql
│           │   ├── stg_department_metrics.sql
│           │   ├── stg_anomaly_flags.sql
│           │   └── schema.yml
│           └── marts/                 # Grocery marts
│               ├── mart_store_metrics.sql
│               ├── mart_anomalies.sql
│               ├── mart_dashboard_summary.sql
│               ├── mart_dim_stores.sql
│               ├── mart_dim_departments.sql
│               └── schema.yml
│
├── tests/
│   ├── __init__.py
│   └── test_dag_structure.py          # 56 structural tests
│
├── logs/                              # Airflow task logs (bind-mounted, git-ignored)
├── plugins/                           # Airflow custom plugins (currently empty)
│
├── docker-compose.yml                 # Full stack: Airflow + 2x Postgres
├── docker-compose.prod.yml            # Production overlay: port hardening, AIRFLOW_ENV
├── .env.example                       # Credentials template (committed)
├── .env                               # Live credentials (git-ignored)
├── .gitignore
├── conftest.py
├── pytest.ini
├── requirements.txt
└── README.md
```

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `AIRFLOW_UID` | Yes (Linux) | Host user ID for file ownership. Use `50000` on Windows. |
| `AIRFLOW_FERNET_KEY` | **Yes** | Encrypts connection passwords in the metadata DB. |
| `AIRFLOW_SECRET_KEY` | **Yes** | Signs webserver session cookies. |
| `AIRFLOW_WEBSERVER_PORT` | No | Host port for Airflow UI. Default: `8080`. |
| `AIRFLOW_ADMIN_USER` | No | Admin username. Default: `admin`. |
| `AIRFLOW_ADMIN_PASSWORD` | **Yes** | Admin password for Airflow UI. |
| `AIRFLOW_ADMIN_EMAIL` | No | Failure alert recipient. Default: `admin@example.com`. |
| `POSTGRES_AIRFLOW_USER` | **Yes** | Airflow metadata DB username. |
| `POSTGRES_AIRFLOW_PASSWORD` | **Yes** | Airflow metadata DB password. |
| `POSTGRES_AIRFLOW_DB` | **Yes** | Airflow metadata DB name. |
| `POSTGRES_ETL_USER` | **Yes** | ETL data DB username. |
| `POSTGRES_ETL_PASSWORD` | **Yes** | ETL data DB password. |
| `POSTGRES_ETL_DB` | **Yes** | ETL data DB name. |
| `POSTGRES_ETL_PORT` | No | Host port for local ETL DB. Default: `5433`. |
| `POSTGRES_ETL_HOST` | No | ETL DB hostname. Leave blank for local Docker container; set to RDS endpoint for production. |
| `FRED_API_KEY` | **Yes** | FRED API key for ETL extract tasks. |
| `BLS_API_KEY` | **Yes** | BLS API key for ETL extract tasks. |
| `AIRFLOW_SMTP_USER` | **Yes** | Gmail address for SMTP sender and alert emails. |
| `AIRFLOW_SMTP_PASSWORD` | **Yes** | Gmail App Password (16 chars, requires 2FA). |
| `SIM_ENGINE_PATH` | No | Local path to the sim engine repo. Default: `../knot-shore-grocery-simulation-engine`. |

---

## Design Decisions

**Why separate databases for Airflow metadata and ETL data?**
The Airflow metadata DB is managed entirely by Airflow and changes schema with every upgrade. The ETL DB is managed by the application and must be stable for downstream consumers. Mixing them creates coupling that is difficult to untangle in production.

**Why LocalExecutor instead of CeleryExecutor?**
CeleryExecutor adds Redis, worker containers, and flower monitoring — significant operational complexity. For a daily pipeline running ten tasks across two parallel branches, LocalExecutor is correct: simple, reliable, and sufficient. CeleryExecutor becomes relevant when you need many concurrent tasks or multi-node scaling.

**Why no business logic in the DAG?**
The DAG is the orchestration layer only. Embedding business logic in Airflow tasks creates tight coupling between the scheduler and the data pipeline, makes both harder to test, and locks the pipeline to Airflow. Task callables are thin wrappers; ETL logic lives in `economic-data-etl`, simulation logic lives in `knot-shore-grocery-simulation-engine`.

**Why two parallel branches in one DAG instead of two separate DAGs?**
Both branches feed the same downstream consumers (the API and portal), so the cache invalidation signal needs to wait for both branches to succeed. A single DAG with a convergence task expresses this dependency directly; two DAGs would require an external sensor or a separate "join" DAG.

**Why ELT instead of ETL?**
Raw data lands in the database first (the load tasks), then dbt transforms it in SQL. This separates concerns cleanly — Python handles extraction and normalization; dbt handles business-domain modeling. SQL transformations are also easier to version, test, and iterate on than in-memory Pandas operations.

**Why call sim engine and ETL public functions directly instead of `main()`?**
The CLI `main()` functions in `sim_cli` and `detect_cli` call `configure_logging()` and may mutate `os.environ`. Inside an Airflow worker, that interferes with the host's logging configuration and can leave the environment dirty for other tasks. The public functions (`run`, `run_department_grain`, `load_dim_stores`, etc.) have no such side effects and are documented in `economic-data-etl/docs/airflow_integration_contract.md` as the supported entry points.

**Why `catchup=False`?**
The pipeline is designed to run forward from today. With `catchup=True`, activating the DAG for the first time would queue a run for every day since `start_date` — thousands of unnecessary historical API calls and sim engine invocations.
