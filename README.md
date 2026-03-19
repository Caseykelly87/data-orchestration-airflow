# data-orchestration-airflow

[![CI](https://github.com/Caseykelly87/data-orchestration-airflow/actions/workflows/ci.yml/badge.svg)](https://github.com/Caseykelly87/data-orchestration-airflow/actions/workflows/ci.yml)

**Project 2** of a modular data engineering portfolio.

A production-grade ELT pipeline orchestrated by Apache Airflow. Extracts U.S. macroeconomic indicator data from FRED and BLS APIs, loads it into AWS RDS (PostgreSQL), and transforms it into analytics-ready mart tables using dbt. Fully containerized with Docker Compose, environment-driven configuration, and validated by a 44-test CI suite.

---

## Portfolio Context

| Project | Repo | Role |
|---|---|---|
| 1 | `economic-data-etl` | Python ETL pipeline — extract, transform, load |
| **2** | **`data-orchestration-airflow`** | **Airflow orchestration + dbt ELT layer — scheduling, retries, monitoring, cloud DB** |

This project orchestrates Project 1. It does **not** duplicate any ETL logic. The DAG tasks call ETL functions directly — Airflow is responsible only for scheduling, retries, and observability.

---

## Architecture

```text
                        ┌─────────────────────────────────────┐
                        │        Docker Compose Stack          │
                        │                                      │
  Host :8080 ──────────►│  airflow-webserver  (UI + REST API) │
                        │                                      │
                        │  airflow-scheduler  (DAG execution)  │
                        │         │                            │
                        │         ▼                            │
                        │  ┌─────────────────────────────────┐ │
                        │  │      economic_data_pipeline      │ │
                        │  │  extract → transform → load      │ │
                        │  │                  → dbt_transform │ │
                        │  └──────────────┬───────────────────┘ │
                        │                 │                    │
                        │         ▼              ▼             │
                        │  postgres-airflow    AWS RDS          │
                        │  (Airflow metadata) (ETL data +       │
                        │   Docker container)  dbt mart tables) │
                        └─────────────────────────────────────┘
                         ../economic-data-etl/ (mounted at /opt/airflow/etl/)
```

**Executor:** LocalExecutor — tasks run as subprocesses on the scheduler container. Suitable for single-node deployments. Upgrade to CeleryExecutor for horizontal scaling.

**Database separation:** Airflow metadata uses a local Docker Postgres container (orchestration layer). ETL application data is stored in AWS RDS (data layer). Each can be migrated or scaled independently.

**ELT pattern:** Data lands in raw tables first (`load` task), then dbt shapes it into typed staging views and materialized mart tables (`dbt_transform` task). This is the modern industry standard — transform after load, not before.

---

## DAG: economic_data_pipeline

| Task | Type | What it does |
|---|---|---|
| `extract_economic_data` | PythonOperator | Pulls FRED + BLS series via API |
| `transform_economic_data` | PythonOperator | Normalizes and validates records |
| `load_economic_data` | PythonOperator | Upserts into `fact_economic_observations` + `dim_series` on RDS |
| `dbt_transform` | BashOperator | Runs `dbt run` — builds 2 staging views + 4 mart tables |

Schedule: `@daily` · Retries: 3 · SLA: 2 hours · Email on failure: yes

---

## dbt ELT Layer

dbt models run inside the Airflow scheduler container after each load. Models land in the same RDS database as the raw tables.

| Model | Type | Contents |
|---|---|---|
| `stg_observations` | view | `fact_economic_observations` — date cast to DATE, nulls filtered |
| `stg_dim_series` | view | `dim_series` — clean passthrough |
| `mart_gdp` | table | GDP, PCE, retail sales, savings rate (FRED) |
| `mart_inflation` | table | CPI, Fed Funds Rate, BLS price series |
| `mart_labor_market` | table | Unemployment, wages, consumer sentiment |
| `mart_economic_summary` | table | Latest value for each of the 14 series (14 rows) |

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

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose v2)
- Python 3.12+ (for running tests locally — optional)
- FRED API key: [register here](https://fred.stlouisfed.org/docs/api/api_key.html)
- BLS API key: [register here](https://data.bls.gov/registrationEngine/)
- AWS account with an RDS PostgreSQL instance (or leave `POSTGRES_ETL_HOST` blank to use the local Docker container)

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
... (44 tests)
44 passed in Xs
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
│   ├── economic_pipeline_dag.py       # 4-task DAG: extract → transform → load → dbt
│   └── tasks/
│       └── __init__.py
│
├── dbt/
│   ├── dbt_project.yml                # staging=view, marts=table
│   ├── profiles.yml                   # Postgres connection via env_var() — safe to commit
│   └── models/
│       ├── staging/
│       │   ├── stg_observations.sql
│       │   ├── stg_dim_series.sql
│       │   └── schema.yml             # Source defs + 32 data tests
│       └── marts/
│           ├── mart_gdp.sql
│           ├── mart_inflation.sql
│           ├── mart_labor_market.sql
│           ├── mart_economic_summary.sql
│           └── schema.yml
│
├── tests/
│   ├── __init__.py
│   └── test_dag_structure.py          # 44 structural tests (TDD)
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

---

## Development Roadmap

### Phase 1 — Infrastructure ✅
- Docker Compose stack, Airflow webserver + scheduler, metadata DB, ETL DB container, skeleton DAG, 22 structural tests

### Phase 2 — ETL Integration ✅
- ETL project mounted into Airflow container, live task callables (extract → transform → load via XCom), 27 structural tests

### Phase 3 — Observability ✅
- Gmail SMTP alerting, SLA of 2 hours, 35 structural tests

### Phase 4 — CI/CD and Cloud Readiness ✅
- [x] GitHub Actions CI (ruff + 44 structural tests on every push/PR)
- [x] Production Docker Compose overlay (`docker-compose.prod.yml`)
- [x] dbt ELT layer — 4 mart tables, 2 staging views, 32 data tests
- [x] AWS RDS — ETL data layer migrated to managed cloud PostgreSQL

---

## Design Decisions

**Why separate databases for Airflow metadata and ETL data?**
The Airflow metadata DB is managed entirely by Airflow and changes schema with every upgrade. The ETL DB is managed by the application and must be stable for downstream consumers. Mixing them creates coupling that is difficult to untangle in production.

**Why LocalExecutor instead of CeleryExecutor?**
CeleryExecutor adds Redis, worker containers, and flower monitoring — significant operational complexity. For a daily pipeline running 4 sequential tasks, LocalExecutor is correct: simple, reliable, and sufficient. CeleryExecutor becomes relevant when you need many concurrent tasks or multi-node scaling.

**Why no business logic in the DAG?**
The DAG is the orchestration layer only. Embedding business logic in Airflow tasks creates tight coupling between the scheduler and the data pipeline, makes both harder to test, and locks the pipeline to Airflow. Task callables are thin wrappers; ETL logic lives in Project 1.

**Why ELT instead of ETL?**
Raw data lands in the database first (load), then dbt transforms it in SQL (transform). This separates concerns cleanly — the Python pipeline handles extraction and normalization; dbt handles business-domain modeling. SQL transformations are also easier to version, test, and iterate on than in-memory Pandas operations.

**Why `catchup=False`?**
The pipeline is designed to run forward from today. With `catchup=True`, activating the DAG for the first time would queue a run for every day since `start_date` — thousands of unnecessary historical API calls.
