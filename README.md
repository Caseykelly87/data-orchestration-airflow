# data-orchestration-airflow

**Project 2** of a modular data engineering portfolio.

An Apache Airflow orchestration layer that runs the U.S. macroeconomic indicator ETL pipeline (github project 1 `economic-data-etl`) on a daily schedule. Fully containerized with Docker Compose, using separate PostgreSQL instances for Airflow metadata and ETL data.

---

## Portfolio Context

| Project | Repo | Role |
|---|---|---|
| 1 | `economic-data-etl` | Python ETL pipeline — extract, transform, load |
| **2** | **`data-orchestration-airflow`** | **Airflow orchestration layer — scheduling, retries, monitoring** |

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
                        │  ┌─────────────────────────┐        │
                        │  │    economic_data_pipeline│        │
                        │  │  extract → transform → load      │
                        │  └─────────────────────────┘        │
                        │         │              │             │
                        │         ▼              ▼             │
                        │  postgres-airflow  postgres-etl      │
                        │  (Airflow metadata) (ETL data)       │
                        │                       │              │
  Host :5433 ──────────►│                       │              │
                        └───────────────────────┼─────────────┘
                                                │
                         ../economic-data-etl/ (mounted at /opt/airflow/etl/)
```

**Executor:** LocalExecutor — tasks run as subprocesses on the scheduler container. Suitable for single-node deployments. Upgrade to CeleryExecutor for horizontal scaling.

**Database separation:** Airflow metadata and ETL application data use dedicated Postgres containers. This enforces the orchestration/application boundary and allows each layer to be migrated or scaled independently.

---

## Current Milestone: Phase 2 — ETL Integration ✅

- [x] Docker Compose stack boots cleanly
- [x] Airflow UI accessible at `http://localhost:8080`
- [x] Both Postgres databases healthy
- [x] DAG loads and displays in the UI
- [x] All structural tests pass (27 tests)
- [x] ETL volume mounted at `/opt/airflow/etl/`
- [x] Live FRED + BLS extract, transform, load wired end-to-end
- [x] 5,817 FRED rows + 264 BLS rows confirmed in `postgres-etl`

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose v2)
- Python 3.12+ (for running tests locally — optional)
- FRED API key: [register here](https://fred.stlouisfed.org/docs/api/api_key.html)
- BLS API key: [register here](https://data.bls.gov/registrationEngine/)

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
```

### 3. Create required directories

The `logs/` and `plugins/` directories must exist for Docker bind mounts:

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

First-time startup takes 2–3 minutes. The `airflow-init` service runs database migrations and creates the admin user, then exits. The webserver and scheduler wait for init to complete before starting.

### Check service status

```bash
docker compose ps
```

All services should show `running` (or `exited 0` for `airflow-init`).

### View logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
docker compose logs -f airflow-init
```

### Stop the stack

```bash
# Stop containers (data persists in Docker volumes)
docker compose down

# Stop and delete all data (full reset)
docker compose down -v
```

---

## Accessing the Airflow UI

Once the stack is running:

1. Open **http://localhost:8080** (or the port set in `AIRFLOW_WEBSERVER_PORT`)
2. Log in with the credentials set in `.env`:
   - Username: `AIRFLOW_ADMIN_USER` (default: `admin`)
   - Password: `AIRFLOW_ADMIN_PASSWORD`

The `economic_data_pipeline` DAG will appear in the DAG list. It starts **paused** — this is intentional (controlled by `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true`).

---

## Triggering the DAG

### Via the Airflow UI

1. Navigate to **DAGs** in the top menu
2. Find `economic_data_pipeline`
3. Toggle the pause button (blue/grey toggle) to **unpause** the DAG
4. Click the **▶ Trigger DAG** button (play icon) for an immediate manual run
5. Click the run to inspect task logs in the **Grid** or **Graph** view

### Via the Airflow CLI

```bash
# Trigger a manual run
docker exec airflow_scheduler airflow dags trigger economic_data_pipeline

# List all DAG runs
docker exec airflow_scheduler airflow dags list-runs --dag-id economic_data_pipeline

# View task logs for a specific run
docker exec airflow_scheduler airflow tasks logs economic_data_pipeline extract_economic_data <run-id>
```

### Via the Airflow REST API

```bash
# Trigger via REST API (replace password with your AIRFLOW_ADMIN_PASSWORD)
curl -X POST http://localhost:8080/api/v1/dags/economic_data_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:<password> \
  -d '{"conf": {}}'
```

---

## Connecting to PostgreSQL

### ETL Data Database (for inspection, development)

The ETL Postgres container is exposed on the host at port `5433` (configurable via `POSTGRES_ETL_PORT`).

**Connect with psql:**

```bash
psql -h localhost -p 5433 -U etl_user -d economic_data
# Password: POSTGRES_ETL_PASSWORD from your .env
```

**Connect with pgAdmin or DBeaver:**

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `5433` (or `POSTGRES_ETL_PORT`) |
| Database | `economic_data` (or `POSTGRES_ETL_DB`) |
| Username | `etl_user` (or `POSTGRES_ETL_USER`) |
| Password | your `POSTGRES_ETL_PASSWORD` |

### Airflow Metadata Database

The Airflow metadata Postgres is accessible within the Docker network at `postgres-airflow:5432`. It is **not** exposed on a host port by design. To inspect it directly:

```bash
docker exec -it postgres_airflow psql -U airflow -d airflow
```

---

## Running Tests

### Locally (outside Docker)

Install Airflow in a local virtualenv using the constraint file to avoid dependency conflicts:

```bash
# Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux

# Install Airflow with constraints
pip install apache-airflow==2.10.2 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

# Install test dependencies
pip install -r requirements.txt

# Run the test suite
pytest

# With coverage
pytest --cov=dags --cov-report=term-missing
```

### Inside the Docker container

```bash
# Run tests inside the scheduler container (Airflow is already installed)
docker exec airflow_scheduler pytest /opt/airflow/tests/ -v
```

### Expected output

```
tests/test_dag_structure.py::TestDagIdentity::test_dag_id PASSED
tests/test_dag_structure.py::TestDagIdentity::test_dag_has_description PASSED
tests/test_dag_structure.py::TestDagIdentity::test_dag_has_tags PASSED
tests/test_dag_structure.py::TestDagIdentity::test_dag_tags_contain_etl PASSED
tests/test_dag_structure.py::TestDagSchedule::test_dag_schedule_is_daily PASSED
tests/test_dag_structure.py::TestDagSchedule::test_dag_catchup_is_disabled PASSED
tests/test_dag_structure.py::TestDagSchedule::test_dag_max_active_runs PASSED
tests/test_dag_structure.py::TestDefaultArgs::test_default_args_exist PASSED
... (all 27 tests pass)
```

---

## Project Structure

```text
data-orchestration-airflow/
│
├── dags/
│   ├── __init__.py                    # Package init (enables test imports)
│   ├── economic_pipeline_dag.py       # Main DAG: extract → transform → load
│   └── tasks/
│       └── __init__.py                # Reserved for Phase 2 task wrappers
│
├── tests/
│   ├── __init__.py
│   └── test_dag_structure.py          # 22 structural tests (TDD)
│
├── logs/                              # Airflow task logs (bind-mounted, git-ignored)
│   └── .gitkeep
│
├── plugins/                           # Airflow custom plugins (currently empty)
│   └── .gitkeep
│
├── docker-compose.yml                 # Full stack: Airflow + 2x Postgres
├── .env.example                       # Credentials template (commit this)
├── .env                               # Live credentials (never commit — git-ignored)
├── .gitignore
├── conftest.py                        # Pytest sys.path configuration
├── pytest.ini                         # Pytest settings
├── requirements.txt                   # Python dependencies
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
| `AIRFLOW_ADMIN_EMAIL` | No | Admin email. Default: `admin@example.com`. |
| `POSTGRES_AIRFLOW_USER` | **Yes** | Airflow metadata DB username. |
| `POSTGRES_AIRFLOW_PASSWORD` | **Yes** | Airflow metadata DB password. |
| `POSTGRES_AIRFLOW_DB` | **Yes** | Airflow metadata DB name. |
| `POSTGRES_ETL_USER` | **Yes** | ETL data DB username. |
| `POSTGRES_ETL_PASSWORD` | **Yes** | ETL data DB password. |
| `POSTGRES_ETL_DB` | **Yes** | ETL data DB name. |
| `POSTGRES_ETL_PORT` | No | Host port for ETL DB. Default: `5433`. |
| `FRED_API_KEY` | **Yes** | FRED API key for ETL extract tasks. |
| `BLS_API_KEY` | **Yes** | BLS API key for ETL extract tasks. |

---

## Development Roadmap

### Phase 1 — Initial Commit (current) ✅
- Docker Compose stack
- Airflow infrastructure (webserver, scheduler, metadata DB)
- ETL data PostgreSQL container
- Skeleton DAG with correct structure
- Full structural test suite (22 tests)

### Phase 2 — ETL Integration ✅
- ETL project mounted into Airflow container
- Live task callables: extract → transform → load via XCom
- Full FRED + BLS historical load confirmed in `postgres-etl`
- 27 structural tests passing

### Phase 3 — Observability
- Airflow alerting (email or Slack on failure)
- DAG SLA configuration
- Metrics export

### Phase 4 — Cloud Readiness
- Replace Docker Postgres with AWS RDS
- Add CI/CD pipeline (GitHub Actions)
- Environment-specific configuration (dev/staging/prod)
- dbt integration for transformation layer

---

## Design Decisions

**Why separate Postgres containers?**
The Airflow metadata database and the ETL data database serve completely different purposes. The metadata DB is managed entirely by Airflow and changes schema with every Airflow upgrade. The ETL DB is managed by the application and must be stable for downstream consumers. Mixing them creates coupling that is difficult to untangle in production.

**Why LocalExecutor instead of CeleryExecutor?**
CeleryExecutor adds Redis, worker containers, and flower monitoring — significant operational complexity. For a daily pipeline running 3 sequential tasks, LocalExecutor is correct: simple, reliable, and sufficient. CeleryExecutor becomes relevant when you have many concurrent tasks or need multi-node scaling.

**Why no business logic in the DAG?**
The DAG is the orchestration layer only. Embedding business logic in Airflow tasks creates tight coupling between the scheduler and the data pipeline, makes both harder to test, and locks the pipeline to Airflow. By keeping task callables as thin wrappers, the ETL pipeline remains independently testable and replaceable.

**Why `catchup=False`?**
The pipeline is designed to run forward from today. With `catchup=True`, activating a DAG for the first time would queue runs for every day since `start_date` — potentially thousands of runs that would flood the scheduler with historical ETL calls that serve no purpose for this pipeline.
