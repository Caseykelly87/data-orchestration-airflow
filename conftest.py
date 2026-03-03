"""
Root conftest.py — data-orchestration-airflow
=============================================

Purpose
-------
Configures the Python path so that the test suite can import from the
`dags/` package without requiring the project to be installed as a package.

    from dags.economic_pipeline_dag import dag  # works after this conftest runs

This file is executed automatically by pytest before any test collection.
It must remain at the project root (same level as the `dags/` and `tests/`
directories) for pytest to discover it.

What This Does NOT Do
---------------------
- This file does NOT start Docker containers or Airflow services.
- It does NOT connect to any database.
- It does NOT require API keys or network access.
- It does NOT mock the Airflow environment — the DAG structural tests
  rely only on the Airflow Python package being installed locally.

Running Tests
-------------
    # Install Airflow locally (one-time setup):
    pip install apache-airflow==2.10.2 --constraint \
      "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"
    pip install -r requirements.txt

    # Run all tests:
    pytest

    # Run inside the Docker scheduler container:
    docker exec airflow_scheduler bash -c "cd /opt/airflow && python -m pytest tests/ -v"
"""

import sys
import os

# Insert the project root at the front of sys.path.
# This enables `from dags.economic_pipeline_dag import dag` in test files,
# resolving correctly whether tests are run locally or inside the container.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
