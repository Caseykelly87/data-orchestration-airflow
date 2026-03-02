"""
DAG package for data-orchestration-airflow.

This package exposes all Airflow DAG objects. The Airflow scheduler
discovers DAGs by scanning this directory for Python files that define
DAG objects at module scope — the __init__.py is not used by the
scheduler but enables clean Python imports during testing.
"""
