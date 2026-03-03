"""
Task wrapper package for data-orchestration-airflow.

Current state: task callables (run_extract, run_transform, run_load) live
directly in dags/economic_pipeline_dag.py. This package is reserved for
future refactoring if the callable count grows large enough to warrant
splitting into separate modules.
"""
