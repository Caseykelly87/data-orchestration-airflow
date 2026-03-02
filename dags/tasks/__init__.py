"""
Task wrapper package for data-orchestration-airflow.

Current Milestone: Initial Commit
----------------------------------
This package is reserved for Phase 2 (ETL Integration). It will contain
thin Python wrapper functions that import and call the ETL layer functions
(`src/extract.py`, `src/transform.py`, `src/load.py` from economic-data-etl).

The wrappers here will be imported by `dags/economic_pipeline_dag.py`,
keeping the DAG file focused on orchestration structure while task
callables are organized in this package.

Phase 2 Structure (planned):
    dags/tasks/
        __init__.py         # (this file)
        extract_tasks.py    # wraps fetch_fred_data(), fetch_bls_data()
        transform_tasks.py  # wraps parse_*, combine_*, build_dim_series()
        load_tasks.py       # wraps ensure_tables_exist(), upsert_*()
"""
