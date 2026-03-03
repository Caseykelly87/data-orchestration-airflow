"""
DAG Structure Validation Tests
================================

Project:  data-orchestration-airflow
Module:   tests/test_dag_structure.py
Author:   data-platform

Architecture Philosophy
-----------------------
These tests validate the Airflow DAG's structural integrity without
requiring a running Airflow instance or any Docker infrastructure.
They assert the DAG's Python object is correctly constructed and
ready to be handed to the Airflow scheduler.

TDD Note
--------
These tests are written BEFORE the DAG implementation, following strict
TDD methodology. The test file defines the contract that the DAG must
satisfy. Run `pytest` after implementing the DAG to validate compliance.

What Is Tested
--------------
- DAG loads without import errors
- DAG has the correct ID and metadata (description, tags)
- Daily scheduling interval is correctly configured
- catchup is disabled to prevent backfill surprises on first deploy
- default_args contain all required production fields (owner, retries,
  retry_delay, email_on_failure, email_on_retry)
- All three pipeline tasks exist with the correct task IDs
- Task dependency graph is correct: extract → transform → load

What Is NOT Tested (and Why)
------------------------------
These are structural unit tests. The following are outside scope by design:

1. **Runtime task execution** — Testing that `run_extract()` successfully
   calls FRED and BLS APIs requires a live network connection and valid
   API keys. These are integration tests, validated manually.

2. **Database connectivity** — Whether the ETL PostgreSQL container is
   reachable is a Docker-level runtime concern, not a DAG structure concern.

3. **Docker Compose behavior** — Container startup order, volume mounts,
   and network reachability cannot be unit tested. Validate with
   `docker compose up` and inspect container logs.

4. **XCom data contracts** — Data passed between tasks via XCom is an
   integration concern. The values pushed/pulled depend on live execution.

Running the Tests
-----------------
    # From the project root, in a virtualenv with apache-airflow installed:
    pytest tests/ -v

    # Inside the Airflow Docker container:
    docker exec airflow_scheduler pytest /opt/airflow/tests/ -v
"""

import pytest
from datetime import timedelta


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dag():
    """
    Import and return the economic pipeline DAG object.

    Uses module scope to load the DAG once per test session. DAG import
    triggers module-level Airflow object construction (task registration,
    dependency wiring), which is expensive to repeat per test.

    Raises ImportError if the DAG file has a syntax error or missing
    dependency — this itself is a meaningful test signal.
    """
    from dags.economic_pipeline_dag import dag as economic_dag
    return economic_dag


# ---------------------------------------------------------------------------
# DAG Identity Tests
# ---------------------------------------------------------------------------

class TestDagIdentity:
    """Validate that the DAG has the correct identifier and metadata."""

    def test_dag_id(self, dag):
        """DAG ID must match the canonical pipeline name used across all tooling."""
        assert dag.dag_id == "economic_data_pipeline"

    def test_dag_has_description(self, dag):
        """DAG must have a non-empty description for UI discoverability."""
        assert dag.description is not None
        assert isinstance(dag.description, str)
        assert len(dag.description.strip()) > 0

    def test_dag_has_tags(self, dag):
        """DAG must be tagged to support filtering in the Airflow UI."""
        assert dag.tags is not None
        assert len(dag.tags) > 0

    def test_dag_tags_contain_etl(self, dag):
        """At least one tag must identify this as an ETL pipeline."""
        tag_strings = [str(t).lower() for t in dag.tags]
        has_etl_tag = any("etl" in tag for tag in tag_strings)
        assert has_etl_tag, f"Expected an 'etl' tag, found: {dag.tags}"


# ---------------------------------------------------------------------------
# Scheduling Tests
# ---------------------------------------------------------------------------

class TestDagSchedule:
    """Validate scheduling and execution configuration."""

    def test_dag_schedule_is_daily(self, dag):
        """
        DAG must be scheduled to run once per day.

        Accepts three equivalent representations:
          - "@daily"        (Airflow preset)
          - "0 0 * * *"    (equivalent cron expression)
          - timedelta(days=1)  (timedelta form)
        """
        schedule = dag.schedule_interval
        valid_cron_forms = {"@daily", "0 0 * * *"}

        if isinstance(schedule, str):
            assert schedule in valid_cron_forms, (
                f"Expected a daily schedule string, got: {repr(schedule)}"
            )
        elif isinstance(schedule, timedelta):
            assert schedule.days == 1 and schedule.seconds == 0, (
                f"Expected timedelta(days=1), got: {schedule}"
            )
        else:
            pytest.fail(
                f"Unexpected schedule_interval type {type(schedule)}: {schedule}"
            )

    def test_dag_catchup_is_disabled(self, dag):
        """
        catchup must be False.

        A daily pipeline with catchup=True would attempt to execute for
        every day since start_date on first activation, flooding the
        scheduler with historical runs. This is never appropriate for
        this pipeline.
        """
        assert dag.catchup is False, (
            "catchup must be explicitly set to False to prevent "
            "historical backfill on first DAG activation."
        )

    def test_dag_max_active_runs(self, dag):
        """
        max_active_runs must be 1.

        The ETL pipeline is stateful (upserts into a shared database).
        Concurrent DAG runs risk data races on the fact table. Enforce
        a single active run at all times.
        """
        assert dag.max_active_runs == 1, (
            f"max_active_runs must be 1 to prevent concurrent ETL runs, "
            f"got: {dag.max_active_runs}"
        )


# ---------------------------------------------------------------------------
# default_args Tests
# ---------------------------------------------------------------------------

class TestDefaultArgs:
    """
    Validate that default_args contains all required production configuration.

    default_args are applied to every task in the DAG. Omitting any of
    these fields results in silent Airflow defaults, which may be
    inappropriate for a production pipeline.
    """

    def test_default_args_exist(self, dag):
        """default_args must be a non-empty dict."""
        assert dag.default_args is not None
        assert isinstance(dag.default_args, dict)
        assert len(dag.default_args) > 0

    def test_owner_is_set(self, dag):
        """owner must be a non-empty string — required for operational alerts."""
        owner = dag.default_args.get("owner")
        assert owner is not None, "default_args must include 'owner'"
        assert isinstance(owner, str)
        assert len(owner.strip()) > 0

    def test_retry_count_is_positive(self, dag):
        """
        retries must be defined and >= 1.

        FRED and BLS APIs have occasional transient 5xx errors.
        At least one retry is required for a resilient daily pipeline.
        """
        retries = dag.default_args.get("retries")
        assert retries is not None, "default_args must include 'retries'"
        assert isinstance(retries, int), f"retries must be int, got {type(retries)}"
        assert retries >= 1, f"retries must be >= 1, got {retries}"

    def test_retry_delay_is_timedelta(self, dag):
        """retry_delay must be a timedelta — Airflow requires this type."""
        retry_delay = dag.default_args.get("retry_delay")
        assert retry_delay is not None, "default_args must include 'retry_delay'"
        assert isinstance(retry_delay, timedelta), (
            f"retry_delay must be a timedelta, got {type(retry_delay)}"
        )

    def test_retry_delay_is_meaningful(self, dag):
        """retry_delay must be at least 1 minute to avoid hammering APIs."""
        retry_delay = dag.default_args.get("retry_delay")
        assert retry_delay is not None
        total_seconds = retry_delay.total_seconds()
        assert total_seconds >= 60, (
            f"retry_delay must be >= 60 seconds, got {total_seconds}s. "
            "Too-short retry delays can cause API rate limit violations."
        )

    def test_email_on_failure_is_configured(self, dag):
        """
        email_on_failure must be explicitly set.

        Leaving it unset relies on Airflow's default (True), which will
        generate errors if no SMTP is configured. Explicit is better.
        """
        assert "email_on_failure" in dag.default_args, (
            "email_on_failure must be explicitly set in default_args"
        )

    def test_email_on_retry_is_configured(self, dag):
        """email_on_retry must be explicitly set for the same reason."""
        assert "email_on_retry" in dag.default_args, (
            "email_on_retry must be explicitly set in default_args"
        )

    def test_depends_on_past_is_configured(self, dag):
        """
        depends_on_past must be explicitly set.

        When True, a task won't run unless the previous DAG run's same
        task succeeded. This is a dangerous implicit default that must
        be consciously set.
        """
        assert "depends_on_past" in dag.default_args, (
            "depends_on_past must be explicitly set in default_args"
        )

    def test_email_on_failure_is_enabled(self, dag):
        """email_on_failure must be True — SMTP is configured for Phase 3 alerting."""
        assert dag.default_args.get("email_on_failure") is True, (
            "email_on_failure must be True now that SMTP is configured"
        )

    def test_alert_email_is_configured(self, dag):
        """email must be a non-empty list — defines where failure alerts are sent."""
        email = dag.default_args.get("email")
        assert email is not None, "default_args must include 'email'"
        assert isinstance(email, list), f"email must be a list, got {type(email)}"
        assert len(email) > 0, "email list must not be empty"


# ---------------------------------------------------------------------------
# SLA Tests
# ---------------------------------------------------------------------------

class TestDagSLA:
    """Validate SLA configuration for production observability."""

    def test_sla_is_configured(self, dag):
        """SLA must be set in default_args to enable Airflow SLA miss tracking."""
        assert "sla" in dag.default_args, (
            "sla must be set in default_args for SLA miss detection"
        )

    def test_sla_is_timedelta(self, dag):
        """SLA must be a timedelta — Airflow requires this type."""
        sla = dag.default_args.get("sla")
        assert isinstance(sla, timedelta), (
            f"sla must be a timedelta, got {type(sla)}"
        )

    def test_sla_is_meaningful(self, dag):
        """SLA must be at least 1 hour — appropriate minimum for a daily pipeline."""
        sla = dag.default_args.get("sla")
        assert sla is not None
        assert sla.total_seconds() >= 3600, (
            f"sla must be >= 1 hour for a daily pipeline, got {sla}"
        )


# ---------------------------------------------------------------------------
# Task Existence Tests
# ---------------------------------------------------------------------------

class TestTaskExistence:
    """Validate that all required pipeline tasks are defined."""

    EXPECTED_TASK_IDS = frozenset({
        "extract_economic_data",
        "transform_economic_data",
        "load_economic_data",
    })

    def test_dag_has_exactly_three_tasks(self, dag):
        """
        DAG must define exactly three tasks.

        The skeleton pipeline is extract → transform → load. No utility
        tasks, sensors, or branches should be present in the initial commit.
        """
        assert len(dag.tasks) == 3, (
            f"Expected 3 tasks, found {len(dag.tasks)}: "
            f"{[t.task_id for t in dag.tasks]}"
        )

    def test_extract_task_exists(self, dag):
        """extract_economic_data task must be present."""
        task_ids = {task.task_id for task in dag.tasks}
        assert "extract_economic_data" in task_ids

    def test_transform_task_exists(self, dag):
        """transform_economic_data task must be present."""
        task_ids = {task.task_id for task in dag.tasks}
        assert "transform_economic_data" in task_ids

    def test_load_task_exists(self, dag):
        """load_economic_data task must be present."""
        task_ids = {task.task_id for task in dag.tasks}
        assert "load_economic_data" in task_ids

    def test_exactly_expected_task_ids(self, dag):
        """
        Full set check: task IDs must match exactly.

        Guards against both missing tasks and unexpected extra tasks
        that would indicate architecture drift from the spec.
        """
        actual_task_ids = {task.task_id for task in dag.tasks}
        assert actual_task_ids == self.EXPECTED_TASK_IDS, (
            f"Task ID mismatch.\n"
            f"  Expected: {sorted(self.EXPECTED_TASK_IDS)}\n"
            f"  Actual:   {sorted(actual_task_ids)}\n"
            f"  Missing:  {sorted(self.EXPECTED_TASK_IDS - actual_task_ids)}\n"
            f"  Extra:    {sorted(actual_task_ids - self.EXPECTED_TASK_IDS)}"
        )


# ---------------------------------------------------------------------------
# Task Dependency Tests
# ---------------------------------------------------------------------------

class TestTaskDependencies:
    """
    Validate that task dependencies form the correct linear pipeline chain.

    Dependency graph required:
        extract_economic_data
              │
              ▼
        transform_economic_data
              │
              ▼
        load_economic_data

    These tests inspect Airflow's task graph model (upstream_task_ids,
    downstream_task_ids) without triggering any execution. They are
    independent of scheduler state or DAG run history.
    """

    def _get_task(self, dag, task_id: str):
        """Helper to retrieve a task by ID, with a helpful error on miss."""
        try:
            return dag.get_task(task_id)
        except Exception:
            pytest.fail(
                f"Task '{task_id}' not found in DAG. "
                f"Available tasks: {[t.task_id for t in dag.tasks]}"
            )

    def test_extract_is_root_node(self, dag):
        """
        extract_economic_data must have no upstream dependencies.

        It is the entry point of the pipeline. Any upstream dependency
        would block the entire pipeline without cause.
        """
        extract = self._get_task(dag, "extract_economic_data")
        assert len(extract.upstream_task_ids) == 0, (
            f"extract_economic_data must have no upstream tasks, "
            f"found: {extract.upstream_task_ids}"
        )

    def test_extract_downstream_is_transform(self, dag):
        """extract must trigger transform as its immediate downstream task."""
        extract = self._get_task(dag, "extract_economic_data")
        assert "transform_economic_data" in extract.downstream_task_ids, (
            f"extract must list transform as downstream, "
            f"found: {extract.downstream_task_ids}"
        )

    def test_transform_upstream_is_extract(self, dag):
        """transform must declare extract as its sole upstream dependency."""
        transform = self._get_task(dag, "transform_economic_data")
        assert "extract_economic_data" in transform.upstream_task_ids, (
            f"transform must list extract as upstream, "
            f"found: {transform.upstream_task_ids}"
        )

    def test_transform_downstream_is_load(self, dag):
        """transform must trigger load as its immediate downstream task."""
        transform = self._get_task(dag, "transform_economic_data")
        assert "load_economic_data" in transform.downstream_task_ids, (
            f"transform must list load as downstream, "
            f"found: {transform.downstream_task_ids}"
        )

    def test_load_upstream_is_transform(self, dag):
        """load must declare transform as its sole upstream dependency."""
        load = self._get_task(dag, "load_economic_data")
        assert "transform_economic_data" in load.upstream_task_ids, (
            f"load must list transform as upstream, "
            f"found: {load.upstream_task_ids}"
        )

    def test_load_is_terminal_node(self, dag):
        """
        load_economic_data must have no downstream tasks.

        It is the final stage of the pipeline. Any dangling downstream
        dependency indicates an architecture error.
        """
        load = self._get_task(dag, "load_economic_data")
        assert len(load.downstream_task_ids) == 0, (
            f"load_economic_data must have no downstream tasks, "
            f"found: {load.downstream_task_ids}"
        )

    def test_full_dependency_chain(self, dag):
        """
        Comprehensive chain validation.

        Verifies the exact topology: extract → transform → load.
        Catches any transitive dependency shortcuts or extra edges.
        """
        extract = self._get_task(dag, "extract_economic_data")
        transform = self._get_task(dag, "transform_economic_data")
        load = self._get_task(dag, "load_economic_data")

        # Extract: no upstream, one downstream (transform)
        assert extract.upstream_task_ids == set()
        assert extract.downstream_task_ids == {"transform_economic_data"}

        # Transform: one upstream (extract), one downstream (load)
        assert "extract_economic_data" in transform.upstream_task_ids
        assert "load_economic_data" in transform.downstream_task_ids

        # Load: one upstream (transform), no downstream
        assert "transform_economic_data" in load.upstream_task_ids
        assert load.downstream_task_ids == set()
