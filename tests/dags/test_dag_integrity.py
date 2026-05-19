"""
DAG integrity tests.

Verifies that every DAG in the dags/ folder:
  - Loads without import errors
  - Has the expected number of tasks
  - Has the correct task dependencies
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///:memory:")
os.environ.setdefault("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")

# Resolve dags folder: env override → container default → local project root
_DAG_FOLDER = (
    os.environ.get("ETL_DAGS_FOLDER")
    or (
        "/opt/airflow/dags"
        if Path("/opt/airflow/dags").is_dir()
        else str(Path(__file__).parents[2] / "dags")
    )
)


@pytest.fixture(scope="module")
def dag_bag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=_DAG_FOLDER, include_examples=False)
    return bag


def _get_dag(dag_bag, dag_id):
    """Access DAG from in-memory dict without hitting the database."""
    dag = dag_bag.dags.get(dag_id)
    if dag is None:
        pytest.fail(f"DAG '{dag_id}' not found. Loaded: {list(dag_bag.dags.keys())}")
    return dag


# ---------------------------------------------------------------------------
# Global: all DAGs load without errors
# ---------------------------------------------------------------------------

class TestDagBagLoads:
    def test_no_import_errors(self, dag_bag):
        assert dag_bag.import_errors == {}, (
            f"DAG import errors:\n" +
            "\n".join(f"  {k}: {v}" for k, v in dag_bag.import_errors.items())
        )

    def test_expected_dag_ids_present(self, dag_bag):
        expected = {
            "northwind_etl_pipeline",
            "northwind_spark_transform",
            "northwind_kafka_producer",
            "northwind_streaming_consumer",
            "northwind_streaming_e2e",
        }
        loaded = set(dag_bag.dags.keys())
        missing = expected - loaded
        assert not missing, f"Missing DAGs: {missing}"


# ---------------------------------------------------------------------------
# northwind_etl_pipeline
# ---------------------------------------------------------------------------

class TestEtlPipelineDag:
    @pytest.fixture
    def dag(self, dag_bag):
        return _get_dag(dag_bag, "northwind_etl_pipeline")

    def test_task_count(self, dag):
        assert len(dag.tasks) == 4

    def test_schedule_is_daily(self, dag):
        assert dag.schedule_interval == "@daily"

    def test_dependency_order(self, dag):
        def ds(task_id):
            return {t.task_id for t in dag.task_dict[task_id].downstream_list}

        assert "clean_task" in ds("extract_task")
        assert "conform_task" in ds("clean_task")
        assert "deliver_task" in ds("conform_task")


# ---------------------------------------------------------------------------
# northwind_spark_transform
# ---------------------------------------------------------------------------

class TestSparkTransformDag:
    @pytest.fixture
    def dag(self, dag_bag):
        return _get_dag(dag_bag, "northwind_spark_transform")

    def test_task_count(self, dag):
        assert len(dag.tasks) == 2

    def test_schedule_is_none(self, dag):
        assert dag.schedule_interval is None

    def test_dependency_order(self, dag):
        downstream = {t.task_id for t in dag.task_dict["spark_clean"].downstream_list}
        assert "spark_deliver" in downstream


# ---------------------------------------------------------------------------
# northwind_kafka_producer
# ---------------------------------------------------------------------------

class TestKafkaProducerDag:
    @pytest.fixture
    def dag(self, dag_bag):
        return _get_dag(dag_bag, "northwind_kafka_producer")

    def test_task_count(self, dag):
        assert len(dag.tasks) == 1

    def test_schedule_is_none(self, dag):
        assert dag.schedule_interval is None

    def test_task_id(self, dag):
        assert "produce_orders" in dag.task_dict

    def test_catchup_false(self, dag):
        assert dag.catchup is False


# ---------------------------------------------------------------------------
# northwind_streaming_consumer
# ---------------------------------------------------------------------------

class TestStreamingConsumerDag:
    @pytest.fixture
    def dag(self, dag_bag):
        return _get_dag(dag_bag, "northwind_streaming_consumer")

    def test_task_count(self, dag):
        assert len(dag.tasks) == 1

    def test_schedule_every_5_minutes(self, dag):
        assert dag.schedule_interval == "*/5 * * * *"

    def test_catchup_false(self, dag):
        assert dag.catchup is False

    def test_task_id(self, dag):
        assert "consume_orders" in dag.task_dict


# ---------------------------------------------------------------------------
# northwind_streaming_e2e
# ---------------------------------------------------------------------------

class TestStreamingE2eDag:
    @pytest.fixture
    def dag(self, dag_bag):
        return _get_dag(dag_bag, "northwind_streaming_e2e")

    def test_task_count(self, dag):
        assert len(dag.tasks) == 5

    def test_schedule_is_none(self, dag):
        assert dag.schedule_interval is None

    def test_catchup_false(self, dag):
        assert dag.catchup is False

    def test_dependency_order(self, dag):
        def ds(task_id):
            return {t.task_id for t in dag.task_dict[task_id].downstream_list}

        assert "wait_after_produce" in ds("produce_orders")
        assert "consume_orders" in ds("wait_after_produce")
        assert "spark_clean" in ds("consume_orders")
        assert "spark_deliver" in ds("spark_clean")

    def test_all_task_ids_present(self, dag):
        task_ids = set(dag.task_dict.keys())
        expected = {
            "produce_orders",
            "wait_after_produce",
            "consume_orders",
            "spark_clean",
            "spark_deliver",
        }
        assert expected == task_ids
