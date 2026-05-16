"""
Airflow DAG — northwind_etl_pipeline.

Schedules the 4-phase Northwind ETL once daily.
Uses dag_run.run_id as batch_id for metadata traceability.

Subsystem #22: Job Scheduler.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _get_pipeline(run_id: str):
    from pathlib import Path
    from src.common.db import get_engine
    from src.orchestration.pipeline import Pipeline

    engine = get_engine()
    return Pipeline(engine=engine), run_id


def run_extract(**context) -> None:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    run_id: str = context["dag_run"].run_id
    pipeline, batch_id = _get_pipeline(run_id)
    pr = pipeline._run_extract(batch_id)
    if not pr.success:
        raise RuntimeError(f"extract failed: {pr.error}")


def run_clean(**context) -> None:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    run_id: str = context["dag_run"].run_id
    pipeline, batch_id = _get_pipeline(run_id)
    pr = pipeline._run_clean(batch_id)
    if not pr.success:
        raise RuntimeError(f"clean failed: {pr.error}")


def run_conform(**context) -> None:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    run_id: str = context["dag_run"].run_id
    pipeline, batch_id = _get_pipeline(run_id)
    pr = pipeline._run_conform(batch_id)
    if not pr.success:
        raise RuntimeError(f"conform failed: {pr.error}")


def run_deliver(**context) -> None:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    run_id: str = context["dag_run"].run_id
    pipeline, batch_id = _get_pipeline(run_id)
    pr = pipeline._run_deliver(batch_id)
    if not pr.success:
        raise RuntimeError(f"deliver failed: {pr.error}")


with DAG(
    dag_id="northwind_etl_pipeline",
    description="Northwind ETL: Extract → Clean → Conform → Deliver (Kimball Subsystem #22)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["northwind", "etl", "kimball"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=run_extract,
    )
    clean_task = PythonOperator(
        task_id="clean_task",
        python_callable=run_clean,
    )
    conform_task = PythonOperator(
        task_id="conform_task",
        python_callable=run_conform,
    )
    deliver_task = PythonOperator(
        task_id="deliver_task",
        python_callable=run_deliver,
    )

    extract_task >> clean_task >> conform_task >> deliver_task
